"""Integration tests for the dashboard_build worker function."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import timedelta
from typing import Any

import httpx
import pytest
import sentry_sdk
import structlog
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from safir.testing.sentry import capture_events_fixture, sentry_init_fixture
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import (
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse_server.config import Configuration
from docverse_server.config import config as runtime_config
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.queue import JobKind, JobStatus
from docverse_server.factory import Factory
from docverse_server.metrics import build_event_manager
from docverse_server.services.lock_service import LockClass, LockKey
from docverse_server.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.objectstore import MockObjectStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import (
    LATE_DELIVERY_IN_PROGRESS_MESSAGE,
    QueueJobStore,
)
from docverse_server.worker.functions.dashboard_build import dashboard_build
from tests.support.arq_cancel import HangUntilCancelled, cancel_when_reached
from tests.support.lock_service_spy import install_recording_lock_service
from tests.support.objectstore import seed_minio_service
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _setup_org_and_project(
    db_session: AsyncSession,
) -> tuple[Any, Any]:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)

    org = await org_store.create(
        OrganizationCreate(
            slug="dash-org",
            title="Dash Org",
            base_domain="dash.example.com",
        )
    )
    await db_session.execute(
        update(SqlOrganization)
        .where(SqlOrganization.id == org.id)
        .values(publishing_store_label="mock-store")
    )
    await db_session.flush()
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="dash-proj",
            title="Dash Project",
            source_url="https://example.com/example/dash",
        ),
    )
    await edition_store.create_internal(
        project_id=project.id,
        slug="__main",
        title="Latest",
        kind=EditionKind.main,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": "main"},
    )
    return org, project


def _mock_create_objectstore(
    mock_store: MockObjectStore,
) -> Any:
    """Return a patched create_objectstore_for_org that returns
    the given mock store.

    Accepts and ignores the upload-transport keywords
    ``create_build_upload_objectstore_for_org`` passes through.
    """

    async def _create(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
        **kwargs: object,
    ) -> MockObjectStore:
        # Mimic the real helper's DB access so callers that invoke it
        # outside an explicit ``session.begin()`` block trigger autobegin
        # and surface the same ``InvalidRequestError`` as production.
        await self._session.execute(select(1))
        return mock_store

    return _create


@pytest.mark.asyncio
async def test_dashboard_build_completes_with_phase_transitions(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful dashboard_build job uploads artifacts and finalizes."""
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dashboard",
        )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(
        http_client=http_client,
        job_id="test-arq-dashboard",
    )
    queue_job_public_id = serialize_base32_id(queue_job.public_id)
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": queue_job_public_id,
    }

    with capture_logs() as captured:
        result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"

    # Log records bind ``queue_job_id`` to the base32 public ID, never the
    # integer database id.
    bound_ids = {
        event.get("queue_job_id")
        for event in captured
        if "queue_job_id" in event
    }
    assert bound_ids == {queue_job_public_id}
    assert queue_job.id not in bound_ids

    # All MVP artifacts written, plus one per-edition JSON for __main.
    assert "dash-proj/__dashboard.html" in mock_store.objects
    assert "dash-proj/__switcher.json" in mock_store.objects
    assert "dash-proj/__404.html" in mock_store.objects
    assert "dash-proj/__editions/__main.json" in mock_store.objects

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.phase == "complete"
            assert job.progress is not None
            assert job.progress["object_count"] == 4
            assert job.progress["total_size_bytes"] > 0


@pytest.mark.asyncio
async def test_dashboard_build_marks_failed_on_render_exception(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render exceptions mark the queue job failed and return ``failed``."""
    logger = _logger()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-fail",
        )

    async def _broken_create_objectstore(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
        **kwargs: object,
    ) -> MockObjectStore:
        msg = "Simulated objectstore resolution failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _broken_create_objectstore,
    )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(
        http_client=http_client,
        job_id="test-arq-dash-fail",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "failed"

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.failed
            assert job.errors is not None
            assert "Simulated objectstore" in job.errors["message"]


@pytest.mark.asyncio
async def test_dashboard_build_publishes_dashboard_built(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A completed dashboard_build emits one ``dashboard_built`` event."""
    logger = _logger()
    mock_store = MockObjectStore()
    _manager, events = await build_event_manager(Configuration())

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-event",
        )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(
        http_client=http_client,
        job_id="test-arq-dash-event",
        events=events,
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    publisher = events.dashboard_built
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == org.slug
    assert event.project == project.slug
    assert event.success is True
    # Four MVP artifacts uploaded (matches the phase-transition test).
    assert event.object_count == 4
    assert event.total_size_bytes is not None
    assert event.total_size_bytes > 0
    assert event.elapsed >= timedelta(0)


@pytest.mark.asyncio
async def test_dashboard_build_publishes_dashboard_built_on_failure(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed dashboard_build emits dashboard_built with success=False."""
    logger = _logger()
    _manager, events = await build_event_manager(Configuration())

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-event-fail",
        )

    async def _broken_create_objectstore(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
        **kwargs: object,
    ) -> MockObjectStore:
        msg = "Simulated objectstore resolution failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _broken_create_objectstore,
    )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(
        http_client=http_client,
        job_id="test-arq-dash-event-fail",
        events=events,
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "failed"

    publisher = events.dashboard_built
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == org.slug
    assert event.project == project.slug
    assert event.success is False
    assert event.object_count is None
    assert event.total_size_bytes is None
    assert event.elapsed >= timedelta(0)


@pytest.mark.asyncio
async def test_dashboard_build_skips_reaped_queue_job(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A job whose row a reaper already failed is skipped, not raised on.

    The late-delivery guard from PRD #538: arq hands the worker a job
    the abandoned sweep has already reaped, so the pickup path must log
    a warning and return without touching the object store or the
    terminal row.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-reaped",
        )
        # Stand in for the reaper's abandoned sweep having failed the row
        # between enqueue and this (late) delivery.
        await queue_job_store.fail(
            queue_job.id,
            errors={
                "message": "Abandoned dashboard_build",
                "type": "AbandonedQueueJob",
            },
        )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(
        http_client=http_client,
        job_id="test-arq-dash-reaped",
    )
    queue_job_public_id = serialize_base32_id(queue_job.public_id)
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": queue_job_public_id,
    }

    with capture_logs() as captured:
        result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "skipped"

    warnings = [
        event
        for event in captured
        if event.get("log_level") == "warning"
        and event.get("queue_job_status") == JobStatus.failed.value
    ]
    assert len(warnings) == 1
    assert warnings[0]["queue_job_id"] == queue_job_public_id
    assert warnings[0]["queue_job_kind"] == JobKind.dashboard_build.value

    # No job body ran: nothing uploaded, and the reaped row is untouched.
    assert mock_store.objects == {}
    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.failed
            assert job.date_started is None
            assert job.phase is None


@pytest.mark.asyncio
async def test_dashboard_build_in_progress_redelivery_reports_to_sentry(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Arq re-delivering a job already ``in_progress`` stays observable.

    Task #547: the terminal-row skip above is expected reap fallout and
    stays log-only, but an ``in_progress`` row means the default pool
    re-delivered a job after worker death or a SIGTERM pod rotation. The
    worker still skips the body — running it twice concurrently is worse
    — but the pickup guard captures a warning-level Sentry event so a
    crash-looping kind is visible immediately rather than hours later
    when the reaper notices.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-redelivered",
        )
        # The first delivery picked the row up; the worker then died
        # before it could complete or fail the row.
        picked_up = await queue_job_store.start_if_queued(queue_job.id)
        assert picked_up is not None

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(
        http_client=http_client,
        job_id="test-arq-dash-redelivered",
    )
    queue_job_public_id = serialize_base32_id(queue_job.public_id)
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": queue_job_public_id,
    }

    with sentry_init_fixture() as init:
        init(environment="test")
        sentry_events = capture_events_fixture(monkeypatch)()
        with capture_logs() as captured:
            result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "skipped"

    warnings = [
        event
        for event in captured
        if event.get("log_level") == "warning"
        and event.get("queue_job_status") == JobStatus.in_progress.value
    ]
    assert len(warnings) == 1
    assert warnings[0]["queue_job_id"] == queue_job_public_id

    assert len(sentry_events.errors) == 1
    event = sentry_events.errors[0]
    assert event["level"] == "warning"
    assert event["message"] == LATE_DELIVERY_IN_PROGRESS_MESSAGE
    assert event["tags"]["job_function"] == JobKind.dashboard_build.value
    assert event["tags"]["job_current_state"] == JobStatus.in_progress.value
    context = event["contexts"]["queue_job_late_delivery"]
    assert context["job_public_id"] == queue_job_public_id
    assert context["job_function"] == JobKind.dashboard_build.value
    assert context["current_state"] == JobStatus.in_progress.value

    # The body never ran: nothing uploaded and no phase progress written
    # over the first delivery's bookkeeping.
    assert mock_store.objects == {}
    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.in_progress
            assert job.phase is None


class _RecordingMockObjectStore(MockObjectStore):
    """``MockObjectStore`` that timestamps every upload."""

    def __init__(self, op_timestamps: list[float]) -> None:
        super().__init__()
        self._op_timestamps = op_timestamps

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> int:
        self._op_timestamps.append(time.monotonic())
        return await super().upload_object(
            key=key, data=data, content_type=content_type
        )


@pytest.mark.asyncio
async def test_dashboard_build_acquires_project_lock_before_render(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """dashboard_build acquires PROJECT before any render upload.

    Verifies the worker wires the lock to ``LockKey.for_project`` for
    the payload's ``(org_id, project_id)`` and that no render upload
    happens before the lock is held.
    """
    logger = _logger()
    op_timestamps: list[float] = []
    mock_store = _RecordingMockObjectStore(op_timestamps=op_timestamps)

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-lock",
        )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )
    events = install_recording_lock_service(monkeypatch)

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(
        http_client=http_client,
        job_id="test-arq-dash-lock",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    expected = LockKey.for_project(org_id=org.id, project_id=project.id)
    proj_enters = [
        e
        for e in events
        if e.event == "enter" and e.lock_key.lock_class == LockClass.PROJECT
    ]
    assert len(proj_enters) == 1
    assert proj_enters[0].lock_key == expected

    assert op_timestamps, "expected dashboard render uploads"
    proj_enter_ts = proj_enters[0].timestamp
    assert all(ts > proj_enter_ts for ts in op_timestamps)


_CUSTOM_TEMPLATE_TOML = b"""\
[dashboard]
template = "dashboard.html.jinja"

[dashboard.assets]
css = []
js = []
images = []

[switcher]
include_kinds = ["main", "release"]
"""

# Intentionally distinctive so we can tell this template rendered vs. the
# packaged built-in. The built-in dashboard renders a <main> element; ours
# renders a single <div id="custom-marker"> with a well-known token.
_CUSTOM_DASHBOARD_JINJA = b"""\
<!DOCTYPE html>
<html lang="en">
<head><title>{{ project.title }}</title></head>
<body>
<div id="custom-marker">GITHUB-TEMPLATE-RENDERED-{{ project.slug }}</div>
</body>
</html>
"""


@pytest.mark.asyncio
async def test_dashboard_build_uses_github_template_when_bound(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bound GitHub template is used for the render instead of built-in.

    Seeds a synced GitHub template with distinctive bytes and an
    org-default binding pointing at it, then runs ``dashboard_build``
    and asserts the rendered HTML reflects the synced bytes rather than
    the packaged ``BuiltInTemplateSource`` defaults.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        # Seed a synced GitHub template + bind it as the org default so
        # TemplateResolver returns it for this project's render.
        template_store = DashboardGitHubTemplateStore(
            session=db_session, logger=logger
        )
        template_result = await template_store.upsert(
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="dashboard-templates",
                github_ref="main",
                root_path="/",
            ),
            commit_sha="deadbeef",
            etag="etag-1",
            template_toml=_CUSTOM_TEMPLATE_TOML,
            files=[
                GitHubTemplateFileInput(
                    relative_path="dashboard.html.jinja",
                    is_text=True,
                    data=_CUSTOM_DASHBOARD_JINJA,
                ),
            ],
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=logger
        )
        binding = await binding_store.create(
            DashboardGitHubTemplateBindingCreate(
                org_id=org.id,
                project_id=None,
                github_owner="acme",
                github_repo="dashboard-templates",
                github_ref="main",
                root_path="/",
            )
        )
        await binding_store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="succeeded",
            github_template_id=template_result.template.id,
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-github",
        )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(
        http_client=http_client,
        job_id="test-arq-dash-github",
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"
    html_obj = mock_store.objects["dash-proj/__dashboard.html"]
    html_text = html_obj.data.decode("utf-8")
    # Marker proves the GitHub-synced template rendered.
    assert "GITHUB-TEMPLATE-RENDERED-dash-proj" in html_text
    assert 'id="custom-marker"' in html_text
    # Built-in template's <main> element must not appear — if it does
    # we resolved to BuiltInTemplateSource, which would be a regression.
    assert "<main>" not in html_text


_CONFIGURED_DASHBOARD_TIMEOUT_SECONDS = 1200
"""A ``dashboard_build_job_timeout_seconds`` other than its 900 s default.

The cancel tests run under it so the payload's ``timeout_seconds`` can
only match if the job read the setting, not arq's default or the stock
value.
"""


@pytest.mark.parametrize(
    ("started_ago", "expected_reason"),
    [
        pytest.param(None, "worker_shutdown", id="worker_shutdown"),
        pytest.param(
            timedelta(seconds=_CONFIGURED_DASHBOARD_TIMEOUT_SECONDS)
            + timedelta(minutes=1),
            "job_timeout",
            id="job_timeout",
        ),
    ],
)
@pytest.mark.asyncio
async def test_dashboard_build_cancel_fails_the_row(
    *,
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    started_ago: timedelta | None,
    expected_reason: str,
) -> None:
    """An arq cancel mid-build fails the row instead of stranding it.

    The job is cancelled while it resolves the org's object store, as its
    arq timeout or a rolling deploy's SIGTERM would catch it. Left
    ``in_progress``, the row would hold the project's
    active-dashboard-build slot until ``dashboard_build_reaper``
    noticed; instead it fails with a ``CancelledError`` payload whose
    ``reason`` and ``timeout_seconds`` are read against the configured
    ``dashboard_build_job_timeout_seconds`` arq enforces on it (#863),
    and the cancel re-raises.
    """
    monkeypatch.setattr(
        runtime_config,
        "dashboard_build_job_timeout_seconds",
        _CONFIGURED_DASHBOARD_TIMEOUT_SECONDS,
    )
    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        queue_job = await QueueJobStore(
            session=db_session, logger=_logger()
        ).create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-cancel",
        )
    hang = HangUntilCancelled()
    monkeypatch.setattr(Factory, "create_objectstore_for_org", hang)
    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(), job_id="test-arq-dash-cancel"
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    async def _backdate() -> None:
        if started_ago is None:
            return
        async for session in db_session_dependency():
            async with session.begin():
                await session.execute(
                    update(SqlQueueJob)
                    .where(SqlQueueJob.id == queue_job.id)
                    .values(date_started=func.now() - started_ago)
                )

    task = asyncio.create_task(dashboard_build(ctx, payload))
    await cancel_when_reached(task, hang.reached, before_cancel=_backdate)
    await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            job = await QueueJobStore(session=session, logger=_logger()).get(
                queue_job.id
            )
            assert job is not None
            assert job.status == JobStatus.failed
            assert job.errors is not None
            assert job.errors["type"] == "CancelledError"
            assert job.errors["reason"] == expected_reason
            assert job.errors["timeout_seconds"] == (
                _CONFIGURED_DASHBOARD_TIMEOUT_SECONDS
            )


@pytest.mark.asyncio
async def test_dashboard_build_uploads_on_the_copy_transport(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Dashboard PUTs go over the copy client, one limiter slot each.

    ``dashboard_build`` fans its edition JSONs out (#863), so it uploads
    through the store ``build_processing`` uses (#877): every PUT goes
    over the worker's copy client and holds a slot of its process-wide
    upload limiter. A single-slot limiter holds the whole dashboard to
    one PUT at a time even though the job alone would run the editions
    together, and none of the PUTs reach the shared client. The org
    resolves a real ``S3ObjectStore``.
    """
    logger = _logger()
    job_id = "test-arq-dash-copy-transport"
    drafts = [f"draft-{index}" for index in range(4)]
    puts: list[str] = []
    in_flight = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        if request.method == "PUT":
            puts.append(request.url.path)
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.01)
            in_flight -= 1
        return httpx.Response(200)

    copy_http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler)
    )
    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        copy_http_client=copy_http_client,
        keeper_sync_upload_limiter=asyncio.Semaphore(1),
        job_id=job_id,
    )
    factory = ctx["factory_builder"](session=db_session, logger=logger)
    org_id = await seed_minio_service(db_session, factory)
    async with db_session.begin():
        await db_session.execute(
            update(SqlOrganization)
            .where(SqlOrganization.id == org_id)
            .values(publishing_store_label="minio")
        )
        org = await OrganizationStore(
            session=db_session, logger=logger
        ).get_by_id(org_id)
        assert org is not None
        project = await ProjectStore(session=db_session, logger=logger).create(
            org_id=org_id,
            data=ProjectCreate(
                slug="dash-proj",
                title="Dash Project",
                source_url="https://example.com/example/dash",
            ),
        )
        edition_store = EditionStore(session=db_session, logger=logger)
        for slug in drafts:
            await edition_store.create_internal(
                project_id=project.id,
                slug=slug,
                title=slug,
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": f"tickets/{slug}"},
            )
        queue_job = await QueueJobStore(
            session=db_session, logger=logger
        ).create(
            kind=JobKind.dashboard_build,
            org_id=org_id,
            project_id=project.id,
            backend_job_id=job_id,
        )
    payload: dict[str, Any] = {
        "org_id": org_id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()
    await copy_http_client.aclose()

    assert result == "completed"
    assert sorted(puts) == sorted(
        [
            "/docs/dash-proj/__dashboard.html",
            "/docs/dash-proj/__404.html",
            "/docs/dash-proj/__switcher.json",
            *(f"/docs/dash-proj/__editions/{slug}.json" for slug in drafts),
        ]
    )
    assert peak == 1


class _EditionUploadFailsStore(MockObjectStore):
    """``MockObjectStore`` whose every edition upload fails."""

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> int:
        """Store project-level artifacts; refuse edition JSONs."""
        if "/__editions/" in key:
            msg = "Object store went away"
            raise RuntimeError(msg)
        return await super().upload_object(
            key=key, data=data, content_type=content_type
        )


@pytest.mark.asyncio
async def test_dashboard_build_edition_upload_failure_fails_the_job(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed edition PUT fails the row with the store's own error.

    The edition uploads run in a task group (#863), but the row's
    ``errors`` and the Sentry event carry the ``RuntimeError`` the store
    raised, not the exception group around it, as the sequential loop
    reported it.
    """
    captured: list[BaseException] = []
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)
    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        queue_job = await QueueJobStore(
            session=db_session, logger=_logger()
        ).create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-edition-fails",
        )
    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(_EditionUploadFailsStore()),
    )
    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(), job_id="test-arq-dash-edition-fails"
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "failed"
    assert [type(exc) for exc in captured] == [RuntimeError]
    async for session in db_session_dependency():
        async with session.begin():
            job = await QueueJobStore(session=session, logger=_logger()).get(
                queue_job.id
            )
            assert job is not None
            assert job.status == JobStatus.failed
            assert job.errors is not None
            assert job.errors["type"] == "RuntimeError"
            assert job.errors["message"] == "Object store went away"


_SWITCHER_TEMPLATE_TOML = b"""\
[dashboard]
template = "dashboard.html.jinja"

[switcher]
include_kinds = ["main", "release"]
template = "switcher.json.jinja"
"""

# Emits ``__main`` and the three newest releases only.
_DEMO_SWITCHER_JINJA = b"""\
[
  {"name": {{ editions.main.title | tojson }},
   "version": {{ editions.main.slug | tojson }},
   "url": {{ editions.main.published_url | tojson }},
   "preferred": true}
  {%- for release in editions.releases[:3] %},
  {"name": {{ release.title | tojson }},
   "version": {{ release.slug | tojson }},
   "url": {{ release.published_url | tojson }}}
  {%- endfor %}
]
"""


async def _bind_switcher_template_set(
    db_session: AsyncSession, *, org_id: int, switcher_jinja: bytes
) -> None:
    """Seed a synced template set with a switcher template as org default."""
    logger = _logger()
    key = GitHubTemplateKey(
        github_owner="acme",
        github_repo="dashboard-templates",
        github_ref="main",
        root_path="/",
    )
    template_result = await DashboardGitHubTemplateStore(
        session=db_session, logger=logger
    ).upsert(
        key=key,
        commit_sha="deadbeef",
        etag="etag-switcher",
        template_toml=_SWITCHER_TEMPLATE_TOML,
        files=[
            GitHubTemplateFileInput(
                relative_path="dashboard.html.jinja",
                is_text=True,
                data=_CUSTOM_DASHBOARD_JINJA,
            ),
            GitHubTemplateFileInput(
                relative_path="switcher.json.jinja",
                is_text=True,
                data=switcher_jinja,
            ),
        ],
    )
    binding_store = DashboardGitHubTemplateBindingStore(
        session=db_session, logger=logger
    )
    binding = await binding_store.create(
        DashboardGitHubTemplateBindingCreate(
            org_id=org_id,
            project_id=None,
            github_owner=key.github_owner,
            github_repo=key.github_repo,
            github_ref=key.github_ref,
            root_path=key.root_path,
        )
    )
    await binding_store.update_sync_state(
        binding_id=binding.id,
        last_sync_status="succeeded",
        github_template_id=template_result.template.id,
    )


@pytest.mark.asyncio
async def test_dashboard_build_renders_template_set_switcher(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A template set's ``switcher.json.jinja`` writes ``__switcher.json``.

    The demo template lists ``__main`` and the three newest of four
    releases, so the uploaded file is exactly those four entries — not
    the fourth release the ``include_kinds`` renderer would have added.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        edition_store = EditionStore(session=db_session, logger=logger)
        for slug in ("v1.0.0", "v1.1.0", "v2.0.0", "v2.1.0"):
            await edition_store.create_internal(
                project_id=project.id,
                slug=slug,
                title=slug,
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": slug},
            )
        await _bind_switcher_template_set(
            db_session, org_id=org.id, switcher_jinja=_DEMO_SWITCHER_JINJA
        )
        queue_job = await QueueJobStore(
            session=db_session, logger=logger
        ).create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-switcher",
        )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )
    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(), job_id="test-arq-dash-switcher"
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"
    switcher = mock_store.objects["dash-proj/__switcher.json"].data
    entries = json.loads(switcher)
    assert [e["version"] for e in entries] == [
        "__main",
        "v2.1.0",
        "v2.0.0",
        "v1.1.0",
    ]
    assert entries[0]["preferred"] is True
    assert all("preferred" not in e for e in entries[1:])
    assert switcher == json.dumps(entries, indent=2).encode("utf-8")


@pytest.mark.asyncio
async def test_dashboard_build_fails_on_invalid_template_set_switcher(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed switcher output fails the job like a broken template.

    The typed error reaches Sentry and the job's ``errors``, and nothing
    is uploaded: the project keeps serving its previous artifacts.
    """
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        await _bind_switcher_template_set(
            db_session,
            org_id=org.id,
            switcher_jinja=b'{"name": "Latest"}',
        )
        queue_job = await QueueJobStore(
            session=db_session, logger=logger
        ).create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-bad-switcher",
        )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )
    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(), job_id="test-arq-dash-bad-switcher"
    )
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    with sentry_init_fixture() as init:
        init(environment="test")
        sentry_events = capture_events_fixture(monkeypatch)()
        result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "failed"
    assert mock_store.objects == {}

    assert len(sentry_events.errors) == 1
    exc_values = sentry_events.errors[0]["exception"]["values"]
    assert any(v["type"] == "InvalidSwitcherJsonError" for v in exc_values)

    async for session in db_session_dependency():
        async with session.begin():
            job = await QueueJobStore(session=session, logger=logger).get(
                queue_job.id
            )
            assert job is not None
            assert job.status == JobStatus.failed
            assert job.errors is not None
            assert job.errors["type"] == "InvalidSwitcherJsonError"
            assert "'switcher.json.jinja'" in job.errors["message"]
            assert "not an array" in job.errors["message"]
