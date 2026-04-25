"""Integration tests for the dashboard_build worker function."""

from __future__ import annotations

import time
from typing import Any

import httpx
import pytest
import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.client.models import (
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.dbschema.organization import SqlOrganization
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.factory import Factory
from docverse.services.lock_service import LockClass, LockKey
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.objectstore import MockObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.dashboard_build import dashboard_build
from tests.support.lock_service_spy import install_recording_lock_service
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
            doc_repo="https://github.com/example/dash",
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
    async def _create(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
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


class _RecordingMockObjectStore(MockObjectStore):
    """``MockObjectStore`` that timestamps every upload."""

    def __init__(self, op_timestamps: list[float]) -> None:
        super().__init__()
        self._op_timestamps = op_timestamps

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> None:
        self._op_timestamps.append(time.monotonic())
        await super().upload_object(
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
