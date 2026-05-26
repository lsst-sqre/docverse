"""Integration tests for the publish_edition worker function."""

from __future__ import annotations

import time
from types import TracebackType
from typing import Any, Self

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.client.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.client.models.queue_enums import PublishStatus
from docverse.config import Configuration
from docverse.dbschema.organization import SqlOrganization
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistory
from docverse.domain.organization import Organization
from docverse.domain.project import Project
from docverse.domain.queue import JobKind, JobStatus, QueueJob
from docverse.factory import Factory
from docverse.services.lock_service import LockClass, LockKey
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.editionpublisher import (
    EditionPublisher,
    MockEditionPublisher,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.publish_edition import publish_edition
from tests.support.arq_testing import get_jobs_by_name
from tests.support.lock_service_spy import install_recording_lock_service
from tests.worker.conftest import make_worker_ctx

_HASH = "sha256:" + "a" * 64


class _FailingPublisher:
    """An EditionPublisher whose ``publish`` raises."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    async def publish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
        build_public_id: str,
        object_key_prefix: str,
    ) -> None:
        _ = (project_slug, edition_slug, build_public_id, object_key_prefix)
        raise self._exc

    async def unpublish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
    ) -> None:
        _ = (project_slug, edition_slug)
        raise self._exc


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _mock_create_edition_publisher(
    publisher: EditionPublisher,
) -> Any:
    """Return a patched ``create_edition_publisher_for_org`` that
    returns the given publisher.
    """  # noqa: D205

    async def _create(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
    ) -> EditionPublisher:
        _ = (self, org_id, service_label)
        return publisher

    return _create


async def _setup_publish_scenario(
    db_session: AsyncSession,
    *,
    org_slug: str,
    cdn_service_label: str | None,
    backend_job_id: str,
) -> tuple[
    Organization,
    Project,
    Edition,
    Build,
    EditionBuildHistory,
    QueueJob,
]:
    """Create org, project, edition, build, history entry, and queue job."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    history_store = EditionBuildHistoryStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    queue_job_store = QueueJobStore(session=db_session, logger=logger)

    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title="Publish Org",
            base_domain=f"{org_slug}.example.com",
        )
    )
    if cdn_service_label is not None:
        await db_session.execute(
            update(SqlOrganization)
            .where(SqlOrganization.id == org.id)
            .values(cdn_service_label=cdn_service_label)
        )
        await db_session.flush()
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="pub-proj",
            title="Publish Project",
            source_url="https://example.com/example/repo",
        ),
    )
    edition = await edition_store.create(
        project_id=project.id,
        data=EditionCreate(
            slug="main",
            title="Latest",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        ),
    )
    build = await build_store.create(
        project_id=project.id,
        data=BuildCreate(git_ref="main", content_hash=_HASH),
        uploader="testuser",
        project_slug=project.slug,
    )
    await build_store.transition_status(
        build_id=build.id, new_status=BuildStatus.processing
    )
    await build_store.transition_status(
        build_id=build.id, new_status=BuildStatus.completed
    )
    refreshed_build = await build_store.get_by_id(build.id)
    assert refreshed_build is not None
    history_entry = await history_store.record(
        edition_id=edition.id, build_id=refreshed_build.id
    )
    queue_job = await queue_job_store.create(
        kind=JobKind.publish_edition,
        org_id=org.id,
        project_id=project.id,
        build_id=refreshed_build.id,
        edition_id=edition.id,
        backend_job_id=backend_job_id,
    )
    return org, project, edition, refreshed_build, history_entry, queue_job


def _make_payload(
    *,
    org: Organization,
    project: Project,
    edition: Edition,
    build: Build,
    queue_job: QueueJob,
) -> dict[str, Any]:
    return {
        "org_id": org.id,
        "project_slug": project.slug,
        "edition_id": edition.id,
        "edition_slug": edition.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }


@pytest.mark.asyncio
async def test_publish_edition_success_lifecycle(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Success path: pending → publishing → published with publisher call."""
    logger = _logger()
    mock_publisher = MockEditionPublisher()

    async with db_session.begin():
        (
            org,
            project,
            edition,
            build,
            history_entry,
            queue_job,
        ) = await _setup_publish_scenario(
            db_session,
            org_slug="pub-success-org",
            cdn_service_label="cdn-prod",
            backend_job_id="test-publish-arq-1",
        )

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_create_edition_publisher(mock_publisher),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-publish-arq-1",
    )
    payload = _make_payload(
        org=org,
        project=project,
        edition=edition,
        build=build,
        queue_job=queue_job,
    )
    queue_job_public_id = serialize_base32_id(queue_job.public_id)

    with capture_logs() as captured:
        result = await publish_edition(ctx, payload)
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
    assert len(mock_publisher.calls) == 1
    call = mock_publisher.calls[0]
    assert call.project_slug == project.slug
    assert call.edition_slug == edition.slug
    assert call.build_public_id == serialize_base32_id(build.public_id)
    assert call.object_key_prefix == build.storage_prefix

    async for session in db_session_dependency():
        async with session.begin():
            ed_store = EditionStore(session=session, logger=logger)
            refreshed_ed = await ed_store.get_by_slug(
                project_id=project.id, slug=edition.slug
            )
            assert refreshed_ed is not None
            assert refreshed_ed.publish_status == PublishStatus.published

            hist_store = EditionBuildHistoryStore(
                session=session, logger=logger
            )
            entries = await hist_store.list_by_edition(edition.id)
            assert entries
            assert entries[0].publish_status == PublishStatus.published

            qjs = QueueJobStore(session=session, logger=logger)
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.phase == "publishing"
            assert job.date_started is not None
            assert job.date_completed is not None
            _ = history_entry


@pytest.mark.asyncio
async def test_publish_edition_failure_lifecycle(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failure path: publish_status=failed + queue_job failed with errors."""
    logger = _logger()
    boom = RuntimeError("publisher exploded")
    failing_publisher = _FailingPublisher(boom)

    async with db_session.begin():
        (
            org,
            project,
            edition,
            build,
            _history_entry,
            queue_job,
        ) = await _setup_publish_scenario(
            db_session,
            org_slug="pub-fail-org",
            cdn_service_label="cdn-prod",
            backend_job_id="test-publish-arq-fail",
        )

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_create_edition_publisher(failing_publisher),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-publish-arq-fail",
    )
    payload = _make_payload(
        org=org,
        project=project,
        edition=edition,
        build=build,
        queue_job=queue_job,
    )

    result = await publish_edition(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "failed"

    async for session in db_session_dependency():
        async with session.begin():
            ed_store = EditionStore(session=session, logger=logger)
            refreshed_ed = await ed_store.get_by_slug(
                project_id=project.id, slug=edition.slug
            )
            assert refreshed_ed is not None
            assert refreshed_ed.publish_status == PublishStatus.failed

            hist_store = EditionBuildHistoryStore(
                session=session, logger=logger
            )
            entries = await hist_store.list_by_edition(edition.id)
            assert entries
            assert entries[0].publish_status == PublishStatus.failed

            qjs = QueueJobStore(session=session, logger=logger)
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.failed
            assert job.errors is not None
            assert "publisher exploded" in job.errors.get("message", "")
            assert job.errors.get("type") == "RuntimeError"


@pytest.mark.asyncio
async def test_publish_edition_no_cdn_shortcut(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Org without cdn_service_label marks published without a publisher."""
    logger = _logger()

    async def _provider_raises(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
    ) -> Any:
        _ = (self, org_id, service_label)
        msg = "publisher provider must not be called"
        raise AssertionError(msg)

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _provider_raises,
    )

    async with db_session.begin():
        (
            org,
            project,
            edition,
            build,
            _history_entry,
            queue_job,
        ) = await _setup_publish_scenario(
            db_session,
            org_slug="no-cdn-worker-org",
            cdn_service_label=None,
            backend_job_id="test-publish-arq-nocdn",
        )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-publish-arq-nocdn",
    )
    payload = _make_payload(
        org=org,
        project=project,
        edition=edition,
        build=build,
        queue_job=queue_job,
    )

    result = await publish_edition(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            ed_store = EditionStore(session=session, logger=logger)
            refreshed_ed = await ed_store.get_by_slug(
                project_id=project.id, slug=edition.slug
            )
            assert refreshed_ed is not None
            assert refreshed_ed.publish_status == PublishStatus.published

            hist_store = EditionBuildHistoryStore(
                session=session, logger=logger
            )
            entries = await hist_store.list_by_edition(edition.id)
            assert entries
            assert entries[0].publish_status == PublishStatus.published

            qjs = QueueJobStore(session=session, logger=logger)
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.completed


_config = Configuration()


@pytest.mark.asyncio
async def test_publish_edition_success_enqueues_dashboard_build(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Success finalize enqueues exactly one dashboard_build QueueJob."""
    mock_publisher = MockEditionPublisher()

    async with db_session.begin():
        (
            org,
            project,
            edition,
            build,
            _history_entry,
            queue_job,
        ) = await _setup_publish_scenario(
            db_session,
            org_slug="pub-dash-org",
            cdn_service_label="cdn-prod",
            backend_job_id="test-publish-arq-dash",
        )

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_create_edition_publisher(mock_publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        arq_queue=mock_arq,
        job_id="test-publish-arq-dash",
    )
    payload = _make_payload(
        org=org,
        project=project,
        edition=edition,
        build=build,
        queue_job=queue_job,
    )

    result = await publish_edition(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            dash_rows = await session.execute(
                select(SqlQueueJob).where(
                    SqlQueueJob.kind == JobKind.dashboard_build.value
                )
            )
            rows = list(dash_rows.scalars().all())
            assert len(rows) == 1
            assert rows[0].org_id == org.id
            assert rows[0].project_id == project.id

    dashboard_jobs = get_jobs_by_name(mock_arq, "dashboard_build")
    assert len(dashboard_jobs) == 1
    dash_payload = dashboard_jobs[0].kwargs["payload"]
    assert dash_payload["org_id"] == org.id
    assert dash_payload["project_id"] == project.id
    assert dash_payload["project_slug"] == project.slug


@pytest.mark.asyncio
async def test_publish_edition_failure_does_not_enqueue_dashboard(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed publish_edition does not enqueue a dashboard_build."""
    failing_publisher = _FailingPublisher(RuntimeError("publisher exploded"))

    async with db_session.begin():
        (
            org,
            project,
            edition,
            build,
            _history_entry,
            queue_job,
        ) = await _setup_publish_scenario(
            db_session,
            org_slug="pub-dash-fail-org",
            cdn_service_label="cdn-prod",
            backend_job_id="test-publish-arq-dash-fail",
        )

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_create_edition_publisher(failing_publisher),
    )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        arq_queue=mock_arq,
        job_id="test-publish-arq-dash-fail",
    )
    payload = _make_payload(
        org=org,
        project=project,
        edition=edition,
        build=build,
        queue_job=queue_job,
    )

    result = await publish_edition(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "failed"

    async for session in db_session_dependency():
        async with session.begin():
            dash_rows = await session.execute(
                select(SqlQueueJob).where(
                    SqlQueueJob.kind == JobKind.dashboard_build.value
                )
            )
            assert list(dash_rows.scalars().all()) == []

    assert get_jobs_by_name(mock_arq, "dashboard_build") == []


class _RecordingMockEditionPublisher(MockEditionPublisher):
    """``MockEditionPublisher`` that timestamps each ``publish`` call."""

    def __init__(self, publish_timestamps: list[float]) -> None:
        super().__init__()
        self._publish_timestamps = publish_timestamps

    async def publish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
        build_public_id: str,
        object_key_prefix: str,
    ) -> None:
        self._publish_timestamps.append(time.monotonic())
        await super().publish(
            project_slug=project_slug,
            edition_slug=edition_slug,
            build_public_id=build_public_id,
            object_key_prefix=object_key_prefix,
        )


@pytest.mark.asyncio
async def test_publish_edition_acquires_edition_update_lock(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """publish_edition takes EDITION_UPDATE before invoking the publisher.

    Verifies the worker wires the lock to the correct
    ``LockKey.for_edition_update`` for the resolved
    ``(org_id, project_id, edition_id)`` tuple, and that the spy
    publisher's ``publish`` call (the externally observable CDN
    mutation) happens strictly after the EDITION_UPDATE acquire.
    """
    publish_timestamps: list[float] = []
    mock_publisher = _RecordingMockEditionPublisher(
        publish_timestamps=publish_timestamps
    )

    async with db_session.begin():
        (
            org,
            project,
            edition,
            build,
            _history_entry,
            queue_job,
        ) = await _setup_publish_scenario(
            db_session,
            org_slug="pub-lock-org",
            cdn_service_label="cdn-prod",
            backend_job_id="test-publish-arq-lock",
        )

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_create_edition_publisher(mock_publisher),
    )
    events = install_recording_lock_service(monkeypatch)

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-publish-arq-lock",
    )
    payload = _make_payload(
        org=org,
        project=project,
        edition=edition,
        build=build,
        queue_job=queue_job,
    )

    result = await publish_edition(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    expected = LockKey.for_edition_update(
        org_id=org.id, project_id=project.id, edition_id=edition.id
    )
    eu_enters = [
        e
        for e in events
        if e.event == "enter"
        and e.lock_key.lock_class == LockClass.EDITION_UPDATE
    ]
    assert len(eu_enters) == 1
    assert eu_enters[0].lock_key == expected

    assert publish_timestamps, "expected publisher.publish to be called"
    eu_enter_ts = eu_enters[0].timestamp
    assert all(ts > eu_enter_ts for ts in publish_timestamps)
