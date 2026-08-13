"""Integration tests for the publish_edition worker function."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType
from typing import Any, Self

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    KeeperSyncRunStatus,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.models.queue_enums import PublishStatus
from docverse_server.config import Configuration
from docverse_server.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import (
    generate_base32_id,
    serialize_base32_id,
    validate_base32_id,
)
from docverse_server.domain.build import Build
from docverse_server.domain.cache_profile import CacheProfile
from docverse_server.domain.edition import Edition
from docverse_server.domain.edition_build_history import EditionBuildHistory
from docverse_server.domain.organization import Organization
from docverse_server.domain.project import Project
from docverse_server.domain.queue import JobKind, JobStatus, QueueJob
from docverse_server.factory import Factory
from docverse_server.metrics import (
    EditionPublishTrigger,
    MetricsEditionKind,
    build_event_manager,
)
from docverse_server.services.lock_service import LockClass, LockKey
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.editionpublisher import (
    EditionPublisher,
    MockEditionPublisher,
)
from docverse_server.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.publish_edition import publish_edition
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
        cache_profile: CacheProfile,
    ) -> None:
        _ = (
            project_slug,
            edition_slug,
            build_public_id,
            object_key_prefix,
            cache_profile,
        )
        raise self._exc

    async def unpublish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
    ) -> None:
        _ = (project_slug, edition_slug)
        raise self._exc


@dataclass(slots=True)
class _PurgeObservation:
    """Database state observed at the moment the CDN purge ran."""

    in_transaction: bool
    """Whether the worker's session had an open transaction."""

    publish_status: PublishStatus | None
    """The edition's ``publish_status`` as visible to another session."""

    job_status: JobStatus | None
    """The publish ``queue_jobs`` status as visible to another session."""

    timestamp: float
    """``time.monotonic()`` when the purge was reached."""


class _ProbingCdnCachePurger:
    """A ``CdnCachePurger`` that snapshots database state when it purges.

    The publish burst that motivates deferring the purge (a keeper-sync
    backfill) only exhausts the engine pool when each waiter holds a
    connection while the coalescer serializes and the purger backs off,
    so the load-bearing assertion is that the worker's session is *not*
    in a transaction by the time the purger is reached.
    """

    def __init__(
        self,
        *,
        session: AsyncSession,
        edition_id: int,
        queue_job_id: int,
        observations: list[_PurgeObservation],
    ) -> None:
        self._session = session
        self._edition_id = edition_id
        self._queue_job_id = queue_job_id
        self._observations = observations

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    async def purge_hostname(self, hostname: str) -> None:
        _ = hostname
        in_transaction = self._session.in_transaction()
        publish_status: PublishStatus | None = None
        job_status: JobStatus | None = None
        # A second session sees only committed rows, so reading the
        # edition here proves the publish transaction already landed.
        async for probe_session in db_session_dependency():
            async with probe_session.begin():
                store = EditionStore(session=probe_session, logger=_logger())
                edition = await store.get_by_id(self._edition_id)
                publish_status = (
                    edition.publish_status if edition is not None else None
                )
                jobs = QueueJobStore(session=probe_session, logger=_logger())
                job = await jobs.get(self._queue_job_id)
                job_status = job.status if job is not None else None
        self._observations.append(
            _PurgeObservation(
                in_transaction=in_transaction,
                publish_status=publish_status,
                job_status=job_status,
                timestamp=time.monotonic(),
            )
        )


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _mock_create_cdn_cache_purger(
    *,
    edition_id: int,
    queue_job_id: int,
    observations: list[_PurgeObservation],
) -> Any:
    """Return a patched ``create_cdn_cache_purger_for_org``."""

    async def _create(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
    ) -> Any:
        _ = (org_id, service_label)
        # Mimic the real helper's database access: resolving an org's
        # CDN credentials reads the database, so resolving the purger
        # outside an explicit ``session.begin()`` block would autobegin
        # a transaction and break the worker's next one — the same
        # failure production would see.
        await self._session.execute(select(1))
        return _ProbingCdnCachePurger(
            session=self._session,
            edition_id=edition_id,
            queue_job_id=queue_job_id,
            observations=observations,
        )

    return _create


def _mock_create_edition_publisher(
    publisher: EditionPublisher,
) -> Any:
    """Return a patched ``create_edition_publisher_for_org`` that
    returns the given publisher.
    """

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
    keeper_sync_run_id: int | None = None,
) -> tuple[
    Organization,
    Project,
    Edition,
    Build,
    EditionBuildHistory,
    QueueJob,
]:
    """Create org, project, edition, build, history entry, and queue job.

    When ``keeper_sync_run_id`` is supplied the publish ``QueueJob`` is
    attributed to that keeper-sync run (mirroring the backfill path), so
    completing the publish drives the run terminal.
    """
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
        keeper_sync_run_id=keeper_sync_run_id,
    )
    return org, project, edition, refreshed_build, history_entry, queue_job


def _make_payload(
    *,
    org: Organization,
    project: Project,
    edition: Edition,
    build: Build,
    queue_job: QueueJob,
    trigger: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "org_id": org.id,
        "project_slug": project.slug,
        "edition_id": edition.id,
        "edition_slug": edition.slug,
        "build_id": build.id,
        "build_public_id": serialize_base32_id(build.public_id),
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }
    if trigger is not None:
        payload["trigger"] = trigger
    return payload


async def _seed_keeper_sync_run(
    db_session: AsyncSession, *, org_id: int
) -> int:
    """Seed an ``in_progress`` keeper-sync run to attribute a publish to."""
    row = SqlKeeperSyncRun(
        public_id=validate_base32_id(generate_base32_id()),
        org_id=org_id,
        kind="backfill",
        status="in_progress",
    )
    db_session.add(row)
    await db_session.flush()
    await db_session.refresh(row)
    return row.id


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
async def test_publish_edition_publishes_edition_published(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful publish emits one ``edition_published`` metric event."""
    mock_publisher = MockEditionPublisher()
    _manager, events = await build_event_manager(Configuration())

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
            org_slug="pub-metrics-org",
            cdn_service_label="cdn-prod",
            backend_job_id="test-publish-arq-metrics",
        )

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_create_edition_publisher(mock_publisher),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-publish-arq-metrics",
        events=events,
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

    publisher = events.edition_published
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == org.slug
    assert event.project == project.slug
    # The scenario edition is ``EditionKind.release``; the worker maps it
    # to the dedicated metrics enum.
    assert event.edition_kind == MetricsEditionKind.release
    # No keeper_sync_run_id on the queue job => a build-driven publish.
    assert event.trigger == EditionPublishTrigger.build
    assert event.elapsed >= timedelta(0)


@pytest.mark.asyncio
async def test_publish_edition_rollback_trigger(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rollback-driven publish reports ``trigger=rollback``.

    The rollback handler enqueues ``publish_edition`` with no
    ``keeper_sync_run_id`` but tags its payload ``trigger=rollback``
    (SQR-112 D7), so the ``edition_published`` metric must distinguish it
    from the ordinary build fan-out.
    """
    mock_publisher = MockEditionPublisher()
    _manager, events = await build_event_manager(Configuration())

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
            org_slug="pub-rollback-org",
            cdn_service_label="cdn-prod",
            backend_job_id="test-publish-arq-rollback",
        )

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_create_edition_publisher(mock_publisher),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-publish-arq-rollback",
        events=events,
    )
    payload = _make_payload(
        org=org,
        project=project,
        edition=edition,
        build=build,
        queue_job=queue_job,
        trigger=EditionPublishTrigger.rollback.value,
    )

    result = await publish_edition(ctx, payload)
    await ctx["http_client"].aclose()
    assert result == "completed"

    publisher = events.edition_published
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    assert publisher.published[0].trigger == EditionPublishTrigger.rollback


@pytest.mark.asyncio
async def test_publish_edition_finalises_keeper_sync_run_succeeded(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run's last publish completing emits ``success=True`` run-completed.

    Attributes the publish job to a keeper-sync run as its only child, so
    completing the publish drives the run to ``succeeded`` and the worker
    publishes one ``keeper_sync_run_completed`` with ``success=True`` —
    covering the ``publish_edition`` finaliser wiring and the clean
    success branch (the keeper_sync_project test only covers
    ``partial_failure``).
    """
    mock_publisher = MockEditionPublisher()
    _manager, events = await build_event_manager(Configuration())

    async with db_session.begin():
        org_store = OrganizationStore(session=db_session, logger=_logger())
        org_seed = await org_store.create(
            OrganizationCreate(
                slug="pub-run-org",
                title="Run Org",
                base_domain="pub-run-org.example.com",
            )
        )
        run_id = await _seed_keeper_sync_run(db_session, org_id=org_seed.id)

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
            org_slug="pub-run-attrib-org",
            cdn_service_label="cdn-prod",
            backend_job_id="test-publish-arq-run",
            keeper_sync_run_id=run_id,
        )

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_create_edition_publisher(mock_publisher),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-publish-arq-run",
        events=events,
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

    publisher = events.keeper_sync_run_completed
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    # A keeper-sync run is org-scoped, so it carries no project.
    assert event.project is None
    assert event.success is True
    assert event.total_count == 1
    assert event.succeeded_count == 1
    assert event.failed_count == 0
    assert event.elapsed >= timedelta(0)

    # The run row itself reached the terminal succeeded status.
    async for session in db_session_dependency():
        async with session.begin():
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.succeeded


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
        cache_profile: CacheProfile,
    ) -> None:
        self._publish_timestamps.append(time.monotonic())
        await super().publish(
            project_slug=project_slug,
            edition_slug=edition_slug,
            build_public_id=build_public_id,
            object_key_prefix=object_key_prefix,
            cache_profile=cache_profile,
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


async def _run_publish_with_purge_probe(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    *,
    org_slug: str,
    backend_job_id: str,
) -> list[_PurgeObservation]:
    """Run one ``publish_edition`` job against a probing CDN purger."""
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
            org_slug=org_slug,
            cdn_service_label="cdn-prod",
            backend_job_id=backend_job_id,
        )

    observations: list[_PurgeObservation] = []
    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_create_edition_publisher(MockEditionPublisher()),
    )
    monkeypatch.setattr(
        Factory,
        "create_cdn_cache_purger_for_org",
        _mock_create_cdn_cache_purger(
            edition_id=edition.id,
            queue_job_id=queue_job.id,
            observations=observations,
        ),
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id=backend_job_id,
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
    assert observations, "expected the purger to be reached"
    return observations


@pytest.mark.asyncio
async def test_publish_edition_purges_with_no_open_transaction(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CDN purge is awaited with no database transaction open.

    The purge queues behind the process-wide per-hostname coalescer and
    can then sit in the purger's own retry backoff for tens of seconds.
    Holding an idle-in-transaction connection across that wait is what
    exhausts the async engine pool during a same-hostname publish burst.
    """
    observations = await _run_publish_with_purge_probe(
        db_session,
        monkeypatch,
        org_slug="pub-purge-txn-org",
        backend_job_id="test-publish-arq-purge-txn",
    )

    assert [o.in_transaction for o in observations] == [False]


@pytest.mark.asyncio
async def test_publish_edition_commits_publish_state_before_purging(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The edition reaches ``published`` before the purge is attempted."""
    observations = await _run_publish_with_purge_probe(
        db_session,
        monkeypatch,
        org_slug="pub-purge-commit-org",
        backend_job_id="test-publish-arq-purge-commit",
    )

    assert [o.publish_status for o in observations] == [
        PublishStatus.published
    ]


@pytest.mark.asyncio
async def test_publish_edition_purges_after_releasing_edition_lock(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The EDITION_UPDATE advisory lock is released before the purge runs.

    ``LockService.acquire`` pins a dedicated ``engine.connect()`` for the
    whole block, and keeper-sync takes the *same* key in ``sync_build``
    and ``_ensure_aggregate_edition``. A publish parked in the purger's
    rate-limit backoff (up to four attempts x a 10 s clamped
    ``Retry-After``) therefore used to block the sync worker's next
    import of that edition while holding a pool connection it does not
    need — the purge touches no database at all.
    """
    events = install_recording_lock_service(monkeypatch)

    observations = await _run_publish_with_purge_probe(
        db_session,
        monkeypatch,
        org_slug="pub-purge-lock-org",
        backend_job_id="test-publish-arq-purge-lock",
    )

    edition_lock_exits = [
        e
        for e in events
        if e.event == "exit"
        and e.lock_key.lock_class == LockClass.EDITION_UPDATE
    ]
    assert len(edition_lock_exits) == 1
    assert observations[0].timestamp > edition_lock_exits[0].timestamp


@pytest.mark.asyncio
async def test_publish_edition_completes_queue_job_before_purging(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The queue job is already terminal when the purge is attempted.

    The purge is the one step that can sleep for tens of seconds, so it
    is also the step arq's per-job timeout is most likely to cancel.
    Committing the completion first means the cancellation can only cost
    the (best-effort) purge, never strand a committed publish as
    ``in_progress`` until the reaper fails it hours later.
    """
    observations = await _run_publish_with_purge_probe(
        db_session,
        monkeypatch,
        org_slug="pub-purge-job-org",
        backend_job_id="test-publish-arq-purge-job",
    )

    assert [o.job_status for o in observations] == [JobStatus.completed]


class _CancellingCdnCachePurger:
    """A ``CdnCachePurger`` whose purge is cancelled mid-flight.

    Stands in for arq's per-job timeout firing while the purger sleeps
    out a Cloudflare 429 backoff. ``CancelledError`` is a
    ``BaseException``, so it escapes ``purge_cdn_cache``'s best-effort
    ``except Exception``.
    """

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    async def purge_hostname(self, hostname: str) -> None:
        _ = hostname
        raise asyncio.CancelledError


@pytest.mark.asyncio
async def test_publish_edition_survives_cancellation_during_purge(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A publish cancelled during its purge still reports terminal state.

    Regression for the ``publish_edition`` timeout budget: a job that
    committed its publish and was then cancelled inside the purge must
    leave the ``queue_jobs`` row terminal, not ``in_progress`` for the
    reaper to fail up to four hours later.
    """
    logger = _logger()

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
            org_slug="pub-purge-cancel-org",
            cdn_service_label="cdn-prod",
            backend_job_id="test-publish-arq-purge-cancel",
        )

    async def _create_purger(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
    ) -> Any:
        _ = (self, org_id, service_label)
        return _CancellingCdnCachePurger()

    monkeypatch.setattr(
        Factory,
        "create_edition_publisher_for_org",
        _mock_create_edition_publisher(MockEditionPublisher()),
    )
    monkeypatch.setattr(
        Factory, "create_cdn_cache_purger_for_org", _create_purger
    )

    ctx = make_worker_ctx(
        http_client=httpx.AsyncClient(),
        job_id="test-publish-arq-purge-cancel",
    )
    payload = _make_payload(
        org=org,
        project=project,
        edition=edition,
        build=build,
        queue_job=queue_job,
    )

    with pytest.raises(asyncio.CancelledError):
        await publish_edition(ctx, payload)
    await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=logger)
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.date_completed is not None

            ed_store = EditionStore(session=session, logger=logger)
            refreshed_ed = await ed_store.get_by_id(edition.id)
            assert refreshed_ed is not None
            assert refreshed_ed.publish_status == PublishStatus.published
