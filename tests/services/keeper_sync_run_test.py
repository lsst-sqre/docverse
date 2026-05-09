"""Integration tests for ``KeeperSyncRunService``."""

from __future__ import annotations

from typing import Literal

import pytest
import structlog
from safir.arq import MockArqQueue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    KeeperSyncConfig,
    KeeperSyncRunStatus,
    OrganizationCreate,
)
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobKind
from docverse.exceptions import BadRequestError, ConflictError, NotFoundError
from docverse.services.keeper_sync_run import (
    KEEPER_SYNC_QUEUE_NAME,
    KeeperSyncRunService,
)
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_backend import ArqQueueBackend
from docverse.storage.queue_job_store import QueueJobStore
from tests.support.arq_testing import register_queue


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(
    db_session: AsyncSession,
    *,
    enabled: bool = True,
    project_slugs: list[str] | Literal["*"] | None = None,
) -> tuple[int, str]:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug="ks-org",
            title="KS Org",
            base_domain="ks.example.com",
        )
    )
    config = (
        KeeperSyncConfig(enabled=enabled)
        if project_slugs is None
        else KeeperSyncConfig(enabled=enabled, project_slugs=project_slugs)
    )
    await org_store.update_keeper_sync_config(slug=org.slug, config=config)
    return org.id, org.slug


def _make_service(
    *, db_session: AsyncSession, mock_arq: MockArqQueue
) -> KeeperSyncRunService:
    logger = _logger()
    return KeeperSyncRunService(
        org_store=OrganizationStore(session=db_session, logger=logger),
        run_store=KeeperSyncRunStore(session=db_session, logger=logger),
        queue_backend=ArqQueueBackend(
            arq_queue=mock_arq, default_queue_name="docverse:queue"
        ),
        queue_job_store=QueueJobStore(session=db_session, logger=logger),
        logger=logger,
    )


@pytest.mark.asyncio
async def test_start_run_creates_row_and_enqueues_discovery(
    app: None,
    db_session: AsyncSession,
) -> None:
    """``start_run`` creates a pending run and enqueues discovery."""
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    async with db_session.begin():
        _, org_slug = await _seed_org(db_session)
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        run, queue_job = await service.start_run(org_slug=org_slug)
        await db_session.commit()

    assert run.status == KeeperSyncRunStatus.pending
    assert queue_job.kind == JobKind.keeper_sync_run_discovery
    assert queue_job.keeper_sync_run_id == run.id
    assert queue_job.backend_job_id is not None

    # Verify the enqueue went to the dedicated queue.
    sync_queue = mock_arq._job_metadata[KEEPER_SYNC_QUEUE_NAME]
    assert len(sync_queue) == 1
    metadata = next(iter(sync_queue.values()))
    assert metadata.name == "keeper_sync_run_discovery"


@pytest.mark.asyncio
async def test_start_run_409_when_disabled(
    app: None,
    db_session: AsyncSession,
) -> None:
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    async with db_session.begin():
        _, org_slug = await _seed_org(db_session, enabled=False)
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        with pytest.raises(ConflictError):
            await service.start_run(org_slug=org_slug)


@pytest.mark.asyncio
async def test_start_run_409_when_concurrent(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A second ``start_run`` against an active run hits the partial UQ."""
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    async with db_session.begin():
        _, org_slug = await _seed_org(db_session)
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        await service.start_run(org_slug=org_slug)
        await db_session.commit()

    async with db_session.begin():
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        with pytest.raises(ConflictError):
            await service.start_run(org_slug=org_slug)


@pytest.mark.asyncio
async def test_get_run_returns_aggregate_counters(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Counters are derived from queue_jobs filtered on run_id."""
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        run, _ = await service.start_run(org_slug=org_slug)
        # Seed three more queue_jobs in mixed states.
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        for _ in range(2):
            await queue_job_store.create(
                kind=JobKind.keeper_sync_project,
                org_id=org_id,
                keeper_sync_run_id=run.id,
            )
        # And one in completed state via direct INSERT.
        db_session.add(
            SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.keeper_sync_project.value,
                status="completed",
                org_id=org_id,
                keeper_sync_run_id=run.id,
            )
        )
        await db_session.commit()

    async with db_session.begin():
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        result = await service.get_run(org_slug=org_slug, run_id=run.id)
    # Discovery (queued) + 2 keeper_sync_project (queued) = 3 pending,
    # 1 completed, 0 failed, total 4.
    assert result.activity.pending_count == 3
    assert result.activity.succeeded_count == 1
    assert result.activity.failed_count == 0
    assert result.activity.total_count == 4


@pytest.mark.asyncio
async def test_get_run_404_for_unknown_run(
    app: None,
    db_session: AsyncSession,
) -> None:
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    async with db_session.begin():
        _, org_slug = await _seed_org(db_session)
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        with pytest.raises(NotFoundError):
            await service.get_run(org_slug=org_slug, run_id=9999)


@pytest.mark.asyncio
async def test_refresh_project_enqueues_keeper_sync_project(
    app: None,
    db_session: AsyncSession,
) -> None:
    """``refresh_project`` enqueues one tier-cron-equivalent job.

    Locks the no-run-attribution invariant: the resulting
    ``queue_jobs`` row carries ``keeper_sync_run_id IS NULL`` and the
    LTD slug as its ``subject_label``, mirroring the tier-cron path.
    """
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    async with db_session.begin():
        _, org_slug = await _seed_org(db_session, project_slugs=["pipelines"])
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        queue_job = await service.refresh_project(
            org_slug=org_slug, ltd_slug="pipelines"
        )
        await db_session.commit()

    assert queue_job.kind == JobKind.keeper_sync_project
    assert queue_job.keeper_sync_run_id is None
    assert queue_job.subject_label == "pipelines"
    assert queue_job.backend_job_id is not None

    # Enqueue went to the dedicated keeper-sync queue and the payload
    # omits ``run_id`` so the receiving worker takes the run-less path.
    sync_queue = mock_arq._job_metadata[KEEPER_SYNC_QUEUE_NAME]
    assert len(sync_queue) == 1
    metadata = next(iter(sync_queue.values()))
    assert metadata.name == "keeper_sync_project"
    payload = metadata.kwargs["payload"]
    assert payload["ltd_slug"] == "pipelines"
    assert "run_id" not in payload


@pytest.mark.asyncio
async def test_refresh_project_404_when_disabled(
    app: None,
    db_session: AsyncSession,
) -> None:
    """``refresh_project`` against a disabled config raises NotFoundError."""
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    async with db_session.begin():
        _, org_slug = await _seed_org(db_session, enabled=False)
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        with pytest.raises(NotFoundError):
            await service.refresh_project(
                org_slug=org_slug, ltd_slug="pipelines"
            )


@pytest.mark.asyncio
async def test_refresh_project_400_when_slug_outside_allowlist(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A slug not in the org's allowlist raises BadRequestError."""
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    async with db_session.begin():
        _, org_slug = await _seed_org(db_session, project_slugs=["pipelines"])
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        with pytest.raises(BadRequestError):
            await service.refresh_project(
                org_slug=org_slug, ltd_slug="not-allowed"
            )


@pytest.mark.asyncio
async def test_refresh_project_409_when_active_job_exists(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A second ``refresh_project`` for the same slug returns 409.

    Locks the QA-driven mutex: an in-flight ``keeper_sync_project``
    for ``(org, ltd_slug)`` must block a second operator-triggered
    refresh; the operator already has a job running.
    """
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    async with db_session.begin():
        _, org_slug = await _seed_org(db_session, project_slugs=["pipelines"])
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        # First call enqueues, leaves a queued row.
        await service.refresh_project(org_slug=org_slug, ltd_slug="pipelines")
        await db_session.commit()

    async with db_session.begin():
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        with pytest.raises(ConflictError):
            await service.refresh_project(
                org_slug=org_slug, ltd_slug="pipelines"
            )

    # Still exactly one row — no duplicate inserted by the failed call.
    async with db_session.begin():
        rows = (
            (
                await db_session.execute(
                    select(SqlQueueJob).where(
                        SqlQueueJob.kind == JobKind.keeper_sync_project.value,
                        SqlQueueJob.subject_label == "pipelines",
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1


@pytest.mark.asyncio
async def test_refresh_project_409_when_tier_cron_job_already_active(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A tier-cron-attributed active row also blocks the operator refresh."""
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["pipelines"]
        )
        # Pre-seed a tier-cron-style row (no run attribution).
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="pipelines",
            backend_job_id="arq-job-tier-cron",
        )
        await db_session.commit()

    async with db_session.begin():
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        with pytest.raises(ConflictError):
            await service.refresh_project(
                org_slug=org_slug, ltd_slug="pipelines"
            )


@pytest.mark.asyncio
async def test_refresh_project_wildcard_allowlist_admits_any_slug(
    app: None,
    db_session: AsyncSession,
) -> None:
    """``project_slugs == "*"`` does not gate any specific slug."""
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    async with db_session.begin():
        _, org_slug = await _seed_org(db_session, project_slugs="*")
        service = _make_service(db_session=db_session, mock_arq=mock_arq)
        queue_job = await service.refresh_project(
            org_slug=org_slug, ltd_slug="any-slug-at-all"
        )
        await db_session.commit()

    assert queue_job.subject_label == "any-slug-at-all"
    assert queue_job.keeper_sync_run_id is None
