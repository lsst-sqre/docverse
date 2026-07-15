"""Tests for the ``keeper_sync_run_discovery`` worker function."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Literal

import httpx
import pytest
import respx
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    JobKind,
    KeeperSyncConfig,
    KeeperSyncRunStatus,
    OrganizationCreate,
)
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.queue import JobStatus
from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.services.keeper_sync_tombstone import KeeperSyncTombstoneService
from docverse.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.keeper_sync import keeper_sync_run_discovery
from tests.support.arq_testing import get_jobs_by_name, register_queue
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(
    db_session: AsyncSession,
    *,
    project_slugs: list[str] | Literal["*"] = "*",
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
    await org_store.update_keeper_sync_config(
        slug=org.slug,
        config=KeeperSyncConfig(
            enabled=True,
            project_slugs=project_slugs,
        ),
    )
    return org.id, org.slug


async def _seed_run(db_session: AsyncSession, *, org_id: int) -> int:
    row = SqlKeeperSyncRun(org_id=org_id, kind="backfill", status="pending")
    db_session.add(row)
    await db_session.flush()
    await db_session.refresh(row)
    return row.id


async def _seed_discovery_queue_job(
    db_session: AsyncSession, *, org_id: int, run_id: int
) -> int:
    queue_job_store = QueueJobStore(session=db_session, logger=_logger())
    queue_job = await queue_job_store.create(
        kind=JobKind.keeper_sync_run_discovery,
        org_id=org_id,
        keeper_sync_run_id=run_id,
        backend_job_id="test-arq-discovery",
    )
    return queue_job.id


def _mock_ltd_products(mock_discovery: respx.Router, slugs: list[str]) -> None:
    products = [f"https://keeper.lsst.codes/products/{s}/" for s in slugs]
    mock_discovery.get("https://keeper.lsst.codes/products/").mock(
        return_value=httpx.Response(
            status_code=200,
            content=json.dumps({"products": products}).encode(),
            headers={"content-type": "application/json"},
        )
    )


@pytest.mark.asyncio
async def test_discovery_fans_out_intersected_slugs(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Discovery enqueues one ``keeper_sync_project`` per allowlisted slug."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["dmtn-001", "sqr-112"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _mock_ltd_products(mock_discovery, ["dmtn-001", "dmtn-002", "sqr-112"])

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "completed"

    # Two child enqueues — one per intersected slug — landing on the
    # dedicated sync queue and not the default queue.
    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert len(project_jobs) == 2
    default_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name="docverse:queue"
    )
    assert default_jobs == []

    # Each child payload carries the snapshot ``ltd_base_url`` so the
    # per-project worker can construct its KeeperSyncService without
    # re-reading the org config (which may have changed mid-run).
    payloads = [job.kwargs["payload"] for job in project_jobs]
    assert {p["ltd_slug"] for p in payloads} == {"dmtn-001", "sqr-112"}
    for payload in payloads:
        assert payload["ltd_base_url"] == "https://keeper.lsst.codes/"

    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.keeper_sync_run_id == run_id,
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            )
            child_rows = (await session.execute(stmt)).scalars().all()
            assert len(child_rows) == 2
            assert all(row.org_id == org_id for row in child_rows)
            assert all(row.backend_job_id is not None for row in child_rows)

            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.in_progress

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            disc = await queue_job_store.get(queue_job_id)
            assert disc is not None
            assert disc.status == JobStatus.completed
            assert disc.progress is not None
            assert disc.progress["in_scope_count"] == 2


@pytest.mark.asyncio
async def test_discovery_with_wildcard_uses_all_ltd_slugs(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """``project_slugs="*"`` keeps every LTD slug in scope."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, project_slugs="*")
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _mock_ltd_products(mock_discovery, ["dmtn-001", "dmtn-002", "sqr-112"])

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.keeper_sync_run_id == run_id,
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            )
            child_rows = (await session.execute(stmt)).scalars().all()
            assert len(child_rows) == 3


@pytest.mark.asyncio
async def test_discovery_with_empty_intersection_finalises_run(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An empty fan-out terminates the run as ``succeeded`` immediately."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["nonexistent"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _mock_ltd_products(mock_discovery, ["dmtn-001"])

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.succeeded


@pytest.mark.asyncio
async def test_discovery_marks_run_failed_when_disabled(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A disabled config aborts discovery and marks the run failed."""
    logger = _logger()
    async with db_session.begin():
        org_store = OrganizationStore(session=db_session, logger=logger)
        org = await org_store.create(
            OrganizationCreate(
                slug="ks-org",
                title="KS Org",
                base_domain="ks.example.com",
            )
        )
        # Persist a disabled config, then re-fetch the org id.
        await org_store.update_keeper_sync_config(
            slug=org.slug,
            config=KeeperSyncConfig(enabled=False),
        )
        run_id = await _seed_run(db_session, org_id=org.id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org.id, run_id=run_id
        )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org.id,
            "org_slug": org.slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "failed"

    async for session in db_session_dependency():
        async with session.begin():
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.failed
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            disc = await queue_job_store.get(queue_job_id)
            assert disc is not None
            assert disc.status == JobStatus.failed


@pytest.mark.asyncio
async def test_discovery_reconciles_orphan_children_from_prior_attempt(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An orphan child from a crashed prior discovery is failed at start.

    Reproduces the race window where ``_enqueue_children`` committed a
    child ``queue_jobs`` row but died before ``arq_queue.enqueue``: the
    row is queued, has no ``backend_job_id``, and would block run
    finalisation forever. The next discovery attempt should sweep it.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["dmtn-001"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        # Pre-seed an orphan child older than the 5-minute idle window.
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        orphan = await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
        )
        orphan_row = await db_session.get(SqlQueueJob, orphan.id)
        assert orphan_row is not None
        orphan_row.date_created = datetime.now(tz=UTC) - timedelta(minutes=10)
        await db_session.flush()

    _mock_ltd_products(mock_discovery, ["dmtn-001"])

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            reaped = await queue_job_store.get(orphan.id)
            assert reaped is not None
            assert reaped.status == JobStatus.failed
            assert reaped.errors is not None
            assert "orphan" in reaped.errors["message"].lower()


@pytest.mark.asyncio
async def test_discovery_skips_slug_with_active_keeper_sync_project(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An active ``keeper_sync_project`` row blocks the per-slug enqueue.

    Reproduces the QA race: a tier-cron-enqueued (run-less) job for
    ``pipelines`` is already queued; a subsequent operator-triggered
    backfill discovery must not enqueue a second job for the same slug,
    because the two would race through ``_ensure_edition`` and one
    would lose the ``uq_editions_project_lower_slug`` race.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["pipelines", "dmtn-001"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        # Pre-seed a tier-cron-style active row (no run attribution) for
        # ``pipelines``. Discovery must skip this slug.
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        existing = await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="pipelines",
            backend_job_id="arq-job-tier-cron",
        )

    _mock_ltd_products(mock_discovery, ["pipelines", "dmtn-001"])

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "completed"

    # Only ``dmtn-001`` got enqueued; ``pipelines`` was skipped.
    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert len(project_jobs) == 1
    assert project_jobs[0].kwargs["payload"]["ltd_slug"] == "dmtn-001"

    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
                SqlQueueJob.subject_label == "pipelines",
                SqlQueueJob.org_id == org_id,
            )
            rows = (await session.execute(stmt)).scalars().all()
            # Exactly one active ``pipelines`` row — the tier-cron's
            # original — survives. Discovery did not insert a duplicate.
            assert len(rows) == 1
            assert rows[0].id == existing.id
            assert rows[0].keeper_sync_run_id is None

            # The skipped slug does NOT count toward the run's progress
            # (its row stays attached to no run). Run-attributed children
            # = 1 (just dmtn-001).
            run_stmt = select(SqlQueueJob).where(
                SqlQueueJob.keeper_sync_run_id == run_id,
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            )
            attributed = (await session.execute(run_stmt)).scalars().all()
            assert len(attributed) == 1


@pytest.mark.asyncio
async def test_discovery_leaves_recent_unenqueued_children_alone(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A queued child younger than the idle window is not reaped.

    Guards against a discovery worker reaping rows that a concurrent
    healthy discovery worker just committed but hasn't yet had a chance
    to write a ``backend_job_id`` back onto.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["dmtn-001"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        # Fresh child with no backend_job_id — within the idle window.
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        recent = await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
        )

    _mock_ltd_products(mock_discovery, ["dmtn-001"])

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            untouched = await queue_job_store.get(recent.id)
            assert untouched is not None
            assert untouched.status == JobStatus.queued


@pytest.mark.asyncio
async def test_discovery_skips_tombstoned_project_slugs(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Tombstoned project slugs drop out of the fan-out candidate set.

    Issue #396 acceptance criterion: an org with a tombstoned project
    produces no ``keeper_sync_project`` child job for that resource.
    Without the filter, ``run_discovery`` would fan out a child per
    in-scope slug and the per-slug ``keeper_sync_project`` worker
    would short-circuit inside ``sync_project`` — wasted queue and DB
    work the filter avoids.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["dmtn-001", "sqr-112"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        # Tombstone one of the two in-scope project slugs.
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        tombstone_service = KeeperSyncTombstoneService(
            session=db_session,
            state_store=state_store,
            logger=_logger(),
        )
        await tombstone_service.record(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="dmtn-001",
            reason=TombstoneReason.manual_delete,
        )

    _mock_ltd_products(mock_discovery, ["dmtn-001", "sqr-112"])

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "completed"

    # Only the non-tombstoned slug is enqueued.
    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert len(project_jobs) == 1
    assert project_jobs[0].kwargs["payload"]["ltd_slug"] == "sqr-112"

    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.keeper_sync_run_id == run_id,
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            )
            child_rows = (await session.execute(stmt)).scalars().all()
            assert len(child_rows) == 1
            assert child_rows[0].subject_label == "sqr-112"
