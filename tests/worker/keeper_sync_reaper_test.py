"""Tests for the ``keeper_sync_reaper`` cron worker function.

The reaper is the cron-driven backstop for the case where arq itself
loses a keeper-sync child job (typically a worker pod OOM-killed
mid-job that never gets to surface a timeout). It marks any silent
child as ``failed`` and finalises the parent run so an operator never
sees a sync stuck in ``in_progress`` forever.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    JobKind,
    KeeperSyncRunStatus,
    OrganizationCreate,
)
from docverse.config import Configuration
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobStatus
from docverse.metrics import build_event_manager
from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.keeper_sync import keeper_sync_reaper
from tests.support.arq_testing import register_queue
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(db_session: AsyncSession, *, slug: str) -> int:
    org_store = OrganizationStore(session=db_session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"KS Org {slug}",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


async def _seed_run(
    db_session: AsyncSession, *, org_id: int, status: str = "in_progress"
) -> int:
    row = SqlKeeperSyncRun(
        public_id=validate_base32_id(generate_base32_id()),
        org_id=org_id,
        kind="backfill",
        status=status,
    )
    db_session.add(row)
    await db_session.flush()
    await db_session.refresh(row)
    return row.id


async def _seed_stuck_child(
    db_session: AsyncSession,
    *,
    org_id: int,
    run_id: int,
    backend_job_id: str,
    started_minutes_ago: int,
) -> int:
    queue_job_store = QueueJobStore(session=db_session, logger=_logger())
    job = await queue_job_store.create(
        kind=JobKind.keeper_sync_project,
        org_id=org_id,
        keeper_sync_run_id=run_id,
        backend_job_id=backend_job_id,
    )
    await queue_job_store.start(job.id)
    row = await db_session.get(SqlQueueJob, job.id)
    assert row is not None
    row.date_started = datetime.now(tz=UTC) - timedelta(
        minutes=started_minutes_ago
    )
    await db_session.flush()
    return job.id


def _make_ctx(http_client: httpx.AsyncClient) -> dict[str, Any]:
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    return make_worker_ctx(http_client=http_client, arq_queue=mock_arq)


@pytest.mark.asyncio
async def test_reaper_fails_stuck_child_and_finalises_run(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Child past the threshold is failed; parent reaches partial_failure."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-1")
        run_id = await _seed_run(db_session, org_id=org_id)
        stuck_id = await _seed_stuck_child(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="arq-stuck-1",
            started_minutes_ago=600,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await keeper_sync_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(stuck_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert qj.date_completed is not None

            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.partial_failure
            assert run.date_finished is not None


@pytest.mark.asyncio
async def test_reaper_publishes_run_completed(
    app: None,
    db_session: AsyncSession,
) -> None:
    """The reaper finalising a run emits one ``keeper_sync_run_completed``.

    Covers the reaper's ``publish_run_completed`` wiring (untested by the
    keeper_sync_project path): a stuck child is failed, the parent run
    reaches ``partial_failure``, and the sweep publishes one org-scoped
    run-completed event with ``success=False`` after the finalisation
    transaction commits.
    """
    _manager, events = await build_event_manager(Configuration())

    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-evt")
        run_id = await _seed_run(db_session, org_id=org_id)
        await _seed_stuck_child(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="arq-stuck-evt",
            started_minutes_ago=600,
        )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(
        http_client=http_client, arq_queue=mock_arq, events=events
    )
    try:
        await keeper_sync_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    publisher = events.keeper_sync_run_completed
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == "ks-reaper-evt"
    # A keeper-sync run is org-scoped, so it carries no project.
    assert event.project is None
    assert event.success is False
    assert event.total_count == 1
    assert event.failed_count == 1
    assert event.elapsed >= timedelta(0)


@pytest.mark.asyncio
async def test_reaper_skips_runs_with_recent_activity(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A child within the idle window leaves its parent run alone."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-2")
        run_id = await _seed_run(db_session, org_id=org_id)
        # Started just now — well within any sane idle window.
        await _seed_stuck_child(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="arq-fresh",
            started_minutes_ago=0,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await keeper_sync_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            # Untouched: still in_progress.
            assert run.status == KeeperSyncRunStatus.in_progress


async def _seed_silent_tier_cron(
    db_session: AsyncSession,
    *,
    org_id: int,
    subject_label: str,
    started_minutes_ago: int,
) -> int:
    """Create a run-less keeper_sync_project row stuck ``in_progress``."""
    queue_job_store = QueueJobStore(session=db_session, logger=_logger())
    job = await queue_job_store.create(
        kind=JobKind.keeper_sync_project,
        org_id=org_id,
        keeper_sync_run_id=None,
        subject_label=subject_label,
        backend_job_id=f"arq-tc-silent-{subject_label}",
    )
    await queue_job_store.start(job.id)
    row = await db_session.get(SqlQueueJob, job.id)
    assert row is not None
    row.date_started = datetime.now(tz=UTC) - timedelta(
        minutes=started_minutes_ago
    )
    await db_session.flush()
    return job.id


async def _seed_orphan_tier_cron(
    db_session: AsyncSession,
    *,
    org_id: int,
    subject_label: str,
    created_minutes_ago: int,
) -> int:
    """Create a run-less keeper_sync_project orphan (queued, no backend id)."""
    queue_job_store = QueueJobStore(session=db_session, logger=_logger())
    job = await queue_job_store.create(
        kind=JobKind.keeper_sync_project,
        org_id=org_id,
        keeper_sync_run_id=None,
        subject_label=subject_label,
    )
    row = await db_session.get(SqlQueueJob, job.id)
    assert row is not None
    row.date_created = datetime.now(tz=UTC) - timedelta(
        minutes=created_minutes_ago
    )
    await db_session.flush()
    return job.id


@pytest.mark.asyncio
async def test_reaper_handles_tier_cron_and_run_attributed_rows_together(
    app: None,
    db_session: AsyncSession,
) -> None:
    """One reaper tick reaps tier-cron silent, tier-cron orphan, and run rows.

    Models the production wedge observed on phalanx: tier-cron-enqueued
    ``keeper_sync_project`` rows (no run attribution) stuck in
    ``in_progress`` or ``queued`` indefinitely, alongside the existing
    run-attributed silent path. All three must transition to ``failed``
    with the right ``errors.type`` tag in a single transaction.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-tc")
        run_id = await _seed_run(db_session, org_id=org_id)
        # Run-attributed stuck child (existing path).
        run_attrib_id = await _seed_stuck_child(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="arq-stuck-run",
            started_minutes_ago=600,
        )
        # Tier-cron silent (in_progress, no run).
        silent_id = await _seed_silent_tier_cron(
            db_session,
            org_id=org_id,
            subject_label="phalanx",
            started_minutes_ago=600,
        )
        # Tier-cron orphan (queued, no run, no backend id).
        orphan_id = await _seed_orphan_tier_cron(
            db_session,
            org_id=org_id,
            subject_label="pipelines",
            created_minutes_ago=10,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await keeper_sync_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())

            run_attrib = await qj_store.get(run_attrib_id)
            assert run_attrib is not None
            assert run_attrib.status == JobStatus.failed
            assert run_attrib.errors is not None
            assert run_attrib.errors["type"] == "SilentWorker"

            silent = await qj_store.get(silent_id)
            assert silent is not None
            assert silent.status == JobStatus.failed
            assert silent.errors is not None
            assert silent.errors["type"] == "SilentTierCronJob"

            orphan = await qj_store.get(orphan_id)
            assert orphan is not None
            assert orphan.status == JobStatus.failed
            assert orphan.errors is not None
            assert orphan.errors["type"] == "OrphanedTierCronJob"

            # Run-attributed reap still rolls the parent up.
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.partial_failure

            # Per-subject mutex unblocked for both tier-cron subjects, so
            # the next tier-cron tick can enqueue a fresh job.
            silent_active = await qj_store.has_active_for_subject(
                org_id=org_id,
                kind=JobKind.keeper_sync_project,
                subject_label="phalanx",
            )
            orphan_active = await qj_store.has_active_for_subject(
                org_id=org_id,
                kind=JobKind.keeper_sync_project,
                subject_label="pipelines",
            )
            assert silent_active is False
            assert orphan_active is False


@pytest.mark.asyncio
async def test_reaper_finalises_each_distinct_parent_run(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Stuck children on multiple runs each trigger their own finalisation."""
    async with db_session.begin():
        # Two orgs because the partial unique index forbids more than
        # one non-terminal run per org.
        org_a_id = await _seed_org(db_session, slug="ks-reaper-3a")
        org_b_id = await _seed_org(db_session, slug="ks-reaper-3b")
        run_a_id = await _seed_run(db_session, org_id=org_a_id)
        run_b_id = await _seed_run(db_session, org_id=org_b_id)
        await _seed_stuck_child(
            db_session,
            org_id=org_a_id,
            run_id=run_a_id,
            backend_job_id="arq-stuck-a",
            started_minutes_ago=600,
        )
        await _seed_stuck_child(
            db_session,
            org_id=org_b_id,
            run_id=run_b_id,
            backend_job_id="arq-stuck-b",
            started_minutes_ago=600,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await keeper_sync_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run_a = await run_store.get(run_a_id)
            run_b = await run_store.get(run_b_id)
            assert run_a is not None
            assert run_b is not None
            assert run_a.status == KeeperSyncRunStatus.partial_failure
            assert run_b.status == KeeperSyncRunStatus.partial_failure
