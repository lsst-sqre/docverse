"""Tests for the ``keeper_sync_reaper`` cron worker function.

The reaper is the cron-driven backstop for the case where arq itself
loses a keeper-sync child job (typically a worker pod OOM-killed
mid-job that never gets to surface a timeout). It marks any silent
child as ``failed`` and finalises the parent run so an operator never
sees a sync stuck in ``in_progress`` forever.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import JobKind, KeeperSyncRunStatus, OrganizationCreate
from docverse_server.config import Configuration
from docverse_server.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import (
    generate_base32_id,
    validate_base32_id,
)
from docverse_server.domain.queue import JobStatus
from docverse_server.metrics import build_event_manager
from docverse_server.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse_server.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.keeper_sync import keeper_sync_reaper
from docverse_server.worker.functions.publish_edition_reaper import (
    publish_edition_reaper,
)
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
    await queue_job_store.start_if_queued(job.id)
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
    await queue_job_store.start_if_queued(job.id)
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


async def _seed_abandoned_row(
    db_session: AsyncSession,
    *,
    org_id: int,
    backend_job_id: str,
    created_minutes_ago: int,
    run_id: int | None = None,
    subject_label: str | None = None,
    kind: JobKind = JobKind.keeper_sync_project,
    backend_queue_name: str | None = None,
) -> int:
    """Create a queued row of ``kind`` with an arq job ID.

    The PRD #538 shape: the row reached arq (so the orphan sweeps, which
    require ``backend_job_id IS NULL``, cannot see it) but never started
    (so the silent sweeps, which require ``in_progress``, cannot either).

    ``backend_queue_name`` defaults to ``None`` — the legacy shape,
    which makes the sweep fall back to probing every pool queue.
    """
    queue_job_store = QueueJobStore(session=db_session, logger=_logger())
    job = await queue_job_store.create(
        kind=kind,
        org_id=org_id,
        keeper_sync_run_id=run_id,
        subject_label=subject_label,
        backend_job_id=backend_job_id,
        backend_queue_name=backend_queue_name,
    )
    row = await db_session.get(SqlQueueJob, job.id)
    assert row is not None
    row.date_created = datetime.now(tz=UTC) - timedelta(
        minutes=created_minutes_ago
    )
    await db_session.flush()
    return job.id


@pytest.mark.asyncio
async def test_reaper_fails_abandoned_tier_cron_row(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A tier-cron row arq lost is reaped and its subject mutex freed.

    Without this sweep the row holds
    ``idx_queue_jobs_keeper_sync_project_subject_active_uq`` forever, so
    ``_enqueue_tier_project_sync`` keeps seeing an active sync for the
    subject and skips every later tier tick.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-ab-tc")
        abandoned_id = await _seed_abandoned_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-tc-lost",
            created_minutes_ago=600,
            subject_label="phalanx",
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(abandoned_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert qj.errors["type"] == "AbandonedQueueJob"
            assert qj.date_completed is not None

            assert not await qj_store.has_active_for_subject(
                org_id=org_id,
                kind=JobKind.keeper_sync_project,
                subject_label="phalanx",
            )


@pytest.mark.asyncio
async def test_reaper_fails_abandoned_run_child_and_finalises_run(
    app: None,
    db_session: AsyncSession,
) -> None:
    """An abandoned child stops blocking its parent run's finalisation.

    A ``queued`` child counts toward the run's ``pending_count``, so an
    arq-lost child parks the run in ``in_progress`` forever. The reaper
    must fail it and roll the parent up in the same tick.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-ab-run")
        run_id = await _seed_run(db_session, org_id=org_id)
        abandoned_id = await _seed_abandoned_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-child-lost",
            created_minutes_ago=600,
            run_id=run_id,
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
            qj = await qj_store.get(abandoned_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert qj.errors["type"] == "AbandonedQueueJob"

            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.partial_failure
            assert run.date_finished is not None


@pytest.mark.asyncio
async def test_run_attributed_publish_row_reaped_only_by_keeper_sync(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A cascaded ``publish_edition`` row has one reaper, not two.

    A keeper-sync project sync enqueues ``publish_edition`` jobs stamped
    with the run's id. Both ``publish_edition_reaper`` and the
    keeper-sync reaper used to match such a row; only the latter rolls
    the parent run up, so when the run-less sweep won, the run stayed
    ``in_progress`` forever and ``has_non_terminal_run`` 409-blocked
    every later run for the org.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-per-run")
        run_id = await _seed_run(db_session, org_id=org_id)
        publish_id = await _seed_abandoned_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-publish-lost",
            # Past the 4 h publish_edition threshold as well as the
            # keeper-sync one, so scoping — not age — decides the owner.
            created_minutes_ago=600,
            run_id=run_id,
            kind=JobKind.publish_edition,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await publish_edition_reaper(ctx)

        async for session in db_session_dependency():
            async with session.begin():
                qj_store = QueueJobStore(session=session, logger=_logger())
                untouched = await qj_store.get(publish_id)
                assert untouched is not None
                assert untouched.status == JobStatus.queued
                assert untouched.errors is None
            break

        await keeper_sync_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            reaped = await qj_store.get(publish_id)
            assert reaped is not None
            assert reaped.status == JobStatus.failed
            assert reaped.errors is not None
            assert reaped.errors["type"] == "AbandonedQueueJob"

            # Last pending child gone, so the run finalises rather than
            # sitting in ``in_progress`` forever.
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.partial_failure
            assert run.date_finished is not None
            assert not await run_store.has_non_terminal_run(org_id=org_id)


async def _seed_abandoned_discovery(
    db_session: AsyncSession,
    *,
    org_id: int,
    run_id: int,
    backend_job_id: str,
    created_minutes_ago: int,
) -> int:
    """Create the run's own queued ``keeper_sync_run_discovery`` row.

    ``KeeperSyncRunService.start_run`` writes this row with the run's id
    and then stamps the arq job ID on it, so a discovery job arq loses
    leaves exactly this shape behind.
    """
    queue_job_store = QueueJobStore(session=db_session, logger=_logger())
    job = await queue_job_store.create(
        kind=JobKind.keeper_sync_run_discovery,
        org_id=org_id,
        keeper_sync_run_id=run_id,
        subject_label="discovery for ks-org",
        backend_job_id=backend_job_id,
    )
    row = await db_session.get(SqlQueueJob, job.id)
    assert row is not None
    row.date_created = datetime.now(tz=UTC) - timedelta(
        minutes=created_minutes_ago
    )
    await db_session.flush()
    return job.id


@pytest.mark.asyncio
async def test_reaper_fails_abandoned_discovery_and_fails_its_run(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A lost discovery row fails its run outright and clears the 409 wedge.

    The run never fanned out, so the reaper must reach the same terminal
    status ``keeper_sync_run_discovery``'s own failure branch writes
    (``failed``) rather than the ``partial_failure`` a child roll-up
    would compute. Until the run is terminal, ``has_non_terminal_run``
    409-blocks every later ``POST .../runs`` for the org.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-ab-disc")
        # ``pending``: a lost discovery never got to flip the run to
        # ``in_progress`` with its first child enqueue.
        run_id = await _seed_run(db_session, org_id=org_id, status="pending")
        discovery_id = await _seed_abandoned_discovery(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="arq-discovery-lost",
            created_minutes_ago=600,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            await keeper_sync_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(discovery_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert qj.errors["type"] == "AbandonedQueueJob"
            assert "run discovery" in qj.errors["message"]

            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.failed
            assert run.date_finished is not None
            # No lingering 409 wedge for the org.
            assert not await run_store.has_non_terminal_run(org_id=org_id)

            public_id = qj.public_id

    warnings = [
        entry
        for entry in captured
        if entry["event"] == "Reaped stuck keeper-sync queue jobs"
    ]
    assert len(warnings) == 1
    assert warnings[0]["run_discovery_abandoned_count"] == 1
    by_public_id = {
        entry["public_id"]: entry for entry in warnings[0]["reaped_jobs"]
    }
    assert by_public_id[public_id] == {
        "public_id": public_id,
        "sweep": "run_discovery_abandoned",
        "backend_job_id": "arq-discovery-lost",
    }


@pytest.mark.asyncio
async def test_reaper_spares_row_whose_job_is_on_the_keeper_sync_queue(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Verification follows the row's own queue, so live work survives.

    Keeper-sync jobs are enqueued onto ``docverse:sync-queue``, not the
    default queue the backend is configured with. arq resolves a queued
    job's status through its own queue, so a lookup aimed anywhere else
    reports "no record" for a perfectly healthy job — and the abandoned
    sweep would reap it. The tier-cron row carries the queue name its
    enqueue recorded (the current shape); the run child leaves it
    ``NULL`` (the legacy shape), so one tick exercises both the direct
    probe and the multi-queue fallback.
    """
    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    live = await ctx["arq_queue"].enqueue(
        "keeper_sync_project", _queue_name=KEEPER_SYNC_QUEUE_NAME
    )

    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-ab-live")
        run_id = await _seed_run(db_session, org_id=org_id)
        tier_cron_id = await _seed_abandoned_row(
            db_session,
            org_id=org_id,
            backend_job_id=live.id,
            backend_queue_name=KEEPER_SYNC_QUEUE_NAME,
            created_minutes_ago=6000,
            subject_label="phalanx",
        )
        child_id = await _seed_abandoned_row(
            db_session,
            org_id=org_id,
            backend_job_id=live.id,
            created_minutes_ago=6000,
            run_id=run_id,
        )

    try:
        await keeper_sync_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            for job_id in (tier_cron_id, child_id):
                qj = await qj_store.get(job_id)
                assert qj is not None
                assert qj.status == JobStatus.queued
                assert qj.errors is None


@pytest.mark.asyncio
async def test_reaper_backend_unreachable_skips_only_abandoned_sweeps(
    app: None,
    db_session: AsyncSession,
) -> None:
    """An unreachable backend costs the tick only its abandoned passes.

    With Redis down the sweep cannot tell a lost job from a live one, so
    it must leave every abandoned candidate alone — while the silent and
    orphan sweeps, which need no backend, still reap that tick.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-ab-down")
        silent_id = await _seed_silent_tier_cron(
            db_session,
            org_id=org_id,
            subject_label="phalanx",
            started_minutes_ago=600,
        )
        orphan_id = await _seed_orphan_tier_cron(
            db_session,
            org_id=org_id,
            subject_label="pipelines",
            created_minutes_ago=10,
        )
        abandoned_id = await _seed_abandoned_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-unverifiable",
            created_minutes_ago=600,
            subject_label="squarebot",
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)

    async def _explode(*args: Any, **kwargs: Any) -> None:
        raise ConnectionError("redis is unreachable")

    ctx["arq_queue"].get_job_metadata = _explode

    try:
        with capture_logs() as captured:
            result = await keeper_sync_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    assert result == "completed"
    assert any(
        entry["event"] == "Queue backend unreachable; skipping abandoned sweep"
        for entry in captured
    )

    warnings = [
        entry
        for entry in captured
        if entry["event"] == "Reaped stuck keeper-sync queue jobs"
    ]
    assert len(warnings) == 1
    assert warnings[0]["tier_cron_silent_count"] == 1
    assert warnings[0]["tier_cron_orphan_count"] == 1
    assert warnings[0]["tier_cron_abandoned_count"] == 0

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            for job_id in (silent_id, orphan_id):
                reaped = await qj_store.get(job_id)
                assert reaped is not None
                assert reaped.status == JobStatus.failed
            spared = await qj_store.get(abandoned_id)
            assert spared is not None
            assert spared.status == JobStatus.queued


@pytest.mark.asyncio
async def test_reaper_backend_stall_keeps_silent_and_orphan_reaps(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A stalled backend costs the tick only its abandoned passes.

    The failure mode task #548 fixes: a hung Redis pushed the tick past
    arq's job timeout while the silent and orphan sweeps' updates sat
    uncommitted in the same transaction. ``CancelledError`` is a
    ``BaseException``, so the sweep's ``except Exception`` soft-abort
    never saw it — the transaction unwound and every reap that tick was
    lost, tick after tick, with row locks held throughout.

    Modelled deterministically by cancelling the reaper only once the
    backend call is actually in flight.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-ab-stall")
        silent_id = await _seed_silent_tier_cron(
            db_session,
            org_id=org_id,
            subject_label="phalanx",
            started_minutes_ago=600,
        )
        orphan_id = await _seed_orphan_tier_cron(
            db_session,
            org_id=org_id,
            subject_label="pipelines",
            created_minutes_ago=10,
        )
        abandoned_id = await _seed_abandoned_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-stalled",
            created_minutes_ago=600,
            subject_label="squarebot",
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    reached_backend = asyncio.Event()

    async def _hang(*args: Any, **kwargs: Any) -> None:
        reached_backend.set()
        await asyncio.Event().wait()

    ctx["arq_queue"].get_job_metadata = _hang

    try:
        task = asyncio.create_task(keeper_sync_reaper(ctx))
        await asyncio.wait_for(reached_backend.wait(), timeout=30)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            for job_id in (silent_id, orphan_id):
                reaped = await qj_store.get(job_id)
                assert reaped is not None
                assert reaped.status == JobStatus.failed
            spared = await qj_store.get(abandoned_id)
            assert spared is not None
            assert spared.status == JobStatus.queued


@pytest.mark.asyncio
async def test_reaper_logs_backend_job_id_and_matched_sweep(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Reaped-row log context names the sweep and the lost arq job ID.

    PRD #538's observability requirement: a postmortem must be able to
    tell which sweep claimed a row, and cross-reference the arq job that
    went missing, without querying the database.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-reaper-ab-log")
        orphan_id = await _seed_orphan_tier_cron(
            db_session,
            org_id=org_id,
            subject_label="pipelines",
            created_minutes_ago=10,
        )
        abandoned_id = await _seed_abandoned_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-tc-log-lost",
            created_minutes_ago=600,
            subject_label="phalanx",
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            await keeper_sync_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    warnings = [
        entry
        for entry in captured
        if entry["event"] == "Reaped stuck keeper-sync queue jobs"
    ]
    assert len(warnings) == 1

    public_ids: dict[str, int] = {}
    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            for label, job_id in (
                ("orphan", orphan_id),
                ("abandoned", abandoned_id),
            ):
                qj = await qj_store.get(job_id)
                assert qj is not None
                public_ids[label] = qj.public_id

    by_public_id = {
        entry["public_id"]: entry for entry in warnings[0]["reaped_jobs"]
    }
    assert by_public_id[public_ids["orphan"]] == {
        "public_id": public_ids["orphan"],
        "sweep": "tier_cron_orphan",
        "backend_job_id": None,
    }
    assert by_public_id[public_ids["abandoned"]] == {
        "public_id": public_ids["abandoned"],
        "sweep": "tier_cron_abandoned",
        "backend_job_id": "arq-tc-log-lost",
    }


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
