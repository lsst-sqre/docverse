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
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    JobKind,
    KeeperSyncRunStatus,
    OrganizationCreate,
)
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.queue import JobStatus
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
    row = SqlKeeperSyncRun(org_id=org_id, kind="backfill", status=status)
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
