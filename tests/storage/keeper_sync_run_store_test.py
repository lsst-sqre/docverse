"""Tests for ``KeeperSyncRunStore``."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import KeeperSyncRunStatus, OrganizationCreate
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse.storage.organization_store import OrganizationStore


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(db_session: AsyncSession) -> int:
    org_store = OrganizationStore(session=db_session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug="ksrs-org",
            title="KSRS Org",
            base_domain="ksrs.example.com",
        )
    )
    return org.id


def _seed_queue_job(
    db_session: AsyncSession,
    *,
    org_id: int,
    run_id: int,
    status: JobStatus,
) -> None:
    db_session.add(
        SqlQueueJob(
            public_id=validate_base32_id(generate_base32_id()),
            kind=JobKind.keeper_sync_project.value,
            status=status.value,
            org_id=org_id,
            keeper_sync_run_id=run_id,
        )
    )


@pytest.mark.asyncio
async def test_aggregate_counters_for_runs_groups_by_run_id(
    db_session: AsyncSession,
) -> None:
    """One ``GROUP BY`` query returns per-run counters for each ``run_id``."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        run_a = await store.create(org_id=org_id)
        # The partial unique index allows only one non-terminal run per
        # org, so transition the first to terminal before seeding the
        # second.
        await store.transition_status(
            run_id=run_a.id, new_status=KeeperSyncRunStatus.in_progress
        )
        await store.transition_status(
            run_id=run_a.id, new_status=KeeperSyncRunStatus.succeeded
        )
        run_b = await store.create(org_id=org_id)

        # Run A: 2 completed, 1 failed.
        for _ in range(2):
            _seed_queue_job(
                db_session,
                org_id=org_id,
                run_id=run_a.id,
                status=JobStatus.completed,
            )
        _seed_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_a.id,
            status=JobStatus.failed,
        )
        # Run B: 1 queued, 1 in_progress.
        _seed_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_b.id,
            status=JobStatus.queued,
        )
        _seed_queue_job(
            db_session,
            org_id=org_id,
            run_id=run_b.id,
            status=JobStatus.in_progress,
        )
        await db_session.commit()

    async with db_session.begin():
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        counters = await store.aggregate_counters_for_runs(
            run_ids=[run_a.id, run_b.id]
        )

    assert set(counters.keys()) == {run_a.id, run_b.id}
    assert counters[run_a.id].pending_count == 0
    assert counters[run_a.id].succeeded_count == 2
    assert counters[run_a.id].failed_count == 1
    assert counters[run_a.id].total_count == 3
    assert counters[run_b.id].pending_count == 2
    assert counters[run_b.id].succeeded_count == 0
    assert counters[run_b.id].failed_count == 0
    assert counters[run_b.id].total_count == 2


@pytest.mark.asyncio
async def test_aggregate_counters_for_runs_zero_for_runs_without_jobs(
    db_session: AsyncSession,
) -> None:
    """Runs with no jobs return zeroed counters, not missing keys."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        run = await store.create(org_id=org_id)
        await db_session.commit()

    async with db_session.begin():
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        counters = await store.aggregate_counters_for_runs(run_ids=[run.id])

    assert run.id in counters
    assert counters[run.id].pending_count == 0
    assert counters[run.id].succeeded_count == 0
    assert counters[run.id].failed_count == 0
    assert counters[run.id].total_count == 0


@pytest.mark.asyncio
async def test_aggregate_counters_for_runs_empty_input(
    db_session: AsyncSession,
) -> None:
    """An empty ``run_ids`` list short-circuits to an empty mapping."""
    async with db_session.begin():
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        counters = await store.aggregate_counters_for_runs(run_ids=[])
    assert counters == {}
