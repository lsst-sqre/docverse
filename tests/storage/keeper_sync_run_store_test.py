"""Tests for ``KeeperSyncRunStore``."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import KeeperSyncRunStatus, OrganizationCreate
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.exceptions import JobNotFoundError
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
    date_created: datetime | None = None,
    date_started: datetime | None = None,
    date_completed: datetime | None = None,
) -> None:
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        kind=JobKind.keeper_sync_project.value,
        status=status.value,
        org_id=org_id,
        keeper_sync_run_id=run_id,
        date_started=date_started,
        date_completed=date_completed,
    )
    if date_created is not None:
        row.date_created = date_created
    db_session.add(row)


@pytest.mark.asyncio
async def test_aggregate_activity_for_runs_groups_by_run_id(
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
        activity = await store.aggregate_activity_for_runs(
            run_ids=[run_a.id, run_b.id]
        )

    assert set(activity.keys()) == {run_a.id, run_b.id}
    assert activity[run_a.id].pending_count == 0
    assert activity[run_a.id].succeeded_count == 2
    assert activity[run_a.id].failed_count == 1
    assert activity[run_a.id].total_count == 3
    assert activity[run_b.id].pending_count == 2
    assert activity[run_b.id].succeeded_count == 0
    assert activity[run_b.id].failed_count == 0
    assert activity[run_b.id].total_count == 2


@pytest.mark.asyncio
async def test_aggregate_activity_for_runs_zero_for_runs_without_jobs(
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
        activity = await store.aggregate_activity_for_runs(run_ids=[run.id])

    assert run.id in activity
    assert activity[run.id].pending_count == 0
    assert activity[run.id].succeeded_count == 0
    assert activity[run.id].failed_count == 0
    assert activity[run.id].total_count == 0
    assert activity[run.id].date_last_activity is None


@pytest.mark.asyncio
async def test_aggregate_activity_for_runs_empty_input(
    db_session: AsyncSession,
) -> None:
    """An empty ``run_ids`` list short-circuits to an empty mapping."""
    async with db_session.begin():
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        activity = await store.aggregate_activity_for_runs(run_ids=[])
    assert activity == {}


@pytest.mark.asyncio
async def test_aggregate_activity_picks_max_coalesced_timestamp(
    db_session: AsyncSession,
) -> None:
    """``date_last_activity`` is the MAX of coalesce on each child row.

    A mixed-status run exercises every branch of the coalesce: a
    completed row contributes its ``date_completed``, a started row
    contributes its ``date_started``, and a queued row contributes its
    ``date_created``. The aggregate picks whichever of those is most
    recent.
    """
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        run = await store.create(org_id=org_id)
        # Queued: only date_created. Earliest of the three by design.
        _seed_queue_job(
            db_session,
            org_id=org_id,
            run_id=run.id,
            status=JobStatus.queued,
            date_created=base,
        )
        # In-progress: date_started fires (more recent than created).
        _seed_queue_job(
            db_session,
            org_id=org_id,
            run_id=run.id,
            status=JobStatus.in_progress,
            date_created=base,
            date_started=base + timedelta(minutes=10),
        )
        # Completed: date_completed fires; this is the latest event.
        latest = base + timedelta(minutes=30)
        _seed_queue_job(
            db_session,
            org_id=org_id,
            run_id=run.id,
            status=JobStatus.completed,
            date_created=base,
            date_started=base + timedelta(minutes=5),
            date_completed=latest,
        )
        await db_session.commit()

    async with db_session.begin():
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        single = await store.aggregate_activity(run_id=run.id)
        batched = await store.aggregate_activity_for_runs(run_ids=[run.id])

    assert single.date_last_activity == latest
    assert batched[run.id].date_last_activity == latest


@pytest.mark.asyncio
async def test_aggregate_activity_null_when_no_jobs(
    db_session: AsyncSession,
) -> None:
    """``date_last_activity`` is None on a run with no attributed jobs."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        run = await store.create(org_id=org_id)
        await db_session.commit()

    async with db_session.begin():
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        activity = await store.aggregate_activity(run_id=run.id)

    assert activity.total_count == 0
    assert activity.date_last_activity is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry_point",
    ["get_row", "transition_status"],
)
async def test_missing_run_raises_job_not_found(
    db_session: AsyncSession,
    entry_point: str,
) -> None:
    """``_get_row`` and methods that delegate to it raise ``JobNotFoundError``.

    Aligns the store with ``QueueJobStore._get_row``'s shape: a missing
    row is a lookup miss, not an invalid state transition.
    """

    async def _call(store: KeeperSyncRunStore) -> None:
        if entry_point == "get_row":
            await store._get_row(99999)
        else:
            await store.transition_status(
                run_id=99999,
                new_status=KeeperSyncRunStatus.in_progress,
            )

    async with db_session.begin():
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        with pytest.raises(JobNotFoundError):
            await _call(store)


@pytest.mark.asyncio
async def test_create_mints_public_id(
    db_session: AsyncSession,
) -> None:
    """A freshly-created run carries a positive time-ordered ``public_id``."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        run = await store.create(org_id=org_id)
        await db_session.commit()

    async with db_session.begin():
        public_id = await db_session.scalar(
            select(SqlKeeperSyncRun.public_id).where(
                SqlKeeperSyncRun.id == run.id
            )
        )
    assert public_id is not None
    assert public_id > 0


@pytest.mark.asyncio
async def test_public_ids_sort_in_creation_order(
    db_session: AsyncSession,
) -> None:
    """Runs created in succession sort by ``public_id`` in creation order.

    The partial unique index permits only one non-terminal run per org, so
    the first run is driven to a terminal status before the second is
    created. A short real-time gap guarantees the two mints land in distinct
    milliseconds so the time-ordered high bits establish the ordering.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        first = await store.create(org_id=org_id)
        await store.transition_status(
            run_id=first.id, new_status=KeeperSyncRunStatus.succeeded
        )
        await asyncio.sleep(0.005)
        second = await store.create(org_id=org_id)
        await db_session.commit()

    async with db_session.begin():
        first_public_id = await db_session.scalar(
            select(SqlKeeperSyncRun.public_id).where(
                SqlKeeperSyncRun.id == first.id
            )
        )
        second_public_id = await db_session.scalar(
            select(SqlKeeperSyncRun.public_id).where(
                SqlKeeperSyncRun.id == second.id
            )
        )
    assert first_public_id is not None
    assert second_public_id is not None
    assert second_public_id > first_public_id


@pytest.mark.asyncio
async def test_create_retries_on_public_id_collision(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A colliding ``public_id`` is re-minted with no error and no merged rows.

    The pre-existing row occupying ``collision_id`` is inserted in a terminal
    status so it does not trip the one-non-terminal-run-per-org partial unique
    index, isolating the collision to ``public_id``.
    """
    collision_id = 525252
    fresh_id = 777777

    def _fake_ids() -> Iterator[int]:
        yield from (collision_id, fresh_id)

    ids = _fake_ids()
    monkeypatch.setattr(
        "docverse.storage._public_id.generate_resource_id",
        lambda: next(ids),
    )

    async with db_session.begin():
        org_id = await _seed_org(db_session)
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        # Pre-insert a terminal run occupying ``collision_id`` and flush it
        # into the outer transaction so the retried insert races a persistent
        # row.
        existing = SqlKeeperSyncRun(
            public_id=collision_id,
            org_id=org_id,
            kind="backfill",
            status=KeeperSyncRunStatus.succeeded.value,
        )
        db_session.add(existing)
        await db_session.flush()

        created = await store.create(org_id=org_id)
        await db_session.commit()

    async with db_session.begin():
        created_public_id = await db_session.scalar(
            select(SqlKeeperSyncRun.public_id).where(
                SqlKeeperSyncRun.id == created.id
            )
        )
        total = await db_session.scalar(
            select(func.count()).select_from(SqlKeeperSyncRun)
        )
    # The retry minted the fresh id, leaving the pre-existing row untouched.
    assert created_public_id == fresh_id
    assert total == 2


@pytest.mark.asyncio
async def test_get_by_public_id_round_trips(
    db_session: AsyncSession,
) -> None:
    """``get_by_public_id`` resolves the same row ``create`` minted.

    Locks the public-id addressing path the run endpoints rely on: the
    domain object round-trips its ``public_id`` and a miss returns
    ``None`` rather than raising.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        created = await store.create(org_id=org_id)
        await db_session.commit()

    async with db_session.begin():
        store = KeeperSyncRunStore(session=db_session, logger=_logger())
        fetched = await store.get_by_public_id(created.public_id)
        missing = await store.get_by_public_id(created.public_id + 1)

    assert fetched is not None
    assert fetched.id == created.id
    assert fetched.public_id == created.public_id
    assert missing is None
