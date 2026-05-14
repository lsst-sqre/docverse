"""Database operations for the ``keeper_sync_runs`` table.

The store is the single read/write path for run rows. Aggregate
counters are derived from ``queue_jobs`` filtered on
``keeper_sync_run_id`` rather than denormalised onto the run row, so
many child jobs finishing in a burst cannot contend on a single
counter row.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import structlog
from safir.database import CountedPaginatedList, CountedPaginatedQueryRunner
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import KeeperSyncRunKind, KeeperSyncRunStatus
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.keeper_sync_run import (
    KeeperSyncRun,
    KeeperSyncRunActivity,
)
from docverse.domain.queue import JobStatus
from docverse.exceptions import InvalidJobStateError
from docverse.storage.pagination import KeeperSyncRunDateStartedCursor

__all__ = ["KeeperSyncRunStore"]


_NON_TERMINAL_STATUSES: frozenset[KeeperSyncRunStatus] = frozenset(
    {KeeperSyncRunStatus.pending, KeeperSyncRunStatus.in_progress}
)


class KeeperSyncRunStore:
    """Direct database operations for keeper-sync runs."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(
        self,
        *,
        org_id: int,
        kind: KeeperSyncRunKind = KeeperSyncRunKind.backfill,
    ) -> KeeperSyncRun:
        """Insert a new run row in ``pending`` status.

        The DB-side partial unique index on ``(org_id) WHERE status IN
        ('pending', 'in_progress')`` enforces the
        one-non-terminal-run-per-org invariant. The caller is expected
        to translate the resulting ``IntegrityError`` into a 409 — the
        store itself stays low-level and lets the constraint speak.
        """
        row = SqlKeeperSyncRun(
            org_id=org_id,
            kind=kind.value,
            status=KeeperSyncRunStatus.pending.value,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return KeeperSyncRun.model_validate(row)

    async def get(self, run_id: int) -> KeeperSyncRun | None:
        """Fetch a run by primary key."""
        result = await self._session.execute(
            select(SqlKeeperSyncRun).where(SqlKeeperSyncRun.id == run_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return KeeperSyncRun.model_validate(row)

    async def list_by_org(
        self,
        *,
        org_id: int,
        status: KeeperSyncRunStatus | None = None,
        cursor: KeeperSyncRunDateStartedCursor | None = None,
        limit: int,
    ) -> CountedPaginatedList[KeeperSyncRun, KeeperSyncRunDateStartedCursor]:
        """List runs for an org, newest first, with optional status filter."""
        stmt = select(SqlKeeperSyncRun).where(
            SqlKeeperSyncRun.org_id == org_id
        )
        if status is not None:
            stmt = stmt.where(SqlKeeperSyncRun.status == status.value)
        runner = CountedPaginatedQueryRunner(
            entry_type=KeeperSyncRun,
            cursor_type=KeeperSyncRunDateStartedCursor,
        )
        return await runner.query_object(
            self._session, stmt, cursor=cursor, limit=limit
        )

    async def has_non_terminal_run(self, *, org_id: int) -> bool:
        """Return True if the org already has a pending/in-progress run."""
        stmt = select(SqlKeeperSyncRun.id).where(
            SqlKeeperSyncRun.org_id == org_id,
            SqlKeeperSyncRun.status.in_(
                [s.value for s in _NON_TERMINAL_STATUSES]
            ),
        )
        result = await self._session.execute(stmt)
        return result.first() is not None

    async def transition_status(
        self, *, run_id: int, new_status: KeeperSyncRunStatus
    ) -> KeeperSyncRun:
        """Update the status column, setting ``date_finished`` on terminal.

        Idempotent: re-applying the same status returns the row
        unchanged. Otherwise the transition is enforced as a forward
        progression through ``pending → in_progress → terminal``;
        backwards moves raise ``InvalidJobStateError``.
        """
        row = await self._get_row(run_id)
        current = KeeperSyncRunStatus(row.status)
        if current is new_status:
            return KeeperSyncRun.model_validate(row)
        if not _is_allowed_transition(current, new_status):
            raise InvalidJobStateError(
                current_state=current.value,
                target_state=new_status.value,
                job_function="KeeperSyncRunStore.transition_status",
                message=(
                    f"Cannot transition keeper sync run {run_id} from "
                    f"{current.value!r} to {new_status.value!r}"
                ),
            )

        row.status = new_status.value
        if new_status not in _NON_TERMINAL_STATUSES:
            row.date_finished = datetime.now(tz=UTC)
        await self._session.flush()
        await self._session.refresh(row)
        return KeeperSyncRun.model_validate(row)

    async def aggregate_activity(
        self, *, run_id: int
    ) -> KeeperSyncRunActivity:
        """Aggregate ``queue_jobs`` rows attributed to this run.

        Computes counters and ``date_last_activity`` with one
        ``GROUP BY status`` query — one DB round-trip even for a
        thousand-child fan-out. Buckets queued / in_progress as
        ``pending_count``; ``completed`` as ``succeeded_count``;
        everything else (failed, cancelled, completed_with_errors) as
        ``failed_count``. ``date_last_activity`` is the MAX over
        ``coalesce(date_completed, date_started, date_created)`` per
        row — the latest meaningful event for each child without
        needing a separate ``date_updated`` column.
        """
        last_activity = func.coalesce(
            SqlQueueJob.date_completed,
            SqlQueueJob.date_started,
            SqlQueueJob.date_created,
        )
        stmt = (
            select(
                SqlQueueJob.status,
                func.count(SqlQueueJob.id),
                func.max(last_activity),
            )
            .where(SqlQueueJob.keeper_sync_run_id == run_id)
            .group_by(SqlQueueJob.status)
        )
        result = await self._session.execute(stmt)
        pending = succeeded = failed = total = 0
        date_last_activity: datetime | None = None
        for status_value, count, max_ts in result.all():
            total += count
            if status_value in (
                JobStatus.queued.value,
                JobStatus.in_progress.value,
            ):
                pending += count
            elif status_value == JobStatus.completed.value:
                succeeded += count
            else:
                failed += count
            if max_ts is not None and (
                date_last_activity is None or max_ts > date_last_activity
            ):
                date_last_activity = max_ts
        return KeeperSyncRunActivity(
            pending_count=pending,
            succeeded_count=succeeded,
            failed_count=failed,
            total_count=total,
            date_last_activity=date_last_activity,
        )

    async def aggregate_activity_for_runs(
        self, *, run_ids: Sequence[int]
    ) -> dict[int, KeeperSyncRunActivity]:
        """Aggregate ``queue_jobs`` activity for many runs in a single query.

        One ``GROUP BY keeper_sync_run_id, status`` instead of N
        per-run queries — keeps ``GET /runs`` at O(1) round-trips
        even at ``MAX_PAGE_LIMIT``. Runs in ``run_ids`` with no
        attributed jobs come back with zeroed counters and
        ``date_last_activity=None`` so callers never have to handle
        missing keys.
        """
        if not run_ids:
            return {}
        zero = KeeperSyncRunActivity(
            pending_count=0,
            succeeded_count=0,
            failed_count=0,
            total_count=0,
            date_last_activity=None,
        )
        result: dict[int, dict[str, Any]] = {
            run_id: {
                "pending": 0,
                "succeeded": 0,
                "failed": 0,
                "total": 0,
                "date_last_activity": None,
            }
            for run_id in run_ids
        }
        last_activity = func.coalesce(
            SqlQueueJob.date_completed,
            SqlQueueJob.date_started,
            SqlQueueJob.date_created,
        )
        stmt = (
            select(
                SqlQueueJob.keeper_sync_run_id,
                SqlQueueJob.status,
                func.count(SqlQueueJob.id),
                func.max(last_activity),
            )
            .where(SqlQueueJob.keeper_sync_run_id.in_(list(run_ids)))
            .group_by(SqlQueueJob.keeper_sync_run_id, SqlQueueJob.status)
        )
        rows = await self._session.execute(stmt)
        for run_id, status_value, count, max_ts in rows.all():
            bucket = result[run_id]
            bucket["total"] += count
            if status_value in (
                JobStatus.queued.value,
                JobStatus.in_progress.value,
            ):
                bucket["pending"] += count
            elif status_value == JobStatus.completed.value:
                bucket["succeeded"] += count
            else:
                bucket["failed"] += count
            current = bucket["date_last_activity"]
            if max_ts is not None and (current is None or max_ts > current):
                bucket["date_last_activity"] = max_ts
        return {
            run_id: KeeperSyncRunActivity(
                pending_count=b["pending"],
                succeeded_count=b["succeeded"],
                failed_count=b["failed"],
                total_count=b["total"],
                date_last_activity=b["date_last_activity"],
            )
            if b["total"]
            else zero
            for run_id, b in result.items()
        }

    async def _get_row(self, run_id: int) -> SqlKeeperSyncRun:
        result = await self._session.execute(
            select(SqlKeeperSyncRun).where(SqlKeeperSyncRun.id == run_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise InvalidJobStateError(
                job_function="KeeperSyncRunStore._get_row",
                message=f"Keeper sync run {run_id} not found",
            )
        return row


_ALLOWED_TRANSITIONS: dict[
    KeeperSyncRunStatus, frozenset[KeeperSyncRunStatus]
] = {
    KeeperSyncRunStatus.pending: frozenset(
        {
            KeeperSyncRunStatus.in_progress,
            KeeperSyncRunStatus.succeeded,
            KeeperSyncRunStatus.partial_failure,
            KeeperSyncRunStatus.failed,
        }
    ),
    KeeperSyncRunStatus.in_progress: frozenset(
        {
            KeeperSyncRunStatus.succeeded,
            KeeperSyncRunStatus.partial_failure,
            KeeperSyncRunStatus.failed,
        }
    ),
}


def _is_allowed_transition(
    current: KeeperSyncRunStatus, new: KeeperSyncRunStatus
) -> bool:
    return new in _ALLOWED_TRANSITIONS.get(current, frozenset())
