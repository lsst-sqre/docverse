"""Database operations for the ``keeper_sync_runs`` table.

The store is the single read/write path for run rows. Aggregate
counters are derived from ``queue_jobs`` filtered on
``keeper_sync_run_id`` rather than denormalised onto the run row, so
many child jobs finishing in a burst cannot contend on a single
counter row.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from safir.database import CountedPaginatedList, CountedPaginatedQueryRunner
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import KeeperSyncRunKind, KeeperSyncRunStatus
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.keeper_sync_run import (
    KeeperSyncRun,
    KeeperSyncRunCounters,
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
            msg = (
                f"Cannot transition keeper sync run {run_id} from "
                f"{current.value!r} to {new_status.value!r}"
            )
            raise InvalidJobStateError(msg)

        row.status = new_status.value
        if new_status not in _NON_TERMINAL_STATUSES:
            row.date_finished = datetime.now(tz=UTC)
        await self._session.flush()
        await self._session.refresh(row)
        return KeeperSyncRun.model_validate(row)

    async def aggregate_counters(
        self, *, run_id: int
    ) -> KeeperSyncRunCounters:
        """Aggregate ``queue_jobs`` rows attributed to this run.

        Computes counters with one ``GROUP BY status`` query — one DB
        round-trip even for a thousand-child fan-out. Buckets queued /
        in_progress as ``pending_count``; ``completed`` as
        ``succeeded_count``; everything else (failed, cancelled,
        completed_with_errors) as ``failed_count``.
        """
        stmt = (
            select(SqlQueueJob.status, func.count(SqlQueueJob.id))
            .where(SqlQueueJob.keeper_sync_run_id == run_id)
            .group_by(SqlQueueJob.status)
        )
        result = await self._session.execute(stmt)
        pending = succeeded = failed = total = 0
        for status_value, count in result.all():
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
        return KeeperSyncRunCounters(
            pending_count=pending,
            succeeded_count=succeeded,
            failed_count=failed,
            total_count=total,
        )

    async def _get_row(self, run_id: int) -> SqlKeeperSyncRun:
        result = await self._session.execute(
            select(SqlKeeperSyncRun).where(SqlKeeperSyncRun.id == run_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            msg = f"Keeper sync run {run_id} not found"
            raise InvalidJobStateError(msg)
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
