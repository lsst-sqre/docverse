"""Database operations for the ``lifecycle_eval_runs`` table.

The store is the single read/write path for ``lifecycle_eval`` run
rows. Aggregate counters are derived from ``queue_jobs`` filtered on
``lifecycle_eval_run_id`` rather than denormalised onto the run row,
mirroring ``KeeperSyncRunStore`` so the dispatcher / per-org / reaper
pattern transfers between subsystems.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import LifecycleEvalRunStatus
from docverse.dbschema.lifecycle_eval_run import SqlLifecycleEvalRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.lifecycle_eval_run import (
    LifecycleEvalRun,
    LifecycleEvalRunActivity,
)
from docverse.domain.queue import JobStatus
from docverse.exceptions import InvalidJobStateError

__all__ = ["LifecycleEvalRunStore"]


_NON_TERMINAL_STATUSES: frozenset[LifecycleEvalRunStatus] = frozenset(
    {
        LifecycleEvalRunStatus.pending,
        LifecycleEvalRunStatus.in_progress,
    }
)


class LifecycleEvalRunStore:
    """Direct database operations for lifecycle_eval runs."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(self) -> LifecycleEvalRun:
        """Insert a new run row in ``pending`` status.

        The DB-side partial unique index
        ``idx_lifecycle_eval_runs_non_terminal_uq`` enforces the
        singleton-non-terminal-run invariant globally (not per-org).
        Two dispatcher cron firings racing into ``create`` surface one
        ``IntegrityError``; the caller is expected to translate that
        into a "skip this tick" decision so a slow tick is never
        doubled up.
        """
        row = SqlLifecycleEvalRun(
            status=LifecycleEvalRunStatus.pending.value,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return LifecycleEvalRun.model_validate(row)

    async def get(self, run_id: int) -> LifecycleEvalRun | None:
        """Fetch a run by primary key."""
        result = await self._session.execute(
            select(SqlLifecycleEvalRun).where(SqlLifecycleEvalRun.id == run_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return LifecycleEvalRun.model_validate(row)

    async def has_non_terminal_run(self) -> bool:
        """Return True if a pending or in-progress run exists anywhere.

        Lifecycle_eval is a system-wide tick, so this check is global
        (not parameterised by org). It backstops the dispatcher's
        pre-check before ``create`` so two ticks racing through the
        cron handler surface as a clean skip rather than an
        ``IntegrityError``.
        """
        stmt = select(SqlLifecycleEvalRun.id).where(
            SqlLifecycleEvalRun.status.in_(
                [s.value for s in _NON_TERMINAL_STATUSES]
            ),
        )
        result = await self._session.execute(stmt)
        return result.first() is not None

    async def transition_status(
        self, *, run_id: int, new_status: LifecycleEvalRunStatus
    ) -> LifecycleEvalRun:
        """Update the status column, setting ``date_finished`` on terminal.

        Idempotent: re-applying the same status returns the row
        unchanged. Otherwise the transition is enforced as a forward
        progression through ``pending → in_progress → terminal``;
        backwards moves raise ``InvalidJobStateError``.
        """
        row = await self._get_row(run_id)
        current = LifecycleEvalRunStatus(row.status)
        if current is new_status:
            return LifecycleEvalRun.model_validate(row)
        if not _is_allowed_transition(current, new_status):
            msg = (
                f"Cannot transition lifecycle eval run {run_id} from "
                f"{current.value!r} to {new_status.value!r}"
            )
            raise InvalidJobStateError(msg)

        row.status = new_status.value
        if new_status not in _NON_TERMINAL_STATUSES:
            row.date_finished = datetime.now(tz=UTC)
        await self._session.flush()
        await self._session.refresh(row)
        return LifecycleEvalRun.model_validate(row)

    async def set_summary(
        self, *, run_id: int, summary: dict[str, Any]
    ) -> LifecycleEvalRun:
        """Replace the run row's JSONB ``summary`` column.

        Called by the dispatcher to record per-tick metadata
        (``orgs_enqueued``, ``orgs_skipped``) in the same transaction
        as the run-row insert, so the counts represent the dispatcher's
        intent and are captured atomically with the run — the summary
        is never missing, even if the per-child fan-out dies later.
        Distinct from ``transition_status`` so the summary write does
        not couple the status state machine to summary writes.
        """
        row = await self._get_row(run_id)
        row.summary = summary
        await self._session.flush()
        await self._session.refresh(row)
        return LifecycleEvalRun.model_validate(row)

    async def aggregate_activity(
        self, *, run_id: int
    ) -> LifecycleEvalRunActivity:
        """Aggregate ``queue_jobs`` rows attributed to this run.

        Same shape as ``KeeperSyncRunStore.aggregate_activity``: one
        ``GROUP BY status`` query returns the four counters plus a
        ``date_last_activity`` derived from ``MAX(coalesce(
        date_completed, date_started, date_created))`` per row.
        Buckets ``queued`` / ``in_progress`` as ``pending_count``,
        ``completed`` as ``succeeded_count``, and everything else
        (``failed`` / ``cancelled`` / ``completed_with_errors``) as
        ``failed_count``.
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
            .where(SqlQueueJob.lifecycle_eval_run_id == run_id)
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
        return LifecycleEvalRunActivity(
            pending_count=pending,
            succeeded_count=succeeded,
            failed_count=failed,
            total_count=total,
            date_last_activity=date_last_activity,
        )

    async def _get_row(self, run_id: int) -> SqlLifecycleEvalRun:
        result = await self._session.execute(
            select(SqlLifecycleEvalRun).where(SqlLifecycleEvalRun.id == run_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            msg = f"Lifecycle eval run {run_id} not found"
            raise InvalidJobStateError(msg)
        return row


_ALLOWED_TRANSITIONS: dict[
    LifecycleEvalRunStatus, frozenset[LifecycleEvalRunStatus]
] = {
    LifecycleEvalRunStatus.pending: frozenset(
        {
            LifecycleEvalRunStatus.in_progress,
            LifecycleEvalRunStatus.succeeded,
            LifecycleEvalRunStatus.partial_failure,
            LifecycleEvalRunStatus.failed,
        }
    ),
    LifecycleEvalRunStatus.in_progress: frozenset(
        {
            LifecycleEvalRunStatus.succeeded,
            LifecycleEvalRunStatus.partial_failure,
            LifecycleEvalRunStatus.failed,
        }
    ),
}


def _is_allowed_transition(
    current: LifecycleEvalRunStatus, new: LifecycleEvalRunStatus
) -> bool:
    return new in _ALLOWED_TRANSITIONS.get(current, frozenset())
