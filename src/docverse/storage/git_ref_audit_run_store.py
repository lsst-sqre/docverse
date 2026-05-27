"""Database operations for the ``git_ref_audit_runs`` table.

The store is the single read/write path for ``git_ref_audit`` run
rows. Aggregate counters are derived from ``queue_jobs`` filtered on
``git_ref_audit_run_id`` rather than denormalised onto the run row,
mirroring :class:`LifecycleEvalRunStore` so the discovery / per-org /
reaper pattern transfers between subsystems.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import GitRefAuditRunStatus
from docverse.dbschema.git_ref_audit_run import SqlGitRefAuditRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.git_ref_audit_run import (
    GitRefAuditRun,
    GitRefAuditRunActivity,
)
from docverse.domain.queue import JobStatus
from docverse.exceptions import InvalidJobStateError, JobNotFoundError

__all__ = ["GitRefAuditRunStore"]


_NON_TERMINAL_STATUSES: frozenset[GitRefAuditRunStatus] = frozenset(
    {
        GitRefAuditRunStatus.pending,
        GitRefAuditRunStatus.in_progress,
    }
)


class GitRefAuditRunStore:
    """Direct database operations for git_ref_audit runs."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(self) -> GitRefAuditRun:
        """Insert a new run row in ``pending`` status.

        The DB-side partial unique index
        ``idx_git_ref_audit_runs_non_terminal_uq`` enforces the
        singleton-non-terminal-run invariant globally (not per-org).
        Two discovery cron firings racing into ``create`` surface one
        ``IntegrityError``; the caller is expected to translate that
        into a "skip this tick" decision so a slow tick is never
        doubled up.
        """
        row = SqlGitRefAuditRun(
            status=GitRefAuditRunStatus.pending.value,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return GitRefAuditRun.model_validate(row)

    async def get(self, run_id: int) -> GitRefAuditRun | None:
        """Fetch a run by primary key."""
        result = await self._session.execute(
            select(SqlGitRefAuditRun).where(SqlGitRefAuditRun.id == run_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return GitRefAuditRun.model_validate(row)

    async def has_non_terminal_run(self) -> bool:
        """Return True if a pending or in-progress run exists anywhere.

        ``git_ref_audit`` is a system-wide tick, so this check is
        global (not parameterised by org). It backstops the discovery
        dispatcher's pre-check before ``create`` so two ticks racing
        through the cron handler surface as a clean skip rather than
        an ``IntegrityError``.
        """
        stmt = select(SqlGitRefAuditRun.id).where(
            SqlGitRefAuditRun.status.in_(
                [s.value for s in _NON_TERMINAL_STATUSES]
            ),
        )
        result = await self._session.execute(stmt)
        return result.first() is not None

    async def transition_status(
        self, *, run_id: int, new_status: GitRefAuditRunStatus
    ) -> GitRefAuditRun:
        """Update the status column, setting ``date_finished`` on terminal.

        Idempotent: re-applying the same status returns the row
        unchanged. Otherwise the transition is enforced as a forward
        progression through ``pending → in_progress → terminal``;
        backwards moves raise ``InvalidJobStateError``.
        """
        row = await self._get_row(run_id)
        current = GitRefAuditRunStatus(row.status)
        if current is new_status:
            return GitRefAuditRun.model_validate(row)
        if not _is_allowed_transition(current, new_status):
            raise InvalidJobStateError(
                current_state=current.value,
                target_state=new_status.value,
                job_function="GitRefAuditRunStore.transition_status",
                message=(
                    f"Cannot transition git_ref_audit run {run_id} from "
                    f"{current.value!r} to {new_status.value!r}"
                ),
            )

        row.status = new_status.value
        if new_status not in _NON_TERMINAL_STATUSES:
            row.date_finished = datetime.now(tz=UTC)
        await self._session.flush()
        await self._session.refresh(row)
        return GitRefAuditRun.model_validate(row)

    async def set_summary(
        self, *, run_id: int, summary: dict[str, Any]
    ) -> GitRefAuditRun:
        """Replace the run row's JSONB ``summary`` column.

        Called by the discovery dispatcher to record per-tick metadata
        (``orgs_enqueued``, ``orgs_skipped``) in the same transaction
        as the run-row insert, so the counts represent the
        dispatcher's intent and are captured atomically with the run
        — the summary is never missing, even if the per-child fan-out
        dies later.
        """
        row = await self._get_row(run_id)
        row.summary = summary
        await self._session.flush()
        await self._session.refresh(row)
        return GitRefAuditRun.model_validate(row)

    async def aggregate_activity(
        self, *, run_id: int
    ) -> GitRefAuditRunActivity:
        """Aggregate ``queue_jobs`` rows attributed to this run.

        Same shape as :meth:`LifecycleEvalRunStore.aggregate_activity`:
        one ``GROUP BY status`` query returns the four counters plus a
        ``date_last_activity`` derived from ``MAX(coalesce(
        date_completed, date_started, date_created))`` per row.
        Buckets ``queued`` / ``in_progress`` as ``pending_count``,
        ``completed`` as ``succeeded_count``, and everything else
        (``failed`` / ``cancelled`` / ``completed_with_errors``) as
        ``failed_count`` — including ``completed_with_errors`` so a
        per-org pass that reported a partial fetch failure rolls the
        parent run to ``partial_failure`` rather than ``succeeded``.
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
            .where(SqlQueueJob.git_ref_audit_run_id == run_id)
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
        return GitRefAuditRunActivity(
            pending_count=pending,
            succeeded_count=succeeded,
            failed_count=failed,
            total_count=total,
            date_last_activity=date_last_activity,
        )

    async def _get_row(self, run_id: int) -> SqlGitRefAuditRun:
        result = await self._session.execute(
            select(SqlGitRefAuditRun).where(SqlGitRefAuditRun.id == run_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise JobNotFoundError(
                job_function="GitRefAuditRunStore._get_row",
                message=f"Git ref audit run {run_id} not found",
            )
        return row


_ALLOWED_TRANSITIONS: dict[
    GitRefAuditRunStatus, frozenset[GitRefAuditRunStatus]
] = {
    GitRefAuditRunStatus.pending: frozenset(
        {
            GitRefAuditRunStatus.in_progress,
            GitRefAuditRunStatus.succeeded,
            GitRefAuditRunStatus.partial_failure,
            GitRefAuditRunStatus.failed,
        }
    ),
    GitRefAuditRunStatus.in_progress: frozenset(
        {
            GitRefAuditRunStatus.succeeded,
            GitRefAuditRunStatus.partial_failure,
            GitRefAuditRunStatus.failed,
        }
    ),
}


def _is_allowed_transition(
    current: GitRefAuditRunStatus, new: GitRefAuditRunStatus
) -> bool:
    return new in _ALLOWED_TRANSITIONS.get(current, frozenset())
