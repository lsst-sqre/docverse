"""Database operations for the queue_jobs table."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from safir.database import CountedPaginatedList, CountedPaginatedQueryRunner
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobKind, JobStatus, QueueJob
from docverse.exceptions import InvalidJobStateError, JobNotFoundError
from docverse.storage.pagination import QueueJobDateCreatedCursor

__all__ = ["QueueJobStore"]


class QueueJobStore:
    """Direct database operations for queue jobs."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(  # noqa: PLR0913
        self,
        *,
        kind: JobKind,
        org_id: int,
        backend_job_id: str | None = None,
        project_id: int | None = None,
        build_id: int | None = None,
        edition_id: int | None = None,
        keeper_sync_run_id: int | None = None,
        subject_label: str | None = None,
    ) -> QueueJob:
        """Insert a new QueueJob row with status=queued.

        Generates a Base32 public_id. Calls flush() to get DB defaults.
        """
        public_id = validate_base32_id(generate_base32_id())
        row = SqlQueueJob(
            public_id=public_id,
            backend_job_id=backend_job_id,
            kind=kind.value,
            status=JobStatus.queued.value,
            org_id=org_id,
            project_id=project_id,
            build_id=build_id,
            edition_id=edition_id,
            keeper_sync_run_id=keeper_sync_run_id,
            subject_label=subject_label,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return QueueJob.model_validate(row, from_attributes=True)

    async def get(self, job_id: int) -> QueueJob | None:
        """Fetch a QueueJob by internal id."""
        result = await self._session.execute(
            select(SqlQueueJob).where(SqlQueueJob.id == job_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return QueueJob.model_validate(row, from_attributes=True)

    async def get_by_public_id(self, public_id: int) -> QueueJob | None:
        """Fetch a QueueJob by public Base32 id (int form)."""
        result = await self._session.execute(
            select(SqlQueueJob).where(SqlQueueJob.public_id == public_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return QueueJob.model_validate(row, from_attributes=True)

    async def get_by_backend_job_id(
        self, backend_job_id: str
    ) -> QueueJob | None:
        """Fetch a QueueJob by its arq backend job ID."""
        result = await self._session.execute(
            select(SqlQueueJob).where(
                SqlQueueJob.backend_job_id == backend_job_id
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return QueueJob.model_validate(row, from_attributes=True)

    async def start(self, job_id: int) -> QueueJob:
        """Mark job as in_progress, set date_started=now().

        Raises
        ------
        InvalidJobStateError
            If the job is not in queued status.
        """
        row = await self._get_row(job_id)
        if row.status != JobStatus.queued.value:
            msg = (
                f"Cannot start job {job_id}: "
                f"expected 'queued', got '{row.status}'"
            )
            raise InvalidJobStateError(msg)
        row.status = JobStatus.in_progress.value
        row.date_started = datetime.now(tz=UTC)
        await self._session.flush()
        await self._session.refresh(row)
        return QueueJob.model_validate(row, from_attributes=True)

    async def set_backend_job_id(
        self,
        job_id: int,
        backend_job_id: str,
    ) -> QueueJob:
        """Record the arq job ID on a previously-created QueueJob row.

        Used by two-phase enqueue flows that insert the row before they
        have a backend job ID (see ``_enqueue_publish_jobs``).
        """
        row = await self._get_row(job_id)
        row.backend_job_id = backend_job_id
        await self._session.flush()
        await self._session.refresh(row)
        return QueueJob.model_validate(row, from_attributes=True)

    async def update_phase(
        self,
        job_id: int,
        phase: str,
        *,
        progress: dict[str, Any] | None = None,
    ) -> QueueJob:
        """Set the phase column and optionally reset progress.

        Resets progress for the new phase when provided.
        """
        row = await self._get_row(job_id)
        row.phase = phase
        if progress is not None:
            row.progress = progress
        await self._session.flush()
        await self._session.refresh(row)
        return QueueJob.model_validate(row, from_attributes=True)

    async def update_progress(
        self,
        job_id: int,
        progress: dict[str, Any],
    ) -> QueueJob:
        """Merge progress dict into existing progress JSONB.

        Uses PostgreSQL ``||`` operator for atomic merge. If current progress
        is NULL, sets it directly.
        """
        row = await self._get_row(job_id)
        if row.progress is None:
            row.progress = progress
            await self._session.flush()
        else:
            # Use SQLAlchemy JSONB concatenation for atomic merge
            stmt = (
                update(SqlQueueJob)
                .where(SqlQueueJob.id == job_id)
                .values(progress=SqlQueueJob.progress.concat(progress))
            )
            await self._session.execute(stmt)
        await self._session.refresh(row)
        return QueueJob.model_validate(row, from_attributes=True)

    async def complete(
        self,
        job_id: int,
        *,
        has_errors: bool = False,
    ) -> QueueJob:
        """Mark job completed, set date_completed=now().

        Raises
        ------
        InvalidJobStateError
            If the job is not in in_progress status.
        """
        row = await self._get_row(job_id)
        if row.status != JobStatus.in_progress.value:
            msg = (
                f"Cannot complete job {job_id}: "
                f"expected 'in_progress', got '{row.status}'"
            )
            raise InvalidJobStateError(msg)
        row.status = (
            JobStatus.completed_with_errors.value
            if has_errors
            else JobStatus.completed.value
        )
        row.date_completed = datetime.now(tz=UTC)
        await self._session.flush()
        await self._session.refresh(row)
        return QueueJob.model_validate(row, from_attributes=True)

    async def fail(
        self,
        job_id: int,
        *,
        errors: dict[str, Any] | None = None,
    ) -> QueueJob:
        """Mark job failed, set date_completed=now(), store error details.

        Raises
        ------
        InvalidJobStateError
            If the job is not in queued or in_progress status.
        """
        row = await self._get_row(job_id)
        allowed = {JobStatus.queued.value, JobStatus.in_progress.value}
        if row.status not in allowed:
            msg = (
                f"Cannot fail job {job_id}: "
                f"expected 'queued'/'in_progress', "
                f"got '{row.status}'"
            )
            raise InvalidJobStateError(msg)
        row.status = JobStatus.failed.value
        row.date_completed = datetime.now(tz=UTC)
        if errors is not None:
            row.errors = errors
        await self._session.flush()
        await self._session.refresh(row)
        return QueueJob.model_validate(row, from_attributes=True)

    async def cancel(self, job_id: int) -> QueueJob:
        """Mark job cancelled, set date_completed=now().

        Raises
        ------
        InvalidJobStateError
            If the job is not in queued or in_progress status.
        """
        row = await self._get_row(job_id)
        allowed = {JobStatus.queued.value, JobStatus.in_progress.value}
        if row.status not in allowed:
            msg = (
                f"Cannot cancel job {job_id}: "
                f"expected 'queued'/'in_progress', "
                f"got '{row.status}'"
            )
            raise InvalidJobStateError(msg)
        row.status = JobStatus.cancelled.value
        row.date_completed = datetime.now(tz=UTC)
        await self._session.flush()
        await self._session.refresh(row)
        return QueueJob.model_validate(row, from_attributes=True)

    async def has_active_for_subject(
        self,
        *,
        org_id: int,
        kind: JobKind,
        subject_label: str,
    ) -> bool:
        """Return True when an active job for the subject already exists.

        "Active" means ``status IN ('queued', 'in_progress')``. The
        primary caller is the keeper-sync per-project mutual exclusion
        gate: ``_enqueue_children`` (run-discovery fan-out),
        ``_enqueue_tier_project_sync`` (tier crons), and
        ``KeeperSyncRunService.refresh_project`` (operator-triggered
        single-project refresh) each pre-check this before enqueuing a
        ``keeper_sync_project`` job, so two concurrent jobs cannot
        race through the per-edition INSERT path inside
        ``_ensure_edition`` and lose the
        ``uq_editions_project_lower_slug`` race.
        """
        stmt = select(SqlQueueJob.id).where(
            SqlQueueJob.org_id == org_id,
            SqlQueueJob.kind == kind.value,
            SqlQueueJob.subject_label == subject_label,
            SqlQueueJob.status.in_(
                [JobStatus.queued.value, JobStatus.in_progress.value]
            ),
        )
        result = await self._session.execute(stmt)
        return result.first() is not None

    async def has_active_dashboard_build(
        self,
        *,
        org_id: int,
        project_id: int,
    ) -> bool:
        """Return True if an active ``dashboard_build`` exists for the project.

        "Active" means ``kind='dashboard_build'`` and
        ``status IN ('queued', 'in_progress')`` for the given
        ``(org_id, project_id)``. Used by
        :meth:`DashboardBuildEnqueuer.enqueue_for_project` to dedup
        cascading enqueues — e.g. a keeper-sync project sync that
        publishes 1000 editions cascades through ``publish_edition``'s
        post-success ``try_enqueue_dashboard_build_by_id`` and would
        otherwise produce 1000 redundant ``dashboard_build`` rows for
        the same project. Once one is queued or in_progress, the gate
        skips subsequent enqueues until that row reaches a terminal
        state.

        Sibling to :meth:`has_active_for_subject`, which keys on
        ``subject_label`` for ``keeper_sync_project`` jobs that have no
        ``project_id`` foreign key for the LTD-side product. The two
        methods stay separate rather than a single generalised helper:
        ``dashboard_build`` rows have a real ``project_id`` column, so
        keying on it directly is cleaner than going through
        ``subject_label``.
        """
        stmt = select(SqlQueueJob.id).where(
            SqlQueueJob.kind == JobKind.dashboard_build.value,
            SqlQueueJob.org_id == org_id,
            SqlQueueJob.project_id == project_id,
            SqlQueueJob.status.in_(
                [JobStatus.queued.value, JobStatus.in_progress.value]
            ),
        )
        result = await self._session.execute(stmt)
        return result.first() is not None

    async def list_by_keeper_sync_run(
        self,
        *,
        run_id: int,
        status: JobStatus | None = None,
        cursor: QueueJobDateCreatedCursor | None = None,
        limit: int,
    ) -> CountedPaginatedList[QueueJob, QueueJobDateCreatedCursor]:
        """List queue jobs attributed to a run, newest first.

        Optional ``status`` narrows to a single :class:`JobStatus`.
        Pagination uses the standard ``date_created`` DESC keyset cursor
        so pages are stable across concurrent inserts.
        """
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.keeper_sync_run_id == run_id
        )
        if status is not None:
            stmt = stmt.where(SqlQueueJob.status == status.value)
        runner = CountedPaginatedQueryRunner(
            entry_type=QueueJob,
            cursor_type=QueueJobDateCreatedCursor,
        )
        return await runner.query_object(
            self._session, stmt, cursor=cursor, limit=limit
        )

    async def fail_silent_run_children(
        self,
        *,
        idle_after: timedelta,
    ) -> list[QueueJob]:
        """Fail keeper-sync child rows that have been ``in_progress`` too long.

        Backstop for the case where arq itself loses a job — typically a
        worker pod OOM-killed mid-job that never gets to surface a
        timeout. ``keeper_sync_project`` transitions its row to
        ``in_progress`` *before* the long copy work begins, so a row
        that has been ``in_progress`` past ``idle_after`` without ever
        reaching ``date_completed`` indicates the worker died silently.

        Scoped to rows attached to a keeper-sync run
        (``keeper_sync_run_id IS NOT NULL``); unrelated long-running
        ``build_processing`` jobs are not the reaper's concern. Queued
        rows without ``date_started`` are left alone — those are the
        ``fail_orphaned_run_children`` shape, swept by the next
        discovery attempt instead.
        """
        cutoff = datetime.now(tz=UTC) - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.keeper_sync_run_id.is_not(None),
            SqlQueueJob.status == JobStatus.in_progress.value,
            SqlQueueJob.date_completed.is_(None),
            SqlQueueJob.date_started.is_not(None),
            SqlQueueJob.date_started < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        now = datetime.now(tz=UTC)
        reaped: list[QueueJob] = []
        for row in rows:
            row.status = JobStatus.failed.value
            row.date_completed = now
            row.errors = {
                "message": (
                    "Reaped by keeper_sync_reaper: worker went silent "
                    "while job was in_progress (likely OOM-killed or "
                    "lost by arq)"
                ),
                "type": "SilentWorker",
            }
            reaped.append(QueueJob.model_validate(row, from_attributes=True))
        if reaped:
            await self._session.flush()
        return reaped

    async def fail_silent_tier_cron_jobs(
        self,
        *,
        idle_after: timedelta,
    ) -> list[QueueJob]:
        """Fail run-less ``keeper_sync_project`` rows stuck ``in_progress``.

        Tier-cron-enqueued ``keeper_sync_project`` rows
        (``keeper_sync_run_id IS NULL``) have no run finalisation hook
        to roll them up, so :meth:`fail_silent_run_children` (which is
        scoped to ``keeper_sync_run_id IS NOT NULL``) cannot reach
        them. A worker that's OOM-killed mid-job leaves the row stuck
        in ``in_progress`` indefinitely, and the
        :meth:`has_active_for_subject` mutex consulted by
        ``_enqueue_tier_project_sync`` keeps observing the stale row
        and skips enqueue forever — wedging that project's tier-cron
        sync. This method is the matching reaper.

        Scoped narrowly: ``kind='keeper_sync_project'``,
        ``keeper_sync_run_id IS NULL``, ``status='in_progress'``,
        ``date_completed IS NULL``, and ``date_started`` older than
        ``now - idle_after``. Run-attributed rows are explicitly out
        of scope (handled by :meth:`fail_silent_run_children`).
        Reaped rows carry ``errors.type='SilentTierCronJob'`` so
        postmortems can distinguish tier-cron reaps from
        run-attributed reaps.
        """
        cutoff = datetime.now(tz=UTC) - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            SqlQueueJob.keeper_sync_run_id.is_(None),
            SqlQueueJob.status == JobStatus.in_progress.value,
            SqlQueueJob.date_completed.is_(None),
            SqlQueueJob.date_started.is_not(None),
            SqlQueueJob.date_started < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        now = datetime.now(tz=UTC)
        reaped: list[QueueJob] = []
        for row in rows:
            row.status = JobStatus.failed.value
            row.date_completed = now
            row.errors = {
                "message": (
                    "Reaped by keeper_sync_reaper: tier-cron "
                    "keeper_sync_project worker went silent while job "
                    "was in_progress (likely OOM-killed or lost by arq)"
                ),
                "type": "SilentTierCronJob",
            }
            reaped.append(QueueJob.model_validate(row, from_attributes=True))
        if reaped:
            await self._session.flush()
        return reaped

    async def fail_orphaned_tier_cron_jobs(
        self,
        *,
        idle_after: timedelta,
    ) -> list[QueueJob]:
        """Fail run-less ``keeper_sync_project`` rows that never reached arq.

        Sibling of :meth:`fail_orphaned_run_children` for tier-cron
        enqueues. ``_enqueue_tier_project_sync`` commits the
        ``queue_jobs`` row before calling ``arq_queue.enqueue``, so a
        worker crash in that window leaves an orphan
        (``status='queued'``, ``backend_job_id IS NULL``). Without a
        ``keeper_sync_run_id``, the run-scoped orphan reaper can't see
        it, and the :meth:`has_active_for_subject` mutex keeps the
        stale row alive so the next tick skips enqueue.

        Scoped narrowly: ``kind='keeper_sync_project'``,
        ``keeper_sync_run_id IS NULL``, ``status='queued'``,
        ``backend_job_id IS NULL``, and ``date_created`` older than
        ``now - idle_after``. Reaped rows carry
        ``errors.type='OrphanedTierCronJob'``.
        """
        cutoff = datetime.now(tz=UTC) - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            SqlQueueJob.keeper_sync_run_id.is_(None),
            SqlQueueJob.status == JobStatus.queued.value,
            SqlQueueJob.backend_job_id.is_(None),
            SqlQueueJob.date_created < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        now = datetime.now(tz=UTC)
        failed: list[QueueJob] = []
        for row in rows:
            row.status = JobStatus.failed.value
            row.date_completed = now
            row.errors = {
                "message": (
                    "Orphaned tier-cron keeper_sync_project: queue_jobs "
                    "row committed without an arq backend_job_id "
                    "(worker likely crashed between SQL commit and "
                    "arq_queue.enqueue)"
                ),
                "type": "OrphanedTierCronJob",
            }
            failed.append(QueueJob.model_validate(row, from_attributes=True))
        if failed:
            await self._session.flush()
        return failed

    async def fail_orphaned_run_children(
        self,
        *,
        run_id: int,
        idle_after: timedelta,
    ) -> list[QueueJob]:
        """Fail child rows for a keeper-sync run that never got an arq job.

        Reconciles the gap left by ``_enqueue_children`` in
        ``docverse.worker.functions.keeper_sync``: the child ``queue_jobs``
        row commits *before* ``arq_queue.enqueue`` is called, so a worker
        crash in that window leaves an orphan — ``status='queued'``,
        ``backend_job_id IS NULL``, no arq job ever scheduled — that
        otherwise blocks run finalisation forever.

        ``idle_after`` keeps in-flight rows safe: only orphans whose
        ``date_created`` is older than ``now - idle_after`` are failed,
        so a concurrently-running discovery worker's freshly-committed
        rows are never reaped before it has a chance to write back the
        backend job ID.
        """
        cutoff = datetime.now(tz=UTC) - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.keeper_sync_run_id == run_id,
            SqlQueueJob.status == JobStatus.queued.value,
            SqlQueueJob.backend_job_id.is_(None),
            SqlQueueJob.date_created < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        now = datetime.now(tz=UTC)
        failed: list[QueueJob] = []
        for row in rows:
            row.status = JobStatus.failed.value
            row.date_completed = now
            row.errors = {
                "message": (
                    "Orphaned: queue_jobs row committed without an arq "
                    "backend_job_id (worker likely crashed mid-fanout)"
                ),
                "type": "OrphanedQueueJob",
            }
            failed.append(QueueJob.model_validate(row, from_attributes=True))
        if failed:
            await self._session.flush()
        return failed

    async def _get_row(self, job_id: int) -> SqlQueueJob:
        """Fetch a SqlQueueJob row by id, raising if not found."""
        result = await self._session.execute(
            select(SqlQueueJob).where(SqlQueueJob.id == job_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            msg = f"Queue job {job_id} not found"
            raise JobNotFoundError(msg)
        return row
