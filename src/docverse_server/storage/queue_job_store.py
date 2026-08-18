"""Database operations for the queue_jobs table."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import structlog
from safir.database import CountedPaginatedList, CountedPaginatedQueryRunner
from sqlalchemy import ColumnExpressionArgument, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.queue import JobKind, JobStatus, QueueJob
from docverse_server.exceptions import InvalidJobStateError, JobNotFoundError
from docverse_server.storage._integrity import (
    is_unique_violation,
    violated_constraint_name,
)
from docverse_server.storage._public_id import (
    insert_with_time_ordered_public_id,
)
from docverse_server.storage.pagination import QueueJobDateCreatedCursor
from docverse_server.storage.queue_backend import QueueBackend

__all__ = ["QueueJobStore"]

ACTIVE_JOB_INDEX_SUFFIX = "_active_uq"
"""Naming convention for the ``queue_jobs`` active-job mutex indexes.

Every partial unique index on ``queue_jobs`` whose predicate is
``status IN ('queued', 'in_progress')`` — the per-``(org, subject_label)``
keeper-sync mutex, the per-org lifecycle-eval and git-ref-audit mutexes,
and the per-``(org, project)`` dashboard-build mutex — is named with this
suffix. :data:`_ACTIVE_JOB_UNIQUE_INDEXES` is derived from the table
metadata rather than hand-listed so a mutex added later participates in
:meth:`QueueJobStore.create_unless_active` automatically.
"""

_ACTIVE_JOB_UNIQUE_INDEXES = frozenset(
    index.name
    for index in SqlQueueJob.metadata.tables[SqlQueueJob.__tablename__].indexes
    if index.unique
    and index.name is not None
    and index.name.endswith(ACTIVE_JOB_INDEX_SUFFIX)
)
"""Names of the ``queue_jobs`` partial unique indexes guarding active jobs."""


def _is_active_job_conflict(exc: IntegrityError) -> bool:
    """Return True when ``exc`` is an active-job mutex unique violation.

    Two conditions must hold: the error is a Postgres ``unique_violation``
    (so a foreign-key or NOT NULL violation is never absorbed), and the
    offending constraint is one of the ``queue_jobs`` active-job partial
    unique indexes (so a ``public_id`` collision — or any future
    constraint on the table — still propagates).
    """
    if not is_unique_violation(exc):
        return False
    name = violated_constraint_name(exc)
    if name is not None:
        return name in _ACTIVE_JOB_UNIQUE_INDEXES
    message = str(exc.orig)
    return any(index in message for index in _ACTIVE_JOB_UNIQUE_INDEXES)


class QueueJobStore:
    """Direct database operations for queue jobs."""

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
        kind: JobKind,
        org_id: int,
        backend_job_id: str | None = None,
        project_id: int | None = None,
        build_id: int | None = None,
        edition_id: int | None = None,
        keeper_sync_run_id: int | None = None,
        lifecycle_eval_run_id: int | None = None,
        git_ref_audit_run_id: int | None = None,
        subject_label: str | None = None,
    ) -> QueueJob:
        """Insert a new QueueJob row with status=queued.

        Mints a time-ordered Base32 ``public_id`` at insert time, re-minting
        on the (rare) same-millisecond collision. Calls flush() to get DB
        defaults.
        """

        def _make_row(public_id: int) -> SqlQueueJob:
            return SqlQueueJob(
                public_id=public_id,
                backend_job_id=backend_job_id,
                kind=kind.value,
                status=JobStatus.queued.value,
                org_id=org_id,
                project_id=project_id,
                build_id=build_id,
                edition_id=edition_id,
                keeper_sync_run_id=keeper_sync_run_id,
                lifecycle_eval_run_id=lifecycle_eval_run_id,
                git_ref_audit_run_id=git_ref_audit_run_id,
                subject_label=subject_label,
            )

        row = await insert_with_time_ordered_public_id(
            self._session, _make_row
        )
        await self._session.refresh(row)
        return QueueJob.model_validate(row, from_attributes=True)

    async def create_unless_active(
        self,
        *,
        kind: JobKind,
        org_id: int,
        backend_job_id: str | None = None,
        project_id: int | None = None,
        build_id: int | None = None,
        edition_id: int | None = None,
        keeper_sync_run_id: int | None = None,
        lifecycle_eval_run_id: int | None = None,
        git_ref_audit_run_id: int | None = None,
        subject_label: str | None = None,
    ) -> QueueJob | None:
        """Insert a QueueJob row, returning ``None`` if the mutex is held.

        Race-tolerant sibling of :meth:`create` for the kinds guarded by
        a ``queue_jobs`` active-job partial unique index. Callers pair a
        cheap ``has_active_*`` pre-check (the fast path, which also keeps
        the reapers' unwedging contract meaningful) with this insert (the
        backstop): between the pre-check's ``SELECT`` and the ``INSERT``
        another worker can win the slot, and the index — not the
        pre-check — is what actually enforces the mutex. Rather than let
        that lost race surface as an ``IntegrityError``, this returns
        ``None``, the same "someone else already holds the slot" signal
        the pre-check produces.

        The insert already runs inside a SAVEPOINT (see
        :func:`~docverse_server.storage._public_id.insert_with_time_ordered_public_id`),
        so absorbing the violation leaves the handler-owned outer
        transaction usable — the caller can keep working (enqueue the
        next project in the tier pass, commit its other writes) instead
        of unwinding.

        Only an active-job mutex violation is absorbed. Any other
        integrity error — a foreign-key violation, a NOT NULL violation,
        an exhausted ``public_id`` re-mint — propagates untouched, so a
        genuine bug still fails loudly.

        Returns
        -------
        QueueJob | None
            The inserted job, or ``None`` when an active job already
            holds the mutex this row would have claimed.
        """
        try:
            return await self.create(
                kind=kind,
                org_id=org_id,
                backend_job_id=backend_job_id,
                project_id=project_id,
                build_id=build_id,
                edition_id=edition_id,
                keeper_sync_run_id=keeper_sync_run_id,
                lifecycle_eval_run_id=lifecycle_eval_run_id,
                git_ref_audit_run_id=git_ref_audit_run_id,
                subject_label=subject_label,
            )
        except IntegrityError as exc:
            if not _is_active_job_conflict(exc):
                raise
            self._logger.info(
                "Lost the active-job race for a queue job",
                kind=kind.value,
                org_id=org_id,
                project_id=project_id,
                subject_label=subject_label,
            )
            return None

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
            raise InvalidJobStateError(
                current_state=row.status,
                target_state=JobStatus.in_progress.value,
                job_public_id=serialize_base32_id(row.public_id),
                job_function=row.kind,
            )
        return await self._mark_started(row)

    async def start_if_queued(self, job_id: int) -> QueueJob | None:
        """Mark job as in_progress, or return ``None`` if it is not queued.

        Late-delivery guard for the reaper triad (PRD #538). The
        abandoned sweep asks arq whether it still knows a queued job
        before failing its row, but a job re-appearing between that
        check and the ``UPDATE`` — or any other residual reap/deliver
        race — can still hand a worker a row that is no longer
        ``queued``. Picking such a row up with :meth:`start` would raise
        :exc:`~docverse_server.exceptions.InvalidJobStateError` into
        Sentry for a race the reaper is meant to absorb, so worker
        pickup paths call this instead and return without running the
        job body when it yields ``None``.

        On the ``queued`` path the transition is identical to
        :meth:`start`; :meth:`start` keeps its raising semantics for
        non-pickup callers, where an unexpected status *is* a bug.

        Returns
        -------
        QueueJob or None
            The started job when the row was ``queued``; ``None`` when
            it exists in any other status — terminal because a reaper
            failed it, or already ``in_progress`` because a duplicate
            delivery beat this one. The skip is logged as a warning
            carrying the job's public id, kind, and current status.

        Raises
        ------
        JobNotFoundError
            If no row with ``job_id`` exists. A missing row is not the
            reap race this guards — it means the caller was handed an id
            that never existed — so it still raises.
        """
        row = await self._get_row(job_id)
        if row.status != JobStatus.queued.value:
            self._logger.warning(
                "Skipping late-delivered queue job",
                queue_job_id=serialize_base32_id(row.public_id),
                queue_job_kind=row.kind,
                queue_job_status=row.status,
            )
            return None
        return await self._mark_started(row)

    async def _mark_started(self, row: SqlQueueJob) -> QueueJob:
        """Apply the queued → in_progress transition to a loaded row."""
        row.status = JobStatus.in_progress.value
        row.date_started = func.now()
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
            target = (
                JobStatus.completed_with_errors.value
                if has_errors
                else JobStatus.completed.value
            )
            raise InvalidJobStateError(
                current_state=row.status,
                target_state=target,
                job_public_id=serialize_base32_id(row.public_id),
                job_function=row.kind,
            )
        row.status = (
            JobStatus.completed_with_errors.value
            if has_errors
            else JobStatus.completed.value
        )
        row.date_completed = func.now()
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
            raise InvalidJobStateError(
                current_state=row.status,
                target_state=JobStatus.failed.value,
                job_public_id=serialize_base32_id(row.public_id),
                job_function=row.kind,
            )
        row.status = JobStatus.failed.value
        row.date_completed = func.now()
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
            raise InvalidJobStateError(
                current_state=row.status,
                target_state=JobStatus.cancelled.value,
                job_public_id=serialize_base32_id(row.public_id),
                job_function=row.kind,
            )
        row.status = JobStatus.cancelled.value
        row.date_completed = func.now()
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

    async def list_by_org(
        self,
        *,
        org_id: int,
        kind: JobKind | None = None,
        status: JobStatus | None = None,
        project_id: int | None = None,
        keeper_sync_run_id: int | None = None,
        cursor: QueueJobDateCreatedCursor | None = None,
        limit: int,
    ) -> CountedPaginatedList[QueueJob, QueueJobDateCreatedCursor]:
        """List an organization's queue jobs, newest first.

        Scoped to a single ``org_id`` so cross-org rows never appear.
        Each optional filter narrows the result set and they combine
        conjunctively: ``kind`` and ``status`` match a single
        :class:`JobKind` / :class:`JobStatus`, ``project_id`` matches the
        job's target project, and ``keeper_sync_run_id`` matches the
        attributed keeper-sync run. Pagination uses the standard
        ``date_created`` DESC keyset cursor so pages are stable across
        concurrent inserts.
        """
        stmt = select(SqlQueueJob).where(SqlQueueJob.org_id == org_id)
        if kind is not None:
            stmt = stmt.where(SqlQueueJob.kind == kind.value)
        if status is not None:
            stmt = stmt.where(SqlQueueJob.status == status.value)
        if project_id is not None:
            stmt = stmt.where(SqlQueueJob.project_id == project_id)
        if keeper_sync_run_id is not None:
            stmt = stmt.where(
                SqlQueueJob.keeper_sync_run_id == keeper_sync_run_id
            )
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
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.keeper_sync_run_id.is_not(None),
            SqlQueueJob.status == JobStatus.in_progress.value,
            SqlQueueJob.date_completed.is_(None),
            SqlQueueJob.date_started.is_not(None),
            SqlQueueJob.date_started < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
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
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
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
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            SqlQueueJob.keeper_sync_run_id.is_(None),
            SqlQueueJob.status == JobStatus.queued.value,
            SqlQueueJob.backend_job_id.is_(None),
            SqlQueueJob.date_created < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
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

    async def fail_silent_lifecycle_eval_jobs(
        self,
        *,
        idle_after: timedelta,
    ) -> list[QueueJob]:
        """Fail ``lifecycle_eval`` rows stuck ``in_progress`` past the window.

        ``lifecycle_reaper``'s silent-row sweep. Mirrors
        :meth:`fail_silent_run_children` but scoped to
        ``kind='lifecycle_eval'`` rather than the keeper-sync row's
        ``keeper_sync_run_id IS NOT NULL`` filter, because the dispatcher
        always writes a ``lifecycle_eval_run_id`` so the run attribution
        is implied by ``kind`` alone. Rows that the worker actually
        picked up but never finished (``status='in_progress'``,
        ``date_completed IS NULL``, ``date_started`` older than
        ``now - idle_after``) are reaped here; ``queued`` rows whose
        worker crashed before arq enqueue go to
        :meth:`fail_orphaned_lifecycle_eval_jobs`.

        Reaped rows carry ``errors.type='SilentWorker'`` matching the
        keeper-sync precedent so postmortem queries that group by
        ``errors.type`` can compare the two subsystems on the same axis.
        """
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.kind == JobKind.lifecycle_eval.value,
            SqlQueueJob.status == JobStatus.in_progress.value,
            SqlQueueJob.date_completed.is_(None),
            SqlQueueJob.date_started.is_not(None),
            SqlQueueJob.date_started < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        reaped: list[QueueJob] = []
        for row in rows:
            row.status = JobStatus.failed.value
            row.date_completed = now
            row.errors = {
                "message": (
                    "Reaped by lifecycle_reaper: worker went silent "
                    "while job was in_progress (likely OOM-killed or "
                    "lost by arq)"
                ),
                "type": "SilentWorker",
            }
            reaped.append(QueueJob.model_validate(row, from_attributes=True))
        if reaped:
            await self._session.flush()
        return reaped

    async def fail_orphaned_lifecycle_eval_jobs(
        self,
        *,
        idle_after: timedelta,
    ) -> list[QueueJob]:
        """Fail ``lifecycle_eval`` rows that never reached arq.

        ``lifecycle_reaper``'s orphan sweep. The dispatcher commits the
        per-org ``queue_jobs`` row *before* calling ``arq_queue.enqueue``,
        so a worker crash in that window leaves an orphan
        (``status='queued'``, ``backend_job_id IS NULL``). Without
        reconciliation the orphan would wedge the per-org mutex
        ``idx_queue_jobs_lifecycle_eval_active_uq`` and block subsequent
        dispatcher ticks from enqueueing fresh work for that org.

        Scoped narrowly: ``kind='lifecycle_eval'``, ``status='queued'``,
        ``backend_job_id IS NULL``, ``date_created`` older than
        ``now - idle_after``. Reaped rows carry
        ``errors.type='OrphanedQueueJob'`` matching the keeper-sync
        precedent for run-attributed orphans.
        """
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.kind == JobKind.lifecycle_eval.value,
            SqlQueueJob.status == JobStatus.queued.value,
            SqlQueueJob.backend_job_id.is_(None),
            SqlQueueJob.date_created < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        failed: list[QueueJob] = []
        for row in rows:
            row.status = JobStatus.failed.value
            row.date_completed = now
            row.errors = {
                "message": (
                    "Orphaned lifecycle_eval: queue_jobs row committed "
                    "without an arq backend_job_id (worker likely "
                    "crashed between SQL commit and arq_queue.enqueue)"
                ),
                "type": "OrphanedQueueJob",
            }
            failed.append(QueueJob.model_validate(row, from_attributes=True))
        if failed:
            await self._session.flush()
        return failed

    async def fail_silent_git_ref_audit_jobs(
        self,
        *,
        idle_after: timedelta,
    ) -> list[QueueJob]:
        """Fail ``git_ref_audit`` rows stuck ``in_progress`` past the window.

        Sibling of :meth:`fail_silent_lifecycle_eval_jobs` for the daily
        ``git_ref_audit`` worker pool: every per-org row carries
        ``git_ref_audit_run_id`` so attribution is implied by
        ``kind='git_ref_audit'`` alone. Rows that the worker actually
        picked up but never finished (``status='in_progress'``,
        ``date_completed IS NULL``, ``date_started`` older than
        ``now - idle_after``) are reaped here; ``queued`` rows whose
        worker crashed before arq enqueue go to
        :meth:`fail_orphaned_git_ref_audit_jobs`.

        Reaped rows carry ``errors.type='SilentWorker'`` matching the
        keeper-sync / lifecycle-eval precedent so postmortem queries
        that group by ``errors.type`` can compare the three subsystems
        on the same axis.
        """
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.kind == JobKind.git_ref_audit.value,
            SqlQueueJob.status == JobStatus.in_progress.value,
            SqlQueueJob.date_completed.is_(None),
            SqlQueueJob.date_started.is_not(None),
            SqlQueueJob.date_started < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        reaped: list[QueueJob] = []
        for row in rows:
            row.status = JobStatus.failed.value
            row.date_completed = now
            row.errors = {
                "message": (
                    "Reaped by lifecycle_reaper: git_ref_audit worker "
                    "went silent while job was in_progress (likely "
                    "OOM-killed or lost by arq)"
                ),
                "type": "SilentWorker",
            }
            reaped.append(QueueJob.model_validate(row, from_attributes=True))
        if reaped:
            await self._session.flush()
        return reaped

    async def fail_orphaned_git_ref_audit_jobs(
        self,
        *,
        idle_after: timedelta,
    ) -> list[QueueJob]:
        """Fail ``git_ref_audit`` rows that never reached arq.

        Sibling of :meth:`fail_orphaned_lifecycle_eval_jobs` for the
        daily ``git_ref_audit`` worker pool. The discovery dispatcher
        commits the per-org ``queue_jobs`` row *before* calling
        ``arq_queue.enqueue``, so a worker crash in that window leaves
        an orphan (``status='queued'``, ``backend_job_id IS NULL``).
        Without reconciliation the orphan would wedge the per-org
        mutex ``idx_queue_jobs_git_ref_audit_active_uq`` and block the
        next day's discovery tick from enqueueing fresh work for
        that org.

        Scoped narrowly: ``kind='git_ref_audit'``,
        ``status='queued'``, ``backend_job_id IS NULL``,
        ``date_created`` older than ``now - idle_after``. Reaped rows
        carry ``errors.type='OrphanedQueueJob'`` matching the
        precedent for the other run-attributed orphan sweeps.
        """
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.kind == JobKind.git_ref_audit.value,
            SqlQueueJob.status == JobStatus.queued.value,
            SqlQueueJob.backend_job_id.is_(None),
            SqlQueueJob.date_created < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        failed: list[QueueJob] = []
        for row in rows:
            row.status = JobStatus.failed.value
            row.date_completed = now
            row.errors = {
                "message": (
                    "Orphaned git_ref_audit: queue_jobs row committed "
                    "without an arq backend_job_id (worker likely "
                    "crashed between SQL commit and arq_queue.enqueue)"
                ),
                "type": "OrphanedQueueJob",
            }
            failed.append(QueueJob.model_validate(row, from_attributes=True))
        if failed:
            await self._session.flush()
        return failed

    async def fail_silent_jobs(
        self,
        kind: JobKind,
        *,
        idle_after: timedelta,
    ) -> list[QueueJob]:
        """Fail rows of ``kind`` stuck ``in_progress`` past the window.

        Shared silent-row sweep used by the run-less reaper modules
        (``dashboard_build_reaper`` and siblings — see
        :mod:`docverse_server.worker.functions._runless_reaper`). Rows the
        worker picked up but never finished (``status='in_progress'``,
        ``date_completed IS NULL``, ``date_started`` older than
        ``now - idle_after``) are reaped here; ``queued`` orphans go
        to :meth:`fail_orphaned_jobs`.

        Reaped rows carry ``errors.type='SilentWorker'`` matching the
        lifecycle/git_ref_audit precedent so postmortem queries that
        group by ``errors.type`` can compare subsystems on the same
        axis. The per-kind reaper-name in the error message lets a
        postmortem reader identify the sweep that produced the row
        without joining against the run table.
        """
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.kind == kind.value,
            SqlQueueJob.status == JobStatus.in_progress.value,
            SqlQueueJob.date_completed.is_(None),
            SqlQueueJob.date_started.is_not(None),
            SqlQueueJob.date_started < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        reaped: list[QueueJob] = []
        for row in rows:
            row.status = JobStatus.failed.value
            row.date_completed = now
            row.errors = {
                "message": (
                    f"Reaped by {kind.value}_reaper: worker went silent "
                    "while job was in_progress (likely OOM-killed or "
                    "lost by arq)"
                ),
                "type": "SilentWorker",
            }
            reaped.append(QueueJob.model_validate(row, from_attributes=True))
        if reaped:
            await self._session.flush()
        return reaped

    async def fail_orphaned_jobs(
        self,
        kind: JobKind,
        *,
        idle_after: timedelta,
    ) -> list[QueueJob]:
        """Fail rows of ``kind`` that never reached arq.

        Shared orphan sweep used by the run-less reaper modules. The
        enqueue path commits the ``queue_jobs`` row before calling
        ``arq_queue.enqueue``, so a crash in that window leaves an
        orphan (``status='queued'``, ``backend_job_id IS NULL``).
        Without reconciliation the orphan wedges any per-kind active
        mutex the row holds (for example
        ``idx_queue_jobs_dashboard_build_active_uq`` for
        ``dashboard_build``), and the operator-facing rebuild/publish
        flow stays blocked.

        Scoped narrowly: rows of the given ``kind`` with
        ``status='queued'``, ``backend_job_id IS NULL``, and
        ``date_created`` older than ``now - idle_after``. Reaped rows
        carry ``errors.type='OrphanedQueueJob'`` matching the
        lifecycle/git_ref_audit precedent.
        """
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.kind == kind.value,
            SqlQueueJob.status == JobStatus.queued.value,
            SqlQueueJob.backend_job_id.is_(None),
            SqlQueueJob.date_created < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        failed: list[QueueJob] = []
        for row in rows:
            row.status = JobStatus.failed.value
            row.date_completed = now
            row.errors = {
                "message": (
                    f"Orphaned {kind.value}: queue_jobs row committed "
                    "without an arq backend_job_id (worker likely "
                    "crashed between SQL commit and arq_queue.enqueue)"
                ),
                "type": "OrphanedQueueJob",
            }
            failed.append(QueueJob.model_validate(row, from_attributes=True))
        if failed:
            await self._session.flush()
        return failed

    async def fail_abandoned_jobs(
        self,
        kind: JobKind,
        *,
        idle_after: timedelta,
        queue_backend: QueueBackend,
    ) -> list[QueueJob]:
        """Fail rows of ``kind`` that reached arq and were then lost.

        Third sweep of the reaper triad, closing the gap PRD #538
        describes: :meth:`fail_silent_jobs` only looks at
        ``in_progress`` rows and :meth:`fail_orphaned_jobs` only at
        ``queued`` rows with ``backend_job_id IS NULL``, so a row that
        *did* get an arq job ID and then had that job vanish (Redis
        eviction, a queue flush, a lost `arq:queue` entry) is swept by
        neither. Because the active-job partial unique indexes count a
        ``queued`` row as holding the mutex, one such row wedges its
        slot permanently — on roundtable-dev this silently froze the
        documenteer version dashboard.

        Age alone cannot separate "arq lost this job" from "arq is
        backed up", so each candidate (``status='queued'``,
        ``backend_job_id IS NOT NULL``, ``date_created`` older than
        ``now - idle_after``) is verified against
        :meth:`QueueBackend.get_job_metadata` first and failed only when
        the backend has no record of it. Reaped rows carry
        ``errors.type='AbandonedQueueJob'``, distinct from
        ``SilentWorker`` and ``OrphanedQueueJob`` so postmortem queries
        can separate the three loss modes.

        Run-attributed rows (``keeper_sync_run_id IS NOT NULL``) are out
        of scope, mirroring :meth:`fail_abandoned_tier_cron_jobs`'
        run-less scoping. A keeper-sync project sync cascades
        ``publish_edition`` jobs that carry the run's id, so without this
        predicate both this sweep and
        :meth:`fail_abandoned_run_children` would claim them — and only
        the latter feeds its reaped rows into ``maybe_finalise_run``.
        Whichever won, the loser's contract broke: a win here left the
        parent ``keeper_sync_runs`` row ``in_progress`` forever, which
        409-blocks every later run for the org. Giving run-attributed
        rows a single owner removes the ambiguity.
        """
        return await self._fail_abandoned_candidates(
            SqlQueueJob.kind == kind.value,
            SqlQueueJob.keeper_sync_run_id.is_(None),
            idle_after=idle_after,
            queue_backend=queue_backend,
            message=(
                f"Abandoned {kind.value}: queue_jobs row still queued past "
                f"the {kind.value}_reaper threshold and arq has no record "
                "of its backend_job_id (reaped by the abandoned sweep)"
            ),
        )

    async def fail_abandoned_tier_cron_jobs(
        self,
        *,
        idle_after: timedelta,
        queue_backend: QueueBackend,
    ) -> list[QueueJob]:
        """Fail run-less ``keeper_sync_project`` rows that arq lost.

        Abandoned-sweep sibling of
        :meth:`fail_orphaned_tier_cron_jobs`, scoped identically
        (``kind='keeper_sync_project'``, ``keeper_sync_run_id IS NULL``)
        but for rows that *did* reach arq. The orphan sweep can only see
        rows whose ``backend_job_id`` is still ``NULL``, so a tier-cron
        row whose arq job later vanished stays ``queued`` forever, and
        :meth:`has_active_for_subject` — which the tier crons consult
        before enqueuing — keeps reporting an in-flight sync and skips
        that subject on every subsequent tick.

        Candidates are verified against the queue backend before being
        failed; see :meth:`_fail_abandoned_candidates` for the shared
        core and its backend-unreachable behaviour. Run-attributed rows
        belong to :meth:`fail_abandoned_run_children`.
        """
        return await self._fail_abandoned_candidates(
            SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            SqlQueueJob.keeper_sync_run_id.is_(None),
            idle_after=idle_after,
            queue_backend=queue_backend,
            message=(
                "Abandoned tier-cron keeper_sync_project: queue_jobs row "
                "still queued past the keeper_sync_reaper threshold and "
                "arq has no record of its backend_job_id (reaped by the "
                "abandoned sweep)"
            ),
        )

    async def fail_abandoned_run_children(
        self,
        *,
        run_id: int | None = None,
        idle_after: timedelta,
        queue_backend: QueueBackend,
    ) -> list[QueueJob]:
        """Fail keeper-sync child rows that arq lost.

        Abandoned-sweep sibling of :meth:`fail_orphaned_run_children`,
        scoped identically (``keeper_sync_run_id == run_id``) but for
        rows that *did* reach arq. Such a child stays ``queued``
        forever, so it counts toward the parent run's ``pending_count``
        and blocks finalisation exactly the way an orphan does —
        callers therefore feed the reaped rows into the same
        ``maybe_finalise_run`` path.

        The run's own ``keeper_sync_run_discovery`` row is run-attributed
        too (``start_run`` stamps it with the run id), but it is the
        parent of the fan-out rather than one of its children, so it is
        excluded here and swept by
        :meth:`fail_abandoned_run_discovery` instead. Rolling a lost
        discovery up as a child would label it "Abandoned keeper-sync
        child" and let ``maybe_finalise_run`` compute
        ``partial_failure`` from the child counters, where a
        worker-failed discovery fails the whole run.

        ``run_id`` narrows the sweep to one run for the discovery-side
        reconciliation shape; ``None`` (the cron reaper's mode) sweeps
        every run-attributed row in one query, since the reaper has no
        run in hand and would otherwise have to enumerate runs just to
        find the ones that are wedged. Callers in that mode take the
        affected run IDs from the returned rows.

        Candidates are verified against the queue backend before being
        failed; see :meth:`_fail_abandoned_candidates` for the shared
        core and its backend-unreachable behaviour.
        """
        scope: ColumnExpressionArgument[bool] = (
            SqlQueueJob.keeper_sync_run_id.is_not(None)
            if run_id is None
            else SqlQueueJob.keeper_sync_run_id == run_id
        )
        return await self._fail_abandoned_candidates(
            scope,
            SqlQueueJob.kind != JobKind.keeper_sync_run_discovery.value,
            idle_after=idle_after,
            queue_backend=queue_backend,
            message=(
                "Abandoned keeper-sync child: queue_jobs row still queued "
                "past the keeper_sync_reaper threshold and arq has no "
                "record of its backend_job_id (reaped by the abandoned "
                "sweep)"
            ),
        )

    async def fail_abandoned_run_discovery(
        self,
        *,
        idle_after: timedelta,
        queue_backend: QueueBackend,
    ) -> list[QueueJob]:
        """Fail ``keeper_sync_run_discovery`` rows that arq lost.

        Counterpart to :meth:`fail_abandoned_run_children` for the one
        run-attributed row that is not a child: the discovery job
        ``KeeperSyncRunService.start_run`` enqueues before any fan-out
        exists. When arq loses it the run never leaves ``pending``, and
        because ``has_non_terminal_run`` treats ``pending`` as active,
        every later ``POST .../runs`` for the org returns 409 — the same
        wedge the children sweep clears, one level up.

        Reaped rows carry a discovery-specific ``errors.message`` (still
        ``errors.type='AbandonedQueueJob'``, so the three loss modes stay
        comparable) and callers drive the parent run to
        :attr:`~docverse.models.KeeperSyncRunStatus.failed` — matching
        ``keeper_sync_run_discovery``'s own failure path — rather than
        through ``maybe_finalise_run``, which would read the lone
        discovery row as a failed child and pick ``partial_failure``.

        Candidates are verified against the queue backend before being
        failed; see :meth:`_fail_abandoned_candidates` for the shared
        core and its backend-unreachable behaviour.
        """
        return await self._fail_abandoned_candidates(
            SqlQueueJob.kind == JobKind.keeper_sync_run_discovery.value,
            SqlQueueJob.keeper_sync_run_id.is_not(None),
            idle_after=idle_after,
            queue_backend=queue_backend,
            message=(
                "Abandoned keeper-sync run discovery: queue_jobs row still "
                "queued past the keeper_sync_reaper threshold and arq has "
                "no record of its backend_job_id, so the run never fanned "
                "out (reaped by the abandoned sweep)"
            ),
        )

    async def fail_abandoned_lifecycle_eval_jobs(
        self,
        *,
        idle_after: timedelta,
        queue_backend: QueueBackend,
    ) -> list[QueueJob]:
        """Fail ``lifecycle_eval`` rows that reached arq and were then lost.

        Abandoned-sweep sibling of
        :meth:`fail_orphaned_lifecycle_eval_jobs`, scoped identically
        (``kind='lifecycle_eval'``) but for rows that *did* reach arq.
        The orphan sweep only sees rows whose ``backend_job_id`` is still
        ``NULL``, so a row whose arq job later vanished stays ``queued``
        forever — and because
        ``idx_queue_jobs_lifecycle_eval_active_uq`` counts a ``queued``
        row as active work, every subsequent dispatcher tick for that org
        is blocked. Reaping the row releases the mutex so the next tick
        enqueues fresh work.

        Candidates are verified against the queue backend before being
        failed; see :meth:`_fail_abandoned_candidates` for the shared
        core and its backend-unreachable behaviour. Reaped rows keep
        their ``lifecycle_eval_run_id``, so the reaper feeds them into
        the same ``maybe_finalise_lifecycle_run`` pass as the silent and
        orphan sweeps.
        """
        return await self._fail_abandoned_candidates(
            SqlQueueJob.kind == JobKind.lifecycle_eval.value,
            idle_after=idle_after,
            queue_backend=queue_backend,
            message=(
                "Abandoned lifecycle_eval: queue_jobs row still queued past "
                "the lifecycle_reaper threshold and arq has no record of its "
                "backend_job_id (reaped by the abandoned sweep)"
            ),
        )

    async def fail_abandoned_git_ref_audit_jobs(
        self,
        *,
        idle_after: timedelta,
        queue_backend: QueueBackend,
    ) -> list[QueueJob]:
        """Fail ``git_ref_audit`` rows that reached arq and were then lost.

        Abandoned-sweep sibling of
        :meth:`fail_orphaned_git_ref_audit_jobs`, scoped identically
        (``kind='git_ref_audit'``) but for rows that *did* reach arq.
        Same wedge as the lifecycle_eval variant one mutex over:
        ``idx_queue_jobs_git_ref_audit_active_uq`` treats the stranded
        ``queued`` row as an in-flight audit, so the next day's discovery
        tick skips that org indefinitely.

        Candidates are verified against the queue backend before being
        failed; see :meth:`_fail_abandoned_candidates` for the shared
        core and its backend-unreachable behaviour. Reaped rows keep
        their ``git_ref_audit_run_id`` so the reaper can roll the parent
        run up in the same tick.
        """
        return await self._fail_abandoned_candidates(
            SqlQueueJob.kind == JobKind.git_ref_audit.value,
            idle_after=idle_after,
            queue_backend=queue_backend,
            message=(
                "Abandoned git_ref_audit: queue_jobs row still queued past "
                "the lifecycle_reaper threshold and arq has no record of its "
                "backend_job_id (reaped by the abandoned sweep)"
            ),
        )

    async def _fail_abandoned_candidates(
        self,
        *scope: ColumnExpressionArgument[bool],
        idle_after: timedelta,
        queue_backend: QueueBackend,
        message: str,
    ) -> list[QueueJob]:
        """Shared candidate-query + arq-verify + fail core.

        ``scope`` narrows the base abandoned predicate
        (``status='queued'``, ``backend_job_id IS NOT NULL``,
        ``date_created < now - idle_after``) to one family — a ``kind``
        equality for :meth:`fail_abandoned_jobs`, or the scoping the
        per-family variants use.

        Every candidate is verified *before* any row is mutated, so a
        backend that becomes unreachable partway through the sweep
        leaves no half-applied reap behind. On such a failure the sweep
        aborts for this tick with a warning and returns an empty list:
        the caller's sibling silent and orphan sweeps are unaffected and
        the next tick retries (PRD #538 §Scope, backend-unreachable
        bullet). Failing open this way is deliberate — reaping a row
        whose arq job may well be alive would cancel healthy work.
        """
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.status == JobStatus.queued.value,
            SqlQueueJob.backend_job_id.is_not(None),
            SqlQueueJob.date_created < cutoff,
            *scope,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())

        abandoned: list[SqlQueueJob] = []
        for row in rows:
            backend_job_id = row.backend_job_id
            if backend_job_id is None:  # pragma: no cover - SQL excludes
                continue
            try:
                metadata = await queue_backend.get_job_metadata(backend_job_id)
            except Exception as exc:
                self._logger.warning(
                    "Queue backend unreachable; skipping abandoned sweep",
                    error=str(exc),
                    error_type=type(exc).__name__,
                    candidate_count=len(rows),
                )
                return []
            if metadata is None:
                abandoned.append(row)

        failed: list[QueueJob] = []
        for row in abandoned:
            row.status = JobStatus.failed.value
            row.date_completed = now
            row.errors = {
                "message": message,
                "type": "AbandonedQueueJob",
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
        ``docverse_server.worker.functions.keeper_sync``: the child
        ``queue_jobs`` row commits *before* ``arq_queue.enqueue`` is
        called, so a worker
        crash in that window leaves an orphan — ``status='queued'``,
        ``backend_job_id IS NULL``, no arq job ever scheduled — that
        otherwise blocks run finalisation forever.

        ``idle_after`` keeps in-flight rows safe: only orphans whose
        ``date_created`` is older than ``now - idle_after`` are failed,
        so a concurrently-running discovery worker's freshly-committed
        rows are never reaped before it has a chance to write back the
        backend job ID.
        """
        now = (await self._session.execute(select(func.now()))).scalar_one()
        cutoff = now - idle_after
        stmt = select(SqlQueueJob).where(
            SqlQueueJob.keeper_sync_run_id == run_id,
            SqlQueueJob.status == JobStatus.queued.value,
            SqlQueueJob.backend_job_id.is_(None),
            SqlQueueJob.date_created < cutoff,
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
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
            raise JobNotFoundError(
                job_function="QueueJobStore._get_row",
                message=f"Queue job {job_id} not found",
            )
        return row
