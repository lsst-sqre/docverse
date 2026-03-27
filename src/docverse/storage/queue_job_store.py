"""Database operations for the queue_jobs table."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobKind, JobStatus, QueueJob
from docverse.exceptions import InvalidJobStateError, JobNotFoundError

__all__ = ["QueueJobStore"]


class QueueJobStore:
    """Direct database operations for queue jobs."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
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
