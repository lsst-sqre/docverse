"""Domain models for queue jobs."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from docverse.client.models.queue_enums import JobKind, JobStatus

from .base32id import Base32Id

__all__ = [
    "JobKind",
    "JobStatus",
    "QueueJob",
]


class QueueJob(BaseModel):
    """Domain model for a queue job."""

    id: int
    public_id: Base32Id
    backend_job_id: str | None = None
    kind: JobKind
    status: JobStatus
    phase: str | None = None
    org_id: int
    project_id: int | None = None
    build_id: int | None = None
    progress: dict[str, Any] | None = None
    errors: dict[str, Any] | None = None
    date_created: datetime
    date_started: datetime | None = None
    date_completed: datetime | None = None
