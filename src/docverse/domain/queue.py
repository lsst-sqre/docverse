"""Domain models for queue jobs."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel

from .base32id import Base32Id

__all__ = [
    "JobKind",
    "JobStatus",
    "QueueJob",
]


class JobKind(StrEnum):
    """Kind of background job."""

    build_processing = "build_processing"
    edition_update = "edition_update"
    dashboard_sync = "dashboard_sync"
    lifecycle_eval = "lifecycle_eval"
    git_ref_audit = "git_ref_audit"
    purgatory_cleanup = "purgatory_cleanup"
    credential_reencrypt = "credential_reencrypt"


class JobStatus(StrEnum):
    """Status of a queue job."""

    queued = "queued"
    in_progress = "in_progress"
    completed = "completed"
    completed_with_errors = "completed_with_errors"
    failed = "failed"
    cancelled = "cancelled"


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
