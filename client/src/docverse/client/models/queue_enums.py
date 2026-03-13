"""Enumerations for queue job resources."""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "JobKind",
    "JobStatus",
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
