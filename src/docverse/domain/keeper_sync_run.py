"""Domain models for keeper-sync runs."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from docverse.client.models import KeeperSyncRunKind, KeeperSyncRunStatus

__all__ = [
    "KeeperSyncRun",
    "KeeperSyncRunCounters",
    "KeeperSyncRunKind",
    "KeeperSyncRunStatus",
    "KeeperSyncRunWithCounters",
]


class KeeperSyncRun(BaseModel):
    """Domain representation of a single ``keeper_sync_runs`` row."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Primary key for the run.")
    org_id: int = Field(description="Organization that owns the run.")
    kind: KeeperSyncRunKind = Field(description="Kind of run.")
    status: KeeperSyncRunStatus = Field(description="Lifecycle status.")
    date_started: datetime = Field(
        description="Timestamp when the run row was created."
    )
    date_finished: datetime | None = Field(
        default=None,
        description="Timestamp when the run reached a terminal status.",
    )


class KeeperSyncRunCounters(BaseModel):
    """Aggregate child-job counters for a single run.

    Computed from ``queue_jobs`` rows filtered on ``keeper_sync_run_id``
    so a row is never out of step with the underlying job state.
    """

    model_config = ConfigDict(frozen=True)

    pending_count: int = Field(
        description=(
            "Number of attributed queue jobs in ``queued`` or"
            " ``in_progress`` state (i.e. non-terminal)."
        )
    )
    succeeded_count: int = Field(
        description=(
            "Number of attributed queue jobs in ``completed`` state."
            " ``completed_with_errors`` is excluded so soft-failure"
            " distinguishes from clean success."
        )
    )
    failed_count: int = Field(
        description=(
            "Number of attributed queue jobs in ``failed``,"
            " ``cancelled`` or ``completed_with_errors`` state."
        )
    )
    total_count: int = Field(
        description="Total number of attributed queue jobs."
    )


class KeeperSyncRunWithCounters(BaseModel):
    """Domain pair of a run plus its derived counters."""

    model_config = ConfigDict(frozen=True)

    run: KeeperSyncRun
    counters: KeeperSyncRunCounters
