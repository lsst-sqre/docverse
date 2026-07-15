"""Domain models for keeper-sync runs."""

from __future__ import annotations

from datetime import datetime

from docverse.client.models import KeeperSyncRunKind, KeeperSyncRunStatus
from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "KeeperSyncRun",
    "KeeperSyncRunActivity",
    "KeeperSyncRunKind",
    "KeeperSyncRunStatus",
    "KeeperSyncRunWithActivity",
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


class KeeperSyncRunActivity(BaseModel):
    """Aggregate child-job activity for a single run.

    Computed from ``queue_jobs`` rows filtered on ``keeper_sync_run_id``
    so a row is never out of step with the underlying job state. Carries
    the four count buckets plus a ``date_last_activity`` timestamp so
    operators have a single top-level signal for "is this run actually
    making progress" without paginating through children.
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
    date_last_activity: datetime | None = Field(
        default=None,
        description=(
            "Most-recent state-transition timestamp across the run's"
            " attributed queue jobs, computed as ``MAX(coalesce("
            "date_completed, date_started, date_created))``. ``None``"
            " when the run has no attributed queue jobs yet."
        ),
    )


class KeeperSyncRunWithActivity(BaseModel):
    """Domain pair of a run plus its derived activity."""

    model_config = ConfigDict(frozen=True)

    run: KeeperSyncRun
    activity: KeeperSyncRunActivity
