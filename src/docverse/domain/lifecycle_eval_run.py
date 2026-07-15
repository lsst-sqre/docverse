"""Domain models for ``lifecycle_eval`` runs."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from docverse.client.models import LifecycleEvalRunStatus
from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "LifecycleEvalRun",
    "LifecycleEvalRunActivity",
    "LifecycleEvalRunStatus",
]


class LifecycleEvalRun(BaseModel):
    """Domain representation of a single ``lifecycle_eval_runs`` row.

    The dispatcher cron writes one of these per tick. Unlike
    ``KeeperSyncRun`` there is no ``org_id``: lifecycle_eval is a
    system-wide tick that fans out per-org child jobs in one pass, so
    the aggregate row is global and the partial-unique non-terminal
    index enforces a singleton in-flight tick at the DB level.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Primary key for the run.")
    status: LifecycleEvalRunStatus = Field(description="Lifecycle status.")
    date_started: datetime = Field(
        description="Timestamp when the dispatcher tick began."
    )
    date_finished: datetime | None = Field(
        default=None,
        description="Timestamp when the run reached a terminal status.",
    )
    summary: dict[str, Any] | None = Field(
        default=None,
        description=(
            "JSONB summary written by the dispatcher: orgs enqueued,"
            " orgs skipped, and any other tick-level metadata."
        ),
    )


class LifecycleEvalRunActivity(BaseModel):
    """Aggregate child-job activity for a single lifecycle_eval run.

    Computed from ``queue_jobs`` rows filtered on
    ``lifecycle_eval_run_id`` so a row is never out of step with the
    underlying job state. Same shape as ``KeeperSyncRunActivity`` so
    ``maybe_finalise_lifecycle_run`` can mirror ``maybe_finalise_run``
    without re-typing the counters.
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
