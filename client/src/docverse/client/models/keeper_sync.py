"""Pydantic models for LTD Keeper sync configuration and runs."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

__all__ = [
    "KeeperSyncConfig",
    "KeeperSyncRun",
    "KeeperSyncRunCreated",
    "KeeperSyncRunKind",
    "KeeperSyncRunStatus",
]


_DEFAULT_LTD_BASE_URL = "https://keeper.lsst.codes"


class KeeperSyncConfig(BaseModel):
    """LTD Keeper sync configuration for an organization.

    Stored as a JSONB blob on the ``organizations`` row and validated
    through this model on read and write. ``GET /orgs/{org}/keeper-sync``
    returns a default-disabled instance when no config has been
    persisted; ``PUT`` replaces the stored config wholesale.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False,
        description="Whether LTD Keeper sync is enabled on the organization.",
    )

    ltd_base_url: HttpUrl = Field(
        default=HttpUrl(_DEFAULT_LTD_BASE_URL),
        description="Base URL of the LTD Keeper API (v1 shape).",
    )

    project_slugs: list[str] | Literal["*"] = Field(
        default_factory=list,
        description=(
            'LTD project slugs to sync, or ``"*"`` for every project'
            " visible on the LTD instance."
        ),
    )


class KeeperSyncRunKind(StrEnum):
    """Kind of LTD Keeper sync run."""

    backfill = "backfill"
    resync = "resync"
    reconcile = "reconcile"


class KeeperSyncRunStatus(StrEnum):
    """Lifecycle status of a keeper sync run.

    ``pending`` — the discovery job has been enqueued but has not yet
    fanned out any children. ``in_progress`` — discovery has enqueued
    at least one child sync job. ``succeeded`` / ``partial_failure`` /
    ``failed`` are terminal states.
    """

    pending = "pending"
    in_progress = "in_progress"
    succeeded = "succeeded"
    partial_failure = "partial_failure"
    failed = "failed"


class KeeperSyncRun(BaseModel):
    """Response model for a keeper sync run resource."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this run resource.")

    id: int = Field(description="Numeric identifier for the run.")

    kind: KeeperSyncRunKind = Field(description="Kind of run.")

    status: KeeperSyncRunStatus = Field(description="Lifecycle status.")

    pending_count: int = Field(
        description=(
            "Number of fanned-out child queue jobs still in a non-terminal"
            " state (queued or in_progress)."
        )
    )

    succeeded_count: int = Field(
        description="Number of fanned-out child queue jobs that succeeded."
    )

    failed_count: int = Field(
        description=(
            "Number of fanned-out child queue jobs that ended in a failure"
            " state (failed or cancelled)."
        )
    )

    total_count: int = Field(
        description=(
            "Total number of fanned-out child queue jobs attributed to"
            " this run."
        )
    )

    date_started: datetime = Field(
        description="Timestamp when the run row was created."
    )

    date_finished: datetime | None = Field(
        default=None,
        description="Timestamp when the run reached a terminal status.",
    )

    date_last_activity: datetime | None = Field(
        default=None,
        description=(
            "Most-recent state-transition timestamp across the run's"
            " attributed child queue jobs. Operators can poll this to"
            " detect a stuck run without paginating its children."
            " ``null`` while the run has no attributed queue jobs yet."
        ),
    )


class KeeperSyncRunCreated(BaseModel):
    """Response body returned by ``POST /orgs/{org}/keeper-sync/runs``."""

    model_config = ConfigDict(from_attributes=True)

    run: KeeperSyncRun = Field(description="The newly created run.")

    queue_job_id: str = Field(
        description=(
            "Public Base32 identifier for the enqueued"
            " ``keeper_sync_run_discovery`` queue job."
        )
    )

    queue_job_url: str = Field(
        description="URL of the enqueued discovery queue job resource."
    )
