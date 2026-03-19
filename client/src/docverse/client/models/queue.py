"""Pydantic models for queue job resources."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .queue_enums import JobKind, JobStatus


class QueueJob(BaseModel):
    """Response model for a queue job."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this queue job resource.")

    id: str = Field(
        description="Public Crockford Base32 identifier for the job."
    )

    kind: JobKind = Field(description="Kind of background job.")

    status: JobStatus = Field(description="Current status of the job.")

    phase: str | None = Field(
        default=None,
        description=(
            "Current processing phase (e.g., inventory, tracking,"
            " editions, dashboard)."
        ),
    )

    progress: dict[str, Any] | None = Field(
        default=None,
        description="Structured progress data, phase-specific.",
    )

    errors: dict[str, Any] | None = Field(
        default=None,
        description="Collected error details.",
    )

    date_created: datetime = Field(
        description="Timestamp when the job was enqueued."
    )

    date_started: datetime | None = Field(
        default=None,
        description="Timestamp when a worker picked up the job.",
    )

    date_completed: datetime | None = Field(
        default=None,
        description="Timestamp when the job finished.",
    )
