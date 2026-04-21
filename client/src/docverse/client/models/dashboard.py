"""Pydantic models for dashboard rebuild responses."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "DashboardRebuildResponse",
    "OrgDashboardRebuildEntry",
]


class DashboardRebuildResponse(BaseModel):
    """Response body for the manual dashboard rebuild endpoint."""

    model_config = ConfigDict(from_attributes=True)

    queue_job_id: str = Field(
        description="Public Base32 identifier for the enqueued job."
    )

    queue_job_url: str = Field(
        description="URL to the enqueued queue job resource."
    )


class OrgDashboardRebuildEntry(BaseModel):
    """One enqueued ``dashboard_build`` in the org-wide rebuild response."""

    model_config = ConfigDict(from_attributes=True)

    project_slug: str = Field(
        description="Slug of the project the job will rebuild."
    )

    queue_job_id: str = Field(
        description="Public Base32 identifier for the enqueued job."
    )

    queue_job_url: str = Field(
        description="URL to the enqueued queue job resource."
    )
