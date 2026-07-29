"""Pydantic models for dashboard rebuild responses."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from ._examples import EXAMPLE_JOB_ID, EXAMPLE_JOB_URL

__all__ = [
    "DashboardRebuildResponse",
    "OrgDashboardRebuildEntry",
    "OrgDashboardRebuildResponse",
]


class DashboardRebuildResponse(BaseModel):
    """Response body for the manual dashboard rebuild endpoint."""

    model_config = ConfigDict(from_attributes=True)

    job_id: str = Field(
        description="Public Base32 identifier for the enqueued job.",
        examples=[EXAMPLE_JOB_ID],
    )

    job_url: str = Field(
        description="URL to the enqueued job resource.",
        examples=[EXAMPLE_JOB_URL],
    )


class OrgDashboardRebuildEntry(BaseModel):
    """One enqueued ``dashboard_build`` in the org-wide rebuild response."""

    model_config = ConfigDict(from_attributes=True)

    project_slug: str = Field(
        description="Slug of the project the job will rebuild.",
        examples=["pipelines"],
    )

    job_id: str = Field(
        description="Public Base32 identifier for the enqueued job.",
        examples=[EXAMPLE_JOB_ID],
    )

    job_url: str = Field(
        description="URL to the enqueued job resource.",
        examples=[EXAMPLE_JOB_URL],
    )


class OrgDashboardRebuildResponse(BaseModel):
    """Response body for the org-wide dashboard rebuild endpoint.

    The enqueued jobs are wrapped in an object envelope (rather than a
    bare JSON array) so the response can grow additional fields without a
    breaking change.
    """

    model_config = ConfigDict(from_attributes=True)

    entries: list[OrgDashboardRebuildEntry] = Field(
        default_factory=list,
        description=(
            "One entry per project for which a ``dashboard_build`` job was"
            " enqueued."
        ),
    )
