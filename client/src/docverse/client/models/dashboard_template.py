"""Pydantic models for dashboard-template binding resources."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "DashboardTemplateBinding",
    "DashboardTemplateBindingCreate",
]


class DashboardTemplateBindingCreate(BaseModel):
    """Request model for creating or updating a dashboard-template binding."""

    model_config = ConfigDict(extra="forbid")

    github_owner: Annotated[
        str,
        Field(
            min_length=1,
            max_length=39,
            pattern=r"^[A-Za-z0-9](?:-?[A-Za-z0-9])*$",
            description="GitHub owner (user or organization login).",
        ),
    ]

    github_repo: Annotated[
        str,
        Field(
            min_length=1,
            max_length=100,
            pattern=r"^[A-Za-z0-9._-]+$",
            description="GitHub repository name.",
        ),
    ]

    github_ref: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            description="Git ref (branch, tag, or commit SHA) to sync from.",
        ),
    ]

    root_path: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Path within the repository where the template lives."
                " Defaults to ``/`` (repo root)."
            ),
        ),
    ] = "/"


class DashboardTemplateBinding(BaseModel):
    """Response model for a dashboard-template binding."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this binding resource.")

    github_owner: str = Field(description="GitHub owner (user or org).")

    github_repo: str = Field(description="GitHub repository name.")

    github_ref: str = Field(description="Git ref (branch, tag, or SHA).")

    root_path: str = Field(
        description="Path within the repo where the template lives."
    )

    last_sync_status: str = Field(
        description="One of ``pending``, ``succeeded``, ``failed``."
    )

    last_sync_error: str | None = Field(
        default=None,
        description="Operator-readable error from the most recent sync.",
    )

    date_created: datetime = Field(
        description="Timestamp when the binding was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )
