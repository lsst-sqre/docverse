"""Pydantic models for project resources."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

from .editions import DefaultEditionConfig
from .editions import Edition as EditionResponse


class ProjectCreate(BaseModel):
    """Request model for creating a project."""

    slug: Annotated[
        str,
        Field(
            pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
            min_length=2,
            max_length=128,
            description="URL-safe identifier for the project.",
            examples=["pipelines"],
        ),
    ]

    title: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            description="Display title for the project.",
            examples=["LSST Science Pipelines"],
        ),
    ]

    doc_repo: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description="URL of the documentation source repository.",
            examples=["https://github.com/lsst/pipelines_lsst_io"],
        ),
    ]

    default_edition: DefaultEditionConfig | None = Field(
        default=None,
        description=(
            "Configuration for the default edition. If omitted, the"
            " organization default or hardcoded default is used."
        ),
    )


class Project(BaseModel):
    """Response model for a project."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this project resource.")

    org_url: str = Field(description="URL to the parent organization.")

    editions_url: str = Field(
        description="URL to list editions for this project."
    )

    builds_url: str = Field(description="URL to list builds for this project.")

    slug: str = Field(description="URL-safe identifier for the project.")

    title: str = Field(description="Display title for the project.")

    doc_repo: str = Field(
        description="URL of the documentation source repository."
    )

    slug_rewrite_rules: list[dict[str, Any]] | None = Field(
        default=None,
        description="Rules for rewriting project slugs in URLs.",
    )

    lifecycle_rules: list[dict[str, Any]] | None = Field(
        default=None,
        description="Rules governing build lifecycle.",
    )

    default_edition: EditionResponse | None = Field(
        default=None,
        description=(
            "The default (__main) edition for this project. Populated on"
            " single-project responses (GET, POST, PATCH) but omitted"
            " from list responses."
        ),
    )

    date_created: datetime = Field(
        description="Timestamp when the project was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )


class ProjectUpdate(BaseModel):
    """Request model for updating a project (PATCH)."""

    title: str | None = Field(
        default=None, description="Display title for the project."
    )

    doc_repo: str | None = Field(
        default=None,
        description="URL of the documentation source repository.",
    )

    slug_rewrite_rules: list[dict[str, Any]] | None = Field(
        default=None,
        description="Rules for rewriting project slugs in URLs.",
    )

    lifecycle_rules: list[dict[str, Any]] | None = Field(
        default=None,
        description="Rules governing build lifecycle.",
    )
