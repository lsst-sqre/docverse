"""Pydantic models for edition resources."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "DefaultEditionConfig",
    "Edition",
    "EditionCreate",
    "EditionKind",
    "EditionUpdate",
    "TrackingMode",
]


class EditionKind(StrEnum):
    """Kind of edition."""

    main = "main"
    release = "release"
    draft = "draft"
    major = "major"
    minor = "minor"
    alternate = "alternate"


class TrackingMode(StrEnum):
    """How an edition tracks builds for automatic updates."""

    git_ref = "git_ref"
    lsst_doc = "lsst_doc"
    eups_major_release = "eups_major_release"
    eups_weekly_release = "eups_weekly_release"
    eups_daily_release = "eups_daily_release"
    semver_release = "semver_release"
    semver_major = "semver_major"
    semver_minor = "semver_minor"
    alternate_git_ref = "alternate_git_ref"


class DefaultEditionConfig(BaseModel):
    """Configuration for the default (__main) edition's tracking behavior.

    Used in project creation requests and as an organization-level default.
    When omitted from a project creation request, the organization's
    default config is used; when the organization has none, the hardcoded
    fallback is ``git_ref`` tracking the ``main`` branch.
    """

    tracking_mode: TrackingMode = Field(
        default=TrackingMode.git_ref,
        description="How the default edition tracks builds.",
    )

    tracking_params: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Parameters for the tracking mode. When None and tracking_mode"
            " is git_ref, defaults to {'git_ref': 'main'} at the service"
            " layer."
        ),
    )

    title: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            description="Display title for the default edition.",
        ),
    ] = "Main"

    lifecycle_exempt: bool = Field(
        default=True,
        description=(
            "Whether the default edition is exempt from lifecycle rules."
        ),
    )


class EditionCreate(BaseModel):
    """Request model for creating an edition."""

    slug: Annotated[
        str,
        Field(
            pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
            min_length=2,
            max_length=128,
            description="URL-safe identifier for the edition.",
            examples=["main", "v1"],
        ),
    ]

    title: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            description="Display title for the edition.",
            examples=["Latest", "v1.0"],
        ),
    ]

    kind: EditionKind = Field(description="Kind of edition.")

    tracking_mode: TrackingMode = Field(
        description="How this edition tracks builds for automatic updates."
    )

    tracking_params: dict[str, Any] | None = Field(
        default=None,
        description="Parameters for the tracking mode.",
    )

    lifecycle_exempt: bool = Field(
        default=False,
        description="Whether this edition is exempt from lifecycle rules.",
    )


class Edition(BaseModel):
    """Response model for an edition."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this edition resource.")

    project_url: str = Field(description="URL to the parent project.")

    build_url: str | None = Field(
        default=None,
        description="URL to the currently published build.",
    )

    published_url: str | None = Field(
        default=None,
        description="Public URL where this edition is served.",
    )

    slug: str = Field(description="URL-safe identifier for the edition.")

    title: str = Field(description="Display title for the edition.")

    kind: EditionKind = Field(description="Kind of edition.")

    tracking_mode: TrackingMode = Field(
        description="How this edition tracks builds for automatic updates."
    )

    tracking_params: dict[str, Any] | None = Field(
        default=None,
        description="Parameters for the tracking mode.",
    )

    lifecycle_exempt: bool = Field(
        description="Whether this edition is exempt from lifecycle rules."
    )

    date_created: datetime = Field(
        description="Timestamp when the edition was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )


class EditionUpdate(BaseModel):
    """Request model for updating an edition (PATCH)."""

    title: str | None = Field(
        default=None, description="Display title for the edition."
    )

    kind: EditionKind | None = Field(
        default=None, description="Kind of edition."
    )

    tracking_mode: TrackingMode | None = Field(
        default=None,
        description="How this edition tracks builds for automatic updates.",
    )

    tracking_params: dict[str, Any] | None = Field(
        default=None,
        description="Parameters for the tracking mode.",
    )

    lifecycle_exempt: bool | None = Field(
        default=None,
        description="Whether this edition is exempt from lifecycle rules.",
    )
