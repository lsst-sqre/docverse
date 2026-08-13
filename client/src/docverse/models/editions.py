"""Pydantic models for edition resources."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

from ._examples import (
    EXAMPLE_BUILD_ID,
    EXAMPLE_BUILD_URL,
    EXAMPLE_EDITION_URL,
    EXAMPLE_PROJECT_URL,
)
from .builds import BuildAnnotations, BuildStatus
from .queue_enums import PublishStatus

__all__ = [
    "DefaultEditionConfig",
    "Edition",
    "EditionAutocreationConfig",
    "EditionBuildHistoryEntry",
    "EditionCreate",
    "EditionKind",
    "EditionRollback",
    "EditionUpdate",
    "TrackingMode",
]


class EditionKind(StrEnum):
    """Kind of edition.

    - ``main`` — the project's default edition (slug ``__main``), the
      one served at the project's root URL.
    - ``release`` — a pinned release version (e.g. ``v1.0.0``).
    - ``draft`` — a work-in-progress edition, typically tracking a
      development branch or ticket branch.
    - ``major`` — an auto-created rollup for a major version line
      (e.g. ``v1``), kept at the highest release within that line.
    - ``minor`` — an auto-created rollup for a minor version line
      (e.g. ``v1.2``), kept at the highest patch within that line.
    - ``alternate`` — a deployment-variant edition matched only by
      builds carrying the same ``alternate_name`` scope.
    """

    main = "main"
    release = "release"
    draft = "draft"
    major = "major"
    minor = "minor"
    alternate = "alternate"


class TrackingMode(StrEnum):
    """How an edition tracks builds for automatic updates.

    - ``git_ref`` — follow builds from one git ref, named in
      ``tracking_params`` (e.g. ``{"git_ref": "main"}``).
    - ``lsst_doc`` — follow the highest LSST document version tag
      (``v<major>.<minor>[.<patch>]``, e.g. ``v1.0``).
    - ``eups_major_release`` — follow the highest two-component EUPS
      release tag (``v<major>_<minor>``, e.g. ``v12_0``).
    - ``eups_weekly_release`` — follow the highest EUPS weekly tag
      (``w_<year>_<week>``, e.g. ``w_2026_30``).
    - ``eups_daily_release`` — follow the highest EUPS daily tag
      (``d_<year>_<month>_<day>``, e.g. ``d_2026_07_29``).
    - ``semver_release`` — follow the highest semantic-version tag
      overall (e.g. ``v2.3.0``).
    - ``semver_major`` — follow the highest release within one major
      version line (used by auto-created ``major`` editions).
    - ``semver_minor`` — follow the highest patch within one minor
      version line (used by auto-created ``minor`` editions).
    - ``alternate_git_ref`` — like ``git_ref``, but match only builds
      whose ``alternate_name`` equals the edition's variant scope.
    """

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


class EditionAutocreationConfig(BaseModel):
    """Which editions Docverse auto-creates when a build lands.

    Set on an organization and/or a project; the project value wins
    whole-object when both are present, and ``null`` at both levels
    means "use the defaults" (every knob below at its default).

    Instances are frozen: the server hands out one shared default config
    to every project that configures none, so a consumer mutating what it
    resolved would rewrite the defaults process-wide. Derive a variant
    with ``model_copy(update=...)`` instead.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    semver_aggregates: bool = Field(
        default=True,
        description=(
            "Whether a stable semver release (e.g. ``1.2.3``) also"
            " auto-creates its ``N`` (major) and ``N.M`` (minor) aggregate"
            " editions. Set to ``false`` for projects that publish only"
            " concrete versions."
        ),
    )


class EditionCreate(BaseModel):
    """Request model for creating an edition."""

    model_config = ConfigDict(extra="forbid")

    slug: Annotated[
        str,
        Field(
            pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9]$",
            min_length=2,
            max_length=128,
            description="URL-safe identifier for the edition.",
            examples=["main", "v1", "DM-54112", "v2.3.0", "foo_bar"],
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

    self_url: str = Field(
        description="URL to this edition resource.",
        examples=[EXAMPLE_EDITION_URL],
    )

    project_url: str = Field(
        description="URL to the parent project.",
        examples=[EXAMPLE_PROJECT_URL],
    )

    build_url: str | None = Field(
        default=None,
        description="URL to the currently published build.",
        examples=[EXAMPLE_BUILD_URL],
    )

    published_url: str | None = Field(
        default=None,
        description="Public URL where this edition is served.",
        examples=["https://pipelines.lsst.io/v/v1"],
    )

    history_url: str = Field(
        description="URL to the build history for this edition.",
        examples=[f"{EXAMPLE_EDITION_URL}/history"],
    )

    rollback_url: str = Field(
        description="URL to roll back this edition to a previous build.",
        examples=[f"{EXAMPLE_EDITION_URL}/rollback"],
    )

    slug: str = Field(
        description="URL-safe identifier for the edition.",
        examples=["v1"],
    )

    title: str = Field(
        description="Display title for the edition.",
        examples=["v1.0"],
    )

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

    publish_status: PublishStatus | None = Field(
        default=None,
        description=(
            "CDN publish state for the edition's current build. ``None`` when"
            " the edition has never been associated with a build or predates"
            " publish-status tracking."
        ),
    )

    date_created: datetime = Field(
        description="Timestamp when the edition was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )


class EditionBuildHistoryEntry(BaseModel):
    """Response model for an edition build history entry."""

    build_id: str = Field(
        description="Base32-encoded public ID of the build.",
        examples=[EXAMPLE_BUILD_ID],
    )

    build_url: str = Field(
        description="URL to the build resource.",
        examples=[EXAMPLE_BUILD_URL],
    )

    git_ref: str = Field(description="Git reference of the build.")

    build_status: BuildStatus = Field(
        description="Status of the build at the time of the query."
    )

    annotations: BuildAnnotations | None = Field(
        default=None,
        description="Build provenance annotations.",
    )

    build_deleted: bool = Field(
        description="Whether the build has been soft-deleted."
    )

    position: int = Field(
        description="Position in history; 1 is the most recent."
    )

    date_created: datetime = Field(
        description="Timestamp when this history entry was recorded."
    )


class EditionRollback(BaseModel):
    """Request body for rolling back an edition to a previous build."""

    build: str = Field(
        description="Base32 public ID of the target build.",
        examples=[EXAMPLE_BUILD_ID],
    )


class EditionUpdate(BaseModel):
    """Request model for updating an edition (PATCH)."""

    model_config = ConfigDict(extra="forbid")

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

    build: str | None = Field(
        default=None,
        description=(
            "Base32 public ID of a build to point this edition at. "
            "Emergency-override path: unlike rollback, the build does not "
            "have to be in the edition's history."
        ),
        examples=[EXAMPLE_BUILD_ID],
    )
