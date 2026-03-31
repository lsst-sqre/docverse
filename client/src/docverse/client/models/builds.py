"""Pydantic models for build resources."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "Build",
    "BuildAnnotations",
    "BuildCreate",
    "BuildStatus",
    "BuildUpdate",
]


class BuildAnnotations(BaseModel):
    """Well-known provenance fields for build annotations.

    All fields are optional. The model uses ``extra="allow"`` so callers
    can attach arbitrary additional metadata.
    """

    model_config = ConfigDict(extra="allow")

    commit_sha: str | None = Field(
        default=None, description="Git commit SHA for the build."
    )

    github_repository: str | None = Field(
        default=None,
        description="GitHub repository (owner/repo) that produced the build.",
    )

    github_run_id: str | None = Field(
        default=None, description="GitHub Actions run ID."
    )

    github_run_url: str | None = Field(
        default=None, description="Full URL to the GitHub Actions run."
    )

    github_run_attempt: str | None = Field(
        default=None, description="GitHub Actions run attempt number."
    )

    github_workflow: str | None = Field(
        default=None, description="GitHub Actions workflow name."
    )

    github_actor: str | None = Field(
        default=None, description="GitHub user or app that triggered the run."
    )

    github_event_name: str | None = Field(
        default=None,
        description="GitHub event that triggered the workflow (e.g. push).",
    )

    ci_platform: str | None = Field(
        default=None,
        description=(
            "CI platform that produced the build (e.g. github-actions)."
        ),
    )


class BuildStatus(StrEnum):
    """Status of a documentation build."""

    pending = "pending"
    uploaded = "uploaded"
    """Signal value used in PATCH requests to indicate upload completion.

    This status is never persisted to the database. The server transitions
    the build directly from ``pending`` to ``processing`` when it receives
    this signal.
    """

    processing = "processing"
    completed = "completed"
    failed = "failed"


class BuildCreate(BaseModel):
    """Request model for creating a build."""

    git_ref: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            description="Git ref (branch, tag, or SHA) for this build.",
            examples=["main", "v1.0.0"],
        ),
    ]

    alternate_name: str | None = Field(
        default=None,
        max_length=128,
        description=(
            "Deployment variant scope for the build (e.g., 'usdf-dev'). "
            "When set, the build is matched only by alternate-aware editions."
        ),
    )

    content_hash: Annotated[
        str,
        Field(
            pattern=r"^sha256:[a-f0-9]{64}$",
            description="SHA-256 hash of the uploaded tarball.",
            examples=[
                "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            ],
        ),
    ]

    annotations: BuildAnnotations | None = Field(
        default=None,
        description="Arbitrary metadata annotations for the build.",
    )


class Build(BaseModel):
    """Response model for a build."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this build resource.")

    project_url: str = Field(description="URL to the parent project.")

    id: str = Field(
        description="Public Crockford Base32 identifier for the build."
    )

    git_ref: str = Field(
        description="Git ref (branch, tag, or SHA) for this build."
    )

    alternate_name: str | None = Field(
        default=None,
        description=(
            "Deployment variant scope for the build (e.g., 'usdf-dev')."
        ),
    )

    content_hash: str = Field(
        description="SHA-256 hash of the uploaded tarball."
    )

    status: BuildStatus = Field(description="Current status of the build.")

    upload_url: str | None = Field(
        default=None,
        description="Pre-signed URL for uploading the build tarball.",
    )

    queue_url: str | None = Field(
        default=None,
        description="URL to the queue job processing this build.",
    )

    object_count: int | None = Field(
        default=None,
        description="Number of objects in the build.",
    )

    total_size_bytes: int | None = Field(
        default=None,
        description="Total size of all objects in bytes.",
    )

    uploader: str = Field(
        description="Username of the person who uploaded the build."
    )

    annotations: BuildAnnotations | None = Field(
        default=None,
        description="Arbitrary metadata annotations for the build.",
    )

    date_created: datetime = Field(
        description="Timestamp when the build was created."
    )

    date_uploaded: datetime | None = Field(
        default=None,
        description="Timestamp when upload completed and processing began.",
    )

    date_completed: datetime | None = Field(
        default=None,
        description="Timestamp when processing completed or failed.",
    )


class BuildUpdate(BaseModel):
    """Request model for updating a build (PATCH).

    Currently supports only the ``status`` field, used to signal
    that an upload is complete (set ``status`` to ``"uploaded"``).
    """

    status: BuildStatus | None = Field(
        default=None,
        description=(
            "Signal value. Set to 'uploaded' to indicate upload is "
            "complete and trigger processing."
        ),
    )
