"""Pydantic models for build resources."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from ._examples import (
    EXAMPLE_BUILD_ID,
    EXAMPLE_BUILD_URL,
    EXAMPLE_JOB_URL,
    EXAMPLE_PROJECT_URL,
)

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
        default=None,
        description="Git commit SHA for the build.",
        examples=["6c8b2f01a5d34e1c9f0b2ad37e8c5d914f6a7b3c"],
    )

    github_repository: str | None = Field(
        default=None,
        description="GitHub repository (owner/repo) that produced the build.",
        examples=["lsst/pipelines_lsst_io"],
    )

    github_run_id: str | None = Field(
        default=None,
        description="GitHub Actions run ID.",
        examples=["16768139904"],
    )

    github_run_url: str | None = Field(
        default=None,
        description="Full URL to the GitHub Actions run.",
        examples=[
            "https://github.com/lsst/pipelines_lsst_io/actions/runs/"
            "16768139904"
        ],
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
    """Status of a documentation build.

    - ``pending`` — the build row exists and Docverse is waiting for the
      tarball upload.
    - ``uploaded`` — signal value used in PATCH requests to indicate
      upload completion. Never persisted or returned: the server
      transitions the build directly from ``pending`` to ``processing``
      when it receives this signal.
    - ``processing`` — a background job is unpacking the upload and
      updating tracking editions.
    - ``completed`` — processing finished successfully.
    - ``failed`` — processing failed; see the build's job for errors.
    """

    pending = "pending"
    uploaded = "uploaded"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class BuildCreate(BaseModel):
    """Request model for creating a build."""

    model_config = ConfigDict(extra="forbid")

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

    self_url: str = Field(
        description="URL to this build resource.",
        examples=[EXAMPLE_BUILD_URL],
    )

    project_url: str = Field(
        description="URL to the parent project.",
        examples=[EXAMPLE_PROJECT_URL],
    )

    id: str = Field(
        description="Public Crockford Base32 identifier for the build.",
        examples=[EXAMPLE_BUILD_ID],
    )

    git_ref: str = Field(
        description="Git ref (branch, tag, or SHA) for this build.",
        examples=["main"],
    )

    alternate_name: str | None = Field(
        default=None,
        description=(
            "Deployment variant scope for the build (e.g., 'usdf-dev')."
        ),
    )

    content_hash: str = Field(
        description="SHA-256 hash of the uploaded tarball.",
        examples=[
            "sha256:abcdef0123456789abcdef0123456789abcdef0123456789"
            "abcdef0123456789"
        ],
    )

    status: BuildStatus = Field(description="Current status of the build.")

    upload_url: str | None = Field(
        default=None,
        description="Pre-signed URL for uploading the build tarball.",
        examples=[
            "https://staging-bucket.s3.amazonaws.com/uploads/"
            "1txq-55pj-1x5m-16.tar.gz?X-Amz-Signature=0ad34c"
        ],
    )

    job_url: str | None = Field(
        default=None,
        description="URL to the job processing this build.",
        examples=[EXAMPLE_JOB_URL],
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
        description="Username of the person who uploaded the build.",
        examples=["jdoe"],
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

    model_config = ConfigDict(extra="forbid")

    status: BuildStatus | None = Field(
        default=None,
        description=(
            "Signal value. Set to 'uploaded' to indicate upload is "
            "complete and trigger processing."
        ),
    )
