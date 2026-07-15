"""Domain model for builds."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from docverse.client.models import BuildStatus
from docverse.client.models.builds import BuildAnnotations

from .base32id import Base32Id


class Build(BaseModel):
    """Domain representation of a documentation build."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the build.")

    public_id: Base32Id = Field(
        description="Public Crockford Base32 identifier for the build."
    )

    project_id: int = Field(
        description="ID of the project this build belongs to."
    )

    git_ref: str = Field(
        description="Git ref (branch, tag, or SHA) for this build."
    )

    alternate_name: str | None = Field(
        default=None,
        description="Alternate identifier for the build.",
    )

    content_hash: str = Field(
        description="SHA-256 hash of the uploaded tarball."
    )

    status: BuildStatus = Field(description="Current status of the build.")

    staging_key: str = Field(
        description="Object store key for the staged tarball."
    )

    storage_prefix: str = Field(
        description=(
            "Object store prefix for build artifacts, e.g. "
            "'{project_slug}/__builds/{base32_id}/'."
        )
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

    date_deleted: datetime | None = Field(
        default=None,
        description="Timestamp when the build was soft-deleted.",
    )
