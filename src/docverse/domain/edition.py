"""Domain model for editions."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from docverse.client.models import EditionKind, TrackingMode

from .base32id import Base32Id


class Edition(BaseModel):
    """Domain representation of an edition."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the edition.")

    slug: str = Field(description="URL-safe identifier for the edition.")

    title: str = Field(description="Display title for the edition.")

    project_id: int = Field(
        description="ID of the project this edition belongs to."
    )

    kind: EditionKind = Field(description="Kind of edition.")

    tracking_mode: TrackingMode = Field(
        description="How this edition tracks builds for automatic updates."
    )

    tracking_params: dict[str, Any] | None = Field(
        default=None,
        description="Parameters for the tracking mode.",
    )

    current_build_id: int | None = Field(
        default=None,
        description="ID of the currently published build.",
    )

    current_build_public_id: Base32Id | None = Field(
        default=None,
        description=(
            "Public Base32 ID of the currently published build. "
            "Populated via join."
        ),
    )

    current_build_git_ref: str | None = Field(
        default=None,
        description=(
            "Git ref of the currently published build. Populated via join."
        ),
    )

    lifecycle_exempt: bool = Field(
        default=False,
        description="Whether this edition is exempt from lifecycle rules.",
    )

    date_created: datetime = Field(
        description="Timestamp when the edition was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )

    date_deleted: datetime | None = Field(
        default=None,
        description="Timestamp when the edition was soft-deleted.",
    )
