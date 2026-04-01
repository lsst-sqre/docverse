"""Domain model for projects."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Project(BaseModel):
    """Domain representation of a project."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the project.")

    slug: str = Field(description="URL-safe identifier for the project.")

    title: str = Field(description="Display title for the project.")

    org_id: int = Field(
        description="ID of the organization this project belongs to."
    )

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

    date_created: datetime = Field(
        description="Timestamp when the project was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )

    date_deleted: datetime | None = Field(
        default=None,
        description="Timestamp when the project was soft-deleted.",
    )
