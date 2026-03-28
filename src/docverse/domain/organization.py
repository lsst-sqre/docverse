"""Domain model for organizations."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from docverse.client.models import UrlScheme


class Organization(BaseModel):
    """Domain representation of an organization."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the organization.")

    slug: str = Field(description="URL-safe identifier for the organization.")

    title: str = Field(description="Display title for the organization.")

    base_domain: str = Field(
        description="Base domain for documentation sites."
    )

    url_scheme: UrlScheme = Field(
        description="URL scheme for project documentation."
    )

    root_path_prefix: str = Field(
        description=("Root path prefix when using path_prefix URL scheme."),
    )

    slug_rewrite_rules: list[dict[str, Any]] | None = Field(
        description="Rules for rewriting project slugs in URLs."
    )

    lifecycle_rules: list[dict[str, Any]] | None = Field(
        description="Rules governing build lifecycle."
    )

    default_edition_config: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Default configuration for the __main edition on new projects."
        ),
    )

    publishing_store_label: str | None = Field(
        default=None,
        description="Label of the object_storage service for publishing.",
    )

    staging_store_label: str | None = Field(
        default=None,
        description="Label of the object_storage service for staging.",
    )

    cdn_service_label: str | None = Field(
        default=None,
        description="Label of the cdn service.",
    )

    dns_service_label: str | None = Field(
        default=None,
        description="Label of the dns service.",
    )

    purgatory_retention: timedelta = Field(
        description=(
            "Duration to retain inactive builds in purgatory before deletion."
        ),
    )

    @property
    def resolved_staging_store_label(self) -> str | None:
        """Staging store label, falling back to the publishing store."""
        return self.staging_store_label or self.publishing_store_label

    date_created: datetime = Field(
        description="Timestamp when the organization was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )
