"""Pydantic models for organization resources."""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class UrlScheme(StrEnum):
    """URL scheme for serving documentation within an organization."""

    subdomain = "subdomain"
    """Each project is served on its own subdomain."""

    path_prefix = "path_prefix"
    """Each project is served under a path prefix."""


class OrganizationCreate(BaseModel):
    """Request model for creating an organization."""

    slug: Annotated[
        str,
        Field(
            pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
            min_length=2,
            max_length=64,
            description="URL-safe identifier for the organization.",
            examples=["lsst"],
        ),
    ]

    title: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            description="Display title for the organization.",
            examples=["Rubin Observatory"],
        ),
    ]

    base_domain: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            description="Base domain for documentation sites.",
            examples=["lsst.io"],
        ),
    ]

    url_scheme: UrlScheme = Field(
        default=UrlScheme.subdomain,
        description="URL scheme for project documentation.",
    )

    root_path_prefix: str = Field(
        default="/",
        description="Root path prefix when using path_prefix URL scheme.",
    )

    slug_rewrite_rules: dict[str, Any] | None = Field(
        default=None,
        description="Rules for rewriting project slugs in URLs.",
    )

    lifecycle_rules: dict[str, Any] | None = Field(
        default=None,
        description="Rules governing build lifecycle.",
    )

    purgatory_retention: int = Field(
        default=2592000,
        ge=0,
        description=(
            "Duration in seconds to retain inactive builds in purgatory"
            " before deletion."
        ),
    )

    publishing_credential_label: str | None = Field(
        default=None,
        description="Label of the credential used for the publishing store.",
    )

    staging_credential_label: str | None = Field(
        default=None,
        description="Label of the credential used for the staging store.",
    )


class Organization(BaseModel):
    """Response model for an organization."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this organization resource.")

    slug: str = Field(description="URL-safe identifier for the organization.")

    title: str = Field(description="Display title for the organization.")

    base_domain: str = Field(
        description="Base domain for documentation sites."
    )

    url_scheme: UrlScheme = Field(
        description="URL scheme for project documentation."
    )

    root_path_prefix: str = Field(
        description="Root path prefix when using path_prefix URL scheme."
    )

    slug_rewrite_rules: dict[str, Any] | None = Field(
        description="Rules for rewriting project slugs in URLs."
    )

    lifecycle_rules: dict[str, Any] | None = Field(
        description="Rules governing build lifecycle."
    )

    purgatory_retention: int = Field(
        description=(
            "Duration in seconds to retain inactive builds in purgatory"
            " before deletion."
        ),
    )

    @field_validator("purgatory_retention", mode="before")
    @classmethod
    def _coerce_timedelta_fields(cls, v: int | timedelta) -> int:
        if isinstance(v, timedelta):
            return int(v.total_seconds())
        return v

    publishing_credential_label: str | None = Field(
        default=None,
        description="Label of the credential used for the publishing store.",
    )

    staging_credential_label: str | None = Field(
        default=None,
        description="Label of the credential used for the staging store.",
    )

    date_created: datetime = Field(
        description="Timestamp when the organization was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )


class OrganizationUpdate(BaseModel):
    """Request model for updating an organization (PATCH)."""

    title: str | None = Field(
        default=None, description="Display title for the organization."
    )

    base_domain: str | None = Field(
        default=None,
        description="Base domain for documentation sites.",
    )

    url_scheme: UrlScheme | None = Field(
        default=None,
        description="URL scheme for project documentation.",
    )

    root_path_prefix: str | None = Field(
        default=None,
        description=("Root path prefix when using path_prefix URL scheme."),
    )

    slug_rewrite_rules: dict[str, Any] | None = Field(
        default=None,
        description="Rules for rewriting project slugs in URLs.",
    )

    lifecycle_rules: dict[str, Any] | None = Field(
        default=None,
        description="Rules governing build lifecycle.",
    )

    purgatory_retention: int | None = Field(
        default=None,
        ge=0,
        description=(
            "Duration in seconds to retain inactive builds in purgatory"
            " before deletion."
        ),
    )

    publishing_credential_label: str | None = Field(
        default=None,
        description="Label of the credential used for the publishing store.",
    )

    staging_credential_label: str | None = Field(
        default=None,
        description="Label of the credential used for the staging store.",
    )
