"""Pydantic models for organization credential resources."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "OrganizationCredential",
    "OrganizationCredentialCreate",
]


class OrganizationCredentialCreate(BaseModel):
    """Request model for creating an organization credential."""

    label: Annotated[
        str,
        Field(
            min_length=1,
            max_length=128,
            pattern=r"^[a-z0-9][a-z0-9._-]*[a-z0-9]$",
            description=(
                "Human-readable label for the credential, unique per"
                " organization."
            ),
            examples=["primary-s3", "staging-r2"],
        ),
    ]

    service_type: Annotated[
        str,
        Field(
            min_length=1,
            max_length=32,
            description="Object store service type.",
            examples=["s3", "r2", "gcs"],
        ),
    ]

    credential: dict[str, Any] = Field(
        description=(
            "Service-specific credential payload. For S3-compatible"
            " stores: ``endpoint_url``, ``bucket``, ``access_key_id``,"
            " ``secret_access_key``, and optionally ``region``."
        ),
    )


class OrganizationCredential(BaseModel):
    """Response model for an organization credential.

    The credential payload is never returned in responses.
    """

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this credential resource.")

    org_url: str = Field(description="URL to the parent organization.")

    label: str = Field(
        description=(
            "Human-readable label for the credential, unique per organization."
        ),
    )

    service_type: str = Field(description="Object store service type.")

    date_created: datetime = Field(
        description="Timestamp when the credential was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )
