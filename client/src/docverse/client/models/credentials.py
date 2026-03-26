"""Pydantic models for organization credential resources."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag

from .infrastructure import CredentialProvider

__all__ = [
    "AwsCredentials",
    "CloudflareCredentials",
    "CredentialPayload",
    "FastlyCredentials",
    "GcpCredentials",
    "OrganizationCredential",
    "OrganizationCredentialCreate",
    "S3Credentials",
]


# --- Typed credential payloads per provider ---


class AwsCredentials(BaseModel):
    """AWS credential payload."""

    provider: Literal["aws"] = "aws"
    access_key_id: str = Field(description="AWS access key ID.")
    secret_access_key: str = Field(description="AWS secret access key.")


class CloudflareCredentials(BaseModel):
    """Cloudflare credential payload."""

    provider: Literal["cloudflare"] = "cloudflare"
    api_token: str = Field(description="Cloudflare API token.")


class FastlyCredentials(BaseModel):
    """Fastly credential payload."""

    provider: Literal["fastly"] = "fastly"
    api_token: str = Field(description="Fastly API token.")


class GcpCredentials(BaseModel):
    """Google Cloud Platform credential payload."""

    provider: Literal["gcp"] = "gcp"
    service_account_json: str = Field(
        description="GCP service account key JSON."
    )


class S3Credentials(BaseModel):
    """Generic S3-compatible credential payload."""

    provider: Literal["s3"] = "s3"
    access_key_id: str = Field(description="S3 access key ID.")
    secret_access_key: str = Field(description="S3 secret access key.")


CredentialPayload = Annotated[
    Annotated[AwsCredentials, Tag("aws")]
    | Annotated[CloudflareCredentials, Tag("cloudflare")]
    | Annotated[FastlyCredentials, Tag("fastly")]
    | Annotated[GcpCredentials, Tag("gcp")]
    | Annotated[S3Credentials, Tag("s3")],
    Discriminator("provider"),
]
"""Discriminated union of all credential payload types."""


# --- API models ---


class OrganizationCredentialCreate(BaseModel):
    """Request model for creating an organization credential."""

    label: Annotated[
        str,
        Field(
            min_length=2,
            max_length=128,
            pattern=r"^[a-z0-9][a-z0-9._-]*[a-z0-9]$",
            description=(
                "Human-readable label for the credential, unique per"
                " organization."
            ),
            examples=["aws", "cloudflare", "fastly"],
        ),
    ]

    credentials: CredentialPayload = Field(
        description=(
            "Provider-specific credential payload. The ``provider``"
            " field determines the schema."
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

    provider: CredentialProvider = Field(
        description="Cloud provider for this credential."
    )

    date_created: datetime = Field(
        description="Timestamp when the credential was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )
