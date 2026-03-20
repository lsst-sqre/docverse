"""Pydantic models for organization service resources."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag

from .infrastructure import ServiceCategory, ServiceProvider

__all__ = [
    "AwsS3Config",
    "CloudflareDnsConfig",
    "CloudflareR2Config",
    "CloudflareWorkersConfig",
    "CloudfrontConfig",
    "FastlyConfig",
    "GcsConfig",
    "GoogleCloudCdnConfig",
    "MinioConfig",
    "OrganizationService",
    "OrganizationServiceCreate",
    "OrganizationServiceSummary",
    "Route53Config",
    "ServiceConfig",
]


# --- Object storage configs ---


class AwsS3Config(BaseModel):
    """Configuration for AWS S3 object storage."""

    provider: Literal["aws_s3"] = "aws_s3"
    bucket: str = Field(description="S3 bucket name.")
    region: str = Field(description="AWS region.")


class CloudflareR2Config(BaseModel):
    """Configuration for Cloudflare R2 object storage."""

    provider: Literal["cloudflare_r2"] = "cloudflare_r2"
    account_id: str = Field(description="Cloudflare account ID.")
    bucket: str = Field(description="R2 bucket name.")


class MinioConfig(BaseModel):
    """Configuration for MinIO object storage."""

    provider: Literal["minio"] = "minio"
    endpoint_url: str = Field(description="MinIO endpoint URL.")
    bucket: str = Field(description="Bucket name.")


class GcsConfig(BaseModel):
    """Configuration for Google Cloud Storage."""

    provider: Literal["gcs"] = "gcs"
    bucket: str = Field(description="GCS bucket name.")
    project_id: str = Field(description="Google Cloud project ID.")


# --- CDN configs ---


class FastlyConfig(BaseModel):
    """Configuration for Fastly CDN."""

    provider: Literal["fastly"] = "fastly"
    service_id: str = Field(description="Fastly service ID.")


class CloudflareWorkersConfig(BaseModel):
    """Configuration for Cloudflare Workers CDN."""

    provider: Literal["cloudflare_workers"] = "cloudflare_workers"
    account_id: str = Field(description="Cloudflare account ID.")
    zone_id: str = Field(description="Cloudflare zone ID.")


class CloudfrontConfig(BaseModel):
    """Configuration for AWS CloudFront CDN."""

    provider: Literal["cloudfront"] = "cloudfront"
    distribution_id: str = Field(description="CloudFront distribution ID.")


class GoogleCloudCdnConfig(BaseModel):
    """Configuration for Google Cloud CDN."""

    provider: Literal["google_cloud_cdn"] = "google_cloud_cdn"
    backend_service: str = Field(description="Backend service name.")


# --- DNS configs ---


class Route53Config(BaseModel):
    """Configuration for AWS Route 53 DNS."""

    provider: Literal["route53"] = "route53"
    hosted_zone_id: str = Field(description="Route 53 hosted zone ID.")


class CloudflareDnsConfig(BaseModel):
    """Configuration for Cloudflare DNS."""

    provider: Literal["cloudflare_dns"] = "cloudflare_dns"
    zone_id: str = Field(description="Cloudflare zone ID.")


# --- Discriminated union of all config types ---

ServiceConfig = Annotated[
    Annotated[AwsS3Config, Tag("aws_s3")]
    | Annotated[CloudflareR2Config, Tag("cloudflare_r2")]
    | Annotated[MinioConfig, Tag("minio")]
    | Annotated[GcsConfig, Tag("gcs")]
    | Annotated[FastlyConfig, Tag("fastly")]
    | Annotated[CloudflareWorkersConfig, Tag("cloudflare_workers")]
    | Annotated[CloudfrontConfig, Tag("cloudfront")]
    | Annotated[GoogleCloudCdnConfig, Tag("google_cloud_cdn")]
    | Annotated[Route53Config, Tag("route53")]
    | Annotated[CloudflareDnsConfig, Tag("cloudflare_dns")],
    Discriminator("provider"),
]
"""Discriminated union of all service configuration types."""


# --- API models ---


class OrganizationServiceCreate(BaseModel):
    """Request model for creating an organization service."""

    label: Annotated[
        str,
        Field(
            min_length=2,
            max_length=128,
            pattern=r"^[a-z0-9][a-z0-9._-]*[a-z0-9]$",
            description=(
                "Human-readable label for the service, unique per"
                " organization."
            ),
            examples=["docs-bucket", "cdn", "dns"],
        ),
    ]

    config: ServiceConfig = Field(
        description=(
            "Provider-specific service configuration. The ``provider``"
            " field determines the schema."
        ),
    )

    credential_label: Annotated[
        str,
        Field(
            min_length=1,
            max_length=128,
            description=(
                "Label of the credential to use for authentication."
                " Must reference an existing credential with a compatible"
                " provider."
            ),
        ),
    ]


class OrganizationServiceSummary(BaseModel):
    """Lightweight service reference embedded in Organization responses."""

    self_url: str = Field(description="URL to the full service resource.")
    label: str = Field(description="Service label.")
    category: ServiceCategory = Field(description="Service category.")
    provider: ServiceProvider = Field(description="Service provider.")


class OrganizationService(BaseModel):
    """Response model for an organization service.

    Includes the full non-secret configuration.
    """

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this service resource.")

    org_url: str = Field(description="URL to the parent organization.")

    credential_url: str = Field(
        description="URL to the credential this service uses."
    )

    label: str = Field(
        description=(
            "Human-readable label for the service, unique per organization."
        ),
    )

    category: ServiceCategory = Field(description="Service category.")

    provider: ServiceProvider = Field(description="Service provider.")

    config: dict[str, Any] = Field(
        description="Provider-specific non-secret configuration."
    )

    credential_label: str = Field(
        description="Label of the credential used for authentication."
    )

    date_created: datetime = Field(
        description="Timestamp when the service was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )
