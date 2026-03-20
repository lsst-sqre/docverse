"""Enums and mappings for infrastructure service configuration."""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "SERVICE_PROVIDER_CATEGORY",
    "SERVICE_PROVIDER_CREDENTIAL",
    "CredentialProvider",
    "ServiceCategory",
    "ServiceProvider",
]


class CredentialProvider(StrEnum):
    """Cloud provider for authentication credentials."""

    aws = "aws"
    """Amazon Web Services (S3, Route 53, CloudFront)."""

    cloudflare = "cloudflare"
    """Cloudflare (R2, Workers, DNS)."""

    fastly = "fastly"
    """Fastly CDN."""

    gcp = "gcp"
    """Google Cloud Platform (GCS, Cloud CDN)."""


class ServiceCategory(StrEnum):
    """Category of infrastructure service."""

    object_storage = "object_storage"
    """Object storage for documentation content."""

    cdn = "cdn"
    """Content delivery network for cache purging and edge data."""

    dns = "dns"
    """DNS management for subdomain registration."""


class ServiceProvider(StrEnum):
    """Specific infrastructure service provider."""

    # Object storage
    aws_s3 = "aws_s3"
    cloudflare_r2 = "cloudflare_r2"
    minio = "minio"
    gcs = "gcs"

    # CDN
    fastly = "fastly"
    cloudflare_workers = "cloudflare_workers"
    cloudfront = "cloudfront"
    google_cloud_cdn = "google_cloud_cdn"

    # DNS
    route53 = "route53"
    cloudflare_dns = "cloudflare_dns"


SERVICE_PROVIDER_CATEGORY: dict[ServiceProvider, ServiceCategory] = {
    # Object storage
    ServiceProvider.aws_s3: ServiceCategory.object_storage,
    ServiceProvider.cloudflare_r2: ServiceCategory.object_storage,
    ServiceProvider.minio: ServiceCategory.object_storage,
    ServiceProvider.gcs: ServiceCategory.object_storage,
    # CDN
    ServiceProvider.fastly: ServiceCategory.cdn,
    ServiceProvider.cloudflare_workers: ServiceCategory.cdn,
    ServiceProvider.cloudfront: ServiceCategory.cdn,
    ServiceProvider.google_cloud_cdn: ServiceCategory.cdn,
    # DNS
    ServiceProvider.route53: ServiceCategory.dns,
    ServiceProvider.cloudflare_dns: ServiceCategory.dns,
}
"""Mapping from service provider to its category."""


SERVICE_PROVIDER_CREDENTIAL: dict[
    ServiceProvider, list[CredentialProvider]
] = {
    # Object storage
    ServiceProvider.aws_s3: [CredentialProvider.aws],
    ServiceProvider.cloudflare_r2: [CredentialProvider.cloudflare],
    ServiceProvider.minio: [CredentialProvider.aws],
    ServiceProvider.gcs: [CredentialProvider.gcp],
    # CDN
    ServiceProvider.fastly: [CredentialProvider.fastly],
    ServiceProvider.cloudflare_workers: [CredentialProvider.cloudflare],
    ServiceProvider.cloudfront: [CredentialProvider.aws],
    ServiceProvider.google_cloud_cdn: [CredentialProvider.gcp],
    # DNS
    ServiceProvider.route53: [CredentialProvider.aws],
    ServiceProvider.cloudflare_dns: [CredentialProvider.cloudflare],
}
"""Mapping from service provider to compatible credential providers."""
