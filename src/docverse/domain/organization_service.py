"""Domain model for organization services."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from docverse.client.models import ServiceCategory, ServiceProvider


class OrganizationService(BaseModel):
    """Domain representation of an organization service.

    A service pairs non-secret infrastructure configuration with a
    reference to a credential for authentication.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the service.")

    organization_id: int = Field(description="ID of the owning organization.")

    label: str = Field(
        description="Human-readable label, unique per organization."
    )

    category: ServiceCategory = Field(
        description="Service category (object_storage, cdn, or dns)."
    )

    provider: ServiceProvider = Field(
        description="Service provider (e.g. aws_s3, cloudflare_r2, fastly)."
    )

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
