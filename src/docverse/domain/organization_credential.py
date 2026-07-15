"""Domain model for organization credentials."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from docverse.client.models import CredentialProvider


class OrganizationCredential(BaseModel):
    """Domain representation of an organization credential.

    The plaintext credential is never exposed through this model.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the credential.")

    organization_id: int = Field(description="ID of the owning organization.")

    label: str = Field(
        description="Human-readable label, unique per organization."
    )

    provider: CredentialProvider = Field(
        description="Cloud provider (e.g. aws, cloudflare, fastly, gcp)."
    )

    date_created: datetime = Field(
        description="Timestamp when the credential was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )
