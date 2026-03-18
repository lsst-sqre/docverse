"""Domain model for organization credentials."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


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

    service_type: str = Field(
        description="Object store service type (e.g. s3, r2, gcs)."
    )

    date_created: datetime = Field(
        description="Timestamp when the credential was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )
