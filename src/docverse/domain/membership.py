"""Domain model for organization memberships."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from docverse.client.models import OrgRole, PrincipalType


class OrgMembership(BaseModel):
    """Domain representation of an organization membership."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the membership.")

    org_id: int = Field(description="ID of the organization.")

    principal: str = Field(description="Username or group name.")

    principal_type: PrincipalType = Field(
        description="Whether the principal is a user or group."
    )

    role: OrgRole = Field(description="Role within the organization.")
