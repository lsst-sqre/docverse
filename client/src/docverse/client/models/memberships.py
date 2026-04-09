"""Pydantic models for organization membership resources."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "OrgMembership",
    "OrgMembershipCreate",
    "OrgRole",
    "PrincipalType",
]


class PrincipalType(StrEnum):
    """Type of principal in an organization membership."""

    user = "user"
    group = "group"


class OrgRole(StrEnum):
    """Role within an organization."""

    reader = "reader"
    uploader = "uploader"
    admin = "admin"


class OrgMembershipCreate(BaseModel):
    """Request model for creating an organization membership."""

    model_config = ConfigDict(extra="forbid")

    principal: str = Field(
        min_length=1,
        max_length=256,
        description="Username or group name.",
        examples=["jdoe", "g_spherex"],
    )

    principal_type: PrincipalType = Field(
        description="Whether the principal is a user or group."
    )

    role: OrgRole = Field(description="Role to assign.")


class OrgMembership(BaseModel):
    """Response model for an organization membership."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this membership resource.")

    org_url: str = Field(description="URL to the parent organization.")

    id: str = Field(
        description=(
            "Membership identifier in the format "
            "``{principal_type}:{principal}``."
        ),
        examples=["user:jdoe", "group:g_spherex"],
    )

    principal: str = Field(description="Username or group name.")

    principal_type: PrincipalType = Field(
        description="Whether the principal is a user or group."
    )

    role: OrgRole = Field(description="Role within the organization.")
