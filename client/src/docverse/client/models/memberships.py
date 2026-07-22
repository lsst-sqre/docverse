"""Pydantic models for organization membership resources."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator

__all__ = [
    "OrgMembership",
    "OrgMembershipCreate",
    "OrgMembershipUpdate",
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


class OrgMembershipUpdate(BaseModel):
    """Request model for updating an organization membership (PATCH).

    Only ``role`` is mutable. ``principal`` and ``principal_type`` identify
    the membership and are immutable — changing identity is a delete plus a
    re-add. ``extra="forbid"`` rejects attempts to patch those or any other
    field. Applied with JSON-Merge-Patch semantics: omitted fields are left
    untouched.
    """

    model_config = ConfigDict(extra="forbid")

    role: OrgRole | None = Field(
        default=None, description="New role to assign to the member."
    )

    @field_validator("role")
    @classmethod
    def _reject_explicit_null(cls, value: OrgRole | None) -> OrgRole | None:
        """Reject an explicit ``null`` for ``role``.

        The validator is skipped for the unset default, so it only fires
        when the client sends ``{"role": null}``. The column is
        non-nullable, so RFC 7386's null-as-remove semantics have no
        meaning here; omit the field to leave the role unchanged.
        """
        if value is None:
            msg = "role may not be null; omit it to leave the role unchanged"
            raise ValueError(msg)
        return value


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
