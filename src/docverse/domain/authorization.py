"""Domain models for authorization results."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from docverse.client.models import OrgRole


class AuthBasis(StrEnum):
    """How a user's role was determined."""

    super_admin = "super_admin"
    user_membership = "user_membership"
    group_membership = "group_membership"


@dataclass(slots=True)
class AuthorizationResult:
    """The outcome of resolving a user's effective role."""

    role: OrgRole
    """The effective role granted."""

    basis: AuthBasis
    """How the role was determined."""

    group: str | None = None
    """The group name when basis is group_membership."""
