"""Service for authorization checks."""

from __future__ import annotations

import structlog

from docverse.client.models import OrgRole
from docverse.constants import SUPERADMIN_SCOPE
from docverse.domain.membership import ROLE_RANK
from docverse.exceptions import PermissionDeniedError
from docverse.storage.membership_store import OrgMembershipStore


class AuthorizationService:
    """Business logic for authorization checks."""

    def __init__(
        self,
        membership_store: OrgMembershipStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._membership_store = membership_store
        self._logger = logger

    async def resolve_role(
        self,
        *,
        org_id: int,
        username: str,
        groups: list[str],
        scopes: list[str] | None = None,
    ) -> OrgRole | None:
        """Resolve the effective role for a user in an organization."""
        if scopes and SUPERADMIN_SCOPE in scopes:
            self._logger.info(
                "Super admin access granted",
                username=username,
                org_id=org_id,
            )
            return OrgRole.admin
        return await self._membership_store.resolve_role(
            org_id=org_id, username=username, groups=groups
        )

    async def require_role(
        self,
        *,
        org_id: int,
        username: str,
        groups: list[str],
        scopes: list[str] | None = None,
        minimum_role: OrgRole,
    ) -> OrgRole:
        """Require at least the given role, raising on failure.

        Returns
        -------
        OrgRole
            The user's effective role.

        Raises
        ------
        PermissionDeniedError
            If the user does not have the required role.
        """
        role = await self.resolve_role(
            org_id=org_id, username=username, groups=groups, scopes=scopes
        )
        if role is None or ROLE_RANK[role] < ROLE_RANK[minimum_role]:
            self._logger.warning(
                "Permission denied",
                username=username,
                org_id=org_id,
                required=minimum_role.value,
                actual=role.value if role else None,
            )
            msg = (
                f"User {username!r} requires at least "
                f"{minimum_role.value!r} role"
            )
            raise PermissionDeniedError(msg)
        return role
