"""Service for authorization checks."""

from __future__ import annotations

import structlog

from docverse.client.models import OrgRole, PrincipalType
from docverse.domain.authorization import AuthBasis, AuthorizationResult
from docverse.domain.membership import ROLE_RANK
from docverse.exceptions import PermissionDeniedError
from docverse.storage.membership_store import OrgMembershipStore


class AuthorizationService:
    """Business logic for authorization checks."""

    def __init__(
        self,
        membership_store: OrgMembershipStore,
        logger: structlog.stdlib.BoundLogger,
        superadmin_usernames: list[str] | None = None,
    ) -> None:
        self._membership_store = membership_store
        self._logger = logger
        self._superadmin_usernames = superadmin_usernames or []

    async def resolve_role(
        self,
        *,
        org_id: int,
        username: str,
        groups: list[str],
    ) -> AuthorizationResult | None:
        """Resolve the effective role for a user in an organization."""
        if username in self._superadmin_usernames:
            self._logger.debug(
                "Super admin access granted via config",
                username=username,
                org_id=org_id,
            )
            return AuthorizationResult(
                role=OrgRole.admin, basis=AuthBasis.super_admin
            )
        self._logger.debug(
            "User is not a super admin",
            username=username,
            org_id=org_id,
        )
        result = await self._membership_store.resolve_role(
            org_id=org_id, username=username, groups=groups
        )
        if result is None:
            return None
        role, principal_type, group_name = result
        if principal_type == PrincipalType.group:
            basis = AuthBasis.group_membership
        else:
            basis = AuthBasis.user_membership
        return AuthorizationResult(role=role, basis=basis, group=group_name)

    async def require_role(
        self,
        *,
        org_id: int,
        username: str,
        groups: list[str],
        minimum_role: OrgRole,
    ) -> AuthorizationResult:
        """Require at least the given role, raising on failure.

        Returns
        -------
        AuthorizationResult
            The user's effective role and how it was determined.

        Raises
        ------
        PermissionDeniedError
            If the user does not have the required role.
        """
        auth_result = await self.resolve_role(
            org_id=org_id, username=username, groups=groups
        )
        if auth_result is None or (
            ROLE_RANK[auth_result.role] < ROLE_RANK[minimum_role]
        ):
            self._logger.warning(
                "Permission denied",
                username=username,
                org_id=org_id,
                required=minimum_role.value,
                actual=auth_result.role.value if auth_result else None,
            )
            msg = (
                f"User {username!r} requires at least "
                f"{minimum_role.value!r} role"
            )
            raise PermissionDeniedError(msg)
        return auth_result
