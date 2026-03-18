"""Authorization dependency for org-scoped endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Request

from docverse.client.models import OrgRole
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.domain.organization import Organization
from docverse.exceptions import NotFoundError, PermissionDeniedError
from docverse.handlers.params import OrgSlugParam

__all__ = [
    "AuthenticatedUser",
    "OrgRoleDependency",
    "require_admin",
    "require_reader",
    "require_uploader",
]


@dataclass(slots=True)
class AuthenticatedUser:
    """Represents an authenticated user with their resolved org role."""

    username: str
    """The authenticated username."""

    role: OrgRole
    """The user's effective role in the organization."""

    org: Organization
    """The resolved organization."""


class OrgRoleDependency:
    """FastAPI dependency that requires a minimum org role.

    This dependency:
    1. Reads the ``X-Auth-Request-User`` header
    2. Gets groups via ``UserInfoStore.get_groups()``
    3. Calls ``AuthorizationService.require_role()``
    4. Returns ``AuthenticatedUser(username, role, org)``

    The resolved organization is stored on the result so handlers
    do not need to re-query.
    """

    def __init__(self, min_role: OrgRole) -> None:
        self._min_role = min_role

    async def __call__(  # noqa: D102
        self,
        org_slug: OrgSlugParam,
        request: Request,
        context: Annotated[RequestContext, Depends(context_dependency)],
    ) -> AuthenticatedUser:
        username = request.headers.get("X-Auth-Request-User")
        if not username:
            msg = "Authentication required"
            raise PermissionDeniedError(msg)

        async with context.session.begin():
            # Resolve the organization
            org_service = context.factory.create_organization_service()
            org = await org_service.get_by_slug(org_slug)
            if org is None:
                msg = f"Organization {org_slug!r} not found"
                raise NotFoundError(msg)

            # Get user's groups
            token = request.headers.get("X-Auth-Request-Token", "")
            user_info_store = context.factory.get_user_info_store()
            groups = await user_info_store.get_groups(token)

            # Check authorization
            auth_service = context.factory.create_authorization_service()
            role = await auth_service.require_role(
                org_id=org.id,
                username=username,
                groups=groups,
                minimum_role=self._min_role,
            )

        return AuthenticatedUser(username=username, role=role, org=org)


require_reader = OrgRoleDependency(OrgRole.reader)
"""Dependency that requires at least reader role."""

require_uploader = OrgRoleDependency(OrgRole.uploader)
"""Dependency that requires at least uploader role."""

require_admin = OrgRoleDependency(OrgRole.admin)
"""Dependency that requires admin role."""
