"""Organization endpoint."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request, status

from docverse.client.models import (
    OrganizationSummary,
    OrganizationUpdate,
    OrgRole,
)
from docverse.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
)
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.domain.organization import Organization as OrganizationDomain
from docverse.exceptions import NotFoundError, PermissionDeniedError
from docverse.handlers.params import OrgSlugParam
from docverse.handlers.responses import error_responses

from .models import Organization

router = APIRouter()


def _organization_summary(
    org: OrganizationDomain, request: Request, *, role: OrgRole
) -> OrganizationSummary:
    """Build an ``OrganizationSummary`` for a listing entry."""
    return OrganizationSummary(
        self_url=str(request.url_for("get_organization", org=org.slug)),
        slug=org.slug,
        title=org.title,
        role=role,
    )


@router.get(
    "/orgs",
    response_model=list[OrganizationSummary],
    summary="List organizations the caller can access",
    name="get_organizations",
    responses=error_responses(status.HTTP_403_FORBIDDEN),
)
async def get_organizations(
    request: Request,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> list[OrganizationSummary]:
    """List organizations in which the caller holds an effective role.

    Any authenticated user may call this. A caller sees each organization
    where they have a direct or group membership, along with their
    effective role; a superadmin sees every organization (role reported as
    ``admin``). An empty list is a valid response.
    """
    username = request.headers.get("X-Auth-Request-User")
    if not username:
        msg = "Authentication required"
        raise PermissionDeniedError(msg)
    context.rebind_logger(username=username)

    async with context.session.begin():
        org_service = context.factory.create_organization_service()
        auth_service = context.factory.create_authorization_service()
        if auth_service.is_superadmin(username):
            orgs = await org_service.list_all()
            summaries = [
                _organization_summary(org, request, role=OrgRole.admin)
                for org in orgs
            ]
        else:
            token = request.headers.get("X-Auth-Request-Token", "")
            user_info_store = context.factory.get_user_info_store()
            groups = await user_info_store.get_groups(token)
            membership_store = context.factory.create_membership_store()
            role_map = await membership_store.list_effective_roles(
                username=username, groups=groups
            )
            org_store = context.factory.create_org_store()
            summaries = []
            for org_id, role in role_map.items():
                org = await org_store.get_by_id(org_id)
                if org is None:
                    continue
                summaries.append(
                    _organization_summary(org, request, role=role)
                )
    summaries.sort(key=lambda summary: summary.slug)
    return summaries


@router.get(
    "/orgs/{org}",
    response_model=Organization,
    summary="Get an organization",
    name="get_organization",
)
async def get_organization(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
) -> Organization:
    # Load services to build embedded summaries for slot assignments
    async with context.session.begin():
        infra_service = context.factory.create_infrastructure_service()
        services = await infra_service.list_by_org_id(org_id=user.org.id)
    return Organization.from_domain(
        user.org, context.request, services=services
    )


@router.patch(
    "/orgs/{org}",
    response_model=Organization,
    summary="Update an organization",
    name="patch_organization",
)
async def patch_organization(
    org_slug: OrgSlugParam,
    data: OrganizationUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> Organization:
    async with context.session.begin():
        service = context.factory.create_organization_service()
        org = await service.update(user.org.slug, data)
        if org is None:
            msg = f"Organization {user.org.slug!r} not found"
            raise NotFoundError(msg)
        infra_service = context.factory.create_infrastructure_service()
        services = await infra_service.list_by_org_id(org_id=org.id)
        await context.session.commit()
    return Organization.from_domain(org, context.request, services=services)
