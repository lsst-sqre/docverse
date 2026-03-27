"""Organization endpoint."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from docverse.client.models import OrganizationUpdate
from docverse.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
)
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.exceptions import NotFoundError
from docverse.handlers.params import OrgSlugParam

from .models import Organization

router = APIRouter()


@router.get(
    "/orgs/{org}",
    response_model=Organization,
    summary="Get an organization",
    name="get_organization",
)
async def get_organization(
    org_slug: OrgSlugParam,  # noqa: ARG001
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
    org_slug: OrgSlugParam,  # noqa: ARG001
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
