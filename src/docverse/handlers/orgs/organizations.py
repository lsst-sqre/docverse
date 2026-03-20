"""Organization endpoint."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from docverse.dependencies.auth import AuthenticatedUser, require_reader
from docverse.dependencies.context import RequestContext, context_dependency
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
