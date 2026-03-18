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
    return Organization.from_domain(user.org, context.request)
