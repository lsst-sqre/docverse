"""Admin endpoints for managing organizations."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from docverse.client.models import OrganizationCreate, OrganizationUpdate
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.exceptions import ConflictError, NotFoundError

from .models import Organization

router = APIRouter(tags=["admin"])


@router.post(
    "/admin/orgs",
    response_model=Organization,
    status_code=status.HTTP_201_CREATED,
    summary="Create an organization",
)
async def post_organization(
    data: OrganizationCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Organization:
    async with context.session.begin():
        service = context.factory.create_organization_service()
        existing = await service.get_by_slug(data.slug)
        if existing is not None:
            msg = f"Organization with slug {data.slug!r} already exists"
            raise ConflictError(msg)
        org = await service.create(data)
        await context.session.commit()
    return Organization.from_domain(org, context.request)


@router.get(
    "/admin/orgs",
    response_model=list[Organization],
    summary="List all organizations",
)
async def get_organizations(
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> list[Organization]:
    async with context.session.begin():
        service = context.factory.create_organization_service()
        orgs = await service.list_all()
    return [Organization.from_domain(o, context.request) for o in orgs]


@router.get(
    "/admin/orgs/{org_slug}",
    response_model=Organization,
    summary="Get an organization",
)
async def get_organization(
    org_slug: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Organization:
    async with context.session.begin():
        service = context.factory.create_organization_service()
        org = await service.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
    return Organization.from_domain(org, context.request)


@router.patch(
    "/admin/orgs/{org_slug}",
    response_model=Organization,
    summary="Update an organization",
)
async def patch_organization(
    org_slug: str,
    data: OrganizationUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Organization:
    async with context.session.begin():
        service = context.factory.create_organization_service()
        org = await service.update(org_slug, data)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        await context.session.commit()
    return Organization.from_domain(org, context.request)


@router.delete(
    "/admin/orgs/{org_slug}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an organization",
)
async def delete_organization(
    org_slug: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> None:
    async with context.session.begin():
        service = context.factory.create_organization_service()
        deleted = await service.delete(org_slug)
        if not deleted:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        await context.session.commit()
