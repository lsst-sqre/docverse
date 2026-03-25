"""Admin endpoints for managing organizations."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from docverse.client.models import OrganizationCreate
from docverse.dependencies.auth import bind_username
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.exceptions import ConflictError, NotFoundError
from docverse.handlers.params import OrgSlugParam

from .models import Organization

router = APIRouter(tags=["admin"], dependencies=[Depends(bind_username)])


@router.post(
    "/admin/orgs",
    response_model=Organization,
    status_code=status.HTTP_201_CREATED,
    summary="Create an organization",
    name="admin_post_organization",
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
        if data.members:
            membership_store = context.factory.create_membership_store()
            seen: set[tuple[str, str]] = set()
            for member in data.members:
                key = (member.principal_type, member.principal)
                if key in seen:
                    continue
                seen.add(key)
                await membership_store.create(org_id=org.id, data=member)
        await context.session.commit()
    # New org has no services yet, so no need to load them
    return Organization.from_domain(org, context.request)


@router.get(
    "/admin/orgs",
    response_model=list[Organization],
    summary="List all organizations",
    name="admin_get_organizations",
)
async def get_organizations(
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> list[Organization]:
    async with context.session.begin():
        service = context.factory.create_organization_service()
        orgs = await service.list_all()
        service_store = context.factory.create_service_store()
        results: list[Organization] = []
        for o in orgs:
            services = await service_store.list_by_org(o.id)
            results.append(
                Organization.from_domain(o, context.request, services=services)
            )
    return results


@router.get(
    "/admin/orgs/{org}",
    response_model=Organization,
    summary="Get an organization",
    name="admin_get_organization",
)
async def get_organization(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Organization:
    async with context.session.begin():
        service = context.factory.create_organization_service()
        org = await service.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        service_store = context.factory.create_service_store()
        services = await service_store.list_by_org(org.id)
    return Organization.from_domain(org, context.request, services=services)


@router.delete(
    "/admin/orgs/{org}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an organization",
    name="admin_delete_organization",
)
async def delete_organization(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> None:
    async with context.session.begin():
        service = context.factory.create_organization_service()
        deleted = await service.delete(org_slug)
        if not deleted:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        await context.session.commit()
