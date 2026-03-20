"""Organization service endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from docverse.client.models import OrganizationServiceCreate
from docverse.client.models.infrastructure import ServiceProvider
from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.handlers.params import OrgSlugParam, ServiceLabelParam

from .models import OrganizationServiceResponse

router = APIRouter()


@router.get(
    "/orgs/{org}/services",
    response_model=list[OrganizationServiceResponse],
    summary="List organization services",
    name="get_services",
)
async def get_services(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> list[OrganizationServiceResponse]:
    async with context.session.begin():
        service = context.factory.create_infrastructure_service()
        services = await service.list_by_org(org_slug=org_slug)
    return [
        OrganizationServiceResponse.from_domain(s, context.request, org_slug)
        for s in services
    ]


@router.post(
    "/orgs/{org}/services",
    response_model=OrganizationServiceResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create an organization service",
    name="post_service",
)
async def post_service(
    org_slug: OrgSlugParam,
    data: OrganizationServiceCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> OrganizationServiceResponse:
    async with context.session.begin():
        infra_service = context.factory.create_infrastructure_service()
        svc = await infra_service.create(
            org_slug=org_slug,
            label=data.label,
            provider=ServiceProvider(data.config.provider),
            config=data.config.model_dump(exclude={"provider"}),
            credential_label=data.credential_label,
        )
        await context.session.commit()
    return OrganizationServiceResponse.from_domain(
        svc, context.request, org_slug
    )


@router.get(
    "/orgs/{org}/services/{service}",
    response_model=OrganizationServiceResponse,
    summary="Get an organization service",
    name="get_service",
)
async def get_service(
    org_slug: OrgSlugParam,
    service_label: ServiceLabelParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> OrganizationServiceResponse:
    async with context.session.begin():
        infra_service = context.factory.create_infrastructure_service()
        svc = await infra_service.get_by_label(
            org_slug=org_slug, label=service_label
        )
    return OrganizationServiceResponse.from_domain(
        svc, context.request, org_slug
    )


@router.delete(
    "/orgs/{org}/services/{service}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an organization service",
    name="delete_service",
)
async def delete_service(
    org_slug: OrgSlugParam,
    service_label: ServiceLabelParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> None:
    async with context.session.begin():
        infra_service = context.factory.create_infrastructure_service()
        await infra_service.delete(org_slug=org_slug, label=service_label)
        await context.session.commit()
