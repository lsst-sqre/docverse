"""Dashboard-template binding endpoints within an organization."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Response, status

from docverse.client.models import DashboardTemplateBindingCreate
from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.handlers.params import OrgSlugParam, ProjectSlugParam

from .models import DashboardTemplateBindingResponse

router = APIRouter()


# ---------------------------------------------------------------------------
# Org-default binding
# ---------------------------------------------------------------------------


@router.get(
    "/orgs/{org}/dashboard-template",
    response_model=DashboardTemplateBindingResponse,
    summary="Get the organization's default dashboard-template binding",
    name="get_org_dashboard_template",
)
async def get_org_dashboard_template(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> DashboardTemplateBindingResponse:
    async with context.session.begin():
        service = context.factory.create_dashboard_template_binding_service()
        binding = await service.get_org_default(org_slug=org_slug)
    return DashboardTemplateBindingResponse.from_domain(
        binding, context.request, org_slug=org_slug
    )


@router.put(
    "/orgs/{org}/dashboard-template",
    response_model=DashboardTemplateBindingResponse,
    summary="Create or update the org-default dashboard-template binding",
    name="put_org_dashboard_template",
)
async def put_org_dashboard_template(
    org_slug: OrgSlugParam,
    data: DashboardTemplateBindingCreate,
    response: Response,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> DashboardTemplateBindingResponse:
    async with context.session.begin():
        service = context.factory.create_dashboard_template_binding_service()
        result = await service.put_org_default(org_slug=org_slug, data=data)
        if result.changed:
            await context.session.commit()
    response.status_code = (
        status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
    )
    return DashboardTemplateBindingResponse.from_domain(
        result.binding, context.request, org_slug=org_slug
    )


@router.delete(
    "/orgs/{org}/dashboard-template",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete the organization's default dashboard-template binding",
    name="delete_org_dashboard_template",
)
async def delete_org_dashboard_template(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> None:
    async with context.session.begin():
        service = context.factory.create_dashboard_template_binding_service()
        await service.delete_org_default(org_slug=org_slug)
        await context.session.commit()


# ---------------------------------------------------------------------------
# Project-override binding
# ---------------------------------------------------------------------------


@router.get(
    "/orgs/{org}/projects/{project}/dashboard-template",
    response_model=DashboardTemplateBindingResponse,
    summary="Get a project's dashboard-template binding override",
    name="get_project_dashboard_template",
)
async def get_project_dashboard_template(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> DashboardTemplateBindingResponse:
    async with context.session.begin():
        service = context.factory.create_dashboard_template_binding_service()
        binding = await service.get_project_override(
            org_slug=org_slug, project_slug=project_slug
        )
    return DashboardTemplateBindingResponse.from_domain(
        binding,
        context.request,
        org_slug=org_slug,
        project_slug=project_slug,
    )


@router.put(
    "/orgs/{org}/projects/{project}/dashboard-template",
    response_model=DashboardTemplateBindingResponse,
    summary="Create or update a project's dashboard-template binding override",
    name="put_project_dashboard_template",
)
async def put_project_dashboard_template(  # noqa: PLR0913
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    data: DashboardTemplateBindingCreate,
    response: Response,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> DashboardTemplateBindingResponse:
    async with context.session.begin():
        service = context.factory.create_dashboard_template_binding_service()
        result = await service.put_project_override(
            org_slug=org_slug, project_slug=project_slug, data=data
        )
        if result.changed:
            await context.session.commit()
    response.status_code = (
        status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
    )
    return DashboardTemplateBindingResponse.from_domain(
        result.binding,
        context.request,
        org_slug=org_slug,
        project_slug=project_slug,
    )


@router.delete(
    "/orgs/{org}/projects/{project}/dashboard-template",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a project's dashboard-template binding override",
    name="delete_project_dashboard_template",
)
async def delete_project_dashboard_template(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> None:
    async with context.session.begin():
        service = context.factory.create_dashboard_template_binding_service()
        await service.delete_project_override(
            org_slug=org_slug, project_slug=project_slug
        )
        await context.session.commit()
