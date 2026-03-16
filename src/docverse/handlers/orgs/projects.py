"""Project endpoints within an organization."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from docverse.client.models import ProjectCreate, ProjectUpdate
from docverse.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
)
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.storage.pagination import (
    DEFAULT_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
    PROJECT_CURSOR_TYPES,
    ProjectSortOrder,
)

from .models import Project

router = APIRouter()


@router.get(
    "/orgs/{org_slug}/projects",
    response_model=list[Project],
    summary="List projects in an organization",
    name="get_projects",
)
async def get_projects(  # noqa: PLR0913
    org_slug: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],  # noqa: ARG001
    order: ProjectSortOrder = ProjectSortOrder.slug,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)] = DEFAULT_PAGE_LIMIT,
) -> list[Project]:
    cursor_type = PROJECT_CURSOR_TYPES[order]
    parsed_cursor = (
        cursor_type.from_str(cursor) if cursor is not None else None
    )
    async with context.session.begin():
        service = context.factory.create_project_service()
        result = await service.list_by_org(
            org_slug,
            cursor_type=cursor_type,
            cursor=parsed_cursor,
            limit=limit,
        )
    context.response.headers["Link"] = result.link_header(context.request.url)
    context.response.headers["X-Total-Count"] = str(result.count)
    return [
        Project.from_domain(p, context.request, org_slug)
        for p in result.entries
    ]


@router.post(
    "/orgs/{org_slug}/projects",
    response_model=Project,
    status_code=status.HTTP_201_CREATED,
    summary="Create a project",
    name="post_project",
)
async def post_project(
    org_slug: str,
    data: ProjectCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> Project:
    async with context.session.begin():
        service = context.factory.create_project_service()
        project = await service.create(org_slug=org_slug, data=data)
        await context.session.commit()
    return Project.from_domain(project, context.request, org_slug)


@router.get(
    "/orgs/{org_slug}/projects/{project_slug}",
    response_model=Project,
    summary="Get a project",
    name="get_project",
)
async def get_project(
    org_slug: str,
    project_slug: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],  # noqa: ARG001
) -> Project:
    async with context.session.begin():
        service = context.factory.create_project_service()
        project = await service.get_by_slug(
            org_slug=org_slug, slug=project_slug
        )
    return Project.from_domain(project, context.request, org_slug)


@router.patch(
    "/orgs/{org_slug}/projects/{project_slug}",
    response_model=Project,
    summary="Update a project",
    name="patch_project",
)
async def patch_project(
    org_slug: str,
    project_slug: str,
    data: ProjectUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> Project:
    async with context.session.begin():
        service = context.factory.create_project_service()
        project = await service.update(
            org_slug=org_slug, slug=project_slug, data=data
        )
        await context.session.commit()
    return Project.from_domain(project, context.request, org_slug)


@router.delete(
    "/orgs/{org_slug}/projects/{project_slug}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a project",
    name="delete_project",
)
async def delete_project(
    org_slug: str,
    project_slug: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> None:
    async with context.session.begin():
        service = context.factory.create_project_service()
        await service.soft_delete(org_slug=org_slug, slug=project_slug)
        await context.session.commit()
