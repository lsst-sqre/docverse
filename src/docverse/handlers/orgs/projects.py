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
from docverse.handlers.params import OrgSlugParam, ProjectSlugParam
from docverse.storage.pagination import (
    DEFAULT_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
    PROJECT_CURSOR_TYPES,
    ProjectSearchCursor,
    ProjectSortOrder,
)

from .models import Project

router = APIRouter()


@router.get(
    "/orgs/{org}/projects",
    response_model=list[Project],
    summary="List projects in an organization",
    name="get_projects",
)
async def get_projects(  # noqa: PLR0913
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],  # noqa: ARG001
    order: Annotated[
        ProjectSortOrder,
        Query(description="Sort order for results."),
    ] = ProjectSortOrder.slug,
    cursor: Annotated[
        str | None,
        Query(
            description=(
                "Opaque pagination cursor from a previous response's"
                " ``Link`` header."
            ),
        ),
    ] = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_PAGE_LIMIT,
            description="Maximum number of results per page.",
        ),
    ] = DEFAULT_PAGE_LIMIT,
    q: Annotated[
        str | None,
        Query(
            min_length=1,
            max_length=256,
            description=(
                "Fuzzy search query matched against project slugs and"
                " titles. Results are ordered by relevance and support"
                " cursor pagination via the ``Link`` header."
            ),
        ),
    ] = None,
) -> list[Project]:
    async with context.session.begin():
        service = context.factory.create_project_service()
        if q is not None:
            search_cursor = (
                ProjectSearchCursor.from_str(cursor)
                if cursor is not None
                else None
            )
            result = await service.list_by_org(
                org_slug, query=q, limit=limit, cursor=search_cursor
            )
        else:
            cursor_type = PROJECT_CURSOR_TYPES[order]
            parsed_cursor = (
                cursor_type.from_str(cursor) if cursor is not None else None
            )
            result = await service.list_by_org(
                org_slug,
                cursor_type=cursor_type,
                cursor=parsed_cursor,
                limit=limit,
            )
    link = result.link_header(context.request.url)
    if link:
        context.response.headers["Link"] = link
    context.response.headers["X-Total-Count"] = str(result.count)
    return [
        Project.from_domain(p, context.request, org_slug)
        for p in result.entries
    ]


@router.post(
    "/orgs/{org}/projects",
    response_model=Project,
    status_code=status.HTTP_201_CREATED,
    summary="Create a project",
    name="post_project",
)
async def post_project(
    org_slug: OrgSlugParam,
    data: ProjectCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> Project:
    async with context.session.begin():
        service = context.factory.create_project_service()
        project, default_edition = await service.create(
            org_slug=org_slug, data=data
        )
        await context.session.commit()
    return Project.from_domain(
        project, context.request, org_slug, default_edition=default_edition
    )


@router.get(
    "/orgs/{org}/projects/{project}",
    response_model=Project,
    summary="Get a project",
    name="get_project",
)
async def get_project(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],  # noqa: ARG001
) -> Project:
    async with context.session.begin():
        service = context.factory.create_project_service()
        project = await service.get_by_slug(
            org_slug=org_slug, slug=project_slug
        )
        default_edition = await service.get_default_edition(project.id)
    return Project.from_domain(
        project, context.request, org_slug, default_edition=default_edition
    )


@router.patch(
    "/orgs/{org}/projects/{project}",
    response_model=Project,
    summary="Update a project",
    name="patch_project",
)
async def patch_project(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    data: ProjectUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> Project:
    async with context.session.begin():
        service = context.factory.create_project_service()
        project = await service.update(
            org_slug=org_slug, slug=project_slug, data=data
        )
        default_edition = await service.get_default_edition(project.id)
        await context.session.commit()
    return Project.from_domain(
        project, context.request, org_slug, default_edition=default_edition
    )


@router.delete(
    "/orgs/{org}/projects/{project}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a project",
    name="delete_project",
)
async def delete_project(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> None:
    async with context.session.begin():
        service = context.factory.create_project_service()
        await service.soft_delete(org_slug=org_slug, slug=project_slug)
        await context.session.commit()
