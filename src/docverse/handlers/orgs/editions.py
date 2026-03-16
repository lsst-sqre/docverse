"""Edition endpoints within an organization's project."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from docverse.client.models import EditionCreate, EditionKind, EditionUpdate
from docverse.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
)
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.storage.pagination import (
    DEFAULT_PAGE_LIMIT,
    EDITION_CURSOR_TYPES,
    MAX_PAGE_LIMIT,
    EditionSortOrder,
)

from .models import Edition

router = APIRouter()


@router.get(
    "/orgs/{org_slug}/projects/{project_slug}/editions",
    response_model=list[Edition],
    summary="List editions for a project",
    name="get_editions",
)
async def get_editions(  # noqa: PLR0913
    org_slug: str,
    project_slug: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],  # noqa: ARG001
    order: EditionSortOrder = EditionSortOrder.slug,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)] = DEFAULT_PAGE_LIMIT,
    kind: Annotated[EditionKind | None, Query()] = None,
) -> list[Edition]:
    cursor_type = EDITION_CURSOR_TYPES[order]
    parsed_cursor = (
        cursor_type.from_str(cursor) if cursor is not None else None
    )
    async with context.session.begin():
        service = context.factory.create_edition_service()
        result = await service.list_by_project(
            org_slug=org_slug,
            project_slug=project_slug,
            cursor_type=cursor_type,
            cursor=parsed_cursor,
            limit=limit,
            kind=kind,
        )
    context.response.headers["Link"] = result.link_header(context.request.url)
    context.response.headers["X-Total-Count"] = str(result.count)
    return [
        Edition.from_domain(e, context.request, org_slug, project_slug)
        for e in result.entries
    ]


@router.post(
    "/orgs/{org_slug}/projects/{project_slug}/editions",
    response_model=Edition,
    status_code=status.HTTP_201_CREATED,
    summary="Create an edition",
    name="post_edition",
)
async def post_edition(
    org_slug: str,
    project_slug: str,
    data: EditionCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> Edition:
    async with context.session.begin():
        service = context.factory.create_edition_service()
        edition = await service.create(
            org_slug=org_slug, project_slug=project_slug, data=data
        )
        await context.session.commit()
    return Edition.from_domain(
        edition, context.request, org_slug, project_slug
    )


@router.get(
    "/orgs/{org_slug}/projects/{project_slug}/editions/{edition_slug}",
    response_model=Edition,
    summary="Get an edition",
    name="get_edition",
)
async def get_edition(
    org_slug: str,
    project_slug: str,
    edition_slug: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],  # noqa: ARG001
) -> Edition:
    async with context.session.begin():
        service = context.factory.create_edition_service()
        edition = await service.get_by_slug(
            org_slug=org_slug,
            project_slug=project_slug,
            slug=edition_slug,
        )
    return Edition.from_domain(
        edition, context.request, org_slug, project_slug
    )


@router.patch(
    "/orgs/{org_slug}/projects/{project_slug}/editions/{edition_slug}",
    response_model=Edition,
    summary="Update an edition",
    name="patch_edition",
)
async def patch_edition(  # noqa: PLR0913
    org_slug: str,
    project_slug: str,
    edition_slug: str,
    data: EditionUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> Edition:
    async with context.session.begin():
        service = context.factory.create_edition_service()
        edition = await service.update(
            org_slug=org_slug,
            project_slug=project_slug,
            slug=edition_slug,
            data=data,
        )
        await context.session.commit()
    return Edition.from_domain(
        edition, context.request, org_slug, project_slug
    )


@router.delete(
    "/orgs/{org_slug}/projects/{project_slug}/editions/{edition_slug}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an edition",
    name="delete_edition",
)
async def delete_edition(
    org_slug: str,
    project_slug: str,
    edition_slug: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> None:
    async with context.session.begin():
        service = context.factory.create_edition_service()
        await service.soft_delete(
            org_slug=org_slug,
            project_slug=project_slug,
            slug=edition_slug,
        )
        await context.session.commit()
