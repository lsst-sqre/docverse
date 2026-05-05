"""Edition endpoints within an organization's project."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from docverse.client.models import (
    EditionCreate,
    EditionKind,
    EditionRollback,
    EditionUpdate,
)
from docverse.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
)
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.domain.published_url import (
    edition_published_url,
    project_published_url,
)
from docverse.exceptions import PermissionDeniedError
from docverse.handlers.params import (
    EditionSlugParam,
    OrgSlugParam,
    ProjectSlugParam,
)
from docverse.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_slug,
)
from docverse.storage.pagination import (
    DEFAULT_PAGE_LIMIT,
    EDITION_CURSOR_TYPES,
    EDITION_HISTORY_CURSOR_TYPE,
    MAX_PAGE_LIMIT,
    EditionSortOrder,
)

from .models import Edition, EditionBuildHistoryResponse

router = APIRouter()


@router.get(
    "/orgs/{org}/projects/{project}/editions",
    response_model=list[Edition],
    summary="List editions for a project",
    name="get_editions",
)
async def get_editions(  # noqa: PLR0913
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],  # noqa: ARG001
    order: Annotated[
        EditionSortOrder,
        Query(description="Sort order for results."),
    ] = EditionSortOrder.slug,
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
    kind: Annotated[
        EditionKind | None,
        Query(description="Filter editions by kind."),
    ] = None,
) -> list[Edition]:
    cursor_type = EDITION_CURSOR_TYPES[order]
    parsed_cursor = (
        cursor_type.from_str(cursor) if cursor is not None else None
    )
    async with context.session.begin():
        service = context.factory.create_edition_service()
        org, project, result = await service.list_by_project(
            org_slug=org_slug,
            project_slug=project_slug,
            cursor_type=cursor_type,
            cursor=parsed_cursor,
            limit=limit,
            kind=kind,
        )
    context.response.headers["Link"] = result.link_header(context.request.url)
    context.response.headers["X-Total-Count"] = str(result.count)
    project_url = project_published_url(org, project)
    return [
        Edition.from_domain(
            e,
            context.request,
            org_slug,
            project_slug,
            published_url=edition_published_url(project_url, e),
        )
        for e in result.entries
    ]


@router.post(
    "/orgs/{org}/projects/{project}/editions",
    response_model=Edition,
    status_code=status.HTTP_201_CREATED,
    summary="Create an edition",
    name="post_edition",
)
async def post_edition(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    data: EditionCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> Edition:
    async with context.session.begin():
        service = context.factory.create_edition_service()
        org, project, edition = await service.create(
            org_slug=org_slug, project_slug=project_slug, data=data
        )
        await context.session.commit()
    await try_enqueue_dashboard_build_by_slug(
        factory=context.factory,
        session=context.session,
        logger=context.logger,
        org_slug=org_slug,
        project_slug=project_slug,
    )
    project_url = project_published_url(org, project)
    return Edition.from_domain(
        edition,
        context.request,
        org_slug,
        project_slug,
        published_url=edition_published_url(project_url, edition),
    )


@router.get(
    "/orgs/{org}/projects/{project}/editions/{edition}",
    response_model=Edition,
    summary="Get an edition",
    name="get_edition",
)
async def get_edition(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],  # noqa: ARG001
) -> Edition:
    async with context.session.begin():
        service = context.factory.create_edition_service()
        org, project, edition = await service.get_by_slug(
            org_slug=org_slug,
            project_slug=project_slug,
            slug=edition_slug,
        )
    project_url = project_published_url(org, project)
    return Edition.from_domain(
        edition,
        context.request,
        org_slug,
        project_slug,
        published_url=edition_published_url(project_url, edition),
    )


@router.get(
    "/orgs/{org}/projects/{project}/editions/{edition}/history",
    response_model=list[EditionBuildHistoryResponse],
    summary="List build history for an edition",
    name="get_edition_history",
)
async def get_edition_history(  # noqa: PLR0913
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],  # noqa: ARG001
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
    include_deleted: Annotated[  # noqa: FBT002
        bool,
        Query(
            description=(
                "Include history entries for soft-deleted builds. "
                "Defaults to false."
            ),
        ),
    ] = False,
) -> list[EditionBuildHistoryResponse]:
    parsed_cursor = (
        EDITION_HISTORY_CURSOR_TYPE.from_str(cursor)
        if cursor is not None
        else None
    )
    async with context.session.begin():
        service = context.factory.create_edition_service()
        result = await service.list_history(
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slug=edition_slug,
            cursor=parsed_cursor,
            limit=limit,
            include_deleted=include_deleted,
        )
    context.response.headers["Link"] = result.link_header(context.request.url)
    context.response.headers["X-Total-Count"] = str(result.count)
    return [
        EditionBuildHistoryResponse.from_domain(
            entry, context.request, org_slug, project_slug
        )
        for entry in result.entries
    ]


@router.post(
    "/orgs/{org}/projects/{project}/editions/{edition}/rollback",
    response_model=Edition,
    summary="Roll back an edition to a previous build",
    name="post_edition_rollback",
)
async def post_edition_rollback(  # noqa: PLR0913
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
    data: EditionRollback,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> Edition:
    async with context.session.begin():
        service = context.factory.create_edition_service()
        org, project, edition = await service.rollback(
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slug=edition_slug,
            build_public_id=data.build,
        )
        await context.session.commit()
    await try_enqueue_dashboard_build_by_slug(
        factory=context.factory,
        session=context.session,
        logger=context.logger,
        org_slug=org_slug,
        project_slug=project_slug,
    )
    project_url = project_published_url(org, project)
    return Edition.from_domain(
        edition,
        context.request,
        org_slug,
        project_slug,
        published_url=edition_published_url(project_url, edition),
    )


@router.patch(
    "/orgs/{org}/projects/{project}/editions/{edition}",
    response_model=Edition,
    summary="Update an edition",
    name="patch_edition",
)
async def patch_edition(  # noqa: PLR0913
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
    data: EditionUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> Edition:
    if edition_slug.lower() == "__main" and data.kind is not None:
        msg = "Cannot change the kind of the default '__main' edition"
        raise PermissionDeniedError(msg)
    async with context.session.begin():
        service = context.factory.create_edition_service()
        org, project, edition = await service.update(
            org_slug=org_slug,
            project_slug=project_slug,
            slug=edition_slug,
            data=data,
        )
        await context.session.commit()
    await try_enqueue_dashboard_build_by_slug(
        factory=context.factory,
        session=context.session,
        logger=context.logger,
        org_slug=org_slug,
        project_slug=project_slug,
    )
    project_url = project_published_url(org, project)
    return Edition.from_domain(
        edition,
        context.request,
        org_slug,
        project_slug,
        published_url=edition_published_url(project_url, edition),
    )


@router.delete(
    "/orgs/{org}/projects/{project}/editions/{edition}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an edition",
    name="delete_edition",
)
async def delete_edition(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> None:
    if edition_slug.lower() == "__main":
        msg = "The default edition '__main' cannot be deleted"
        raise PermissionDeniedError(msg)
    async with context.session.begin():
        service = context.factory.create_edition_service()
        await service.soft_delete(
            org_slug=org_slug,
            project_slug=project_slug,
            slug=edition_slug,
        )
        await context.session.commit()
    await try_enqueue_dashboard_build_by_slug(
        factory=context.factory,
        session=context.session,
        logger=context.logger,
        org_slug=org_slug,
        project_slug=project_slug,
    )
