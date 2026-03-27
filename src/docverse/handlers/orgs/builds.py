"""Build endpoints within an organization's project."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from docverse.client.models import BuildCreate, BuildStatus, BuildUpdate
from docverse.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
    require_uploader,
)
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.domain.base32id import serialize_base32_id
from docverse.handlers.params import (
    BuildIdParam,
    OrgSlugParam,
    ProjectSlugParam,
)
from docverse.storage.pagination import (
    BUILD_CURSOR_TYPE,
    DEFAULT_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
)

from .models import Build

router = APIRouter()


@router.get(
    "/orgs/{org}/projects/{project}/builds",
    response_model=list[Build],
    summary="List builds for a project",
    name="get_builds",
)
async def get_builds(  # noqa: PLR0913
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
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
    status_filter: Annotated[
        BuildStatus | None,
        Query(alias="status", description="Filter builds by status."),
    ] = None,
) -> list[Build]:
    parsed_cursor = (
        BUILD_CURSOR_TYPE.from_str(cursor) if cursor is not None else None
    )
    async with context.session.begin():
        service = context.factory.create_build_service()
        result = await service.list_by_project(
            org_slug=org_slug,
            project_slug=project_slug,
            cursor=parsed_cursor,
            limit=limit,
            status=status_filter,
        )
    context.response.headers["Link"] = result.link_header(context.request.url)
    context.response.headers["X-Total-Count"] = str(result.count)
    return [
        Build.from_domain(b, context.request, org_slug, project_slug)
        for b in result.entries
    ]


@router.post(
    "/orgs/{org}/projects/{project}/builds",
    response_model=Build,
    status_code=status.HTTP_201_CREATED,
    summary="Create a build",
    name="post_build",
)
async def post_build(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    data: BuildCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_uploader)],
) -> Build:
    upload_url: str | None = None
    async with context.session.begin():
        service = context.factory.create_build_service()
        build = await service.create(
            org_slug=org_slug,
            project_slug=project_slug,
            data=data,
            uploader=user.username,
        )

        # Generate a presigned upload URL if the org has a store
        service_label = user.org.resolved_staging_store_label
        if service_label is not None:
            object_store = await context.factory.create_objectstore_for_org(
                org_id=user.org.id,
                service_label=service_label,
            )
            async with object_store:
                upload_url = await object_store.generate_presigned_upload_url(
                    key=build.staging_key,
                    content_type="application/gzip",
                )

        await context.session.commit()
    return Build.from_domain(
        build,
        context.request,
        org_slug,
        project_slug,
        upload_url=upload_url,
    )


@router.get(
    "/orgs/{org}/projects/{project}/builds/{build}",
    response_model=Build,
    summary="Get a build",
    name="get_build",
)
async def get_build(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    build_id: BuildIdParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],  # noqa: ARG001
) -> Build:
    async with context.session.begin():
        service = context.factory.create_build_service()
        build = await service.get_by_public_id(
            org_slug=org_slug,
            project_slug=project_slug,
            build_id=build_id,
        )
    return Build.from_domain(build, context.request, org_slug, project_slug)


@router.patch(
    "/orgs/{org}/projects/{project}/builds/{build}",
    response_model=Build,
    summary="Update a build (signal upload complete)",
    name="patch_build",
)
async def patch_build(  # noqa: PLR0913
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    build_id: BuildIdParam,
    data: BuildUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_uploader)],  # noqa: ARG001
) -> Build:
    queue_url: str | None = None
    async with context.session.begin():
        service = context.factory.create_build_service()

        if data.status == BuildStatus.uploaded:
            build, queue_job = await service.signal_upload_complete(
                org_slug=org_slug,
                project_slug=project_slug,
                build_id=build_id,
            )
            queue_url = str(
                context.request.url_for(
                    "get_queue_job",
                    job=serialize_base32_id(queue_job.public_id),
                )
            )
        else:
            # No-op update: just fetch the build
            build = await service.get_by_public_id(
                org_slug=org_slug,
                project_slug=project_slug,
                build_id=build_id,
            )

        await context.session.commit()

    return Build.from_domain(
        build,
        context.request,
        org_slug,
        project_slug,
        queue_url=queue_url,
    )


@router.delete(
    "/orgs/{org}/projects/{project}/builds/{build}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a build",
    name="delete_build",
)
async def delete_build(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    build_id: BuildIdParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> None:
    async with context.session.begin():
        service = context.factory.create_build_service()
        await service.soft_delete(
            org_slug=org_slug,
            project_slug=project_slug,
            build_id=build_id,
        )
        await context.session.commit()
