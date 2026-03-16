"""Build endpoints within an organization's project."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from docverse.client.models import (
    BuildCreate,
    BuildStatus,
    BuildUpdate,
    JobKind,
)
from docverse.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
    require_uploader,
)
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.domain.base32id import serialize_base32_id, validate_base32_id
from docverse.exceptions import NotFoundError

from .models import Build

router = APIRouter()


async def _resolve_project(
    context: RequestContext, org_id: int, project_slug: str
) -> int:
    """Resolve a project slug to its ID."""
    project = await context.factory.create_project_service().get_by_slug(
        org_id=org_id, slug=project_slug
    )
    if project is None:
        msg = f"Project {project_slug!r} not found"
        raise NotFoundError(msg)
    return project.id


@router.get(
    "/orgs/{org_slug}/projects/{project_slug}/builds",
    response_model=list[Build],
    summary="List builds for a project",
    name="get_builds",
)
async def get_builds(
    org_slug: str,
    project_slug: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
) -> list[Build]:
    async with context.session.begin():
        project_id = await _resolve_project(context, user.org.id, project_slug)
        service = context.factory.create_build_service()
        builds = await service.list_by_project(project_id)
    return [
        Build.from_domain(b, context.request, org_slug, project_slug)
        for b in builds
    ]


@router.post(
    "/orgs/{org_slug}/projects/{project_slug}/builds",
    response_model=Build,
    status_code=status.HTTP_201_CREATED,
    summary="Create a build",
    name="post_build",
)
async def post_build(
    org_slug: str,
    project_slug: str,
    data: BuildCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_uploader)],
) -> Build:
    async with context.session.begin():
        project_id = await _resolve_project(context, user.org.id, project_slug)
        service = context.factory.create_build_service()
        build = await service.create(
            project_id=project_id,
            data=data,
            uploader=user.username,
        )
        await context.session.commit()
    # Placeholder upload_url until ObjectStore is implemented
    upload_url = f"https://placeholder.example.com/upload/{build.staging_key}"
    return Build.from_domain(
        build,
        context.request,
        org_slug,
        project_slug,
        upload_url=upload_url,
    )


@router.get(
    "/orgs/{org_slug}/projects/{project_slug}/builds/{build_id}",
    response_model=Build,
    summary="Get a build",
    name="get_build",
)
async def get_build(
    org_slug: str,
    project_slug: str,
    build_id: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
) -> Build:
    try:
        public_id = validate_base32_id(build_id)
    except ValueError as exc:
        msg = f"Invalid build ID {build_id!r}"
        raise NotFoundError(msg) from exc
    async with context.session.begin():
        project_id = await _resolve_project(context, user.org.id, project_slug)
        service = context.factory.create_build_service()
        build = await service.get_by_public_id(
            project_id=project_id, public_id=public_id
        )
        if build is None:
            msg = f"Build {build_id!r} not found"
            raise NotFoundError(msg)
    return Build.from_domain(build, context.request, org_slug, project_slug)


@router.patch(
    "/orgs/{org_slug}/projects/{project_slug}/builds/{build_id}",
    response_model=Build,
    summary="Update a build (signal upload complete)",
    name="patch_build",
)
async def patch_build(  # noqa: PLR0913
    org_slug: str,
    project_slug: str,
    build_id: str,
    data: BuildUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_uploader)],
) -> Build:
    try:
        public_id = validate_base32_id(build_id)
    except ValueError as exc:
        msg = f"Invalid build ID {build_id!r}"
        raise NotFoundError(msg) from exc

    queue_url: str | None = None
    async with context.session.begin():
        project = await context.factory.create_project_service().get_by_slug(
            org_id=user.org.id, slug=project_slug
        )
        if project is None:
            msg = f"Project {project_slug!r} not found"
            raise NotFoundError(msg)

        build_service = context.factory.create_build_service()
        build = await build_service.get_by_public_id(
            project_id=project.id, public_id=public_id
        )
        if build is None:
            msg = f"Build {build_id!r} not found"
            raise NotFoundError(msg)

        # Signal upload complete → transition to processing
        if data.status == BuildStatus.uploaded:
            build = await build_service.signal_upload_complete(
                build_id=build.id
            )
            # Enqueue a build_processing job
            queue_backend = context.factory.create_queue_backend()
            queue_job_store = context.factory.create_queue_job_store()
            backend_job_id = await queue_backend.enqueue(
                "build_processing",
                {
                    "org_id": user.org.id,
                    "project_id": project.id,
                    "build_id": build.id,
                },
            )
            queue_job = await queue_job_store.create(
                kind=JobKind.build_processing,
                org_id=user.org.id,
                backend_job_id=backend_job_id,
                project_id=project.id,
                build_id=build.id,
            )
            queue_url = str(
                context.request.url_for(
                    "get_queue_job",
                    job_id=serialize_base32_id(queue_job.public_id),
                )
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
    "/orgs/{org_slug}/projects/{project_slug}/builds/{build_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a build",
    name="delete_build",
)
async def delete_build(
    org_slug: str,  # noqa: ARG001
    project_slug: str,
    build_id: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> None:
    try:
        public_id = validate_base32_id(build_id)
    except ValueError as exc:
        msg = f"Invalid build ID {build_id!r}"
        raise NotFoundError(msg) from exc
    async with context.session.begin():
        project_id = await _resolve_project(context, user.org.id, project_slug)
        service = context.factory.create_build_service()
        build = await service.get_by_public_id(
            project_id=project_id, public_id=public_id
        )
        if build is None:
            msg = f"Build {build_id!r} not found"
            raise NotFoundError(msg)
        deleted = await service.soft_delete(build_id=build.id)
        if not deleted:
            msg = f"Build {build_id!r} not found"
            raise NotFoundError(msg)
        await context.session.commit()
