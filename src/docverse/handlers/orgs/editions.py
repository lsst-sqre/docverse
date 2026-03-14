"""Edition endpoints within an organization's project."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from docverse.client.models import EditionCreate, EditionUpdate
from docverse.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
)
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.exceptions import ConflictError, NotFoundError

from .models import Edition

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
    "/orgs/{org_slug}/projects/{project_slug}/editions",
    response_model=list[Edition],
    summary="List editions for a project",
    name="get_editions",
)
async def get_editions(
    org_slug: str,
    project_slug: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
) -> list[Edition]:
    async with context.session.begin():
        project_id = await _resolve_project(context, user.org.id, project_slug)
        service = context.factory.create_edition_service()
        editions = await service.list_by_project(project_id)
    return [
        Edition.from_domain(e, context.request, org_slug, project_slug)
        for e in editions
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
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> Edition:
    async with context.session.begin():
        project_id = await _resolve_project(context, user.org.id, project_slug)
        service = context.factory.create_edition_service()
        existing = await service.get_by_slug(
            project_id=project_id, slug=data.slug
        )
        if existing is not None:
            msg = f"Edition with slug {data.slug!r} already exists"
            raise ConflictError(msg)
        edition = await service.create(project_id=project_id, data=data)
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
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
) -> Edition:
    async with context.session.begin():
        project_id = await _resolve_project(context, user.org.id, project_slug)
        service = context.factory.create_edition_service()
        edition = await service.get_by_slug(
            project_id=project_id, slug=edition_slug
        )
        if edition is None:
            msg = f"Edition {edition_slug!r} not found"
            raise NotFoundError(msg)
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
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> Edition:
    async with context.session.begin():
        project_id = await _resolve_project(context, user.org.id, project_slug)
        service = context.factory.create_edition_service()
        edition = await service.update(
            project_id=project_id, slug=edition_slug, data=data
        )
        if edition is None:
            msg = f"Edition {edition_slug!r} not found"
            raise NotFoundError(msg)
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
    org_slug: str,  # noqa: ARG001
    project_slug: str,
    edition_slug: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> None:
    async with context.session.begin():
        project_id = await _resolve_project(context, user.org.id, project_slug)
        service = context.factory.create_edition_service()
        deleted = await service.soft_delete(
            project_id=project_id, slug=edition_slug
        )
        if not deleted:
            msg = f"Edition {edition_slug!r} not found"
            raise NotFoundError(msg)
        await context.session.commit()
