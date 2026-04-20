"""Dashboard rebuild endpoints within an organization's project."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, Field

from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.domain.base32id import serialize_base32_id
from docverse.handlers.params import OrgSlugParam, ProjectSlugParam

router = APIRouter()


class DashboardRebuildResponse(BaseModel):
    """Response body for the manual rebuild endpoint."""

    queue_job_id: str = Field(
        description="Public Base32 identifier for the enqueued job."
    )


class OrgDashboardRebuildEntry(BaseModel):
    """One enqueued ``dashboard_build`` in the org-wide rebuild response."""

    project_slug: str = Field(
        description="Slug of the project the job will rebuild."
    )
    queue_job_id: str = Field(
        description="Public Base32 identifier for the enqueued job."
    )


@router.post(
    "/orgs/{org}/projects/{project}/dashboard/rebuild",
    response_model=DashboardRebuildResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Rebuild a project's dashboard",
    name="post_dashboard_rebuild",
)
async def post_dashboard_rebuild(
    org_slug: OrgSlugParam,  # noqa: ARG001
    project_slug: ProjectSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> DashboardRebuildResponse:
    async with context.session.begin():
        service = context.factory.create_dashboard_publishing_service()
        queue_job = await service.enqueue_for_project_slug(
            org_slug=user.org.slug, project_slug=project_slug
        )
        await context.session.commit()

    return DashboardRebuildResponse(
        queue_job_id=serialize_base32_id(queue_job.public_id)
    )


@router.post(
    "/orgs/{org}/dashboard/rebuild",
    response_model=list[OrgDashboardRebuildEntry],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Rebuild every project's dashboard in an organization",
    name="post_org_dashboard_rebuild",
)
async def post_org_dashboard_rebuild(
    org_slug: OrgSlugParam,  # noqa: ARG001
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> list[OrgDashboardRebuildEntry]:
    async with context.session.begin():
        service = context.factory.create_dashboard_publishing_service()
        results = await service.enqueue_for_org(org_id=user.org.id)
        await context.session.commit()

    return [
        OrgDashboardRebuildEntry(
            project_slug=project.slug,
            queue_job_id=serialize_base32_id(queue_job.public_id),
        )
        for project, queue_job in results
    ]
