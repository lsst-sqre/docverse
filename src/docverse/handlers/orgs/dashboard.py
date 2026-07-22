"""Dashboard rebuild endpoints within an organization's project."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.exceptions import ConflictError
from docverse.handlers.params import OrgSlugParam, ProjectSlugParam
from docverse.handlers.responses import error_responses

from .models import DashboardRebuildResponse, OrgDashboardRebuildEntry

router = APIRouter()


@router.post(
    "/orgs/{org}/projects/{project}/dashboard/rebuild",
    response_model=DashboardRebuildResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Rebuild a project's dashboard",
    name="post_dashboard_rebuild",
    responses=error_responses(status.HTTP_409_CONFLICT),
)
async def post_dashboard_rebuild(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> DashboardRebuildResponse:
    async with context.session.begin():
        service = context.factory.create_dashboard_build_enqueuer()
        queue_job = await service.enqueue_for_project_slug(
            org_slug=user.org.slug, project_slug=project_slug
        )
        await context.session.commit()

    if queue_job is None:
        msg = f"dashboard_build already queued for project {project_slug!r}"
        raise ConflictError(msg)
    response_model = DashboardRebuildResponse.from_queue_job(
        queue_job, context.request, org_slug
    )
    context.response.headers["Location"] = response_model.job_url
    return response_model


@router.post(
    "/orgs/{org}/dashboard/rebuild",
    response_model=list[OrgDashboardRebuildEntry],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Rebuild every project's dashboard in an organization",
    name="post_org_dashboard_rebuild",
)
async def post_org_dashboard_rebuild(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> list[OrgDashboardRebuildEntry]:
    async with context.session.begin():
        service = context.factory.create_dashboard_build_enqueuer()
        results = await service.enqueue_for_org(org_id=user.org.id)
        await context.session.commit()

    # This batch enqueues one job per project, so there is no single job
    # resource to point at. Per RFC 7231 the 202 ``Location`` names a status
    # monitor for the request; the org-scoped jobs collection serves that
    # role here.
    context.response.headers["Location"] = str(
        context.request.url_for("get_org_jobs", org=org_slug)
    )
    return [
        OrgDashboardRebuildEntry.from_domain(
            project, queue_job, context.request, org_slug
        )
        for project, queue_job in results
    ]
