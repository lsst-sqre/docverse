"""Org-scoped job status endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from docverse.dependencies.auth import AuthenticatedUser, require_reader
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.exceptions import NotFoundError
from docverse.handlers.params import JobIdParam, OrgSlugParam
from docverse.handlers.queue.models import QueueJob
from docverse.validation import parse_base32_id

router = APIRouter()


@router.get(
    "/orgs/{org}/jobs/{job}",
    response_model=QueueJob,
    summary="Get a job",
    name="get_org_job",
)
async def get_org_job(
    org_slug: OrgSlugParam,
    job_id: JobIdParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
) -> QueueJob:
    public_id = parse_base32_id(job_id, resource="job")
    context.rebind_logger(job_id=job_id)
    async with context.session.begin():
        store = context.factory.create_queue_job_store()
        job = await store.get_by_public_id(public_id)
        # 404 both when the job is absent and when it belongs to another
        # org, so cross-org existence never leaks.
        if job is None or job.org_id != user.org.id:
            msg = f"Job {job_id!r} not found"
            raise NotFoundError(msg)
        return await QueueJob.from_domain(
            job, context.request, context.factory, org_slug=org_slug
        )
