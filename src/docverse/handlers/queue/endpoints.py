"""Queue endpoints for viewing job status."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from docverse.dependencies.context import RequestContext, context_dependency
from docverse.exceptions import NotFoundError
from docverse.handlers.params import JobIdParam
from docverse.validation import parse_base32_id

from .models import QueueJob

router = APIRouter(tags=["queue"])


@router.get(
    "/queue/jobs/{job}",
    response_model=QueueJob,
    summary="Get a queue job",
)
async def get_queue_job(
    job_id: JobIdParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> QueueJob:
    public_id = parse_base32_id(job_id, resource="job")
    context.rebind_logger(job_id=job_id)
    async with context.session.begin():
        store = context.factory.create_queue_job_store()
        job = await store.get_by_public_id(public_id)
        if job is None:
            msg = f"Queue job {job_id!r} not found"
            raise NotFoundError(msg)
        return await QueueJob.from_domain(
            job, context.request, context.factory
        )
