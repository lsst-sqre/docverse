"""Queue endpoints for viewing job status."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from docverse.dependencies.context import RequestContext, context_dependency
from docverse.domain.base32id import validate_base32_id
from docverse.exceptions import NotFoundError

from .models import QueueJob

router = APIRouter(tags=["queue"])


@router.get(
    "/queue/jobs/{job_id}",
    response_model=QueueJob,
    summary="Get a queue job",
)
async def get_queue_job(
    job_id: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> QueueJob:
    try:
        public_id = validate_base32_id(job_id)
    except ValueError as exc:
        msg = f"Invalid job ID {job_id!r}"
        raise NotFoundError(msg) from exc
    context.rebind_logger(job_id=job_id)
    async with context.session.begin():
        store = context.factory.create_queue_job_store()
        job = await store.get_by_public_id(public_id)
        if job is None:
            msg = f"Queue job {job_id!r} not found"
            raise NotFoundError(msg)
    return QueueJob.from_domain(job, context.request)
