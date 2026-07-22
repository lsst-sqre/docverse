"""Org-scoped job status endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from docverse.client.models import JobKind, JobStatus
from docverse.dependencies.auth import AuthenticatedUser, require_reader
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.exceptions import NotFoundError
from docverse.handlers.params import JobIdParam, OrgSlugParam
from docverse.handlers.queue.models import QueueJob
from docverse.storage.pagination import (
    DEFAULT_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
    QUEUE_JOB_CURSOR_TYPE,
)
from docverse.validation import parse_base32_id

router = APIRouter()


@router.get(
    "/orgs/{org}/jobs",
    response_model=list[QueueJob],
    summary="List jobs",
    name="get_org_jobs",
)
async def get_org_jobs(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
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
    kind_filter: Annotated[
        JobKind | None,
        Query(alias="kind", description="Filter jobs by kind."),
    ] = None,
    status_filter: Annotated[
        JobStatus | None,
        Query(alias="status", description="Filter jobs by status."),
    ] = None,
    project_slug: Annotated[
        str | None,
        Query(
            alias="project",
            description="Filter jobs by target project slug.",
        ),
    ] = None,
    run: Annotated[
        str | None,
        Query(
            description=(
                "Filter jobs by attributed keeper-sync run"
                " (Base32 public identifier)."
            ),
        ),
    ] = None,
) -> list[QueueJob]:
    parsed_cursor = (
        QUEUE_JOB_CURSOR_TYPE.from_str(cursor) if cursor is not None else None
    )
    context.rebind_logger(org=org_slug)
    async with context.session.begin():
        # Resolve the optional project-slug filter to an internal id,
        # scoped to this org so another org's project can never match.
        project_id: int | None = None
        if project_slug is not None:
            project = await context.factory.create_project_store().get_by_slug(
                org_id=user.org.id, slug=project_slug
            )
            if project is None:
                msg = f"Project {project_slug!r} not found"
                raise NotFoundError(msg)
            project_id = project.id

        # Resolve the optional keeper-sync run filter to an internal id.
        # A run belonging to another org 404s so cross-org existence
        # never leaks (mirrors the run-scoped jobs handler).
        keeper_sync_run_id: int | None = None
        if run is not None:
            run_public_id = parse_base32_id(run, resource="run")
            run_store = context.factory.create_keeper_sync_run_store()
            run_row = await run_store.get_by_public_id(run_public_id)
            if run_row is None or run_row.org_id != user.org.id:
                msg = f"Keeper sync run {run!r} not found"
                raise NotFoundError(msg)
            keeper_sync_run_id = run_row.id

        store = context.factory.create_queue_job_store()
        result = await store.list_by_org(
            org_id=user.org.id,
            kind=kind_filter,
            status=status_filter,
            project_id=project_id,
            keeper_sync_run_id=keeper_sync_run_id,
            cursor=parsed_cursor,
            limit=limit,
        )
        # Jobs on a page frequently share a keeper-sync run, so memoize the
        # run FK -> public-id resolution to collapse an N+1 run-store query.
        run_public_id_cache: dict[int, str | None] = {}
        jobs = [
            await QueueJob.from_domain(
                job,
                context.request,
                context.factory,
                org_slug=org_slug,
                run_public_id_cache=run_public_id_cache,
            )
            for job in result.entries
        ]
    context.response.headers["Link"] = result.link_header(context.request.url)
    context.response.headers["X-Total-Count"] = str(result.count)
    return jobs


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
