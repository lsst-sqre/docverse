"""LTD Keeper sync configuration and run endpoints within an organization."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, status

from docverse.client.models import (
    JobStatus,
    KeeperSyncConfig,
    KeeperSyncRunStatus,
)
from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.handlers.params import OrgSlugParam
from docverse.handlers.queue.models import QueueJob
from docverse.storage.pagination import (
    DEFAULT_PAGE_LIMIT,
    KEEPER_SYNC_EDITION_CURSOR_TYPE,
    KEEPER_SYNC_RUN_CURSOR_TYPE,
    MAX_PAGE_LIMIT,
    QUEUE_JOB_CURSOR_TYPE,
)

from .keeper_sync_models import (
    KeeperSyncEditionStatus,
    KeeperSyncProjectRefreshAccepted,
    KeeperSyncProjectStatus,
    KeeperSyncRun,
    KeeperSyncRunCreated,
)

router = APIRouter()


@router.get(
    "/orgs/{org}/keeper-sync",
    response_model=KeeperSyncConfig,
    summary="Get the organization's LTD Keeper sync configuration",
    name="get_org_keeper_sync_config",
)
async def get_org_keeper_sync_config(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> KeeperSyncConfig:
    async with context.session.begin():
        service = context.factory.create_keeper_sync_config_service()
        return await service.get(org_slug=org_slug)


@router.put(
    "/orgs/{org}/keeper-sync",
    response_model=KeeperSyncConfig,
    summary="Replace the organization's LTD Keeper sync configuration",
    name="put_org_keeper_sync_config",
)
async def put_org_keeper_sync_config(
    org_slug: OrgSlugParam,
    data: KeeperSyncConfig,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> KeeperSyncConfig:
    async with context.session.begin():
        service = context.factory.create_keeper_sync_config_service()
        result = await service.put(org_slug=org_slug, config=data)
        await context.session.commit()
    return result


@router.post(
    "/orgs/{org}/keeper-sync/runs",
    response_model=KeeperSyncRunCreated,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start a new LTD Keeper backfill run",
    name="post_org_keeper_sync_run",
)
async def post_org_keeper_sync_run(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> KeeperSyncRunCreated:
    async with context.session.begin():
        service = context.factory.create_keeper_sync_run_service()
        run, queue_job = await service.start_run(org_slug=org_slug)
        # The run was just created and the discovery queue-job is
        # already attributed to it; derive activity via the same store
        # call as ``GET`` so the response shape stays uniform.
        run_store = context.factory.create_keeper_sync_run_store()
        activity = await run_store.aggregate_activity(run_id=run.id)
        await context.session.commit()
    return KeeperSyncRunCreated.from_domain(
        run, activity, queue_job, context.request, org_slug
    )


@router.get(
    "/orgs/{org}/keeper-sync/projects/{ltd_slug}",
    response_model=KeeperSyncProjectStatus,
    summary="Get the keeper-sync status of one LTD project on this org",
    name="get_org_keeper_sync_project_status",
)
async def get_org_keeper_sync_project_status(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
    ltd_slug: Annotated[
        str,
        Path(description="LTD project slug to inspect."),
    ],
    ltd: Annotated[  # noqa: FBT002
        bool,
        Query(
            description=(
                "When true, the response includes a live-LTD edition"
                " reconciliation diff (``missing_in_docverse`` and"
                " ``missing_in_ltd``). Default false to keep the"
                " endpoint cheap for routine polling."
            ),
        ),
    ] = False,
) -> KeeperSyncProjectStatus:
    async with context.session.begin():
        service = context.factory.create_keeper_sync_project_service()
        result = await service.get_project_status(
            org_slug=org_slug,
            ltd_slug=ltd_slug,
            include_ltd_diff=ltd,
        )
    return KeeperSyncProjectStatus.from_domain(result, context.request)


@router.get(
    "/orgs/{org}/keeper-sync/projects/{ltd_slug}/editions",
    response_model=list[KeeperSyncEditionStatus],
    summary="List Docverse editions for one keeper-sync project",
    name="get_org_keeper_sync_project_editions",
)
async def get_org_keeper_sync_project_editions(  # noqa: PLR0913
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
    ltd_slug: Annotated[
        str,
        Path(description="LTD project slug to list editions for."),
    ],
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
) -> list[KeeperSyncEditionStatus]:
    parsed_cursor = (
        KEEPER_SYNC_EDITION_CURSOR_TYPE.from_str(cursor)
        if cursor is not None
        else None
    )
    async with context.session.begin():
        service = context.factory.create_keeper_sync_project_service()
        result = await service.list_project_editions(
            org_slug=org_slug,
            ltd_slug=ltd_slug,
            cursor=parsed_cursor,
            limit=limit,
        )
    context.response.headers["Link"] = result.page.link_header(
        context.request.url
    )
    context.response.headers["X-Total-Count"] = str(result.page.count)
    return [
        KeeperSyncEditionStatus.from_domain(
            edition,
            result.state_by_docverse_id.get(edition.id),
            context.request,
            result.org_slug,
            result.docverse_project_slug,
        )
        for edition in result.page.entries
    ]


@router.post(
    "/orgs/{org}/keeper-sync/projects/{ltd_slug}/refresh",
    response_model=KeeperSyncProjectRefreshAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger an immediate sync of one LTD project",
    name="post_org_keeper_sync_project_refresh",
)
async def post_org_keeper_sync_project_refresh(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
    ltd_slug: Annotated[
        str,
        Path(description="LTD project slug to refresh."),
    ],
) -> KeeperSyncProjectRefreshAccepted:
    async with context.session.begin():
        service = context.factory.create_keeper_sync_run_service()
        queue_job = await service.refresh_project(
            org_slug=org_slug, ltd_slug=ltd_slug
        )
        await context.session.commit()
    return KeeperSyncProjectRefreshAccepted.from_domain(
        queue_job, context.request
    )


@router.get(
    "/orgs/{org}/keeper-sync/runs",
    response_model=list[KeeperSyncRun],
    summary="List LTD Keeper sync runs for an organization",
    name="get_org_keeper_sync_runs",
)
async def get_org_keeper_sync_runs(  # noqa: PLR0913
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
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
        KeeperSyncRunStatus | None,
        Query(alias="status", description="Filter runs by status."),
    ] = None,
) -> list[KeeperSyncRun]:
    parsed_cursor = (
        KEEPER_SYNC_RUN_CURSOR_TYPE.from_str(cursor)
        if cursor is not None
        else None
    )
    async with context.session.begin():
        service = context.factory.create_keeper_sync_run_service()
        result = await service.list_runs(
            org_slug=org_slug,
            status=status_filter,
            cursor=parsed_cursor,
            limit=limit,
        )
        run_store = context.factory.create_keeper_sync_run_store()
        # One ``GROUP BY`` query covers every run in the page; avoids
        # an N+1 round-trip pattern at ``MAX_PAGE_LIMIT``.
        activity_by_id = await run_store.aggregate_activity_for_runs(
            run_ids=[run.id for run in result.entries]
        )
    context.response.headers["Link"] = result.link_header(context.request.url)
    context.response.headers["X-Total-Count"] = str(result.count)
    return [
        KeeperSyncRun.from_domain(
            run, activity_by_id[run.id], context.request, org_slug
        )
        for run in result.entries
    ]


@router.get(
    "/orgs/{org}/keeper-sync/runs/{run_id}",
    response_model=KeeperSyncRun,
    summary="Get an LTD Keeper sync run with aggregate counters",
    name="get_org_keeper_sync_run",
)
async def get_org_keeper_sync_run(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
    run_id: Annotated[
        int, Path(description="Numeric identifier for the run.")
    ],
) -> KeeperSyncRun:
    async with context.session.begin():
        service = context.factory.create_keeper_sync_run_service()
        result = await service.get_run(org_slug=org_slug, run_id=run_id)
    return KeeperSyncRun.from_domain(
        result.run, result.activity, context.request, org_slug
    )


@router.get(
    "/orgs/{org}/keeper-sync/runs/{run_id}/jobs",
    response_model=list[QueueJob],
    summary="List child queue jobs for an LTD Keeper sync run",
    name="get_org_keeper_sync_run_jobs",
)
async def get_org_keeper_sync_run_jobs(  # noqa: PLR0913
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
    run_id: Annotated[
        int, Path(description="Numeric identifier for the run.")
    ],
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
        JobStatus | None,
        Query(alias="status", description="Filter jobs by status."),
    ] = None,
) -> list[QueueJob]:
    parsed_cursor = (
        QUEUE_JOB_CURSOR_TYPE.from_str(cursor) if cursor is not None else None
    )
    async with context.session.begin():
        service = context.factory.create_keeper_sync_run_service()
        result = await service.list_run_jobs(
            org_slug=org_slug,
            run_id=run_id,
            status=status_filter,
            cursor=parsed_cursor,
            limit=limit,
        )
    context.response.headers["Link"] = result.link_header(context.request.url)
    context.response.headers["X-Total-Count"] = str(result.count)
    return [
        QueueJob.from_domain(job, context.request) for job in result.entries
    ]
