"""LTD Keeper sync configuration and run endpoints within an organization."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, status

from docverse.client.models import KeeperSyncConfig, KeeperSyncRunStatus
from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.handlers.params import OrgSlugParam
from docverse.storage.pagination import (
    DEFAULT_PAGE_LIMIT,
    KEEPER_SYNC_RUN_CURSOR_TYPE,
    MAX_PAGE_LIMIT,
)

from .keeper_sync_models import KeeperSyncRun, KeeperSyncRunCreated

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
        # already attributed to it; derive counters via the same store
        # call as ``GET`` so the response shape stays uniform.
        run_store = context.factory.create_keeper_sync_run_store()
        counters = await run_store.aggregate_counters(run_id=run.id)
        await context.session.commit()
    return KeeperSyncRunCreated.from_domain(
        run, counters, queue_job, context.request, org_slug
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
        counters_by_id = await run_store.aggregate_counters_for_runs(
            run_ids=[run.id for run in result.entries]
        )
    context.response.headers["Link"] = result.link_header(context.request.url)
    context.response.headers["X-Total-Count"] = str(result.count)
    return [
        KeeperSyncRun.from_domain(
            run, counters_by_id[run.id], context.request, org_slug
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
        result.run, result.counters, context.request, org_slug
    )
