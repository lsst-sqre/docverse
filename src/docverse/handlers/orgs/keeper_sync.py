"""LTD Keeper sync configuration and run endpoints within an organization."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Response, status

from docverse.client.models import (
    KeeperSyncConfig,
    KeeperSyncConfigUpdate,
    KeeperSyncEditionStatus,
    KeeperSyncResourceType,
    KeeperSyncRun,
    KeeperSyncRunStatus,
    KeeperSyncTombstoneReason,
)
from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.handlers.params import OrgSlugParam, RunIdParam, TombstoneIdParam
from docverse.storage.keeper_sync import ResourceType, TombstoneReason
from docverse.storage.pagination import (
    DEFAULT_PAGE_LIMIT,
    KEEPER_SYNC_EDITION_CURSOR_TYPE,
    KEEPER_SYNC_PROJECT_STATE_CURSOR_TYPE,
    KEEPER_SYNC_RUN_CURSOR_TYPE,
    KEEPER_SYNC_TOMBSTONE_CURSOR_TYPE,
    MAX_PAGE_LIMIT,
)
from docverse.validation import parse_base32_id

from .keeper_sync_models import (
    KeeperSyncProjectRefreshAccepted,
    KeeperSyncProjectStatus,
    KeeperSyncRunCreated,
    KeeperSyncTombstone,
    keeper_sync_edition_status_from_domain,
    keeper_sync_run_from_domain,
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
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
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
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> KeeperSyncConfig:
    async with context.session.begin():
        service = context.factory.create_keeper_sync_config_service()
        result = await service.put(org_slug=org_slug, config=data)
        await context.session.commit()
    return result


@router.patch(
    "/orgs/{org}/keeper-sync",
    response_model=KeeperSyncConfig,
    summary="Partially update the LTD Keeper sync configuration",
    name="patch_org_keeper_sync_config",
)
async def patch_org_keeper_sync_config(
    org_slug: OrgSlugParam,
    data: KeeperSyncConfigUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> KeeperSyncConfig:
    """Merge-patch the org's keeper-sync config.

    Applies JSON-Merge-Patch semantics: omitted fields are left untouched;
    ``project_slugs``, when provided, replaces the stored array wholesale (no
    append). ``PUT`` remains available for a full replacement.
    """
    async with context.session.begin():
        service = context.factory.create_keeper_sync_config_service()
        result = await service.patch(org_slug=org_slug, update=data)
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
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
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
    response_model = KeeperSyncRunCreated.from_domain(
        run, activity, queue_job, context.request, org_slug
    )
    context.response.headers["Location"] = str(response_model.job_url)
    return response_model


@router.get(
    "/orgs/{org}/keeper-sync/projects",
    response_model=list[KeeperSyncProjectStatus],
    summary="List keeper-sync projects on an organization",
    name="get_org_keeper_sync_projects",
)
async def get_org_keeper_sync_projects(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
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
) -> list[KeeperSyncProjectStatus]:
    parsed_cursor = (
        KEEPER_SYNC_PROJECT_STATE_CURSOR_TYPE.from_str(cursor)
        if cursor is not None
        else None
    )
    async with context.session.begin():
        service = context.factory.create_keeper_sync_project_service()
        result = await service.list_project_statuses(
            org_slug=org_slug, cursor=parsed_cursor, limit=limit
        )
    context.response.headers["Link"] = result.page.link_header(
        context.request.url
    )
    context.response.headers["X-Total-Count"] = str(result.page.count)
    return [
        KeeperSyncProjectStatus.from_domain(entry, context.request)
        for entry in result.entries
    ]


@router.get(
    "/orgs/{org}/keeper-sync/projects/{ltd_slug}",
    response_model=KeeperSyncProjectStatus,
    summary="Get the keeper-sync status of one LTD project on this org",
    name="get_org_keeper_sync_project_status",
)
async def get_org_keeper_sync_project_status(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
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
async def get_org_keeper_sync_project_editions(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
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
        keeper_sync_edition_status_from_domain(
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
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
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
    response_model = KeeperSyncProjectRefreshAccepted.from_domain(
        queue_job, context.request, org_slug
    )
    context.response.headers["Location"] = str(response_model.job_url)
    return response_model


@router.get(
    "/orgs/{org}/keeper-sync/runs",
    response_model=list[KeeperSyncRun],
    summary="List LTD Keeper sync runs for an organization",
    name="get_org_keeper_sync_runs",
)
async def get_org_keeper_sync_runs(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
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
        keeper_sync_run_from_domain(
            run, activity_by_id[run.id], context.request, org_slug
        )
        for run in result.entries
    ]


@router.get(
    "/orgs/{org}/keeper-sync/runs/{run}",
    response_model=KeeperSyncRun,
    summary="Get an LTD Keeper sync run with aggregate counters",
    name="get_org_keeper_sync_run",
)
async def get_org_keeper_sync_run(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
    run_id: RunIdParam,
) -> KeeperSyncRun:
    public_id = parse_base32_id(run_id, resource="run")
    async with context.session.begin():
        service = context.factory.create_keeper_sync_run_service()
        result = await service.get_run(org_slug=org_slug, public_id=public_id)
    return keeper_sync_run_from_domain(
        result.run, result.activity, context.request, org_slug
    )


@router.get(
    "/orgs/{org}/keeper-sync/tombstones",
    response_model=list[KeeperSyncTombstone],
    summary="List sync tombstones for an organization",
    name="get_org_keeper_sync_tombstones",
)
async def get_org_keeper_sync_tombstones(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
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
    resource_type: Annotated[
        KeeperSyncResourceType | None,
        Query(description="Filter tombstones by resource type."),
    ] = None,
    tombstone_reason: Annotated[
        KeeperSyncTombstoneReason | None,
        Query(description="Filter tombstones by reason."),
    ] = None,
) -> list[KeeperSyncTombstone]:
    parsed_cursor = (
        KEEPER_SYNC_TOMBSTONE_CURSOR_TYPE.from_str(cursor)
        if cursor is not None
        else None
    )
    storage_resource_type = (
        ResourceType(resource_type.value)
        if resource_type is not None
        else None
    )
    storage_tombstone_reason = (
        TombstoneReason(tombstone_reason.value)
        if tombstone_reason is not None
        else None
    )
    context.rebind_logger(actor=user.username)
    async with context.session.begin():
        service = context.factory.create_keeper_sync_tombstone_service()
        result = await service.list_for_org(
            org_id=user.org.id,
            cursor=parsed_cursor,
            limit=limit,
            resource_type=storage_resource_type,
            tombstone_reason=storage_tombstone_reason,
        )
    context.response.headers["Link"] = result.page.link_header(
        context.request.url
    )
    context.response.headers["X-Total-Count"] = str(result.page.count)
    context.logger.info(
        "Listed sync tombstones",
        org=org_slug,
        resource_type=(
            resource_type.value if resource_type is not None else None
        ),
        tombstone_reason=(
            tombstone_reason.value if tombstone_reason is not None else None
        ),
        count=len(result.page.entries),
    )
    return [
        KeeperSyncTombstone.from_domain(
            entry,
            result.display_path_by_state_id[entry.id],
            context.request,
            org_slug,
        )
        for entry in result.page.entries
    ]


@router.delete(
    "/orgs/{org}/keeper-sync/tombstones/{tombstone}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Clear a sync tombstone",
    name="delete_org_keeper_sync_tombstone",
)
async def delete_org_keeper_sync_tombstone(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
    tombstone_id: TombstoneIdParam,
) -> Response:
    public_id = parse_base32_id(tombstone_id, resource="tombstone")
    context.rebind_logger(actor=user.username)
    async with context.session.begin():
        service = context.factory.create_keeper_sync_tombstone_service()
        cleared = await service.clear(public_id=public_id, org_id=user.org.id)
        await context.session.commit()
    context.logger.info(
        "Cleared sync tombstone",
        org=org_slug,
        tombstone_id=tombstone_id,
        resource_type=cleared.state.resource_type,
        ltd_id=cleared.state.ltd_id,
        ltd_slug=cleared.state.ltd_slug,
        revived_docverse_row=cleared.revived_docverse_row,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
