"""Edition endpoints within an organization's project."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from docverse.client.models import (
    Edition,
    EditionCreate,
    EditionKind,
    EditionRollback,
    EditionUpdate,
)
from docverse.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
)
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.domain.published_url import (
    edition_published_url,
    project_published_url,
)
from docverse.exceptions import NotFoundError, PermissionDeniedError
from docverse.handlers.params import (
    EditionSlugParam,
    OrgSlugParam,
    ProjectSlugParam,
)
from docverse.metrics import (
    EditionLifecycleEvent,
    LifecycleAction,
    MetricsEditionKind,
)
from docverse.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_slug,
)
from docverse.storage.keeper_sync import TombstoneReason
from docverse.storage.pagination import (
    DEFAULT_PAGE_LIMIT,
    EDITION_CURSOR_TYPES,
    EDITION_HISTORY_CURSOR_TYPE,
    MAX_PAGE_LIMIT,
    EditionSortOrder,
)

from .models import EditionBuildHistoryResponse, edition_from_domain

router = APIRouter()


@router.get(
    "/orgs/{org}/projects/{project}/editions",
    response_model=list[Edition],
    summary="List editions for a project",
    name="get_editions",
)
async def get_editions(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
    order: Annotated[
        EditionSortOrder,
        Query(description="Sort order for results."),
    ] = EditionSortOrder.slug,
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
    kind: Annotated[
        EditionKind | None,
        Query(description="Filter editions by kind."),
    ] = None,
) -> list[Edition]:
    cursor_type = EDITION_CURSOR_TYPES[order]
    parsed_cursor = (
        cursor_type.from_str(cursor) if cursor is not None else None
    )
    async with context.session.begin():
        service = context.factory.create_edition_service()
        org, project, result = await service.list_by_project(
            org_slug=org_slug,
            project_slug=project_slug,
            cursor_type=cursor_type,
            cursor=parsed_cursor,
            limit=limit,
            kind=kind,
        )
    context.response.headers["Link"] = result.link_header(context.request.url)
    context.response.headers["X-Total-Count"] = str(result.count)
    project_url = project_published_url(org, project)
    return [
        edition_from_domain(
            e,
            context.request,
            org_slug,
            project_slug,
            published_url=edition_published_url(project_url, e),
        )
        for e in result.entries
    ]


@router.post(
    "/orgs/{org}/projects/{project}/editions",
    response_model=Edition,
    status_code=status.HTTP_201_CREATED,
    summary="Create an edition",
    name="post_edition",
)
async def post_edition(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    data: EditionCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> Edition:
    async with context.session.begin():
        service = context.factory.create_edition_service()
        org, project, edition = await service.create(
            org_slug=org_slug, project_slug=project_slug, data=data
        )
        await context.session.commit()
    # Emit after the commit so the event reflects durably persisted state.
    # Production runs raise_on_error=False, so a metrics-backend outage
    # cannot fail this request (no defensive try/except).
    await context.events.edition_lifecycle.publish(
        EditionLifecycleEvent(
            organization=org_slug,
            project=project_slug,
            action=LifecycleAction.create,
            edition_kind=MetricsEditionKind.from_api(edition.kind),
        )
    )
    await try_enqueue_dashboard_build_by_slug(
        factory=context.factory,
        session=context.session,
        logger=context.logger,
        org_slug=org_slug,
        project_slug=project_slug,
    )
    project_url = project_published_url(org, project)
    return edition_from_domain(
        edition,
        context.request,
        org_slug,
        project_slug,
        published_url=edition_published_url(project_url, edition),
    )


@router.get(
    "/orgs/{org}/projects/{project}/editions/{edition}",
    response_model=Edition,
    summary="Get an edition",
    name="get_edition",
)
async def get_edition(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
) -> Edition:
    async with context.session.begin():
        service = context.factory.create_edition_service()
        org, project, edition = await service.get_by_slug(
            org_slug=org_slug,
            project_slug=project_slug,
            slug=edition_slug,
        )
    project_url = project_published_url(org, project)
    return edition_from_domain(
        edition,
        context.request,
        org_slug,
        project_slug,
        published_url=edition_published_url(project_url, edition),
    )


@router.get(
    "/orgs/{org}/projects/{project}/editions/{edition}/history",
    response_model=list[EditionBuildHistoryResponse],
    summary="List build history for an edition",
    name="get_edition_history",
)
async def get_edition_history(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
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
    include_deleted: Annotated[  # noqa: FBT002
        bool,
        Query(
            description=(
                "Include history entries for soft-deleted builds. "
                "Defaults to false."
            ),
        ),
    ] = False,
) -> list[EditionBuildHistoryResponse]:
    parsed_cursor = (
        EDITION_HISTORY_CURSOR_TYPE.from_str(cursor)
        if cursor is not None
        else None
    )
    async with context.session.begin():
        service = context.factory.create_edition_service()
        result = await service.list_history(
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slug=edition_slug,
            cursor=parsed_cursor,
            limit=limit,
            include_deleted=include_deleted,
        )
    context.response.headers["Link"] = result.link_header(context.request.url)
    context.response.headers["X-Total-Count"] = str(result.count)
    return [
        EditionBuildHistoryResponse.from_domain(
            entry, context.request, org_slug, project_slug
        )
        for entry in result.entries
    ]


@router.post(
    "/orgs/{org}/projects/{project}/editions/{edition}/rollback",
    response_model=Edition,
    summary="Roll back an edition to a previous build",
    name="post_edition_rollback",
)
async def post_edition_rollback(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
    data: EditionRollback,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> Edition:
    async with context.session.begin():
        service = context.factory.create_edition_service()
        org, project, edition = await service.rollback(
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slug=edition_slug,
            build_public_id=data.build,
        )
        await context.session.commit()
    # Publish after the commit (best-effort; raise_on_error=False).
    await context.events.edition_lifecycle.publish(
        EditionLifecycleEvent(
            organization=org_slug,
            project=project_slug,
            action=LifecycleAction.rollback,
            edition_kind=MetricsEditionKind.from_api(edition.kind),
        )
    )
    await try_enqueue_dashboard_build_by_slug(
        factory=context.factory,
        session=context.session,
        logger=context.logger,
        org_slug=org_slug,
        project_slug=project_slug,
    )
    project_url = project_published_url(org, project)
    return edition_from_domain(
        edition,
        context.request,
        org_slug,
        project_slug,
        published_url=edition_published_url(project_url, edition),
    )


@router.patch(
    "/orgs/{org}/projects/{project}/editions/{edition}",
    response_model=Edition,
    summary="Update an edition",
    name="patch_edition",
)
async def patch_edition(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
    data: EditionUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> Edition:
    if edition_slug.lower() == "__main" and data.kind is not None:
        msg = "Cannot change the kind of the default '__main' edition"
        raise PermissionDeniedError(msg)
    async with context.session.begin():
        service = context.factory.create_edition_service()
        org, project, edition = await service.update(
            org_slug=org_slug,
            project_slug=project_slug,
            slug=edition_slug,
            data=data,
        )
        await context.session.commit()
    # Publish after the commit (best-effort; raise_on_error=False).
    await context.events.edition_lifecycle.publish(
        EditionLifecycleEvent(
            organization=org_slug,
            project=project_slug,
            action=LifecycleAction.update,
            edition_kind=MetricsEditionKind.from_api(edition.kind),
        )
    )
    await try_enqueue_dashboard_build_by_slug(
        factory=context.factory,
        session=context.session,
        logger=context.logger,
        org_slug=org_slug,
        project_slug=project_slug,
    )
    project_url = project_published_url(org, project)
    return edition_from_domain(
        edition,
        context.request,
        org_slug,
        project_slug,
        published_url=edition_published_url(project_url, edition),
    )


@router.delete(
    "/orgs/{org}/projects/{project}/editions/{edition}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an edition",
    name="delete_edition",
)
async def delete_edition(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> None:
    if edition_slug.lower() == "__main":
        msg = "The default edition '__main' cannot be deleted"
        raise PermissionDeniedError(msg)
    async with context.session.begin():
        org_store = context.factory.create_org_store()
        project_store = context.factory.create_project_store()
        edition_store = context.factory.create_edition_store()
        org = await org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        project = await project_store.get_by_slug(
            org_id=org.id, slug=project_slug
        )
        if project is None:
            msg = f"Project {project_slug!r} not found"
            raise NotFoundError(msg)
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug=edition_slug
        )
        if edition is None:
            msg = f"Edition {edition_slug!r} not found"
            raise NotFoundError(msg)
        service = context.factory.create_edition_service()
        deleted = await service.soft_delete(
            org_id=org.id,
            project_id=project.id,
            edition_id=edition.id,
            edition_slug=edition.slug,
            reason=TombstoneReason.manual_delete,
        )
        if not deleted:
            msg = f"Edition {edition_slug!r} not found"
            raise NotFoundError(msg)
        await context.session.commit()
    # Remove the CDN pointer after the soft-delete commit so the public
    # URL stops resolving once the row is gone. ``unpublish`` is
    # idempotent and a no-op for orgs without a configured CDN, so it
    # can be called unconditionally. Wrapped in its own ``begin()``
    # block because the publishing service reads the org row (and may
    # read service config + credentials) — without an explicit
    # transaction SQLAlchemy auto-begins an implicit one that would
    # then conflict with the dashboard enqueue's own ``session.begin()``.
    #
    # Failure semantics: if ``unpublish`` raises, the soft-delete is
    # already committed and is not rolled back — the client sees a 5xx
    # but the edition row stays soft-deleted, and the dashboard rebuild
    # below is skipped because the exception unwinds before reaching it.
    # The stale CDN pointer is then cleaned up on the next lifecycle
    # pass (``unpublish`` is idempotent, so re-running is safe). This is
    # the opposite of the lifecycle worker, which runs ``unpublish``
    # inside the DB transaction so a CDN failure rolls back the batch;
    # the asymmetry is deliberate because the handler path is driven by
    # a single user action with no automatic retry, while the worker
    # path is re-driven on every dispatcher tick.
    async with context.session.begin():
        publishing_service = (
            context.factory.create_edition_publishing_service()
        )
        await publishing_service.unpublish(
            org_id=org.id,
            project_slug=project_slug,
            edition_slug=edition_slug,
        )
    await try_enqueue_dashboard_build_by_slug(
        factory=context.factory,
        session=context.session,
        logger=context.logger,
        org_slug=org_slug,
        project_slug=project_slug,
    )
    # Delete is multi-transaction (soft-delete commit + CDN unpublish);
    # publish only after that final step succeeds, so the event signals a
    # fully-completed delete (best-effort, raise_on_error=False). The
    # ``edition`` domain object was read above and stays usable here.
    await context.events.edition_lifecycle.publish(
        EditionLifecycleEvent(
            organization=org_slug,
            project=project_slug,
            action=LifecycleAction.delete,
            edition_kind=MetricsEditionKind.from_api(edition.kind),
        )
    )
