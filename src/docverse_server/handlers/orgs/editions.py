"""Edition endpoints within an organization's project."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from docverse.models import (
    Edition,
    EditionCreate,
    EditionKind,
    EditionRollback,
    EditionUpdate,
)
from docverse_server.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
)
from docverse_server.dependencies.context import (
    RequestContext,
    context_dependency,
)
from docverse_server.domain.published_url import (
    edition_published_url,
    project_published_url,
)
from docverse_server.exceptions import NotFoundError, PermissionDeniedError
from docverse_server.handlers.params import (
    EditionSlugParam,
    OrgSlugParam,
    ProjectSlugParam,
)
from docverse_server.metrics import (
    EditionLifecycleEvent,
    LifecycleAction,
    MetricsEditionKind,
)
from docverse_server.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_slug,
)
from docverse_server.storage.keeper_sync import TombstoneReason
from docverse_server.storage.pagination import (
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
    *,
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
    *,
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
    response_model = edition_from_domain(
        edition,
        context.request,
        org_slug,
        project_slug,
        published_url=edition_published_url(project_url, edition),
    )
    context.response.headers["Location"] = response_model.self_url
    return response_model


@router.get(
    "/orgs/{org}/projects/{project}/editions/{edition}",
    response_model=Edition,
    summary="Get an edition",
    name="get_edition",
)
async def get_edition(
    *,
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
    *,
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
    include_deleted: Annotated[
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
    *,
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
    data: EditionRollback,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> Edition:
    """Point an edition back at a build already in its history.

    Naming the build the edition **already serves** is answered with
    ``200`` and the unchanged edition, not a ``409``: the request's
    postcondition already holds, and an operator retrying after a
    dropped connection — or two operators reacting to the same
    incident — should not have to tell a conflict from a success.
    Whether it already serves that build is decided under the
    edition's row lock, so a rollback racing another operator's
    repoint answers on what that repoint actually left behind: an
    edition that has moved on is rolled back for real, rather than
    handed a ``200`` naming a build it no longer serves. The response
    is the edition as it stands, so its ``publish_status`` reports the
    real state of the publish rather than a ``pending`` nothing will
    clear.

    Such a request is otherwise inert, and inert all the way out: it
    records no history entry, enqueues no ``publish_edition`` job,
    publishes no ``edition_lifecycle`` event, enqueues no
    ``dashboard_build``, and leaves the project's ``date_updated``
    where it was, so a consumer polling ``GET /orgs/{org}/projects``
    with ``updated_since`` or an ``ETag`` is not told to refetch a
    project whose content did not move. The last two matter because
    this endpoint invites the retry: the metric would report a rollback
    with no history row behind it, and the dashboard enqueue dedupes
    only against a job still queued or in flight, so a client retrying
    a dropped connection would otherwise buy a full dashboard render
    and object-store upload per attempt.

    The exception is a publish that **failed**: re-requesting the build
    being served is the only way to retry it, so that request does
    record a history entry, return the edition to ``pending``, enqueue
    the job, and announce itself as the change it is — still without
    moving the project's clock, since the build being served is the
    same one.

    A build that is not in this edition's history is still a ``404``,
    checked first: an emergency ``build`` override can leave an edition
    serving a build that rollback was never offered.
    """
    async with context.session.begin():
        service = context.factory.create_edition_service()
        written = await service.rollback(
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slug=edition_slug,
            build_public_id=data.build,
        )
        await context.session.commit()
    # A no-op rollback deferred nothing, so this dispatch is itself a
    # no-op; it stays unconditional because the dispatcher is what
    # hands over whatever a *real* rollback queued.
    await context.factory.queue_dispatcher.dispatch()
    if written.changed:
        # Publish after the commit (best-effort; raise_on_error=False).
        await context.events.edition_lifecycle.publish(
            EditionLifecycleEvent(
                organization=org_slug,
                project=project_slug,
                action=LifecycleAction.rollback,
                edition_kind=MetricsEditionKind.from_api(written.edition.kind),
            )
        )
        await try_enqueue_dashboard_build_by_slug(
            factory=context.factory,
            session=context.session,
            logger=context.logger,
            org_slug=org_slug,
            project_slug=project_slug,
        )
    project_url = project_published_url(written.organization, written.project)
    return edition_from_domain(
        written.edition,
        context.request,
        org_slug,
        project_slug,
        published_url=edition_published_url(project_url, written.edition),
    )


@router.patch(
    "/orgs/{org}/projects/{project}/editions/{edition}",
    response_model=Edition,
    summary="Update an edition",
    name="patch_edition",
)
async def patch_edition(
    *,
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    edition_slug: EditionSlugParam,
    data: EditionUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> Edition:
    """Update an edition's metadata, or override the build it serves.

    A ``build`` in the payload is an emergency override: it points the
    edition at that build even if the build is not in the edition's
    history and even if it is older than the one being served.

    Naming the build the edition **already serves** is answered with
    ``200`` and the unchanged edition, on the same reasoning as
    ``POST .../rollback``, and decided the same way — under the
    edition's row lock, so an override racing another operator's
    repoint repoints for real rather than reporting a build the
    edition no longer serves. Such a request is otherwise inert, and
    inert all the way out: no history entry, no ``publish_edition``
    job, no ``edition_lifecycle`` event, no ``dashboard_build``, and
    the project's ``date_updated`` stays where it was, so a consumer
    polling ``GET /orgs/{org}/projects`` with ``updated_since`` or an
    ``ETag`` is not told to refetch a project whose content did not
    move. The exception, again as for rollback, is a publish that
    **failed**: re-requesting the served build is the only way to retry
    it, so it records a history entry, returns the edition to
    ``pending``, enqueues the job, and announces itself as the change
    it is — without moving the project's clock.

    Only a payload with nothing in it but that already-current
    ``build`` is inert, though. A metadata field alongside it is
    applied, and its write is a real update: it sets the fields it
    names whatever their values, so the edition's own ``date_updated``
    moves and the event and dashboard rebuild are owed.
    """
    if edition_slug.lower() == "__main" and data.kind is not None:
        msg = "Cannot change the kind of the default '__main' edition"
        raise PermissionDeniedError(msg)
    async with context.session.begin():
        service = context.factory.create_edition_service()
        written = await service.update(
            org_slug=org_slug,
            project_slug=project_slug,
            slug=edition_slug,
            data=data,
        )
        await context.session.commit()
    # An inert PATCH deferred nothing, so this dispatch is itself a
    # no-op; it stays unconditional because the dispatcher is what
    # hands over whatever a *real* override queued.
    await context.factory.queue_dispatcher.dispatch()
    if written.changed:
        # Publish after the commit (best-effort; raise_on_error=False).
        await context.events.edition_lifecycle.publish(
            EditionLifecycleEvent(
                organization=org_slug,
                project=project_slug,
                action=LifecycleAction.update,
                edition_kind=MetricsEditionKind.from_api(written.edition.kind),
            )
        )
        await try_enqueue_dashboard_build_by_slug(
            factory=context.factory,
            session=context.session,
            logger=context.logger,
            org_slug=org_slug,
            project_slug=project_slug,
        )
    project_url = project_published_url(written.organization, written.project)
    return edition_from_domain(
        written.edition,
        context.request,
        org_slug,
        project_slug,
        published_url=edition_published_url(project_url, written.edition),
    )


@router.delete(
    "/orgs/{org}/projects/{project}/editions/{edition}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an edition",
    name="delete_edition",
)
async def delete_edition(
    *,
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
