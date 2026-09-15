"""Project endpoints within an organization."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Query, Response, status
from pydantic import AwareDatetime

from docverse.models import ProjectCreate, ProjectUpdate
from docverse_server.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
)
from docverse_server.dependencies.context import (
    RequestContext,
    context_dependency,
)
from docverse_server.domain.conditional_get import (
    datetime_to_microseconds,
    make_weak_etag,
)
from docverse_server.domain.project import ProjectListingWatermark
from docverse_server.handlers.conditional import evaluate_conditional_get
from docverse_server.handlers.params import OrgSlugParam, ProjectSlugParam
from docverse_server.metrics import (
    ConditionalGetEndpoint,
    LifecycleAction,
    ProjectLifecycleEvent,
)
from docverse_server.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_slug,
)
from docverse_server.services.project_github_resolve_enqueue import (
    try_enqueue_project_github_resolve_by_id,
)
from docverse_server.storage.pagination import (
    DEFAULT_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
    PROJECT_CURSOR_TYPES,
    ProjectSearchCursor,
    ProjectSortOrder,
)

from .models import Project

router = APIRouter()


def _listing_etag(
    context: RequestContext,
    *,
    org_public_id: int,
    watermark: ProjectListingWatermark,
) -> str:
    """Build the entity-tag for one page of the project listing.

    The material is the endpoint's identity, the org's public id, the
    whole listing watermark, and the request's query string
    canonicalized by sorting — which is what keeps every page and every
    filter combination on its own tag without this function having to
    know which parameters the listing supports. A parameter added later
    is covered the day it is added.

    All three parts of the watermark go in, not just its newest clock.
    A ``date_updated`` is PostgreSQL's transaction *start* time and
    commit order is not start order, so a slow writer can land a clock
    below the maximum a poller already holds; the row count and the sum
    of every row's clock move when that happens and the maximum does
    not. ``Last-Modified`` can still only carry the maximum — it has to
    name an instant — but the tag is opaque, so it says more.
    """
    return make_weak_etag(
        (
            ConditionalGetEndpoint.projects_list.value,
            org_public_id,
            datetime_to_microseconds(watermark.date_updated),
            watermark.project_count,
            watermark.clock_sum,
            urlencode(sorted(context.request.query_params.multi_items())),
        )
    )


@router.get(
    "/orgs/{org}/projects",
    response_model=list[Project],
    summary="List projects in an organization",
    name="get_projects",
    responses={
        status.HTTP_304_NOT_MODIFIED: {
            "description": (
                "The caller's ``If-None-Match`` or ``If-Modified-Since``"
                " already matched this page, so no body is sent. The"
                " ``ETag`` and ``Last-Modified`` validators are repeated"
                " so a poller can carry them into its next request."
            )
        }
    },
)
async def get_projects(
    *,
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
    order: Annotated[
        ProjectSortOrder,
        Query(description="Sort order for results."),
    ] = ProjectSortOrder.slug,
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
    q: Annotated[
        str | None,
        Query(
            min_length=1,
            max_length=256,
            description=(
                "Fuzzy search query matched against project slugs and"
                " titles. Results are ordered by relevance and support"
                " cursor pagination via the ``Link`` header."
            ),
        ),
    ] = None,
    updated_since: Annotated[
        AwareDatetime | None,
        Query(
            description=(
                "Only return projects whose ``date_updated`` is at or"
                " after this instant, so a poller can ask for just what"
                " changed since its last pass. The bound is inclusive"
                " and must carry a timezone offset; a naive timestamp"
                " is rejected. A project's clock advances on a metadata"
                " edit, a GitHub-binding resolve, a deletion, and a"
                " repoint of its default edition."
            ),
        ),
    ] = None,
    include_deleted: Annotated[
        bool,
        Query(
            description=(
                "Also return soft-deleted projects, each with its"
                " ``date_deleted`` set, and count them in"
                " ``X-Total-Count``. Defaults to false. Applies to the"
                " ``q`` search path as well as the ordered listing, so"
                " a consumer mirroring the listing sees a deletion as a"
                " row it can act on rather than as a project that"
                " silently stopped appearing."
            ),
        ),
    ] = False,
) -> list[Project] | Response:
    async with context.session.begin():
        service = context.factory.create_project_service()
        # Evaluate the caller's preconditions before the listing query:
        # a poller that is up to date should pay for the watermark
        # aggregate and nothing else.
        watermark = await service.get_org_watermark(user.org)
        not_modified = await evaluate_conditional_get(
            context,
            endpoint=ConditionalGetEndpoint.projects_list,
            organization=org_slug,
            etag=_listing_etag(
                context,
                org_public_id=user.org.public_id,
                watermark=watermark,
            ),
            last_modified=watermark.date_updated,
            now=datetime.now(tz=UTC),
        )
        if not_modified is not None:
            return not_modified
        if q is not None:
            search_cursor = (
                ProjectSearchCursor.from_str(cursor)
                if cursor is not None
                else None
            )
            org, result = await service.list_by_org(
                org_slug,
                query=q,
                limit=limit,
                cursor=search_cursor,
                updated_since=updated_since,
                include_deleted=include_deleted,
            )
        else:
            cursor_type = PROJECT_CURSOR_TYPES[order]
            parsed_cursor = (
                cursor_type.from_str(cursor) if cursor is not None else None
            )
            org, result = await service.list_by_org(
                org_slug,
                cursor_type=cursor_type,
                cursor=parsed_cursor,
                limit=limit,
                updated_since=updated_since,
                include_deleted=include_deleted,
            )
    link = result.link_header(context.request.url)
    if link:
        context.response.headers["Link"] = link
    context.response.headers["X-Total-Count"] = str(result.count)
    return [
        Project.from_domain(
            p,
            context.request,
            org,
            app_url=context.factory.github_app_html_url,
        )
        for p in result.entries
    ]


@router.post(
    "/orgs/{org}/projects",
    response_model=Project,
    status_code=status.HTTP_201_CREATED,
    summary="Create a project",
    name="post_project",
)
async def post_project(
    *,
    org_slug: OrgSlugParam,
    data: ProjectCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> Project:
    async with context.session.begin():
        service = context.factory.create_project_service()
        org, project, default_edition = await service.create(
            org_slug=org_slug, data=data
        )
        await context.session.commit()
    # Emit after the commit so the event reflects durably persisted state.
    # Production runs raise_on_error=False, so a metrics-backend outage
    # cannot fail this request (no defensive try/except).
    await context.events.project_lifecycle.publish(
        ProjectLifecycleEvent(
            organization=org_slug,
            project=project.slug,
            action=LifecycleAction.create,
        )
    )
    await try_enqueue_project_github_resolve_by_id(
        factory=context.factory,
        session=context.session,
        logger=context.logger,
        project_id=project.id,
    )
    response_model = Project.from_domain(
        project,
        context.request,
        org,
        default_edition=default_edition,
        app_url=context.factory.github_app_html_url,
    )
    context.response.headers["Location"] = response_model.self_url
    return response_model


def _project_etag(
    *, project_public_id: int, watermark: datetime, include_deleted: bool
) -> str:
    """Build the entity-tag for one project's representation.

    Unlike the listing, this endpoint names its material explicitly
    rather than hashing the request's whole query string:
    ``include_deleted`` is the only parameter it takes, and hashing the
    *parsed* boolean means a caller that spells it ``false`` and one
    that omits it entirely — the same representation — share a tag
    instead of churning one.

    The flag is part of the material because it is part of the
    request's identity here: with it a soft-deleted project is a
    representation, without it the same URL is a 404.
    """
    return make_weak_etag(
        (
            ConditionalGetEndpoint.project.value,
            project_public_id,
            datetime_to_microseconds(watermark),
            include_deleted,
        )
    )


@router.get(
    "/orgs/{org}/projects/{project}",
    response_model=Project,
    summary="Get a project",
    name="get_project",
    responses={
        status.HTTP_304_NOT_MODIFIED: {
            "description": (
                "The caller's ``If-None-Match`` or ``If-Modified-Since``"
                " already matched this project, so no body is sent. The"
                " ``ETag`` and ``Last-Modified`` validators are repeated"
                " so a poller can carry them into its next request."
            )
        }
    },
)
async def get_project(
    *,
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
    include_deleted: Annotated[
        bool,
        Query(
            description=(
                "Return the project even if it has been soft-deleted,"
                " with its ``date_deleted`` set. Defaults to false, in"
                " which case a deleted project is a 404. Slugs are never"
                " reused after a delete, so this can only ever resolve"
                " the project that already owned the slug. Write"
                " endpoints ignore this and still 404 on a deleted"
                " project."
            ),
        ),
    ] = False,
) -> Project | Response:
    async with context.session.begin():
        service = context.factory.create_project_service()
        org, project = await service.get_by_slug(
            org_slug=org_slug,
            slug=project_slug,
            include_deleted=include_deleted,
        )
        default_edition = await service.get_default_edition(project.id)
        # Three rows feed this representation, so three clocks feed the
        # validator and the watermark is the latest of them. The project
        # row is the obvious one. The default edition is embedded whole,
        # so an edition retitled or repointed rewrites the body without
        # necessarily touching the project. And the org supplies
        # ``base_domain``, ``url_scheme``, and ``root_path_prefix``, the
        # three fields the embedded edition's ``published_url`` is built
        # from — all patchable through ``PATCH /orgs/{org}``, which
        # writes the org row and nothing else. Drop any one clock and a
        # poller is told 304 about a body that has already changed.
        watermark = max(project.date_updated, org.date_updated)
        if default_edition is not None:
            watermark = max(watermark, default_edition.date_updated)
        not_modified = await evaluate_conditional_get(
            context,
            endpoint=ConditionalGetEndpoint.project,
            organization=org_slug,
            project=project_slug,
            etag=_project_etag(
                project_public_id=project.public_id,
                watermark=watermark,
                include_deleted=include_deleted,
            ),
            last_modified=watermark,
            now=datetime.now(tz=UTC),
        )
        if not_modified is not None:
            return not_modified
    return Project.from_domain(
        project,
        context.request,
        org,
        default_edition=default_edition,
        app_url=context.factory.github_app_html_url,
    )


@router.patch(
    "/orgs/{org}/projects/{project}",
    response_model=Project,
    summary="Update a project",
    name="patch_project",
)
async def patch_project(
    *,
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    data: ProjectUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> Project:
    async with context.session.begin():
        service = context.factory.create_project_service()
        org, project = await service.update(
            org_slug=org_slug, slug=project_slug, data=data
        )
        default_edition = await service.get_default_edition(project.id)
        await context.session.commit()
    # Publish after the commit (best-effort; raise_on_error=False).
    await context.events.project_lifecycle.publish(
        ProjectLifecycleEvent(
            organization=org_slug,
            project=project.slug,
            action=LifecycleAction.update,
        )
    )
    await try_enqueue_dashboard_build_by_slug(
        factory=context.factory,
        session=context.session,
        logger=context.logger,
        org_slug=org_slug,
        project_slug=project_slug,
    )
    await try_enqueue_project_github_resolve_by_id(
        factory=context.factory,
        session=context.session,
        logger=context.logger,
        project_id=project.id,
    )
    return Project.from_domain(
        project,
        context.request,
        org,
        default_edition=default_edition,
        app_url=context.factory.github_app_html_url,
    )


@router.delete(
    "/orgs/{org}/projects/{project}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a project",
    name="delete_project",
)
async def delete_project(
    *,
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> None:
    async with context.session.begin():
        service = context.factory.create_project_service()
        org, edition_slugs = await service.soft_delete(
            org_slug=org_slug, slug=project_slug
        )
        await context.session.commit()
    # Remove each edition's CDN pointer after the soft-delete commit so
    # the public URLs stop resolving once the project row is gone.
    # ``unpublish`` is idempotent and a no-op for orgs without a
    # configured CDN, so it is called unconditionally per edition.
    # Wrapped in its own ``begin()`` block because the publishing
    # service reads the org row (and may read service config +
    # credentials) — without an explicit transaction SQLAlchemy
    # auto-begins an implicit one that we then never commit.
    #
    # Failure semantics: if ``unpublish`` raises on edition k of n, the
    # soft-delete is already committed and is not rolled back. The loop
    # aborts on that first failing edition, the client sees a 5xx, and
    # the project row stays soft-deleted. Unlike ``delete_edition``
    # (which leaves the project row live, so a later lifecycle pass can
    # revisit it), a soft-deleted project drops out of every recovery
    # path: both the webhook (``list_by_github_repo``) and the daily
    # audit (``list_github_bound_by_org``) filter ``date_deleted IS
    # NULL``, and a re-issued DELETE returns 404 without re-attempting
    # unpublish (see ``test_delete_project_idempotent_re_run``). So
    # editions k..n are orphaned with stale CDN pointers until a manual
    # sweep. A future improvement could make this loop best-effort
    # (continue past a failing edition) and/or have a cleanup sweep
    # revisit soft-deleted projects so the orphaned pointers are
    # reclaimed automatically.
    #
    # Dashboard: a deleted project has no project dashboard to
    # rebuild (``DashboardBuildEnqueuer.enqueue_for_project`` would
    # reject on the ``date_deleted`` filter anyway), and there is no
    # org-level listing/dashboard rebuild hook today — scoping this
    # slice to the CDN unpublish, matching PRD #346's webhook fast-
    # path's same trade-off.
    if edition_slugs:
        async with context.session.begin():
            publishing_service = (
                context.factory.create_edition_publishing_service()
            )
            for edition_slug in edition_slugs:
                await publishing_service.unpublish(
                    org_id=org.id,
                    project_slug=project_slug,
                    edition_slug=edition_slug,
                )
    # Delete is multi-transaction (soft-delete commit + per-edition CDN
    # unpublish); publish only after that final step succeeds, so the
    # event signals a fully-completed delete (best-effort,
    # raise_on_error=False).
    await context.events.project_lifecycle.publish(
        ProjectLifecycleEvent(
            organization=org_slug,
            project=project_slug,
            action=LifecycleAction.delete,
        )
    )
