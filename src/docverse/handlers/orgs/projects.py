"""Project endpoints within an organization."""

from __future__ import annotations

from typing import Annotated

from docverse.client.models import ProjectCreate, ProjectUpdate
from fastapi import APIRouter, Depends, Query, status

from docverse.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
)
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.handlers.params import OrgSlugParam, ProjectSlugParam
from docverse.metrics import LifecycleAction, ProjectLifecycleEvent
from docverse.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_slug,
)
from docverse.services.project_github_resolve_enqueue import (
    try_enqueue_project_github_resolve_by_id,
)
from docverse.storage.pagination import (
    DEFAULT_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
    PROJECT_CURSOR_TYPES,
    ProjectSearchCursor,
    ProjectSortOrder,
)

from .models import Project

router = APIRouter()


@router.get(
    "/orgs/{org}/projects",
    response_model=list[Project],
    summary="List projects in an organization",
    name="get_projects",
)
async def get_projects(
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
) -> list[Project]:
    async with context.session.begin():
        service = context.factory.create_project_service()
        if q is not None:
            search_cursor = (
                ProjectSearchCursor.from_str(cursor)
                if cursor is not None
                else None
            )
            org, result = await service.list_by_org(
                org_slug, query=q, limit=limit, cursor=search_cursor
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
    return Project.from_domain(
        project,
        context.request,
        org,
        default_edition=default_edition,
        app_url=context.factory.github_app_html_url,
    )


@router.get(
    "/orgs/{org}/projects/{project}",
    response_model=Project,
    summary="Get a project",
    name="get_project",
)
async def get_project(
    org_slug: OrgSlugParam,
    project_slug: ProjectSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
) -> Project:
    async with context.session.begin():
        service = context.factory.create_project_service()
        org, project = await service.get_by_slug(
            org_slug=org_slug, slug=project_slug
        )
        default_edition = await service.get_default_edition(project.id)
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
