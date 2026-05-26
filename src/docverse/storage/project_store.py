"""Database operations for the projects table."""

from __future__ import annotations

from typing import Any

import structlog
from safir.database import (
    CountedPaginatedList,
    CountedPaginatedQueryRunner,
    PaginationCursor,
)
from sqlalchemy import REAL, cast, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import expression, func

from docverse.client.models import ProjectCreate, ProjectUpdate
from docverse.dbschema.project import SqlProject
from docverse.domain.project import Project
from docverse.storage.pagination import ProjectSearchCursor

_TRGM_SIMILARITY_THRESHOLD = 0.1
"""Minimum trigram similarity score for fuzzy search results."""


class ProjectStore:
    """Direct database operations for projects."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(
        self,
        *,
        org_id: int,
        data: ProjectCreate,
        github_owner: str | None = None,
        github_repo: str | None = None,
    ) -> Project:
        """Insert a new project row.

        ``github_owner`` and ``github_repo`` are passed in by the caller
        (``ProjectService``) after resolving the ``github`` sub-object
        from the request payload. The validator guarantees a
        GitHub-bound project sends no ``source_url``, so the column is
        persisted NULL for those rows.
        """
        lifecycle_rules = None
        if data.lifecycle_rules is not None:
            lifecycle_rules = data.lifecycle_rules.model_dump(mode="json")
        row = SqlProject(
            slug=data.slug,
            title=data.title,
            org_id=org_id,
            source_url=data.source_url,
            github_owner=github_owner,
            github_repo=github_repo,
            lifecycle_rules=lifecycle_rules,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return Project.model_validate(row)

    async def get_by_id(self, project_id: int) -> Project | None:
        """Fetch a project by internal ID."""
        result = await self._session.execute(
            select(SqlProject).where(
                SqlProject.id == project_id,
                SqlProject.date_deleted.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return Project.model_validate(row)

    async def get_by_slug(self, *, org_id: int, slug: str) -> Project | None:
        """Fetch a project by org_id and slug."""
        result = await self._session.execute(
            select(SqlProject).where(
                SqlProject.org_id == org_id,
                SqlProject.slug == slug,
                SqlProject.date_deleted.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return Project.model_validate(row)

    async def list_by_ids(self, project_ids: list[int]) -> list[Project]:
        """Return non-deleted projects with ids in ``project_ids``.

        Used by callers that already have a small set of project ids
        (e.g. a paginated keeper-sync state-row window) and want to
        resolve them to projects in a single round-trip instead of N
        per-id ``get_by_id`` calls. Passing an empty list returns
        ``[]`` without hitting the database.
        """
        if not project_ids:
            return []
        result = await self._session.execute(
            select(SqlProject).where(
                SqlProject.id.in_(project_ids),
                SqlProject.date_deleted.is_(None),
            )
        )
        return [Project.model_validate(row) for row in result.scalars().all()]

    async def list_org_ids_with_lifecycle_rules(self) -> set[int]:
        """Return every ``org_id`` that owns a project with lifecycle rules.

        The ``lifecycle_eval_dispatcher`` pre-flight uses the union of
        this set with orgs whose own ``lifecycle_rules`` column is
        non-null to decide which orgs are in-scope for the tick.
        Soft-deleted projects are excluded so an org whose only
        rule-bearing project has been deleted is correctly classified
        as "no rules anywhere" — otherwise the dispatcher would burn a
        queue slot on a per-org pass that the evaluator would short-
        circuit anyway.
        """
        stmt = select(SqlProject.org_id).where(
            SqlProject.lifecycle_rules.is_not(None),
            SqlProject.date_deleted.is_(None),
        )
        result = await self._session.execute(stmt)
        return set(result.scalars().all())

    async def list_org_ids_with_github_bound_projects(self) -> set[int]:
        """Return every ``org_id`` that owns a GitHub-bound project.

        The ``git_ref_audit_discovery`` pre-flight uses this set
        directly: an org is in-scope iff it owns at least one
        non-deleted project whose ``github_owner`` and ``github_repo``
        are both populated. Orgs whose every project points at a
        non-GitHub ``source_url`` get no audit job at all — the audit
        has nothing to do for them and a queued no-op would only
        muddy operator queue-inspection.
        """
        stmt = select(SqlProject.org_id).where(
            SqlProject.github_owner.is_not(None),
            SqlProject.github_repo.is_not(None),
            SqlProject.date_deleted.is_(None),
        )
        result = await self._session.execute(stmt)
        return set(result.scalars().all())

    async def list_github_bound_by_org(self, org_id: int) -> list[Project]:
        """List every non-deleted GitHub-bound project for an organization.

        Used by the ``git_ref_audit`` per-org worker to find every
        project whose ``(github_owner, github_repo)`` is populated, so
        the per-project audit pass can resolve the binding and fetch
        the live ref set. Non-GitHub projects (``source_url`` pointing
        at GitLab / Codeberg / on-prem, or no source URL at all) are
        excluded — the ``ref_deleted`` rule does not apply to them
        and including them would generate spurious skip-logged lines
        in the per-org pass. Ordered by slug ascending for stable
        iteration and operator-readable logs.
        """
        result = await self._session.execute(
            select(SqlProject)
            .where(
                SqlProject.org_id == org_id,
                SqlProject.github_owner.is_not(None),
                SqlProject.github_repo.is_not(None),
                SqlProject.date_deleted.is_(None),
            )
            .order_by(SqlProject.slug.asc())
        )
        return [Project.model_validate(row) for row in result.scalars().all()]

    async def list_all_by_org(self, org_id: int) -> list[Project]:
        """List every non-deleted project for an organization.

        Used by bulk operations (e.g. org-wide dashboard rebuild) where
        every project is processed in a single request and pagination
        would only complicate the caller. Ordered by slug ascending for
        stable iteration.
        """
        result = await self._session.execute(
            select(SqlProject)
            .where(
                SqlProject.org_id == org_id,
                SqlProject.date_deleted.is_(None),
            )
            .order_by(SqlProject.slug.asc())
        )
        return [Project.model_validate(row) for row in result.scalars().all()]

    async def list_by_org(
        self,
        org_id: int,
        *,
        cursor_type: type[PaginationCursor[Project]],
        cursor: PaginationCursor[Project] | None = None,
        limit: int,
    ) -> CountedPaginatedList[Project, PaginationCursor[Project]]:
        """List non-deleted projects for an organization with pagination."""
        stmt = select(SqlProject).where(
            SqlProject.org_id == org_id,
            SqlProject.date_deleted.is_(None),
        )
        runner = CountedPaginatedQueryRunner(
            entry_type=Project, cursor_type=cursor_type
        )
        return await runner.query_object(
            self._session, stmt, cursor=cursor, limit=limit
        )

    async def search_by_org(
        self,
        org_id: int,
        *,
        query: str,
        limit: int,
        cursor: ProjectSearchCursor | None = None,
    ) -> CountedPaginatedList[Project, PaginationCursor[Project]]:
        """Search non-deleted projects by trigram similarity on slug/title."""
        relevance = func.greatest(
            func.similarity(SqlProject.slug, query),
            func.similarity(SqlProject.title, query),
        ).label("relevance")

        base_filter = expression.and_(
            SqlProject.org_id == org_id,
            SqlProject.date_deleted.is_(None),
            relevance > _TRGM_SIMILARITY_THRESHOLD,
        )

        # Count total matches (no cursor so count is stable across pages)
        count_stmt = (
            select(func.count()).select_from(SqlProject).where(base_filter)
        )
        count_result = await self._session.execute(count_stmt)
        total = count_result.scalar_one()

        # Build fetch query with compound keyset cursor
        fetch_stmt = select(SqlProject, relevance).where(base_filter)

        if cursor is None:
            fetch_stmt = fetch_stmt.order_by(
                relevance.desc(), SqlProject.id.desc()
            )
        elif not cursor.previous:
            # Forward pagination: rows after the cursor.
            # Cast cursor.score to REAL (float4) to match the precision of
            # PostgreSQL's similarity() return type and avoid float8 vs float4
            # comparison mismatches.
            score = cast(cursor.score, REAL)
            fetch_stmt = fetch_stmt.where(
                expression.or_(
                    relevance < score,
                    expression.and_(
                        relevance == score,
                        SqlProject.id < cursor.id,
                    ),
                )
            ).order_by(relevance.desc(), SqlProject.id.desc())
        else:
            # Backward pagination: rows before the cursor (reversed order)
            score = cast(cursor.score, REAL)
            fetch_stmt = fetch_stmt.where(
                expression.or_(
                    relevance > score,
                    expression.and_(
                        relevance == score,
                        SqlProject.id > cursor.id,
                    ),
                )
            ).order_by(relevance.asc(), SqlProject.id.asc())

        fetch_stmt = fetch_stmt.limit(limit + 1)
        result = await self._session.execute(fetch_stmt)
        rows = result.all()

        has_more = len(rows) > limit
        rows = rows[:limit]

        if cursor is not None and cursor.previous:
            rows = list(reversed(rows))

        entries = [Project.model_validate(row.SqlProject) for row in rows]

        # Build next/prev cursors
        next_cursor: ProjectSearchCursor | None = None
        prev_cursor: ProjectSearchCursor | None = None

        if cursor is None or not cursor.previous:
            # Forward traversal
            if has_more and entries:
                last = rows[-1]
                next_cursor = ProjectSearchCursor(
                    score=float(last.relevance),
                    id=last.SqlProject.id,
                    previous=False,
                )
            if cursor is not None and entries:
                first = rows[0]
                prev_cursor = ProjectSearchCursor(
                    score=float(first.relevance),
                    id=first.SqlProject.id,
                    previous=True,
                )
        else:
            # Backward traversal
            if has_more and entries:
                first = rows[0]
                prev_cursor = ProjectSearchCursor(
                    score=float(first.relevance),
                    id=first.SqlProject.id,
                    previous=True,
                )
            if cursor is not None and entries:
                last = rows[-1]
                next_cursor = ProjectSearchCursor(
                    score=float(last.relevance),
                    id=last.SqlProject.id,
                    previous=False,
                )

        return CountedPaginatedList[Project, PaginationCursor[Project]](
            entries=entries,
            count=total,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

    async def update(
        self,
        *,
        org_id: int,
        slug: str,
        data: ProjectUpdate,
        extra_updates: dict[str, Any] | None = None,
    ) -> Project | None:
        """Update a project by org_id and slug.

        ``extra_updates`` carries server-derived column updates (e.g.
        the resolved ``github_owner``/``github_repo`` pair plus the
        ``github_*_id`` clears that accompany a binding change) that
        the service computed from the ``github`` sub-object on the
        request body. The ``github`` field is removed from the model
        dump because it has no direct column mapping.
        """
        result = await self._session.execute(
            select(SqlProject).where(
                SqlProject.org_id == org_id,
                SqlProject.slug == slug,
                SqlProject.date_deleted.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        updates = data.model_dump(mode="json", exclude_unset=True)
        updates.pop("github", None)
        if extra_updates:
            updates.update(extra_updates)
        for key, value in updates.items():
            setattr(row, key, value)
        await self._session.flush()
        await self._session.refresh(row)
        return Project.model_validate(row)

    async def list_by_github_repo(
        self,
        *,
        repo_id: int | None,
        owner: str,
        repo: str,
    ) -> list[Project]:
        """Find non-deleted projects matching a GitHub repo.

        Used by :class:`docverse.services.ref_deleted_processor
        .RefDeletedWebhookProcessor` (and any future webhook routing
        keyed on a repository, not a binding) to walk every project
        that backs the delivered ``(owner, repo, repository.id)``.
        Mirrors the dashboard binding store's
        ``list_by_repo_id_and_ref`` + ``list_unsynced_by_repo_ref``
        pair: the numeric id path is the rename-robust primary, and
        the name path covers pre-resolve projects whose
        ``github_repo_id`` column is still NULL.

        Two queries unioned and deduplicated by project id:
        - When ``repo_id`` is supplied (the webhook payload's
          ``repository.id`` was populated), select projects with that
          ``github_repo_id``. Rename-robust: a transferred or renamed
          repo keeps its numeric id, so the row still matches even
          when display names diverge.
        - Always also select projects whose
          ``(lower(github_owner), lower(github_repo))`` matches **and**
          whose ``github_repo_id`` is NULL. Restricting the name path
          to NULL-id rows defends against a different repo coming to
          live at an old display name: any already-resolved project
          would have a stable id that the id path covers, while a
          coincidentally-same-named row sitting at a different numeric
          id stays out.

        Multiple projects may match (one repo shared across slugs);
        callers iterate the returned list.
        """
        seen: set[int] = set()
        results: list[Project] = []
        if repo_id is not None:
            by_id = await self._session.execute(
                select(SqlProject).where(
                    SqlProject.github_repo_id == repo_id,
                    SqlProject.date_deleted.is_(None),
                )
            )
            for row in by_id.scalars().all():
                if row.id in seen:
                    continue
                seen.add(row.id)
                results.append(Project.model_validate(row))
        by_name = await self._session.execute(
            select(SqlProject).where(
                func.lower(SqlProject.github_owner) == owner.lower(),
                func.lower(SqlProject.github_repo) == repo.lower(),
                SqlProject.github_repo_id.is_(None),
                SqlProject.date_deleted.is_(None),
            )
        )
        for row in by_name.scalars().all():
            if row.id in seen:
                continue
            seen.add(row.id)
            results.append(Project.model_validate(row))
        return results

    async def rename_repo_by_repo_id(
        self,
        *,
        github_repo_id: int,
        new_repo: str,
    ) -> list[int]:
        """Rewrite ``github_repo`` on projects backing a renamed repo.

        Used by :class:`docverse.services.dashboard_templates
        .RenameEventProcessor` to mirror the dashboard binding's
        ``rename_repo_by_repo_id`` for projects on the same repo. Only
        the structured ``github_repo`` column flips; the
        operator-visible source URL is derived from the binding
        (:attr:`docverse.domain.project.Project.effective_source_url`),
        so it follows automatically without a stored value to rewrite.

        ``date_updated`` is explicitly preserved (pinned to its current
        value so the column's ``onupdate=func.now()`` does not fire): a
        GitHub-side rename is sync-bookkeeping, not an operator-visible
        source-coordinate edit, which arrives through PUT/PATCH. This
        mirrors the discipline of the dashboard binding store's
        ``rename_repo_by_repo_id`` and of ``apply_installation_scope``.

        Returns the list of updated project ids.
        """
        stmt = (
            update(SqlProject)
            .where(
                SqlProject.github_repo_id == github_repo_id,
                SqlProject.date_deleted.is_(None),
            )
            .values(
                github_repo=new_repo,
                date_updated=SqlProject.date_updated,
            )
            .returning(SqlProject.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def transfer_repo_by_repo_id(
        self,
        *,
        github_repo_id: int,
        new_owner: str,
        new_owner_id: int,
        new_repo: str,
    ) -> list[int]:
        """Flip owner + owner_id + repo on projects backing a transfer.

        Mirrors :meth:`docverse.storage.dashboard_templates.github
        .DashboardGitHubTemplateBindingStore.transfer_repo_by_repo_id`:
        a ``repository.transferred`` event keeps ``repository.id``
        stable but moves the repo to a new owner namespace, so the
        binding has to switch its owner-side identity to keep matching
        push events from the new namespace. Only the structured columns
        flip; the operator-visible source URL is derived from the
        binding, so it follows automatically.

        ``date_updated`` is explicitly preserved (pinned to its current
        value so the column's ``onupdate=func.now()`` does not fire): a
        GitHub-side transfer is sync-bookkeeping, not an operator-
        visible source-coordinate edit, which arrives through PUT/PATCH.
        This mirrors the discipline of the dashboard binding store's
        ``transfer_repo_by_repo_id`` and of ``apply_installation_scope``.

        Returns the list of updated project ids.
        """
        stmt = (
            update(SqlProject)
            .where(
                SqlProject.github_repo_id == github_repo_id,
                SqlProject.date_deleted.is_(None),
            )
            .values(
                github_owner=new_owner,
                github_owner_id=new_owner_id,
                github_repo=new_repo,
                date_updated=SqlProject.date_updated,
            )
            .returning(SqlProject.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def apply_installation_scope(
        self,
        *,
        installation_id: int,
        owner: str,
        owner_id: int,
        repo: str,
        repo_id: int,
    ) -> list[int]:
        """Backfill the three github_*_id columns from a webhook payload.

        Used by :class:`docverse.services.dashboard_templates
        .InstallationEventProcessor` to capture
        ``(github_installation_id, github_owner_id, github_repo_id)``
        whenever GitHub announces that ``owner/repo`` is now in scope
        of an installation. The match is case-insensitive on
        ``(github_owner, github_repo)`` so a project registered as
        ``Acme/Docs`` still matches a payload that delivers
        ``acme/docs``.

        ``date_updated`` is explicitly preserved: this write is
        sync-bookkeeping, not an operator-visible source-coordinate
        edit, and bumping ``date_updated`` here would mislead any
        consumer that reads it as ``last operator change``. Mirrors
        the same discipline the dashboard binding store's
        ``rename_*`` / ``mark_unreachable_by_installation_id``
        methods already apply.

        Returns the list of project ids that were updated, so the
        caller can log a count (``projects_updated=N``) without a
        separate round-trip.
        """
        stmt = (
            update(SqlProject)
            .where(
                func.lower(SqlProject.github_owner) == owner.lower(),
                func.lower(SqlProject.github_repo) == repo.lower(),
                SqlProject.date_deleted.is_(None),
            )
            .values(
                github_installation_id=installation_id,
                github_owner_id=owner_id,
                github_repo_id=repo_id,
                date_updated=SqlProject.date_updated,
            )
            .returning(SqlProject.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return [row[0] for row in result.all()]

    async def update_github_metadata(  # noqa: PLR0913
        self,
        *,
        project_id: int,
        expected_owner: str,
        expected_repo: str,
        installation_id: int,
        owner_id: int,
        repo_id: int,
    ) -> bool:
        """Persist the three opportunistic github_*_id columns.

        Used by :func:`docverse.worker.functions.project_github_resolve`
        after a successful GitHub round-trip. The
        ``expected_owner``/``expected_repo`` guard short-circuits when
        the binding has flipped between enqueue and the worker run
        (e.g. a PATCH rewrote ``github`` to a different repo, which
        cleared the three id columns and re-enqueued resolution). In
        that case the stale numeric ids would clobber the new
        binding's columns — better to lose this update than to write
        ids that disagree with ``github_owner`` / ``github_repo``.

        ``date_updated`` is explicitly preserved: capturing the three
        opportunistic ``github_*_id`` columns is sync-bookkeeping, not
        an operator-visible source-coordinate edit, so bumping
        ``date_updated`` here would mislead any consumer that reads it
        as ``last operator change``. Mirrors ``apply_installation_scope``
        and the dashboard binding store.

        Returns ``True`` when the row was updated, ``False`` when no
        row matched (project deleted, or binding changed).
        """
        stmt = (
            update(SqlProject)
            .where(
                SqlProject.id == project_id,
                SqlProject.github_owner == expected_owner,
                SqlProject.github_repo == expected_repo,
                SqlProject.date_deleted.is_(None),
            )
            .values(
                github_installation_id=installation_id,
                github_owner_id=owner_id,
                github_repo_id=repo_id,
                date_updated=SqlProject.date_updated,
            )
            .returning(SqlProject.id)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.first() is not None

    async def soft_delete(self, *, org_id: int, slug: str) -> bool:
        """Soft-delete a project by setting date_deleted.

        Returns
        -------
        bool
            True if the project was soft-deleted, False if not found.
        """
        result = await self._session.execute(
            select(SqlProject).where(
                SqlProject.org_id == org_id,
                SqlProject.slug == slug,
                SqlProject.date_deleted.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return False
        row.date_deleted = func.now()
        await self._session.flush()
        return True
