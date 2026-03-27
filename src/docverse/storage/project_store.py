"""Database operations for the projects table."""

from __future__ import annotations

import structlog
from safir.database import (
    CountedPaginatedList,
    CountedPaginatedQueryRunner,
    PaginationCursor,
)
from sqlalchemy import REAL, cast, select
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session
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
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(self, *, org_id: int, data: ProjectCreate) -> Project:
        """Insert a new project row."""
        row = SqlProject(
            slug=data.slug,
            title=data.title,
            org_id=org_id,
            doc_repo=data.doc_repo,
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
        self, *, org_id: int, slug: str, data: ProjectUpdate
    ) -> Project | None:
        """Update a project by org_id and slug."""
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
        updates = data.model_dump(exclude_unset=True)
        for key, value in updates.items():
            setattr(row, key, value)
        await self._session.flush()
        await self._session.refresh(row)
        return Project.model_validate(row)

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
