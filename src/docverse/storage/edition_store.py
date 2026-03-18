"""Database operations for the editions table."""

from __future__ import annotations

from typing import Any

import structlog
from safir.database import (
    CountedPaginatedList,
    CountedPaginatedQueryRunner,
    PaginationCursor,
)
from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session
from sqlalchemy.sql import func

from docverse.client.models import (
    EditionCreate,
    EditionKind,
    EditionUpdate,
    TrackingMode,
)
from docverse.dbschema.build import SqlBuild
from docverse.dbschema.edition import SqlEdition
from docverse.domain.edition import Edition


class EditionStore:
    """Direct database operations for editions."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    def _base_query(self) -> Select[tuple[SqlEdition, int]]:
        """Build the base query with optional build public_id join."""
        return select(
            SqlEdition,
            SqlBuild.public_id.label("current_build_public_id"),
        ).outerjoin(SqlBuild, SqlEdition.current_build_id == SqlBuild.id)

    def _column_query(self) -> Select[tuple[Any, ...]]:
        """Build a column-based query for paginated results.

        Returns all Edition domain fields as flat columns, including
        the joined build public_id. Suitable for use with
        ``query_row`` so the flat row validates directly into Edition.
        """
        return select(
            SqlEdition.id,
            SqlEdition.slug,
            SqlEdition.title,
            SqlEdition.project_id,
            SqlEdition.kind,
            SqlEdition.tracking_mode,
            SqlEdition.tracking_params,
            SqlEdition.current_build_id,
            SqlBuild.public_id.label("current_build_public_id"),
            SqlEdition.lifecycle_exempt,
            SqlEdition.date_created,
            SqlEdition.date_updated,
            SqlEdition.date_deleted,
        ).outerjoin(SqlBuild, SqlEdition.current_build_id == SqlBuild.id)

    def _validate(
        self, row: SqlEdition, build_public_id: int | None
    ) -> Edition:
        """Validate a row into an Edition domain model."""
        edition = Edition.model_validate(row)
        edition.current_build_public_id = build_public_id
        return edition

    async def create(self, *, project_id: int, data: EditionCreate) -> Edition:
        """Insert a new edition row."""
        row = SqlEdition(
            slug=data.slug,
            title=data.title,
            project_id=project_id,
            kind=data.kind,
            tracking_mode=data.tracking_mode,
            tracking_params=data.tracking_params,
            lifecycle_exempt=data.lifecycle_exempt,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return self._validate(row, None)

    async def get_by_slug(
        self, *, project_id: int, slug: str
    ) -> Edition | None:
        """Fetch an edition by project_id and slug."""
        stmt = self._base_query().where(
            SqlEdition.project_id == project_id,
            SqlEdition.slug == slug,
            SqlEdition.date_deleted.is_(None),
        )
        result = await self._session.execute(stmt)
        row_tuple = result.one_or_none()
        if row_tuple is None:
            return None
        edition_row, build_public_id = row_tuple
        return self._validate(edition_row, build_public_id)

    async def list_by_project(
        self,
        project_id: int,
        *,
        cursor_type: type[PaginationCursor[Edition]],
        cursor: PaginationCursor[Edition] | None = None,
        limit: int,
        kind: EditionKind | None = None,
    ) -> CountedPaginatedList[Edition, PaginationCursor[Edition]]:
        """List non-deleted editions for a project with pagination."""
        stmt = self._column_query().where(
            SqlEdition.project_id == project_id,
            SqlEdition.date_deleted.is_(None),
        )
        if kind is not None:
            stmt = stmt.where(SqlEdition.kind == kind)
        runner = CountedPaginatedQueryRunner(
            entry_type=Edition, cursor_type=cursor_type
        )
        return await runner.query_row(
            self._session, stmt, cursor=cursor, limit=limit
        )

    async def update(
        self, *, project_id: int, slug: str, data: EditionUpdate
    ) -> Edition | None:
        """Update an edition by project_id and slug."""
        result = await self._session.execute(
            select(SqlEdition).where(
                SqlEdition.project_id == project_id,
                SqlEdition.slug == slug,
                SqlEdition.date_deleted.is_(None),
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
        # Re-query to get current_build_public_id via join
        return await self.get_by_slug(project_id=project_id, slug=slug)

    async def set_current_build(
        self, *, edition_id: int, build_id: int
    ) -> Edition:
        """Set the current build for an edition."""
        result = await self._session.execute(
            select(SqlEdition).where(SqlEdition.id == edition_id)
        )
        row = result.scalar_one()
        row.current_build_id = build_id
        await self._session.flush()
        await self._session.refresh(row)
        # Re-query to get current_build_public_id
        stmt = self._base_query().where(SqlEdition.id == edition_id)
        result2 = await self._session.execute(stmt)
        edition_row, build_public_id = result2.one()
        return self._validate(edition_row, build_public_id)

    async def soft_delete(self, *, project_id: int, slug: str) -> bool:
        """Soft-delete an edition by setting date_deleted.

        Returns
        -------
        bool
            True if the edition was soft-deleted, False if not found.
        """
        result = await self._session.execute(
            select(SqlEdition).where(
                SqlEdition.project_id == project_id,
                SqlEdition.slug == slug,
                SqlEdition.date_deleted.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return False
        row.date_deleted = func.now()
        await self._session.flush()
        return True

    async def find_matching_editions(
        self,
        *,
        project_id: int,
        git_ref: str,
        alternate_name: str | None = None,
    ) -> list[Edition]:
        """Find editions that match a git_ref or alternate_name.

        Used by the build processing worker to determine which editions
        should be updated when a build completes.
        """
        conditions = [
            SqlEdition.project_id == project_id,
            SqlEdition.date_deleted.is_(None),
        ]

        stmt = self._base_query().where(*conditions).order_by(SqlEdition.slug)
        result = await self._session.execute(stmt)
        rows = result.all()

        matching = []
        for edition_row, build_public_id in rows:
            edition = self._validate(edition_row, build_public_id)
            if edition.tracking_mode == TrackingMode.git_ref:
                if alternate_name is None:
                    params = edition.tracking_params or {}
                    if params.get("git_ref") == git_ref:
                        matching.append(edition)
            elif (
                edition.tracking_mode == TrackingMode.alternate_git_ref
                and alternate_name is not None
            ):
                params = edition.tracking_params or {}
                if (
                    params.get("git_ref") == git_ref
                    and params.get("alternate_name") == alternate_name
                ):
                    matching.append(edition)
        return matching
