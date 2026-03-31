"""Database operations for the edition_build_history table."""

from __future__ import annotations

import structlog
from safir.database import CountedPaginatedList, CountedPaginatedQueryRunner
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.dbschema.build import SqlBuild
from docverse.dbschema.edition_build_history import SqlEditionBuildHistory
from docverse.domain.edition_build_history import (
    EditionBuildHistory,
    EditionBuildHistoryWithBuild,
)
from docverse.storage.pagination import EditionBuildHistoryPositionCursor


class EditionBuildHistoryStore:
    """Direct database operations for edition build history."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def record(
        self, *, edition_id: int, build_id: int
    ) -> EditionBuildHistory:
        """Record a new build pointer for an edition.

        Shifts all existing positions for this edition up by one and
        inserts the new entry at position 1 (most recent).
        """
        # Shift existing positions up
        stmt = (
            update(SqlEditionBuildHistory)
            .where(SqlEditionBuildHistory.edition_id == edition_id)
            .values(position=SqlEditionBuildHistory.position + 1)
        )
        await self._session.execute(stmt)

        # Insert new entry at position 1
        row = SqlEditionBuildHistory(
            edition_id=edition_id,
            build_id=build_id,
            position=1,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return EditionBuildHistory.model_validate(row)

    async def get_by_edition_and_build(
        self, *, edition_id: int, build_id: int
    ) -> EditionBuildHistory | None:
        """Look up a history entry for an edition and build combination.

        Returns the first match or ``None`` if the build was never
        recorded for this edition.
        """
        stmt = select(SqlEditionBuildHistory).where(
            SqlEditionBuildHistory.edition_id == edition_id,
            SqlEditionBuildHistory.build_id == build_id,
        )
        result = await self._session.execute(stmt)
        row = result.scalars().first()
        if row is None:
            return None
        return EditionBuildHistory.model_validate(row)

    async def list_by_edition(
        self, edition_id: int
    ) -> list[EditionBuildHistory]:
        """List history entries for an edition, ordered by position.

        Position 1 (most recent) is returned first.
        """
        stmt = (
            select(SqlEditionBuildHistory)
            .where(SqlEditionBuildHistory.edition_id == edition_id)
            .order_by(SqlEditionBuildHistory.position.asc())
        )
        result = await self._session.execute(stmt)
        return [
            EditionBuildHistory.model_validate(r) for r in result.scalars()
        ]

    async def list_by_edition_with_build_info(
        self,
        edition_id: int,
        *,
        cursor: EditionBuildHistoryPositionCursor | None = None,
        limit: int,
        include_deleted: bool = False,
    ) -> CountedPaginatedList[
        EditionBuildHistoryWithBuild, EditionBuildHistoryPositionCursor
    ]:
        """List history entries with joined build metadata.

        Parameters
        ----------
        edition_id
            The edition to list history for.
        cursor
            Pagination cursor.
        limit
            Maximum number of results.
        include_deleted
            When ``False`` (default), history entries whose build has been
            soft-deleted are excluded.

        Returns paginated results ordered by position ASC (most recent
        first).
        """
        stmt = (
            select(
                SqlEditionBuildHistory.id,
                SqlEditionBuildHistory.edition_id,
                SqlEditionBuildHistory.build_id,
                SqlBuild.public_id.label("build_public_id"),
                SqlBuild.git_ref.label("build_git_ref"),
                SqlBuild.status.label("build_status"),
                SqlBuild.date_deleted.label("build_date_deleted"),
                SqlEditionBuildHistory.position,
                SqlEditionBuildHistory.date_created,
            )
            .join(
                SqlBuild,
                SqlEditionBuildHistory.build_id == SqlBuild.id,
            )
            .where(SqlEditionBuildHistory.edition_id == edition_id)
        )
        if not include_deleted:
            stmt = stmt.where(SqlBuild.date_deleted.is_(None))
        runner = CountedPaginatedQueryRunner(
            entry_type=EditionBuildHistoryWithBuild,
            cursor_type=EditionBuildHistoryPositionCursor,
        )
        return await runner.query_row(
            self._session, stmt, cursor=cursor, limit=limit
        )
