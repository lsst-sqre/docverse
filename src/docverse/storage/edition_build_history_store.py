"""Database operations for the edition_build_history table."""

from __future__ import annotations

import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.dbschema.edition_build_history import SqlEditionBuildHistory
from docverse.domain.edition_build_history import EditionBuildHistory


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
