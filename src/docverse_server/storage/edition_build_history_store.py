"""Database operations for the edition_build_history table."""

from __future__ import annotations

from collections.abc import Sequence

import structlog
from safir.database import CountedPaginatedList, CountedPaginatedQueryRunner
from sqlalchemy import select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models.queue_enums import PublishStatus
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.edition_build_history import (
    SqlEditionBuildHistory,
)
from docverse_server.domain.edition_build_history import (
    EditionBuildHistory,
    EditionBuildHistoryWithBuild,
)
from docverse_server.storage.pagination import (
    EditionBuildHistoryPositionCursor,
)


class EditionBuildHistoryStore:
    """Direct database operations for edition build history."""

    def __init__(
        self,
        session: AsyncSession,
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

    async def set_publish_status(
        self, *, history_id: int, status: PublishStatus
    ) -> None:
        """Set the ``publish_status`` column on a history row."""
        result = await self._session.execute(
            select(SqlEditionBuildHistory).where(
                SqlEditionBuildHistory.id == history_id
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            msg = f"EditionBuildHistory id={history_id} not found"
            raise RuntimeError(msg)
        row.publish_status = status.value
        await self._session.flush()

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

    async def list_by_edition_ids(
        self, edition_ids: list[int]
    ) -> list[EditionBuildHistory]:
        """List history rows for the given editions in a single round-trip.

        Ordered by ``(edition_id, position)`` so callers that group by
        edition see each edition's history sorted oldest-position-first.
        Used by the ``lifecycle_eval`` per-org worker to load every
        edition's rollback history in one query rather than N. Passing
        an empty ``edition_ids`` returns ``[]`` without hitting the
        database.
        """
        if not edition_ids:
            return []
        stmt = (
            select(SqlEditionBuildHistory)
            .where(SqlEditionBuildHistory.edition_id.in_(edition_ids))
            .order_by(
                SqlEditionBuildHistory.edition_id,
                SqlEditionBuildHistory.position.asc(),
            )
        )
        result = await self._session.execute(stmt)
        return [
            EditionBuildHistory.model_validate(r) for r in result.scalars()
        ]

    async def list_by_edition_build_pairs(
        self, pairs: Sequence[tuple[int, int]]
    ) -> list[EditionBuildHistory]:
        """Load history rows for specific ``(edition_id, build_id)`` pairs.

        The batched form of :meth:`get_by_edition_and_build`: one
        round-trip answers "has a publish ever been enqueued for this
        edition's *current* build?" for a whole set of editions at once.
        Used by keeper-sync's aggregate self-heal, which asks that
        question of every ``N`` / ``N.M`` row on a project and would
        otherwise open a transaction per aggregate. Passing an empty
        ``pairs`` returns ``[]`` without hitting the database.

        Matching is on the pair, not on the two columns independently —
        an ``edition_id IN (...) AND build_id IN (...)`` filter would
        return the cross product, reporting a history row for a pair
        that was never recorded.

        Rows come back ordered by ``(edition_id, position)``, so a
        caller grouping by pair and keeping the first row it sees gets
        the edition's most recent pointer at that build. Duplicate pairs
        need an edition to have been pointed back at a build it had
        already left, which the aggregates this serves never do (they
        only advance), but the order makes the pick deterministic
        regardless.
        """
        if not pairs:
            return []
        stmt = (
            select(SqlEditionBuildHistory)
            .where(
                tuple_(
                    SqlEditionBuildHistory.edition_id,
                    SqlEditionBuildHistory.build_id,
                ).in_(pairs)
            )
            .order_by(
                SqlEditionBuildHistory.edition_id,
                SqlEditionBuildHistory.position.asc(),
            )
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
                SqlBuild.annotations.label("build_annotations"),
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
