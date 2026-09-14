"""Database operations for the edition_build_history table."""

from __future__ import annotations

from collections.abc import Sequence

import structlog
from safir.database import CountedPaginatedList, CountedPaginatedQueryRunner
from sqlalchemy import select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models.queue_enums import PublishStatus
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.edition import SqlEdition
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

_PAIR_CHUNK_SIZE = 5000
"""``(edition_id, build_id)`` pairs per ``list_by_edition_build_pairs``
SELECT.

The pair filter renders as ``tuple_(edition_id, build_id).in_(pairs)``,
which asyncpg binds as *two* parameters per pair against a PostgreSQL
wire protocol that caps a statement at 32,767 of them. The org-wide
reconciliation read passes one pair per pointed edition, and an
lsst-the-docs-sized organization has roughly 29,000 of those — so an
unchunked query there raises ``InterfaceError`` on every tick, fails the
``edition_reconcile`` row, and reconciles nothing while paging an
operator twice an hour.

5,000 pairs is 10,000 binds: enough headroom that a future column added
to the filter cannot quietly reach the ceiling, while keeping even that
organization to six round-trips.
"""


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

        The edition's own row is taken ``FOR UPDATE`` first, so two
        writers for one edition run this one after the other. The bump
        and the insert are separate statements, and under READ COMMITTED
        an unserialized pair of callers — an API rollback in
        :class:`~docverse_server.services.edition.EditionService`, which
        takes no ``EDITION_UPDATE`` advisory lock, racing a worker's
        tracking update — would each bump a history the other's insert is
        not yet visible in and both commit a row at position 1.
        ``uq_ebh_edition_position`` refuses that outright; the lock is
        what turns the refusal into a wait, so the loser bumps the
        winner's row and lands at position 1 itself rather than raising
        an ``IntegrityError`` at some caller that has no way to retry.

        The lock is released when the caller's transaction ends, per the
        handler-owns-the-transaction rule, and is taken on ``editions``
        *after* any ``builds`` lock the caller already holds — the
        build-then-edition order every other writer on this pair uses.
        An ``edition_id`` naming no row locks nothing: the store does not
        own that referential check, and the constraint still backstops
        the position.
        """
        await self._session.execute(
            select(SqlEdition.id)
            .where(SqlEdition.id == edition_id)
            .with_for_update()
        )

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

        Returns the pair's most recent row — the one with the lowest
        ``position`` — or ``None`` if the build was never recorded for
        this edition.

        The ordering is load-bearing, not tidiness. ``record()`` appends
        and nothing constrains ``(edition_id, build_id)`` to be unique,
        so an edition rolled back onto a build it already served has two
        or more rows for its current pair. Every writer of
        ``publish_status`` resolves its row through this lookup, while
        the reconciliation planner
        (:func:`docverse_server.domain.edition_reconcile.plan_edition_reconcile`)
        reads the position-ordered row from
        :meth:`list_by_edition_build_pairs`. Handing the writers an
        older row would put the two permanently out of step: the publish
        would mark the stale row ``published`` while the position-1 row
        stayed ``pending``, and every reconcile tick would re-drive a
        publish that had already happened.

        ``id DESC`` breaks a tie on ``position``.
        ``uq_ebh_edition_position`` forbids one being written, but rows
        that predate the constraint outlive it, and a tie resolved one
        way here and the other way in
        :meth:`list_by_edition_build_pairs` is exactly the split this
        ordering exists to prevent. Newest row wins, which is what
        position 1 already means.
        """
        stmt = (
            select(SqlEditionBuildHistory)
            .where(
                SqlEditionBuildHistory.edition_id == edition_id,
                SqlEditionBuildHistory.build_id == build_id,
            )
            .order_by(
                SqlEditionBuildHistory.position.asc(),
                SqlEditionBuildHistory.id.desc(),
            )
        )
        result = await self._session.execute(stmt)
        row = result.scalars().first()
        if row is None:
            return None
        return EditionBuildHistory.model_validate(row)

    async def get_by_id(self, history_id: int) -> EditionBuildHistory | None:
        """Look up one history row by its primary key.

        :meth:`get_by_edition_and_build` answers "which row does this
        ``(edition, build)`` pair point at *now*", which is the wrong
        question for a ``publish_edition`` job that was enqueued for one
        specific row and then sat on a backed-up queue while the edition
        was rolled away and back onto the same build. Such a job carries
        its row's id and resolves it here, so a late delivery cannot
        pick up — and overwrite — the newer row the pair has since
        acquired.

        Returns ``None`` when no row carries that id.
        """
        result = await self._session.execute(
            select(SqlEditionBuildHistory).where(
                SqlEditionBuildHistory.id == history_id
            )
        )
        row = result.scalar_one_or_none()
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
            .order_by(
                SqlEditionBuildHistory.position.asc(),
                SqlEditionBuildHistory.id.desc(),
            )
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
                SqlEditionBuildHistory.id.desc(),
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

        The batched form of :meth:`get_by_edition_and_build`: a handful
        of round-trips answer "has a publish ever been enqueued for this
        edition's *current* build?" for a whole set of editions at once.
        Used by keeper-sync's aggregate self-heal, which asks that
        question of every ``N`` / ``N.M`` row on a project, and by the
        reconciliation loop, which asks it of every pointed edition in
        an organization; both would otherwise open a transaction per
        row. Passing an empty ``pairs`` returns ``[]`` without hitting
        the database.

        Matching is on the pair, not on the two columns independently —
        an ``edition_id IN (...) AND build_id IN (...)`` filter would
        return the cross product, reporting a history row for a pair
        that was never recorded.

        The pair list is de-duplicated and then split into chunks of
        `_PAIR_CHUNK_SIZE`, one SELECT each, whose results are
        concatenated in request order. See that constant for the
        asyncpg bind ceiling this exists to stay under. De-duplicating
        first is what keeps the chunking invisible to callers: ``IN``
        collapsed a repeated pair on its own, but two chunks are two
        queries and would each answer it.

        Rows for any one pair come back ordered by ``position``, then
        by ``id`` descending — a caller grouping by pair and keeping the
        first row it sees gets the edition's most recent pointer at that
        build, and gets the same row
        :meth:`get_by_edition_and_build` would hand a publish writer
        even where two rows share a position (see that method). That
        holds across chunking because a pair belongs to exactly one
        chunk and each chunk carries the same ordering; only the
        relative order of *different* pairs depends on how the list was
        cut. Duplicate rows for a pair need an edition to have been
        pointed back at a build it had already left, which the
        aggregates this serves never do (they only advance), but the
        order makes the pick deterministic regardless.
        """
        if not pairs:
            return []
        unique_pairs = list(dict.fromkeys(pairs))
        rows: list[EditionBuildHistory] = []
        for start in range(0, len(unique_pairs), _PAIR_CHUNK_SIZE):
            chunk = unique_pairs[start : start + _PAIR_CHUNK_SIZE]
            stmt = (
                select(SqlEditionBuildHistory)
                .where(
                    tuple_(
                        SqlEditionBuildHistory.edition_id,
                        SqlEditionBuildHistory.build_id,
                    ).in_(chunk)
                )
                .order_by(
                    SqlEditionBuildHistory.edition_id,
                    SqlEditionBuildHistory.position.asc(),
                    SqlEditionBuildHistory.id.desc(),
                )
            )
            result = await self._session.execute(stmt)
            rows.extend(
                EditionBuildHistory.model_validate(r) for r in result.scalars()
            )
        return rows

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

        The ordering lives in
        :class:`~docverse_server.storage.pagination.EditionBuildHistoryPositionCursor`,
        which keys on ``position`` alone and so carries no ``id``
        tiebreaker. That is safe rather than an oversight:
        ``uq_ebh_edition_position`` makes an edition's positions unique,
        so the cursor names exactly one row, and this reader lists a
        whole edition rather than resolving a pair to a single row — the
        pick the tiebreaker on the other readers protects.
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
