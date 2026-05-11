"""Pagination cursors, sort order enums, and constants."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Self, override

from safir.database import (
    DatetimeIdCursor,
    InvalidCursorError,
    PaginationCursor,
)
from sqlalchemy import Select, and_, or_
from sqlalchemy.orm import InstrumentedAttribute

from docverse.dbschema.build import SqlBuild
from docverse.dbschema.edition import SqlEdition
from docverse.dbschema.edition_build_history import SqlEditionBuildHistory
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.project import SqlProject
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistoryWithBuild
from docverse.domain.keeper_sync_run import KeeperSyncRun
from docverse.domain.project import Project
from docverse.domain.queue import QueueJob

__all__ = [
    "BUILD_CURSOR_TYPE",
    "DEFAULT_PAGE_LIMIT",
    "EDITION_CURSOR_TYPES",
    "EDITION_HISTORY_CURSOR_TYPE",
    "KEEPER_SYNC_EDITION_CURSOR_TYPE",
    "KEEPER_SYNC_RUN_CURSOR_TYPE",
    "MAX_PAGE_LIMIT",
    "PROJECT_CURSOR_TYPES",
    "QUEUE_JOB_CURSOR_TYPE",
    "BuildDateCreatedCursor",
    "EditionBuildHistoryPositionCursor",
    "EditionDateCreatedCursor",
    "EditionDateUpdatedCursor",
    "EditionSlugCursor",
    "EditionSortOrder",
    "KeeperSyncEditionSlugCursor",
    "KeeperSyncRunDateStartedCursor",
    "ProjectDateCreatedCursor",
    "ProjectSearchCursor",
    "ProjectSlugCursor",
    "ProjectSortOrder",
    "QueueJobDateCreatedCursor",
]

DEFAULT_PAGE_LIMIT = 25
"""Default number of entries per page."""

MAX_PAGE_LIMIT = 100
"""Maximum number of entries per page."""


# ---------------------------------------------------------------------------
# Sort order enums
# ---------------------------------------------------------------------------


class ProjectSortOrder(StrEnum):
    """Sort orders for project listings."""

    slug = "slug"
    date_created = "date_created"


class EditionSortOrder(StrEnum):
    """Sort orders for edition listings."""

    slug = "slug"
    date_created = "date_created"
    date_updated = "date_updated"


# ---------------------------------------------------------------------------
# Slug-based cursors (ASC ordering)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ProjectSlugCursor(PaginationCursor[Project]):
    """Keyset cursor for projects ordered by slug ASC."""

    slug: str

    @override
    @classmethod
    def from_entry(cls, entry: Project, *, reverse: bool = False) -> Self:
        return cls(slug=entry.slug, previous=reverse)

    @override
    @classmethod
    def from_str(cls, cursor: str) -> Self:
        if cursor.startswith("p__"):
            return cls(slug=cursor[3:], previous=True)
        return cls(slug=cursor, previous=False)

    @override
    @classmethod
    def apply_order(
        cls, stmt: Select[tuple[Any, ...]], *, reverse: bool = False
    ) -> Select[tuple[Any, ...]]:
        if reverse:
            return stmt.order_by(SqlProject.slug.desc())
        return stmt.order_by(SqlProject.slug)

    @override
    def apply_cursor(
        self, stmt: Select[tuple[Any, ...]]
    ) -> Select[tuple[Any, ...]]:
        if self.previous:
            return stmt.where(SqlProject.slug < self.slug)
        return stmt.where(SqlProject.slug >= self.slug)

    @override
    def invert(self) -> Self:
        return type(self)(slug=self.slug, previous=not self.previous)

    def __str__(self) -> str:  # noqa: D105
        prefix = "p__" if self.previous else ""
        return f"{prefix}{self.slug}"


@dataclass(slots=True)
class EditionSlugCursor(PaginationCursor[Edition]):
    """Keyset cursor for editions ordered by slug ASC."""

    slug: str

    @override
    @classmethod
    def from_entry(cls, entry: Edition, *, reverse: bool = False) -> Self:
        return cls(slug=entry.slug, previous=reverse)

    @override
    @classmethod
    def from_str(cls, cursor: str) -> Self:
        if cursor.startswith("p__"):
            return cls(slug=cursor[3:], previous=True)
        return cls(slug=cursor, previous=False)

    @override
    @classmethod
    def apply_order(
        cls, stmt: Select[tuple[Any, ...]], *, reverse: bool = False
    ) -> Select[tuple[Any, ...]]:
        if reverse:
            return stmt.order_by(SqlEdition.slug.desc())
        return stmt.order_by(SqlEdition.slug)

    @override
    def apply_cursor(
        self, stmt: Select[tuple[Any, ...]]
    ) -> Select[tuple[Any, ...]]:
        if self.previous:
            return stmt.where(SqlEdition.slug < self.slug)
        return stmt.where(SqlEdition.slug >= self.slug)

    @override
    def invert(self) -> Self:
        return type(self)(slug=self.slug, previous=not self.previous)

    def __str__(self) -> str:  # noqa: D105
        prefix = "p__" if self.previous else ""
        return f"{prefix}{self.slug}"


@dataclass(slots=True)
class KeeperSyncEditionSlugCursor(PaginationCursor[Edition]):
    """Keyset cursor for editions ordered by slug ASC, id ASC.

    Used by the keeper-sync per-project editions collection so an
    operator paginating with the default lexicographic ordering can
    scan through `__main` first, then alphabetically. The id tiebreaker
    is defensive: ``uq_editions_project_lower_slug`` makes the slug
    unique per project today, but the composite keeps pagination
    stable if that invariant is ever relaxed (e.g. case-sensitive
    duplicates).
    """

    slug: str
    id: int

    @override
    @classmethod
    def from_entry(cls, entry: Edition, *, reverse: bool = False) -> Self:
        return cls(slug=entry.slug, id=entry.id, previous=reverse)

    @override
    @classmethod
    def from_str(cls, cursor: str) -> Self:
        try:
            previous = cursor.startswith("p__")
            raw = cursor[3:] if previous else cursor
            slug, id_str = raw.rsplit(":", 1)
            return cls(slug=slug, id=int(id_str), previous=previous)
        except (ValueError, TypeError) as exc:
            msg = f"Invalid cursor: {cursor!r}"
            raise InvalidCursorError(msg) from exc

    @override
    @classmethod
    def apply_order(
        cls, stmt: Select[tuple[Any, ...]], *, reverse: bool = False
    ) -> Select[tuple[Any, ...]]:
        if reverse:
            return stmt.order_by(SqlEdition.slug.desc(), SqlEdition.id.desc())
        return stmt.order_by(SqlEdition.slug, SqlEdition.id)

    @override
    def apply_cursor(
        self, stmt: Select[tuple[Any, ...]]
    ) -> Select[tuple[Any, ...]]:
        if self.previous:
            return stmt.where(
                or_(
                    SqlEdition.slug < self.slug,
                    and_(
                        SqlEdition.slug == self.slug,
                        SqlEdition.id < self.id,
                    ),
                )
            )
        return stmt.where(
            or_(
                SqlEdition.slug > self.slug,
                and_(
                    SqlEdition.slug == self.slug,
                    SqlEdition.id >= self.id,
                ),
            )
        )

    @override
    def invert(self) -> Self:
        return type(self)(
            slug=self.slug, id=self.id, previous=not self.previous
        )

    def __str__(self) -> str:  # noqa: D105
        prefix = "p__" if self.previous else ""
        return f"{prefix}{self.slug}:{self.id}"


# ---------------------------------------------------------------------------
# Datetime+ID cursors (DESC ordering)
# ---------------------------------------------------------------------------


class _TzAwareDatetimeIdCursor[E: Any](DatetimeIdCursor[E]):
    """``DatetimeIdCursor`` whose filter keeps the cursor time tz-aware.

    Why: safir's ``DatetimeIdCursor.apply_cursor`` calls ``datetime_to_db``
    to strip ``tzinfo`` before binding the value. When the resulting naive
    datetime is compared via asyncpg to a ``TIMESTAMP WITH TIME ZONE``
    column, the implicit cast does not honour the database session's
    ``TimeZone`` setting — ``<`` then matches every row and ``=`` matches
    none, so any cursor that should land on a tied timestamp returns the
    same page again instead of advancing. Keeping the value timezone-aware
    sidesteps the broken cast.
    """

    @override
    def apply_cursor(
        self, stmt: Select[tuple[Any, ...]]
    ) -> Select[tuple[Any, ...]]:
        time_column = self.time_column()
        id_column = self.id_column()
        if self.previous:
            return stmt.where(
                or_(
                    time_column > self.time,
                    and_(time_column == self.time, id_column > self.id),
                )
            )
        return stmt.where(
            or_(
                time_column < self.time,
                and_(time_column == self.time, id_column <= self.id),
            )
        )


@dataclass(slots=True)
class ProjectDateCreatedCursor(_TzAwareDatetimeIdCursor[Project]):
    """Keyset cursor for projects ordered by date_created DESC, id DESC."""

    @staticmethod
    @override
    def id_column() -> InstrumentedAttribute[int]:
        return SqlProject.id

    @staticmethod
    @override
    def time_column() -> InstrumentedAttribute[datetime]:
        return SqlProject.date_created

    @override
    @classmethod
    def from_entry(cls, entry: Project, *, reverse: bool = False) -> Self:
        return cls(time=entry.date_created, id=entry.id, previous=reverse)


@dataclass(slots=True)
class EditionDateCreatedCursor(_TzAwareDatetimeIdCursor[Edition]):
    """Keyset cursor for editions ordered by date_created DESC, id DESC."""

    @staticmethod
    @override
    def id_column() -> InstrumentedAttribute[int]:
        return SqlEdition.id

    @staticmethod
    @override
    def time_column() -> InstrumentedAttribute[datetime]:
        return SqlEdition.date_created

    @override
    @classmethod
    def from_entry(cls, entry: Edition, *, reverse: bool = False) -> Self:
        return cls(time=entry.date_created, id=entry.id, previous=reverse)


@dataclass(slots=True)
class EditionDateUpdatedCursor(_TzAwareDatetimeIdCursor[Edition]):
    """Keyset cursor for editions ordered by date_updated DESC, id DESC."""

    @staticmethod
    @override
    def id_column() -> InstrumentedAttribute[int]:
        return SqlEdition.id

    @staticmethod
    @override
    def time_column() -> InstrumentedAttribute[datetime]:
        return SqlEdition.date_updated

    @override
    @classmethod
    def from_entry(cls, entry: Edition, *, reverse: bool = False) -> Self:
        return cls(time=entry.date_updated, id=entry.id, previous=reverse)


@dataclass(slots=True)
class BuildDateCreatedCursor(_TzAwareDatetimeIdCursor[Build]):
    """Keyset cursor for builds ordered by date_created DESC, id DESC."""

    @staticmethod
    @override
    def id_column() -> InstrumentedAttribute[int]:
        return SqlBuild.id

    @staticmethod
    @override
    def time_column() -> InstrumentedAttribute[datetime]:
        return SqlBuild.date_created

    @override
    @classmethod
    def from_entry(cls, entry: Build, *, reverse: bool = False) -> Self:
        return cls(time=entry.date_created, id=entry.id, previous=reverse)


@dataclass(slots=True)
class KeeperSyncRunDateStartedCursor(_TzAwareDatetimeIdCursor[KeeperSyncRun]):
    """Keyset cursor for runs ordered by date_started DESC, id DESC."""

    @staticmethod
    @override
    def id_column() -> InstrumentedAttribute[int]:
        return SqlKeeperSyncRun.id

    @staticmethod
    @override
    def time_column() -> InstrumentedAttribute[datetime]:
        return SqlKeeperSyncRun.date_started

    @override
    @classmethod
    def from_entry(
        cls, entry: KeeperSyncRun, *, reverse: bool = False
    ) -> Self:
        return cls(time=entry.date_started, id=entry.id, previous=reverse)


@dataclass(slots=True)
class QueueJobDateCreatedCursor(_TzAwareDatetimeIdCursor[QueueJob]):
    """Keyset cursor for queue jobs ordered by date_created DESC, id DESC."""

    @staticmethod
    @override
    def id_column() -> InstrumentedAttribute[int]:
        return SqlQueueJob.id

    @staticmethod
    @override
    def time_column() -> InstrumentedAttribute[datetime]:
        return SqlQueueJob.date_created

    @override
    @classmethod
    def from_entry(cls, entry: QueueJob, *, reverse: bool = False) -> Self:
        return cls(time=entry.date_created, id=entry.id, previous=reverse)


# ---------------------------------------------------------------------------
# Search cursor (score+id compound keyset)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ProjectSearchCursor(PaginationCursor[Project]):
    """Keyset cursor for search results ordered by score DESC, id DESC.

    Encodes a ``(score, id)`` pair so pagination over relevance-ranked
    results is stable even when two projects share the same trigram
    similarity score.
    """

    score: float
    id: int

    @override
    @classmethod
    def from_entry(cls, entry: Project, *, reverse: bool = False) -> Self:
        # Only called by CountedPaginatedQueryRunner, which is not used here.
        raise NotImplementedError

    @override
    @classmethod
    def from_str(cls, cursor: str) -> Self:
        try:
            previous = cursor.startswith("p__")
            raw = cursor[3:] if previous else cursor
            score_str, id_str = raw.split(":", 1)
            return cls(
                score=float(score_str), id=int(id_str), previous=previous
            )
        except (ValueError, TypeError) as exc:
            msg = f"Invalid cursor: {cursor!r}"
            raise InvalidCursorError(msg) from exc

    @override
    @classmethod
    def apply_order(
        cls, stmt: Select[tuple[Any, ...]], *, reverse: bool = False
    ) -> Select[tuple[Any, ...]]:
        # Ordering is applied manually in search_by_org.
        return stmt

    @override
    def apply_cursor(
        self, stmt: Select[tuple[Any, ...]]
    ) -> Select[tuple[Any, ...]]:
        # Filtering is applied manually in search_by_org.
        return stmt

    @override
    def invert(self) -> Self:
        return type(self)(
            score=self.score, id=self.id, previous=not self.previous
        )

    def __str__(self) -> str:  # noqa: D105
        prefix = "p__" if self.previous else ""
        return f"{prefix}{self.score:.8f}:{self.id}"


# ---------------------------------------------------------------------------
# Lookup dicts for handlers
# ---------------------------------------------------------------------------

PROJECT_CURSOR_TYPES: dict[
    ProjectSortOrder, type[PaginationCursor[Project]]
] = {
    ProjectSortOrder.slug: ProjectSlugCursor,
    ProjectSortOrder.date_created: ProjectDateCreatedCursor,
}

EDITION_CURSOR_TYPES: dict[
    EditionSortOrder, type[PaginationCursor[Edition]]
] = {
    EditionSortOrder.slug: EditionSlugCursor,
    EditionSortOrder.date_created: EditionDateCreatedCursor,
    EditionSortOrder.date_updated: EditionDateUpdatedCursor,
}

BUILD_CURSOR_TYPE: type[BuildDateCreatedCursor] = BuildDateCreatedCursor

KEEPER_SYNC_EDITION_CURSOR_TYPE: type[KeeperSyncEditionSlugCursor] = (
    KeeperSyncEditionSlugCursor
)

KEEPER_SYNC_RUN_CURSOR_TYPE: type[KeeperSyncRunDateStartedCursor] = (
    KeeperSyncRunDateStartedCursor
)

QUEUE_JOB_CURSOR_TYPE: type[QueueJobDateCreatedCursor] = (
    QueueJobDateCreatedCursor
)


# ---------------------------------------------------------------------------
# Edition build history cursor (position ASC)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class EditionBuildHistoryPositionCursor(
    PaginationCursor[EditionBuildHistoryWithBuild]
):
    """Keyset cursor for edition history ordered by position ASC."""

    position: int

    @override
    @classmethod
    def from_entry(
        cls,
        entry: EditionBuildHistoryWithBuild,
        *,
        reverse: bool = False,
    ) -> Self:
        return cls(position=entry.position, previous=reverse)

    @override
    @classmethod
    def from_str(cls, cursor: str) -> Self:
        try:
            if cursor.startswith("p__"):
                return cls(position=int(cursor[3:]), previous=True)
            return cls(position=int(cursor), previous=False)
        except ValueError as exc:
            msg = f"Invalid cursor: {cursor!r}"
            raise InvalidCursorError(msg) from exc

    @override
    @classmethod
    def apply_order(
        cls, stmt: Select[tuple[Any, ...]], *, reverse: bool = False
    ) -> Select[tuple[Any, ...]]:
        if reverse:
            return stmt.order_by(SqlEditionBuildHistory.position.desc())
        return stmt.order_by(SqlEditionBuildHistory.position)

    @override
    def apply_cursor(
        self, stmt: Select[tuple[Any, ...]]
    ) -> Select[tuple[Any, ...]]:
        if self.previous:
            return stmt.where(SqlEditionBuildHistory.position < self.position)
        return stmt.where(SqlEditionBuildHistory.position >= self.position)

    @override
    def invert(self) -> Self:
        return type(self)(position=self.position, previous=not self.previous)

    def __str__(self) -> str:  # noqa: D105
        prefix = "p__" if self.previous else ""
        return f"{prefix}{self.position}"


EDITION_HISTORY_CURSOR_TYPE: type[EditionBuildHistoryPositionCursor] = (
    EditionBuildHistoryPositionCursor
)
