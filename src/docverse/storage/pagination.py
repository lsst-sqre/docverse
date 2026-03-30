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
from sqlalchemy import Select
from sqlalchemy.orm import InstrumentedAttribute

from docverse.dbschema.build import SqlBuild
from docverse.dbschema.edition import SqlEdition
from docverse.dbschema.edition_build_history import SqlEditionBuildHistory
from docverse.dbschema.project import SqlProject
from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistoryWithBuild
from docverse.domain.project import Project

__all__ = [
    "BUILD_CURSOR_TYPE",
    "DEFAULT_PAGE_LIMIT",
    "EDITION_CURSOR_TYPES",
    "EDITION_HISTORY_CURSOR_TYPE",
    "MAX_PAGE_LIMIT",
    "PROJECT_CURSOR_TYPES",
    "BuildDateCreatedCursor",
    "EditionBuildHistoryPositionCursor",
    "EditionDateCreatedCursor",
    "EditionDateUpdatedCursor",
    "EditionSlugCursor",
    "EditionSortOrder",
    "ProjectDateCreatedCursor",
    "ProjectSearchCursor",
    "ProjectSlugCursor",
    "ProjectSortOrder",
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


# ---------------------------------------------------------------------------
# Datetime+ID cursors (DESC ordering)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ProjectDateCreatedCursor(DatetimeIdCursor[Project]):
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
class EditionDateCreatedCursor(DatetimeIdCursor[Edition]):
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
class EditionDateUpdatedCursor(DatetimeIdCursor[Edition]):
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
class BuildDateCreatedCursor(DatetimeIdCursor[Build]):
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
