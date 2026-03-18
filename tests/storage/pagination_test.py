"""Tests for pagination cursors."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from safir.database import InvalidCursorError

from docverse.storage.pagination import (
    BuildDateCreatedCursor,
    EditionDateCreatedCursor,
    EditionDateUpdatedCursor,
    EditionSlugCursor,
    ProjectDateCreatedCursor,
    ProjectSlugCursor,
)

# ---------------------------------------------------------------------------
# Slug cursor tests
# ---------------------------------------------------------------------------


def test_project_slug_cursor_roundtrip_forward() -> None:
    """ProjectSlugCursor forward serialization roundtrip."""
    cursor = ProjectSlugCursor(slug="my-project", previous=False)
    serialized = str(cursor)
    assert serialized == "my-project"
    restored = ProjectSlugCursor.from_str(serialized)
    assert restored.slug == "my-project"
    assert restored.previous is False


def test_project_slug_cursor_roundtrip_previous() -> None:
    """ProjectSlugCursor previous serialization roundtrip."""
    cursor = ProjectSlugCursor(slug="my-project", previous=True)
    serialized = str(cursor)
    assert serialized == "p__my-project"
    restored = ProjectSlugCursor.from_str(serialized)
    assert restored.slug == "my-project"
    assert restored.previous is True


def test_project_slug_cursor_invert() -> None:
    """ProjectSlugCursor invert flips direction."""
    cursor = ProjectSlugCursor(slug="abc", previous=False)
    inverted = cursor.invert()
    assert inverted.slug == "abc"
    assert inverted.previous is True
    assert inverted.invert().previous is False


def test_edition_slug_cursor_roundtrip() -> None:
    """EditionSlugCursor serialization roundtrip."""
    cursor = EditionSlugCursor(slug="__main", previous=False)
    assert str(cursor) == "__main"
    restored = EditionSlugCursor.from_str("__main")
    assert restored.slug == "__main"
    assert restored.previous is False


def test_edition_slug_cursor_previous() -> None:
    """EditionSlugCursor previous direction."""
    cursor = EditionSlugCursor(slug="v2.0", previous=True)
    assert str(cursor) == "p__v2.0"
    restored = EditionSlugCursor.from_str("p__v2.0")
    assert restored.slug == "v2.0"
    assert restored.previous is True


# ---------------------------------------------------------------------------
# Datetime+ID cursor tests
# ---------------------------------------------------------------------------


def test_project_date_created_cursor_roundtrip() -> None:
    """ProjectDateCreatedCursor serialization roundtrip."""
    t = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
    cursor = ProjectDateCreatedCursor(time=t, id=42, previous=False)
    serialized = str(cursor)
    restored = ProjectDateCreatedCursor.from_str(serialized)
    assert restored.id == 42
    assert restored.previous is False
    assert abs(restored.time.timestamp() - t.timestamp()) < 0.001


def test_project_date_created_cursor_previous() -> None:
    """ProjectDateCreatedCursor previous direction prefix."""
    t = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
    cursor = ProjectDateCreatedCursor(time=t, id=42, previous=True)
    serialized = str(cursor)
    assert serialized.startswith("p")
    restored = ProjectDateCreatedCursor.from_str(serialized)
    assert restored.id == 42
    assert restored.previous is True


def test_project_date_created_cursor_invert() -> None:
    """ProjectDateCreatedCursor invert flips direction."""
    t = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
    cursor = ProjectDateCreatedCursor(time=t, id=42, previous=False)
    inverted = cursor.invert()
    assert inverted.id == 42
    assert inverted.previous is True


def test_project_date_created_cursor_invalid() -> None:
    """Invalid cursor string raises InvalidCursorError."""
    with pytest.raises(InvalidCursorError):
        ProjectDateCreatedCursor.from_str("not_a_cursor")


def test_edition_date_created_cursor_roundtrip() -> None:
    """EditionDateCreatedCursor serialization roundtrip."""
    t = datetime(2026, 1, 1, tzinfo=UTC)
    cursor = EditionDateCreatedCursor(time=t, id=10, previous=False)
    restored = EditionDateCreatedCursor.from_str(str(cursor))
    assert restored.id == 10
    assert restored.previous is False


def test_edition_date_updated_cursor_roundtrip() -> None:
    """EditionDateUpdatedCursor serialization roundtrip."""
    t = datetime(2026, 2, 1, tzinfo=UTC)
    cursor = EditionDateUpdatedCursor(time=t, id=20, previous=False)
    restored = EditionDateUpdatedCursor.from_str(str(cursor))
    assert restored.id == 20
    assert restored.previous is False


def test_build_date_created_cursor_roundtrip() -> None:
    """BuildDateCreatedCursor serialization roundtrip."""
    t = datetime(2026, 3, 1, tzinfo=UTC)
    cursor = BuildDateCreatedCursor(time=t, id=99, previous=False)
    restored = BuildDateCreatedCursor.from_str(str(cursor))
    assert restored.id == 99
    assert restored.previous is False
