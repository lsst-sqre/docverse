"""Tests for ``docverse.keeper_sync.mappers``.

Pure-function table tests with no DB or HTTP. Intended to lock the
mapping rules from PRD #275 ("Data model and state tracking — Mapping
rules") so a future change has to update both the rule and the test.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import HttpUrl

from docverse.client.models import EditionKind, TrackingMode
from docverse.keeper_sync.mappers import (
    derive_edition_kind,
    derive_edition_slug,
    map_edition_tracking,
)
from docverse.keeper_sync.models import LtdEdition


def _edition(
    *,
    slug: str = "main",
    mode: str = "git_refs",
    tracked_refs: list[str] | None = None,
) -> LtdEdition:
    return LtdEdition(
        self_url=HttpUrl("https://keeper.lsst.codes/editions/1"),
        product_url=HttpUrl("https://keeper.lsst.codes/products/p"),
        published_url=HttpUrl("https://example.com/"),
        slug=slug,
        title=slug,
        date_created=datetime(2026, 4, 1, tzinfo=UTC),
        mode=mode,
        tracked_refs=tracked_refs,
    )


@pytest.mark.parametrize(
    ("ltd_slug", "expected"),
    [
        ("main", EditionKind.main),
        ("u-jsick-feature", EditionKind.draft),
        ("DM-54112", EditionKind.draft),
        ("v1.0", EditionKind.draft),
    ],
)
def test_derive_edition_kind(ltd_slug: str, expected: EditionKind) -> None:
    assert derive_edition_kind(ltd_slug) == expected


@pytest.mark.parametrize(
    ("ltd_slug", "expected"),
    [
        ("main", "__main"),
        ("u-jsick-feature", "u-jsick-feature"),
        ("DM-54112", "DM-54112"),
    ],
)
def test_derive_edition_slug(ltd_slug: str, expected: str) -> None:
    assert derive_edition_slug(ltd_slug) == expected


def test_map_edition_tracking_git_refs_picks_first_tracked_ref() -> None:
    """``git_refs`` collapses to ``git_ref`` with the first tracked ref."""
    edition = _edition(mode="git_refs", tracked_refs=["main"])
    mode, params = map_edition_tracking(edition)
    assert mode == TrackingMode.git_ref
    assert params == {"git_ref": "main"}


def test_map_edition_tracking_git_refs_uses_first_when_multi() -> None:
    """Multi-ref ``git_refs`` (rare but valid) takes the first ref."""
    edition = _edition(mode="git_refs", tracked_refs=["main", "tickets/DM-1"])
    _, params = map_edition_tracking(edition)
    assert params == {"git_ref": "main"}


def test_map_edition_tracking_git_refs_branch_slug() -> None:
    edition = _edition(
        slug="u-jsick-feature",
        mode="git_refs",
        tracked_refs=["u/jsick/feature"],
    )
    mode, params = map_edition_tracking(edition)
    assert mode == TrackingMode.git_ref
    assert params == {"git_ref": "u/jsick/feature"}


def test_map_edition_tracking_git_refs_missing_tracked_refs_raises() -> None:
    edition = _edition(mode="git_refs", tracked_refs=None)
    with pytest.raises(ValueError, match="tracked_refs"):
        map_edition_tracking(edition)


def test_map_edition_tracking_git_refs_empty_tracked_refs_raises() -> None:
    edition = _edition(mode="git_refs", tracked_refs=[])
    with pytest.raises(ValueError, match="tracked_refs"):
        map_edition_tracking(edition)


@pytest.mark.parametrize(
    "mode",
    [
        "lsst_doc",
        "eups_major_release",
        "eups_weekly_release",
        "eups_daily_release",
        "manual",
    ],
)
def test_map_edition_tracking_other_modes_not_implemented(mode: str) -> None:
    """Non-``git_refs`` modes must raise NotImplementedError until #289."""
    edition = _edition(mode=mode, tracked_refs=["main"])
    with pytest.raises(NotImplementedError, match="#289"):
        map_edition_tracking(edition)
