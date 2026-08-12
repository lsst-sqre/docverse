"""Tests for ``docverse.services.keeper_sync.mappers``.

Pure-function table tests with no DB or HTTP. Intended to lock the
mapping rules from PRD #275 ("Data model and state tracking — Mapping
rules") so a future change has to update both the rule and the test.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import HttpUrl

from docverse.models import EditionKind, TrackingMode
from docverse_server.domain.slug import parse_slug_rewrite_rules
from docverse_server.services.keeper_sync.mappers import (
    EditionKindSource,
    derive_edition_kind,
    derive_edition_slug,
    derive_edition_source_prefix,
    map_edition_tracking,
)
from docverse_server.storage.ltd import LtdBuild, LtdEdition


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


def _build(*, git_refs: list[str] | None = None) -> LtdBuild:
    return LtdBuild(
        self_url=HttpUrl("https://keeper.lsst.codes/builds/42"),
        product_url=HttpUrl("https://keeper.lsst.codes/products/p"),
        slug="42",
        date_created=datetime(2026, 4, 1, tzinfo=UTC),
        uploaded=True,
        bucket_name="lsst-the-docs",
        bucket_root_dir="p/builds/42",
        git_refs=git_refs,
        published_url=HttpUrl("https://example.com/"),
    )


class TestDeriveEditionKind:
    """``derive_edition_kind`` is mode-first, then rule-driven."""

    def test_git_refs_version_ref_is_a_release(self) -> None:
        edition = _edition(
            slug="15.2.1", mode="git_refs", tracked_refs=["15.2.1"]
        )
        derivation = derive_edition_kind(edition, git_ref="15.2.1")
        assert derivation.kind == EditionKind.release
        assert derivation.source == EditionKindSource.rule
        assert derivation.detail == "semver"

    def test_main_edition_is_main(self) -> None:
        edition = _edition(slug="main", tracked_refs=["main"])
        derivation = derive_edition_kind(edition, git_ref="main")
        assert derivation.kind == EditionKind.main
        assert derivation.source == EditionKindSource.ltd_main

    def test_git_refs_ticket_branch_is_a_draft(self) -> None:
        edition = _edition(
            slug="DM-54112",
            mode="git_refs",
            tracked_refs=["tickets/DM-54112"],
        )
        derivation = derive_edition_kind(edition, git_ref="tickets/DM-54112")
        assert derivation.kind == EditionKind.draft
        assert derivation.source == EditionKindSource.fallback
        assert derivation.detail is None

    @pytest.mark.parametrize(
        ("ltd_mode", "expected"),
        [
            ("lsst_doc", EditionKind.release),
            ("eups_major_release", EditionKind.release),
            ("eups_weekly_release", EditionKind.release),
            ("eups_daily_release", EditionKind.draft),
        ],
    )
    def test_version_modes_map_directly(
        self, ltd_mode: str, expected: EditionKind
    ) -> None:
        """Version modes decide the kind without consulting the ref.

        The tracked ref here is deliberately ticket-shaped: a mode match
        must win before the ref heuristic ever runs.
        """
        edition = _edition(
            slug="v1.0", mode=ltd_mode, tracked_refs=["tickets/DM-1"]
        )
        derivation = derive_edition_kind(edition, git_ref="tickets/DM-1")
        assert derivation.kind == expected
        assert derivation.source == EditionKindSource.ltd_mode
        assert derivation.detail == ltd_mode

    def test_manual_uses_the_supplied_build_ref(self) -> None:
        """``manual`` editions classify on the ref their tracking pins."""
        edition = _edition(slug="current", mode="manual", tracked_refs=None)
        derivation = derive_edition_kind(edition, git_ref="v22_0")
        assert derivation.kind == EditionKind.release
        assert derivation.source == EditionKindSource.rule
        assert derivation.detail == "eups_major"

    def test_falls_back_to_tracked_ref_when_git_ref_omitted(self) -> None:
        edition = _edition(
            slug="w-2026-10", mode="git_refs", tracked_refs=["w_2026_10"]
        )
        derivation = derive_edition_kind(edition)
        assert derivation.kind == EditionKind.release
        assert derivation.detail == "eups_weekly"

    def test_falls_back_to_ltd_slug_without_any_ref(self) -> None:
        edition = _edition(slug="1.2.3", mode="manual", tracked_refs=None)
        derivation = derive_edition_kind(edition)
        assert derivation.kind == EditionKind.release
        assert derivation.detail == "semver"

    def test_user_rule_overrides_the_builtin_heuristic(self) -> None:
        rules = parse_slug_rewrite_rules(
            [{"type": "semver", "edition_kind": "draft"}]
        )
        edition = _edition(
            slug="15.2.1", mode="git_refs", tracked_refs=["15.2.1"]
        )
        derivation = derive_edition_kind(
            edition, git_ref="15.2.1", rules=rules
        )
        assert derivation.kind == EditionKind.draft
        assert derivation.source == EditionKindSource.rule

    def test_unknown_mode_degrades_to_the_rule_chain(self) -> None:
        """Schema drift must not crash kind derivation.

        ``map_edition_tracking`` — which every caller runs first —
        already raises on an unknown mode, so this only pins the
        defensive branch.
        """
        edition = _edition(
            slug="1.2.3", mode="some_future_mode", tracked_refs=["1.2.3"]
        )
        derivation = derive_edition_kind(edition, git_ref="1.2.3")
        assert derivation.kind == EditionKind.release
        assert derivation.source == EditionKindSource.rule


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


@pytest.mark.parametrize(
    ("ltd_mode", "expected_mode"),
    [
        ("lsst_doc", TrackingMode.lsst_doc),
        ("eups_major_release", TrackingMode.eups_major_release),
        ("eups_weekly_release", TrackingMode.eups_weekly_release),
        ("eups_daily_release", TrackingMode.eups_daily_release),
    ],
)
def test_map_edition_tracking_version_modes_pass_through(
    ltd_mode: str, expected_mode: TrackingMode
) -> None:
    """``lsst_doc`` and ``eups_*`` map onto same-named Docverse modes.

    These are version-based modes whose match logic uses the build's
    git_ref directly (not ``tracking_params``), so the mapper emits an
    empty params dict — the columns are NOT NULL JSONB.
    """
    edition = _edition(mode=ltd_mode, tracked_refs=["main"])
    mode, params = map_edition_tracking(edition)
    assert mode == expected_mode
    assert params == {}


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


def test_map_edition_tracking_manual_uses_build_git_refs() -> None:
    """``manual`` collapses to ``git_ref`` pinned to the build's first ref.

    LTD's `manual` mode has no Docverse equivalent yet (PRD #275 "Out
    of scope"), so the importer pins the edition to whichever ref the
    currently-published build was built from. State preservation of the
    original ``manual`` mode is the service's responsibility; this
    mapper just emits the tracking pair.
    """
    edition = _edition(mode="manual", tracked_refs=["main"])
    build = _build(git_refs=["v1.2.3"])
    mode, params = map_edition_tracking(edition, build=build)
    assert mode == TrackingMode.git_ref
    assert params == {"git_ref": "v1.2.3"}


def test_map_edition_tracking_manual_picks_first_when_multi() -> None:
    edition = _edition(mode="manual", tracked_refs=None)
    build = _build(git_refs=["main", "feature/x"])
    _, params = map_edition_tracking(edition, build=build)
    assert params == {"git_ref": "main"}


def test_map_edition_tracking_manual_without_build_raises() -> None:
    """``manual`` cannot be mapped without the build's git_refs."""
    edition = _edition(mode="manual", tracked_refs=["main"])
    with pytest.raises(ValueError, match="manual"):
        map_edition_tracking(edition)


def test_map_edition_tracking_manual_missing_build_git_refs_raises() -> None:
    edition = _edition(mode="manual", tracked_refs=None)
    build = _build(git_refs=None)
    with pytest.raises(ValueError, match="git_refs"):
        map_edition_tracking(edition, build=build)


def test_map_edition_tracking_manual_empty_build_git_refs_raises() -> None:
    edition = _edition(mode="manual", tracked_refs=None)
    build = _build(git_refs=[])
    with pytest.raises(ValueError, match="git_refs"):
        map_edition_tracking(edition, build=build)


def test_map_edition_tracking_unknown_mode_raises() -> None:
    """Unknown LTD modes (schema drift) surface as ValueError, not silent."""
    edition = _edition(mode="some_future_mode", tracked_refs=["main"])
    with pytest.raises(ValueError, match="some_future_mode"):
        map_edition_tracking(edition)


@pytest.mark.parametrize(
    ("ltd_mode", "tracked_refs", "build_git_refs", "expected"),
    [
        (
            "git_refs",
            ["main"],
            None,
            (TrackingMode.git_ref, {"git_ref": "main"}),
        ),
        ("lsst_doc", ["main"], None, (TrackingMode.lsst_doc, {})),
        (
            "eups_major_release",
            ["main"],
            None,
            (TrackingMode.eups_major_release, {}),
        ),
        (
            "eups_weekly_release",
            ["main"],
            None,
            (TrackingMode.eups_weekly_release, {}),
        ),
        (
            "eups_daily_release",
            ["main"],
            None,
            (TrackingMode.eups_daily_release, {}),
        ),
        (
            "manual",
            None,
            ["v22_0_0"],
            (TrackingMode.git_ref, {"git_ref": "v22_0_0"}),
        ),
    ],
)
def test_map_edition_tracking_table(
    ltd_mode: str,
    tracked_refs: list[str] | None,
    build_git_refs: list[str] | None,
    expected: tuple[TrackingMode, dict[str, Any]],
) -> None:
    """Single table test covering every ``LtdEditionMode`` value."""
    edition = _edition(mode=ltd_mode, tracked_refs=tracked_refs)
    build = _build(git_refs=build_git_refs) if build_git_refs else None
    assert map_edition_tracking(edition, build=build) == expected


class TestDeriveEditionSourcePrefix:
    """``derive_edition_source_prefix`` mirrors LTD's publish layout."""

    def test_versioned_edition_gets_its_published_prefix(self) -> None:
        """LTD publishes an edition's copy at ``<product>/v/<slug>/``."""
        prefix = derive_edition_source_prefix(
            bucket_root_dir="documenteer/builds/33",
            ltd_edition_slug="0.3.0",
        )
        assert prefix == "documenteer/v/0.3.0/"

    def test_trailing_slash_on_the_build_prefix_is_tolerated(self) -> None:
        prefix = derive_edition_source_prefix(
            bucket_root_dir="documenteer/builds/33/",
            ltd_edition_slug="0.3.0",
        )
        assert prefix == "documenteer/v/0.3.0/"

    @pytest.mark.parametrize("ltd_slug", ["main", "__main"])
    def test_default_edition_has_no_versioned_prefix(
        self, ltd_slug: str
    ) -> None:
        """LTD serves the default edition from the product root.

        Both spellings of the default edition's slug are refused, so the
        guard holds whichever one the v1 API reports.
        """
        assert (
            derive_edition_source_prefix(
                bucket_root_dir="documenteer/builds/33",
                ltd_edition_slug=ltd_slug,
            )
            is None
        )

    @pytest.mark.parametrize(
        "bucket_root_dir",
        ["documenteer", "documenteer/33", "documenteer/v/0.3.0", ""],
    )
    def test_unrecognized_build_layout_yields_no_prefix(
        self, bucket_root_dir: str
    ) -> None:
        """Only ``<product>/builds/<slug>`` locates a product root."""
        assert (
            derive_edition_source_prefix(
                bucket_root_dir=bucket_root_dir, ltd_edition_slug="0.3.0"
            )
            is None
        )
