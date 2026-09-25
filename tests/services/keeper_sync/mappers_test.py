"""Tests for ``docverse.services.keeper_sync.mappers``.

Pure-function table tests with no DB or HTTP. Intended to lock the
mapping rules from PRD #275 ("Data model and state tracking — Mapping
rules") so a future change has to update both the rule and the test.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import HttpUrl

from docverse.models import EditionKind, TrackingMode
from docverse_server.domain.slug import parse_slug_rewrite_rules
from docverse_server.exceptions import KeeperSyncGitRefUnresolvableError
from docverse_server.services.keeper_sync import service as keeper_sync_service
from docverse_server.services.keeper_sync.mappers import (
    KindDerivationSource,
    TrackingDerivationSource,
    derive_edition_dates,
    derive_edition_kind,
    derive_edition_slug,
    derive_edition_source_prefix,
    derive_synced_build_git_ref,
    derive_tracking_source,
    map_edition_tracking,
    tracking_reads_live_refs,
)
from docverse_server.storage.ltd import LtdBuild, LtdEdition

_LTD_FIXTURES_DIR = Path(__file__).parents[2] / "storage" / "ltd" / "fixtures"


def _load_ltd_edition(name: str) -> LtdEdition:
    """Validate a captured ``keeper.lsst.codes`` edition payload."""
    return LtdEdition.model_validate(
        json.loads((_LTD_FIXTURES_DIR / name).read_text())
    )


def _edition(
    *,
    slug: str = "main",
    mode: str = "git_refs",
    tracked_refs: list[str] | None = None,
    date_created: datetime | None = None,
    date_rebuilt: datetime | None = None,
) -> LtdEdition:
    return LtdEdition(
        self_url=HttpUrl("https://keeper.lsst.codes/editions/1"),
        product_url=HttpUrl("https://keeper.lsst.codes/products/p"),
        published_url=HttpUrl("https://example.com/"),
        slug=slug,
        title=slug,
        date_created=date_created or datetime(2026, 4, 1, tzinfo=UTC),
        date_rebuilt=date_rebuilt,
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
        assert derivation.source == KindDerivationSource.rule
        assert derivation.detail == "semver"

    def test_main_edition_is_main(self) -> None:
        edition = _edition(slug="main", tracked_refs=["main"])
        derivation = derive_edition_kind(edition, git_ref="main")
        assert derivation.kind == EditionKind.main
        assert derivation.source == KindDerivationSource.ltd_main

    def test_git_refs_ticket_branch_is_a_draft(self) -> None:
        edition = _edition(
            slug="DM-54112",
            mode="git_refs",
            tracked_refs=["tickets/DM-54112"],
        )
        derivation = derive_edition_kind(edition, git_ref="tickets/DM-54112")
        assert derivation.kind == EditionKind.draft
        assert derivation.source == KindDerivationSource.fallback
        assert derivation.detail is None

    @pytest.mark.parametrize(
        "ltd_mode",
        ["lsst_doc", "eups_major_release", "eups_weekly_release"],
    )
    def test_version_modes_are_confirmed_by_the_ref(
        self, ltd_mode: str
    ) -> None:
        """A version mode narrows; the ref has to confirm the release.

        LTD's version modes only describe which grammar an edition
        *prefers* — ``lsst_doc`` explicitly publishes ``main`` builds
        until a version tag exists — so the mode alone cannot certify a
        release. A ticket-shaped ref under any of them stays ``draft``.
        """
        edition = _edition(
            slug="u-jsick-feature",
            mode=ltd_mode,
            tracked_refs=["tickets/DM-1"],
        )
        derivation = derive_edition_kind(edition, git_ref="tickets/DM-1")
        assert derivation.kind == EditionKind.draft
        assert derivation.source == KindDerivationSource.fallback

    @pytest.mark.parametrize(
        ("ltd_mode", "slug", "expected_rule"),
        [
            ("lsst_doc", "v1.2", "lsst_doc"),
            ("eups_major_release", "v27_0", "eups_major"),
            ("eups_weekly_release", "w_2026_10", "eups_weekly"),
        ],
    )
    def test_version_modes_confirmed_by_the_slug(
        self, ltd_mode: str, slug: str, expected_rule: str
    ) -> None:
        """LTD sends ``tracked_refs: null`` for its version modes.

        With no ref to inspect, the LTD slug — which for these editions
        *is* the version string — is what the heuristics confirm.
        """
        edition = _edition(slug=slug, mode=ltd_mode, tracked_refs=None)
        derivation = derive_edition_kind(edition)
        assert derivation.kind == EditionKind.release
        assert derivation.source == KindDerivationSource.rule
        assert derivation.detail == expected_rule

    def test_eups_daily_release_is_always_a_draft(self) -> None:
        """Dailies stay drafts so they keep ageing out under lifecycle."""
        edition = _edition(
            slug="d_2026_08_10",
            mode="eups_daily_release",
            tracked_refs=None,
        )
        derivation = derive_edition_kind(edition)
        assert derivation.kind == EditionKind.draft
        assert derivation.source == KindDerivationSource.ltd_mode
        assert derivation.detail == "eups_daily_release"

    def test_branch_tracking_lsst_doc_fixture_is_a_draft(self) -> None:
        """The captured mis-moded LTD edition imports as a draft.

        ``edition_branch_lsst_doc.json`` is an LTD ``lsst_doc`` edition
        pointed at ``u/jsick/feature``. Before the ref confirmation it
        imported as ``release``: a long CDN cache profile and permanent
        lifecycle exemption for a ticket branch.
        """
        edition = _load_ltd_edition("edition_branch_lsst_doc.json")
        derivation = derive_edition_kind(edition)
        assert derivation.kind == EditionKind.draft

    def test_manual_uses_the_supplied_build_ref(self) -> None:
        """``manual`` editions classify on the ref their tracking pins."""
        edition = _edition(slug="current", mode="manual", tracked_refs=None)
        derivation = derive_edition_kind(edition, git_ref="v22_0")
        assert derivation.kind == EditionKind.release
        assert derivation.source == KindDerivationSource.rule
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
        assert derivation.source == KindDerivationSource.rule

    def test_ignore_rule_does_not_draft_a_version_ref(self) -> None:
        """An ignore rule gating auto-creation must not demote imports.

        Orgs use ``{"type": "ignore", "glob": "v*"}`` to stop the native
        path auto-creating a per-tag edition. Keeper-sync's editions
        already exist, so the ignore match must fall through to the
        built-in semver rule rather than classifying them ``draft`` and
        feeding them back to the preemptive lifecycle pass.
        """
        rules = parse_slug_rewrite_rules([{"type": "ignore", "glob": "v*"}])
        edition = _edition(
            slug="v15.2.1", mode="git_refs", tracked_refs=["v15.2.1"]
        )
        derivation = derive_edition_kind(
            edition, git_ref="v15.2.1", rules=rules
        )
        assert derivation.kind == EditionKind.release
        assert derivation.source == KindDerivationSource.rule
        assert derivation.detail == "semver"

    def test_ignore_rule_leaves_non_version_refs_drafts(self) -> None:
        rules = parse_slug_rewrite_rules(
            [{"type": "ignore", "glob": "tickets/*"}]
        )
        edition = _edition(
            slug="DM-54112",
            mode="git_refs",
            tracked_refs=["tickets/DM-54112"],
        )
        derivation = derive_edition_kind(
            edition, git_ref="tickets/DM-54112", rules=rules
        )
        assert derivation.kind == EditionKind.draft
        assert derivation.source == KindDerivationSource.fallback

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
        assert derivation.source == KindDerivationSource.rule


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


@pytest.mark.parametrize("git_refs", [None, []])
def test_map_edition_tracking_manual_build_without_git_refs_is_unresolvable(
    git_refs: list[str] | None,
) -> None:
    """A ``manual`` edition whose published build names no ref is typed.

    The build's ``git_refs`` is fixed at upload, so this is as permanent
    as :func:`derive_synced_build_git_ref`'s unresolvable case — and it
    has to carry the same type, or ``sync_project`` would count it as a
    systemic-outage candidate and fail the job on every tier tick.
    """
    edition = _edition(slug="current", mode="manual", tracked_refs=None)
    build = _build(git_refs=git_refs)
    with pytest.raises(KeeperSyncGitRefUnresolvableError) as exc_info:
        map_edition_tracking(edition, build=build)
    assert exc_info.value.ltd_edition_slug == "current"
    assert exc_info.value.ltd_build_id == 42
    assert "current" in str(exc_info.value)
    assert "42" in str(exc_info.value)
    assert "manual" in str(exc_info.value)


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


class TestMainFollowsDefaultBranch:
    """LTD's ``main`` edition follows the default branch once its ref is gone.

    A default-branch rename (``master`` → ``main``) leaves LTD still
    naming ``master`` for its ``main`` edition, and keeper-sync
    realigns ``__main`` with LTD on every visit — so without this arm it
    would revert the ``__main`` the ``repository.edited`` webhook just
    converged (PRD #721). The rule is the one ``DefaultBranchService``
    applies: LTD's ref must be *gone* from a live set that was actually
    fetched, and the project's default branch must be known.
    """

    def test_gone_ref_maps_onto_the_default_branch(self) -> None:
        edition = _edition(slug="main", tracked_refs=["master"])
        mode, params = map_edition_tracking(
            edition,
            default_branch="main",
            live_refs=frozenset({"main", "v1.0"}),
        )
        assert mode == TrackingMode.git_ref
        assert params == {"git_ref": "main"}

    @pytest.mark.parametrize(
        ("default_branch", "live_refs"),
        [
            pytest.param(
                "main", frozenset({"main", "master"}), id="ref-still-live"
            ),
            pytest.param("main", None, id="live-refs-unavailable"),
            pytest.param(None, frozenset({"main"}), id="default-branch-null"),
        ],
    )
    def test_ltd_ref_stands_without_evidence_it_is_gone(
        self, default_branch: str | None, live_refs: frozenset[str] | None
    ) -> None:
        """A live ref, an unfetched live set, or a ``NULL`` column keep LTD.

        A ``master`` that still exists is deliberate non-default
        tracking; a failed or unconfigured ref fetch is not evidence of
        anything; and a project whose default branch Docverse has not
        learned has nothing to follow.
        """
        edition = _edition(slug="main", tracked_refs=["master"])
        mode, params = map_edition_tracking(
            edition, default_branch=default_branch, live_refs=live_refs
        )
        assert mode == TrackingMode.git_ref
        assert params == {"git_ref": "master"}

    def test_non_main_editions_keep_their_gone_ref(self) -> None:
        """Only ``__main`` follows the default branch.

        A draft tracking a deleted branch is ``ref_deleted``'s to
        retire, not a candidate for rewriting.
        """
        edition = _edition(slug="master", tracked_refs=["master"])
        _, params = map_edition_tracking(
            edition, default_branch="main", live_refs=frozenset({"main"})
        )
        assert params == {"git_ref": "master"}

    @pytest.mark.parametrize(
        ("ltd_mode", "build_git_refs", "expected"),
        [
            pytest.param(
                "lsst_doc", None, (TrackingMode.lsst_doc, {}), id="lsst_doc"
            ),
            pytest.param(
                "manual",
                ["master"],
                (TrackingMode.git_ref, {"git_ref": "master"}),
                id="manual",
            ),
        ],
    )
    def test_non_git_refs_modes_are_unaffected(
        self,
        ltd_mode: str,
        build_git_refs: list[str] | None,
        expected: tuple[TrackingMode, dict[str, Any]],
    ) -> None:
        """The arm belongs to LTD's ``git_refs`` mode alone.

        ``lsst_doc`` carries no ref to rewrite — it follows the default
        branch through edition tracking instead — and a ``manual``
        edition is pinned to its published build by design.
        """
        edition = _edition(slug="main", mode=ltd_mode, tracked_refs=None)
        build = _build(git_refs=build_git_refs) if build_git_refs else None
        assert (
            map_edition_tracking(
                edition,
                build=build,
                default_branch="main",
                live_refs=frozenset({"main"}),
            )
            == expected
        )


class TestDeriveTrackingSource:
    """``derive_tracking_source`` names the arm ``map_edition_tracking`` took.

    Carried on keeper-sync's derivation log so an operator can tell a
    ``__main`` that follows the project's default branch from one that
    mirrors LTD verbatim.
    """

    def test_gone_ltd_main_ref_is_the_default_branch_arm(self) -> None:
        edition = _edition(slug="main", tracked_refs=["master"])
        source = derive_tracking_source(
            edition, default_branch="main", live_refs=frozenset({"main"})
        )
        assert source == TrackingDerivationSource.default_branch

    def test_standing_ltd_ref_is_the_ltd_arm(self) -> None:
        edition = _edition(slug="main", tracked_refs=["master"])
        source = derive_tracking_source(
            edition,
            default_branch="main",
            live_refs=frozenset({"main", "master"}),
        )
        assert source == TrackingDerivationSource.ltd


@pytest.mark.parametrize(
    ("slug", "mode", "tracked_refs", "default_branch", "expected"),
    [
        pytest.param(
            "main", "git_refs", ["master"], "main", True, id="diverged"
        ),
        pytest.param("main", "git_refs", ["main"], "main", False, id="agrees"),
        pytest.param(
            "main", "git_refs", ["master"], None, False, id="default-unknown"
        ),
        pytest.param(
            "master", "git_refs", ["master"], "main", False, id="not-ltd-main"
        ),
        pytest.param("main", "lsst_doc", None, "main", False, id="lsst_doc"),
    ],
)
def test_tracking_reads_live_refs(
    slug: str,
    mode: str,
    tracked_refs: list[str] | None,
    default_branch: str | None,
    expected: bool,  # noqa: FBT001
) -> None:
    """Only a diverged LTD ``main`` needs the live ref set to be mapped.

    ``sync_project`` fetches the set on the tracking's behalf only for
    these editions, so every other project syncs without the GitHub
    round-trip.
    """
    edition = _edition(slug=slug, mode=mode, tracked_refs=tracked_refs)
    assert (
        tracking_reads_live_refs(edition, default_branch=default_branch)
        is expected
    )


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


class TestDeriveSyncedBuildGitRef:
    """The synced build's ``git_ref`` is the ref its bytes were built from."""

    def test_tracked_ref_wins_over_the_build_ref(self) -> None:
        """A ``git_refs`` edition keeps naming its build after its ref."""
        edition = _edition(mode="git_refs", tracked_refs=["main"])
        build = _build(git_refs=["v22_0_0", "main"])
        assert derive_synced_build_git_ref(edition, build) == "main"

    def test_falls_back_to_the_published_build_ref(self) -> None:
        """``lsst_doc`` editions carry no ``tracked_refs`` (#682).

        LTD's ``lsst_doc`` mode follows the newest semver tag with a
        ``main`` / ``master`` fallback and never fills ``tracked_refs``,
        so the only record of what the published bytes were built from
        is the build's own ``git_refs``.
        """
        edition = _edition(mode="lsst_doc", tracked_refs=None)
        build = _build(git_refs=["master"])
        assert derive_synced_build_git_ref(edition, build) == "master"

    def test_raises_when_neither_side_names_a_ref(self) -> None:
        edition = _edition(slug="main", mode="lsst_doc", tracked_refs=None)
        build = _build(git_refs=None)
        with pytest.raises(KeeperSyncGitRefUnresolvableError) as exc_info:
            derive_synced_build_git_ref(edition, build)
        assert exc_info.value.ltd_edition_slug == "main"
        assert exc_info.value.ltd_build_id == 42
        assert "main" in str(exc_info.value)
        assert "42" in str(exc_info.value)


class TestDeriveEditionDates:
    """A synced edition's clock is LTD's, not the import moment (PRD #706)."""

    def test_date_rebuilt_is_the_update_time(self) -> None:
        """LTD's ``date_rebuilt`` is its "content last moved" analogue."""
        created = datetime(2019, 3, 4, 5, 6, 7, tzinfo=UTC)
        rebuilt = datetime(2024, 8, 9, 10, 11, 12, tzinfo=UTC)
        edition = _edition(date_created=created, date_rebuilt=rebuilt)
        assert derive_edition_dates(edition) == (created, rebuilt)

    def test_never_rebuilt_falls_back_to_date_created(self) -> None:
        """An edition LTD never rebuilt was last updated when created."""
        created = datetime(2019, 3, 4, 5, 6, 7, tzinfo=UTC)
        edition = _edition(date_created=created, date_rebuilt=None)
        assert derive_edition_dates(edition) == (created, created)

    def test_proactive_transient_edition_uses_the_helper(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The proactive lifecycle stub reads its clock from the helper.

        ``draft_inactivity`` judges a not-yet-imported edition on the
        transient's ``date_updated`` and an imported one on the stamped
        row's, so the two must come from one mapping or a draft could be
        kept before import and reaped after it on the same LTD data.
        """
        stamped = (
            datetime(2001, 1, 1, tzinfo=UTC),
            datetime(2002, 2, 2, tzinfo=UTC),
        )
        seen: list[LtdEdition] = []

        def fake_derive(ltd_edition: LtdEdition) -> tuple[datetime, datetime]:
            seen.append(ltd_edition)
            return stamped

        monkeypatch.setattr(
            keeper_sync_service, "derive_edition_dates", fake_derive
        )
        edition = _edition(
            slug="u-jsick-feature",
            tracked_refs=["u/jsick/feature"],
            date_rebuilt=datetime(2024, 8, 9, tzinfo=UTC),
        )

        transient = keeper_sync_service._transient_edition_from_ltd(
            ltd_edition=edition, project_id=1
        )

        assert seen == [edition]
        assert transient is not None
        assert (transient.date_created, transient.date_updated) == stamped
