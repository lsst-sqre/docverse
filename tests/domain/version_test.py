"""Tests for docverse.domain.version."""

from __future__ import annotations

import pytest

from docverse.models import TrackingMode
from docverse_server.domain.version import (
    EupsDailyVersion,
    EupsMajorVersion,
    EupsReleaseVersion,
    EupsWeeklyVersion,
    LsstDocVersion,
    SemverVersion,
    accepts_version_advance,
    parse_stable_semver,
    parse_version_for_mode,
)

# ── SemverVersion ──────────────────────────────────────────────────────────


class TestSemverVersion:
    """Parsing and comparison for SemverVersion."""

    @pytest.mark.parametrize(
        ("ref", "expected"),
        [
            ("1.2.3", SemverVersion(1, 2, 3)),
            ("v1.2.3", SemverVersion(1, 2, 3)),
            ("0.0.1", SemverVersion(0, 0, 1)),
            ("v10.20.30", SemverVersion(10, 20, 30)),
            ("1.0.0-rc.1", SemverVersion(1, 0, 0, "rc.1")),
            ("v2.1.0-alpha", SemverVersion(2, 1, 0, "alpha")),
            ("1.0.0-beta.2", SemverVersion(1, 0, 0, "beta.2")),
            ("1.0.0-rc-1", SemverVersion(1, 0, 0, "rc-1")),
            ("v1.0.0-alpha-1", SemverVersion(1, 0, 0, "alpha-1")),
        ],
    )
    def test_parse_valid(self, ref: str, expected: SemverVersion) -> None:
        assert SemverVersion.parse(ref) == expected

    @pytest.mark.parametrize(
        "ref",
        [
            "main",
            "1.2",
            "v1.2",
            "1.2.3.4",
            "abc",
            "w_2024_05",
            "v12_0",
        ],
    )
    def test_parse_invalid(self, ref: str) -> None:
        assert SemverVersion.parse(ref) is None

    def test_ordering_numeric(self) -> None:
        assert SemverVersion(1, 0, 0) < SemverVersion(2, 0, 0)
        assert SemverVersion(1, 0, 0) < SemverVersion(1, 1, 0)
        assert SemverVersion(1, 1, 0) < SemverVersion(1, 1, 1)

    def test_ordering_prerelease_vs_release(self) -> None:
        # Release > prerelease for same numeric version
        assert SemverVersion(1, 0, 0, "rc.1") < SemverVersion(1, 0, 0)
        assert not SemverVersion(1, 0, 0) < SemverVersion(1, 0, 0, "rc.1")

    def test_ordering_prerelease_lexicographic(self) -> None:
        assert SemverVersion(1, 0, 0, "alpha") < SemverVersion(1, 0, 0, "beta")
        assert SemverVersion(1, 0, 0, "rc.1") < SemverVersion(1, 0, 0, "rc.2")

    def test_equality(self) -> None:
        assert SemverVersion(1, 0, 0) == SemverVersion(1, 0, 0)
        assert SemverVersion(1, 0, 0) != SemverVersion(1, 0, 1)
        assert SemverVersion(1, 0, 0, "rc.1") == SemverVersion(1, 0, 0, "rc.1")
        assert SemverVersion(1, 0, 0) != SemverVersion(1, 0, 0, "rc.1")

    def test_ge(self) -> None:
        assert SemverVersion(2, 0, 0) >= SemverVersion(1, 0, 0)
        assert SemverVersion(1, 0, 0) >= SemverVersion(1, 0, 0)


class TestParseStableSemver:
    """The stable-release grammar both aggregate paths gate on."""

    @pytest.mark.parametrize(
        ("ref", "expected"),
        [
            ("1.2.3", SemverVersion(1, 2, 3)),
            ("v16.0.0", SemverVersion(16, 0, 0)),
            ("0.0.1", SemverVersion(0, 0, 1)),
        ],
    )
    def test_stable(self, ref: str, expected: SemverVersion) -> None:
        assert parse_stable_semver(ref) == expected

    @pytest.mark.parametrize(
        "ref",
        [
            "1.0.0-rc.1",
            "v2.1.0-alpha",
            "main",
            "v12_0",
            "w_2026_10",
        ],
    )
    def test_not_a_stable_release(self, ref: str) -> None:
        assert parse_stable_semver(ref) is None

    def test_none_ref(self) -> None:
        """Callers whose tracking params carry no ref get ``None``."""
        assert parse_stable_semver(None) is None


# ── EupsMajorVersion ──────────────────────────────────────────────────────


class TestEupsMajorVersion:
    @pytest.mark.parametrize(
        ("ref", "expected"),
        [
            ("v12_0", EupsMajorVersion(12, 0)),
            ("12.0", EupsMajorVersion(12, 0)),
            ("v1_2", EupsMajorVersion(1, 2)),
            ("0.0", EupsMajorVersion(0, 0)),
            ("13-0", EupsMajorVersion(13, 0)),  # legacy LTD slug
            ("v16-0", EupsMajorVersion(16, 0)),
        ],
    )
    def test_parse_valid(self, ref: str, expected: EupsMajorVersion) -> None:
        assert EupsMajorVersion.parse(ref) == expected

    @pytest.mark.parametrize(
        "ref",
        [
            "1.2.3",  # three components → not EUPS major
            "v1.2.3",
            "main",
            "w_2024_05",
            "d_2024_01_15",
        ],
    )
    def test_parse_invalid(self, ref: str) -> None:
        assert EupsMajorVersion.parse(ref) is None

    def test_ordering(self) -> None:
        assert EupsMajorVersion(11, 0) < EupsMajorVersion(12, 0)
        assert EupsMajorVersion(12, 0) < EupsMajorVersion(12, 1)

    def test_equality(self) -> None:
        assert EupsMajorVersion(12, 0) == EupsMajorVersion(12, 0)


# ── EupsReleaseVersion ────────────────────────────────────────────────────


class TestEupsReleaseVersion:
    """Parsing and ordering for EUPS release tags (``v30_0_12``)."""

    @pytest.mark.parametrize(
        ("ref", "expected"),
        [
            ("v30_0_12", EupsReleaseVersion(30, 0, 12)),
            ("v17_0_1", EupsReleaseVersion(17, 0, 1)),
            ("v17_0", EupsReleaseVersion(17, 0, 0)),
            ("v30.0.12", EupsReleaseVersion(30, 0, 12)),
            ("13-0", EupsReleaseVersion(13, 0, 0)),
            ("v16-0", EupsReleaseVersion(16, 0, 0)),
            ("30_0_12", EupsReleaseVersion(30, 0, 12)),
            ("v30_0_12_rc3", EupsReleaseVersion(30, 0, 12, rc=3)),
            ("v15_0_rc1", EupsReleaseVersion(15, 0, 0, rc=1)),
        ],
    )
    def test_parse_valid(self, ref: str, expected: EupsReleaseVersion) -> None:
        assert EupsReleaseVersion.parse(ref) == expected

    @pytest.mark.parametrize(
        "ref",
        [
            "w_2026_45",  # weekly
            "w-2018-06",
            "d_2026_11_04",  # daily
            "30",  # one component
            "v30",
            "v30_0_12_rc",  # rc without a number
            "v30_0_12_beta1",
            "v30_0_12_1",  # four numeric components
            "main",
            "tickets/DM-56964",
            "v30_0_12-rc.3",
        ],
    )
    def test_parse_invalid(self, ref: str) -> None:
        assert EupsReleaseVersion.parse(ref) is None

    def test_ordering(self) -> None:
        """Two-component releases sort as patch 0 below later patches."""
        v17_0 = EupsReleaseVersion.parse("v17_0")
        v17_0_1 = EupsReleaseVersion.parse("v17_0_1")
        v30_0_12 = EupsReleaseVersion.parse("v30_0_12")
        assert v17_0 is not None
        assert v17_0_1 is not None
        assert v30_0_12 is not None
        assert v17_0 < v17_0_1 < v30_0_12

    def test_ordering_is_numeric(self) -> None:
        assert EupsReleaseVersion(9, 0, 0) < EupsReleaseVersion(13, 0, 0)
        assert EupsReleaseVersion(30, 0, 9) < EupsReleaseVersion(30, 0, 11)

    def test_release_candidate_orders_below_final(self) -> None:
        rc = EupsReleaseVersion.parse("v30_0_12_rc3")
        final = EupsReleaseVersion.parse("v30_0_12")
        previous = EupsReleaseVersion.parse("v30_0_11")
        assert rc is not None
        assert final is not None
        assert previous is not None
        assert previous < rc < final

    def test_release_candidates_order_numerically(self) -> None:
        assert EupsReleaseVersion(30, 0, 12, rc=9) < EupsReleaseVersion(
            30, 0, 12, rc=10
        )

    def test_is_final(self) -> None:
        final = EupsReleaseVersion.parse("v30_0_12")
        rc = EupsReleaseVersion.parse("v30_0_12_rc3")
        assert final is not None
        assert rc is not None
        assert final.is_final
        assert not rc.is_final

    def test_separators_do_not_affect_equality(self) -> None:
        dotted = EupsReleaseVersion.parse("v30.0.12")
        assert dotted == EupsReleaseVersion.parse("v30_0_12")


# ── EupsWeeklyVersion ─────────────────────────────────────────────────────


class TestEupsWeeklyVersion:
    @pytest.mark.parametrize(
        ("ref", "expected"),
        [
            ("w_2024_05", EupsWeeklyVersion(2024, 5)),
            ("w.2024.05", EupsWeeklyVersion(2024, 5)),
            ("w_2023_52", EupsWeeklyVersion(2023, 52)),
            ("w-2018-06", EupsWeeklyVersion(2018, 6)),  # legacy LTD slug
        ],
    )
    def test_parse_valid(self, ref: str, expected: EupsWeeklyVersion) -> None:
        assert EupsWeeklyVersion.parse(ref) == expected

    @pytest.mark.parametrize(
        "ref",
        [
            "main",
            "v1.0.0",
            "d_2024_01_15",
            "w2024_05",  # missing separator after w
        ],
    )
    def test_parse_invalid(self, ref: str) -> None:
        assert EupsWeeklyVersion.parse(ref) is None

    def test_ordering(self) -> None:
        assert EupsWeeklyVersion(2024, 4) < EupsWeeklyVersion(2024, 5)
        assert EupsWeeklyVersion(2023, 52) < EupsWeeklyVersion(2024, 1)


# ── EupsDailyVersion ──────────────────────────────────────────────────────


class TestEupsDailyVersion:
    @pytest.mark.parametrize(
        ("ref", "expected"),
        [
            ("d_2024_01_15", EupsDailyVersion(2024, 1, 15)),
            ("d.2024.01.15", EupsDailyVersion(2024, 1, 15)),
            ("d-2018-06-01", EupsDailyVersion(2018, 6, 1)),
        ],
    )
    def test_parse_valid(self, ref: str, expected: EupsDailyVersion) -> None:
        assert EupsDailyVersion.parse(ref) == expected

    @pytest.mark.parametrize(
        "ref",
        [
            "main",
            "v1.0.0",
            "w_2024_05",
            "d2024_01_15",  # missing separator
        ],
    )
    def test_parse_invalid(self, ref: str) -> None:
        assert EupsDailyVersion.parse(ref) is None

    def test_ordering(self) -> None:
        assert EupsDailyVersion(2024, 1, 14) < EupsDailyVersion(2024, 1, 15)
        assert EupsDailyVersion(2023, 12, 31) < EupsDailyVersion(2024, 1, 1)


# ── LsstDocVersion ────────────────────────────────────────────────────────


class TestLsstDocVersion:
    @pytest.mark.parametrize(
        ("ref", "expected"),
        [
            ("v1.0", LsstDocVersion(1, 0, 0)),
            ("1.0", LsstDocVersion(1, 0, 0)),
            ("v1.0.1", LsstDocVersion(1, 0, 1)),
            ("1.2.3", LsstDocVersion(1, 2, 3)),
        ],
    )
    def test_parse_valid(self, ref: str, expected: LsstDocVersion) -> None:
        assert LsstDocVersion.parse(ref) == expected

    @pytest.mark.parametrize(
        "ref",
        [
            "main",
            "w_2024_05",
            "abc",
        ],
    )
    def test_parse_invalid(self, ref: str) -> None:
        assert LsstDocVersion.parse(ref) is None

    def test_ordering(self) -> None:
        assert LsstDocVersion(0, 9, 0) < LsstDocVersion(1, 0, 0)
        assert LsstDocVersion(1, 0, 0) < LsstDocVersion(1, 0, 1)
        assert LsstDocVersion(1, 0, 0) < LsstDocVersion(1, 1, 0)

    def test_two_component_equals_three_with_zero_patch(self) -> None:
        assert LsstDocVersion(1, 0, 0) == LsstDocVersion(1, 0, 0)


# ── parse_version_for_mode ────────────────────────────────────────────────


class TestParseVersionForMode:
    def test_semver_release(self) -> None:
        v = parse_version_for_mode("v1.2.3", TrackingMode.semver_release)
        assert isinstance(v, SemverVersion)
        assert v == SemverVersion(1, 2, 3)

    def test_semver_major(self) -> None:
        v = parse_version_for_mode("v2.0.0", TrackingMode.semver_major)
        assert isinstance(v, SemverVersion)

    def test_semver_minor(self) -> None:
        v = parse_version_for_mode("v2.1.0", TrackingMode.semver_minor)
        assert isinstance(v, SemverVersion)

    def test_eups_major(self) -> None:
        v = parse_version_for_mode("v12_0", TrackingMode.eups_major_release)
        assert isinstance(v, EupsMajorVersion)

    def test_eups_weekly(self) -> None:
        v = parse_version_for_mode(
            "w_2024_05", TrackingMode.eups_weekly_release
        )
        assert isinstance(v, EupsWeeklyVersion)

    def test_eups_daily(self) -> None:
        v = parse_version_for_mode(
            "d_2024_01_15", TrackingMode.eups_daily_release
        )
        assert isinstance(v, EupsDailyVersion)

    def test_lsst_doc(self) -> None:
        v = parse_version_for_mode("v1.0", TrackingMode.lsst_doc)
        assert isinstance(v, LsstDocVersion)

    def test_eups_release(self) -> None:
        v = parse_version_for_mode("v30_0_12", TrackingMode.eups_release)
        assert v == EupsReleaseVersion(30, 0, 12)

    def test_eups_release_candidate(self) -> None:
        """Candidates parse; matching, not parsing, keeps them out."""
        v = parse_version_for_mode("v30_0_12_rc3", TrackingMode.eups_release)
        assert v == EupsReleaseVersion(30, 0, 12, rc=3)

    def test_eups_release_rejects_weekly(self) -> None:
        assert (
            parse_version_for_mode("w_2026_45", TrackingMode.eups_release)
            is None
        )

    def test_git_ref_returns_none(self) -> None:
        assert parse_version_for_mode("main", TrackingMode.git_ref) is None

    def test_unparseable_returns_none(self) -> None:
        assert (
            parse_version_for_mode("main", TrackingMode.semver_release) is None
        )


# ── accepts_version_advance ────────────────────────────────────────────────


class TestAcceptsVersionAdvance:
    """The version guard shared by native tracking and keeper-sync."""

    def test_no_current_build(self) -> None:
        """An edition with no current build accepts any version."""
        assert accepts_version_advance(
            candidate=SemverVersion(1, 0, 0),
            current_build_id=None,
            current_build_git_ref=None,
            mode=TrackingMode.semver_major,
        )

    def test_current_build_without_ref(self) -> None:
        """A current build id with no ref is treated as no current build."""
        assert accepts_version_advance(
            candidate=SemverVersion(1, 0, 0),
            current_build_id=7,
            current_build_git_ref=None,
            mode=TrackingMode.semver_major,
        )

    def test_unparseable_current_ref(self) -> None:
        """A leftover non-version ref like ``main`` is always overtaken."""
        assert accepts_version_advance(
            candidate=SemverVersion(1, 0, 0),
            current_build_id=7,
            current_build_git_ref="main",
            mode=TrackingMode.semver_major,
        )

    def test_greater_candidate(self) -> None:
        assert accepts_version_advance(
            candidate=SemverVersion(15, 3, 0),
            current_build_id=7,
            current_build_git_ref="15.2.1",
            mode=TrackingMode.semver_major,
        )

    def test_equal_candidate(self) -> None:
        """An equal version is accepted, so a re-sync can repoint."""
        assert accepts_version_advance(
            candidate=SemverVersion(15, 2, 1),
            current_build_id=7,
            current_build_git_ref="15.2.1",
            mode=TrackingMode.semver_major,
        )

    def test_lesser_candidate(self) -> None:
        """An out-of-order older release must not move the pointer."""
        assert not accepts_version_advance(
            candidate=SemverVersion(15, 2, 0),
            current_build_id=7,
            current_build_git_ref="15.2.1",
            mode=TrackingMode.semver_major,
        )

    def test_eups_release_overtakes_a_branch_build(self) -> None:
        """A ``main`` build on an ``eups_release`` edition is replaced."""
        assert accepts_version_advance(
            candidate=EupsReleaseVersion(30, 0, 11),
            current_build_id=7,
            current_build_git_ref="main",
            mode=TrackingMode.eups_release,
        )

    def test_eups_release_refuses_an_older_release(self) -> None:
        assert not accepts_version_advance(
            candidate=EupsReleaseVersion(30, 0, 11),
            current_build_id=7,
            current_build_git_ref="v30_0_12",
            mode=TrackingMode.eups_release,
        )

    def test_uses_the_mode_grammar(self) -> None:
        """The current ref is parsed with the edition's own grammar."""
        assert not accepts_version_advance(
            candidate=EupsWeeklyVersion(2024, 4),
            current_build_id=7,
            current_build_git_ref="w_2024_05",
            mode=TrackingMode.eups_weekly_release,
        )
