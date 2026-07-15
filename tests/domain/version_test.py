"""Tests for docverse.domain.version."""

from __future__ import annotations

import pytest

from docverse.client.models import TrackingMode
from docverse.domain.version import (
    EupsDailyVersion,
    EupsMajorVersion,
    EupsWeeklyVersion,
    LsstDocVersion,
    SemverVersion,
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


# ── EupsMajorVersion ──────────────────────────────────────────────────────


class TestEupsMajorVersion:
    @pytest.mark.parametrize(
        ("ref", "expected"),
        [
            ("v12_0", EupsMajorVersion(12, 0)),
            ("12.0", EupsMajorVersion(12, 0)),
            ("v1_2", EupsMajorVersion(1, 2)),
            ("0.0", EupsMajorVersion(0, 0)),
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


# ── EupsWeeklyVersion ─────────────────────────────────────────────────────


class TestEupsWeeklyVersion:
    @pytest.mark.parametrize(
        ("ref", "expected"),
        [
            ("w_2024_05", EupsWeeklyVersion(2024, 5)),
            ("w.2024.05", EupsWeeklyVersion(2024, 5)),
            ("w_2023_52", EupsWeeklyVersion(2023, 52)),
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

    def test_git_ref_returns_none(self) -> None:
        assert parse_version_for_mode("main", TrackingMode.git_ref) is None

    def test_unparseable_returns_none(self) -> None:
        assert (
            parse_version_for_mode("main", TrackingMode.semver_release) is None
        )
