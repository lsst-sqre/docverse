"""Version parsing for edition tracking modes.

This module is pure logic with no database or I/O dependencies.  Each
version type is a frozen dataclass with a ``parse`` classmethod and
comparison operators via ``functools.total_ordering``.
"""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Self

from docverse.client.models import TrackingMode

__all__ = [
    "EupsDailyVersion",
    "EupsMajorVersion",
    "EupsWeeklyVersion",
    "LsstDocVersion",
    "SemverVersion",
    "parse_version_for_mode",
]

# ---------------------------------------------------------------------------
# Semver
# ---------------------------------------------------------------------------

_SEMVER_RE = re.compile(
    r"^v?(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<pre>[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*))?$"
)


@functools.total_ordering
@dataclass(frozen=True, slots=True)
class SemverVersion:
    """A parsed semantic version."""

    major: int
    minor: int
    patch: int
    prerelease: str | None = None

    @classmethod
    def parse(cls, git_ref: str) -> Self | None:
        """Parse a git ref as a semver version.

        Returns ``None`` if the ref does not match the expected pattern.
        """
        m = _SEMVER_RE.match(git_ref)
        if m is None:
            return None
        return cls(
            major=int(m.group("major")),
            minor=int(m.group("minor")),
            patch=int(m.group("patch")),
            prerelease=m.group("pre"),
        )

    def __eq__(self, other: object) -> bool:  # noqa: D105
        if not isinstance(other, SemverVersion):
            return NotImplemented
        return (
            self.major == other.major
            and self.minor == other.minor
            and self.patch == other.patch
            and self.prerelease == other.prerelease
        )

    def __lt__(self, other: object) -> bool:  # noqa: D105
        if not isinstance(other, SemverVersion):
            return NotImplemented
        if (self.major, self.minor, self.patch) != (
            other.major,
            other.minor,
            other.patch,
        ):
            return (self.major, self.minor, self.patch) < (
                other.major,
                other.minor,
                other.patch,
            )
        # Prerelease ordering: release (None) > prerelease (non-None)
        if self.prerelease is None and other.prerelease is None:
            return False
        if self.prerelease is None:
            # self is release, other is prerelease → self > other
            return False
        if other.prerelease is None:
            # self is prerelease, other is release → self < other
            return True
        # Both have prereleases: compare lexicographically
        return self.prerelease < other.prerelease

    def __hash__(self) -> int:  # noqa: D105
        return hash((self.major, self.minor, self.patch, self.prerelease))


# ---------------------------------------------------------------------------
# EUPS Major  (e.g. v12_0, 12.0)
# ---------------------------------------------------------------------------

_EUPS_MAJOR_RE = re.compile(
    r"^v?(?P<major>0|[1-9]\d*)[_.](?P<minor>0|[1-9]\d*)$"
)


@functools.total_ordering
@dataclass(frozen=True, slots=True)
class EupsMajorVersion:
    """A parsed EUPS major release version (two-component)."""

    major: int
    minor: int

    @classmethod
    def parse(cls, git_ref: str) -> Self | None:  # noqa: D102
        m = _EUPS_MAJOR_RE.match(git_ref)
        if m is None:
            return None
        return cls(major=int(m.group("major")), minor=int(m.group("minor")))

    def __eq__(self, other: object) -> bool:  # noqa: D105
        if not isinstance(other, EupsMajorVersion):
            return NotImplemented
        return self.major == other.major and self.minor == other.minor

    def __lt__(self, other: object) -> bool:  # noqa: D105
        if not isinstance(other, EupsMajorVersion):
            return NotImplemented
        return (self.major, self.minor) < (other.major, other.minor)

    def __hash__(self) -> int:  # noqa: D105
        return hash((self.major, self.minor))


# ---------------------------------------------------------------------------
# EUPS Weekly  (e.g. w_2024_05, w.2024.05)
# ---------------------------------------------------------------------------

_EUPS_WEEKLY_RE = re.compile(r"^w[_.](?P<year>\d{4})[_.](?P<week>\d{2})$")


@functools.total_ordering
@dataclass(frozen=True, slots=True)
class EupsWeeklyVersion:
    """A parsed EUPS weekly release version."""

    year: int
    week: int

    @classmethod
    def parse(cls, git_ref: str) -> Self | None:  # noqa: D102
        m = _EUPS_WEEKLY_RE.match(git_ref)
        if m is None:
            return None
        return cls(year=int(m.group("year")), week=int(m.group("week")))

    def __eq__(self, other: object) -> bool:  # noqa: D105
        if not isinstance(other, EupsWeeklyVersion):
            return NotImplemented
        return self.year == other.year and self.week == other.week

    def __lt__(self, other: object) -> bool:  # noqa: D105
        if not isinstance(other, EupsWeeklyVersion):
            return NotImplemented
        return (self.year, self.week) < (other.year, other.week)

    def __hash__(self) -> int:  # noqa: D105
        return hash((self.year, self.week))


# ---------------------------------------------------------------------------
# EUPS Daily  (e.g. d_2024_01_15, d.2024.01.15)
# ---------------------------------------------------------------------------

_EUPS_DAILY_RE = re.compile(
    r"^d[_.](?P<year>\d{4})[_.](?P<month>\d{2})[_.](?P<day>\d{2})$"
)


@functools.total_ordering
@dataclass(frozen=True, slots=True)
class EupsDailyVersion:
    """A parsed EUPS daily release version."""

    year: int
    month: int
    day: int

    @classmethod
    def parse(cls, git_ref: str) -> Self | None:  # noqa: D102
        m = _EUPS_DAILY_RE.match(git_ref)
        if m is None:
            return None
        return cls(
            year=int(m.group("year")),
            month=int(m.group("month")),
            day=int(m.group("day")),
        )

    def __eq__(self, other: object) -> bool:  # noqa: D105
        if not isinstance(other, EupsDailyVersion):
            return NotImplemented
        return (
            self.year == other.year
            and self.month == other.month
            and self.day == other.day
        )

    def __lt__(self, other: object) -> bool:  # noqa: D105
        if not isinstance(other, EupsDailyVersion):
            return NotImplemented
        return (self.year, self.month, self.day) < (
            other.year,
            other.month,
            other.day,
        )

    def __hash__(self) -> int:  # noqa: D105
        return hash((self.year, self.month, self.day))


# ---------------------------------------------------------------------------
# LSST Doc  (e.g. v1.0, 1.0.1)
# ---------------------------------------------------------------------------

_LSST_DOC_RE = re.compile(
    r"^v?(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)"
    r"(?:\.(?P<patch>0|[1-9]\d*))?$"
)


@functools.total_ordering
@dataclass(frozen=True, slots=True)
class LsstDocVersion:
    """A parsed LSST document version (2- or 3-component)."""

    major: int
    minor: int
    patch: int = 0

    @classmethod
    def parse(cls, git_ref: str) -> Self | None:  # noqa: D102
        m = _LSST_DOC_RE.match(git_ref)
        if m is None:
            return None
        return cls(
            major=int(m.group("major")),
            minor=int(m.group("minor")),
            patch=int(m.group("patch") or "0"),
        )

    def __eq__(self, other: object) -> bool:  # noqa: D105
        if not isinstance(other, LsstDocVersion):
            return NotImplemented
        return (
            self.major == other.major
            and self.minor == other.minor
            and self.patch == other.patch
        )

    def __lt__(self, other: object) -> bool:  # noqa: D105
        if not isinstance(other, LsstDocVersion):
            return NotImplemented
        return (self.major, self.minor, self.patch) < (
            other.major,
            other.minor,
            other.patch,
        )

    def __hash__(self) -> int:  # noqa: D105
        return hash((self.major, self.minor, self.patch))


# ---------------------------------------------------------------------------
# Union type + dispatch
# ---------------------------------------------------------------------------

VersionType = (
    SemverVersion
    | EupsMajorVersion
    | EupsWeeklyVersion
    | EupsDailyVersion
    | LsstDocVersion
)


def parse_version_for_mode(
    git_ref: str, mode: TrackingMode
) -> VersionType | None:
    """Parse a git ref as the version type implied by *mode*.

    Returns ``None`` if parsing fails or the mode is not version-based.
    """
    if mode in (
        TrackingMode.semver_release,
        TrackingMode.semver_major,
        TrackingMode.semver_minor,
    ):
        return SemverVersion.parse(git_ref)
    if mode == TrackingMode.eups_major_release:
        return EupsMajorVersion.parse(git_ref)
    if mode == TrackingMode.eups_weekly_release:
        return EupsWeeklyVersion.parse(git_ref)
    if mode == TrackingMode.eups_daily_release:
        return EupsDailyVersion.parse(git_ref)
    if mode == TrackingMode.lsst_doc:
        return LsstDocVersion.parse(git_ref)
    return None
