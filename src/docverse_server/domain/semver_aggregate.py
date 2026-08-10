"""The ``N`` / ``N.M`` aggregate editions a stable semver release implies.

Two call sites create these rows: the native upload path
(:class:`~docverse_server.services.edition_tracking.EditionTrackingService`)
and the keeper-sync importer. They must create *identical* rows — a
migrated project has to render the same dashboard groups as a natively
built one — so the shape lives here rather than being spelled out twice.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from docverse.models import EditionKind, TrackingMode
from docverse_server.domain.version import SemverVersion

__all__ = ["SemverAggregateSpec", "semver_aggregate_specs"]


@dataclass(frozen=True, slots=True)
class SemverAggregateSpec:
    """The row one aggregate edition should be created with."""

    slug: str
    """Edition slug — ``"15"`` for a major, ``"15.2"`` for a minor."""

    title: str
    kind: EditionKind
    tracking_mode: TrackingMode
    tracking_params: dict[str, Any]


def semver_aggregate_specs(
    version: SemverVersion,
) -> tuple[SemverAggregateSpec, ...]:
    """Return the major/minor aggregate specs implied by *version*.

    Parameters
    ----------
    version
        A parsed semantic version, typically from a build's git ref.

    Returns
    -------
    tuple of SemverAggregateSpec
        The ``N`` (``semver_major``) and ``N.M`` (``semver_minor``)
        specs, in that order — or empty for a prerelease, which must
        neither create nor advance an aggregate.
    """
    if version.prerelease is not None:
        return ()
    return (
        SemverAggregateSpec(
            slug=str(version.major),
            title=f"Latest {version.major}.x",
            kind=EditionKind.major,
            tracking_mode=TrackingMode.semver_major,
            tracking_params={"major_version": version.major},
        ),
        SemverAggregateSpec(
            slug=f"{version.major}.{version.minor}",
            title=f"Latest {version.major}.{version.minor}.x",
            kind=EditionKind.minor,
            tracking_mode=TrackingMode.semver_minor,
            tracking_params={
                "major_version": version.major,
                "minor_version": version.minor,
            },
        ),
    )
