"""Domain rules for the excluded-path scrub.

The scrub removes, from builds already stored, the cache paths that every
ingest point now leaves out (`docverse.excluded_paths`). This module holds
the part of that job with no I/O: which of a build's objects are
excluded, and how they are grouped for the report an operator reads.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

from docverse.excluded_paths import EXCLUDED_PATH_NAMES
from docverse.models import BuildStatus

from .build import Build

__all__ = [
    "SCRUBBED_ANNOTATION",
    "BuildScrubMatch",
    "ExcludedEntry",
    "SizedObject",
    "excluded_entry_path",
    "match_excluded_objects",
    "scrub_skip_reason",
]

SCRUBBED_ANNOTATION = "excluded_paths_scrubbed"
"""Build annotation key a real scrub stamps on each build it finishes.

A build carrying it is left out of later scrubs of its project, so a
re-run after a partial failure revisits only the builds the first run
did not get through.
"""


class SizedObject(Protocol):
    """A stored object's key and size, as an object-store listing gives.

    Structural, so the object store's ``ObjectEntry`` satisfies it
    without this module depending on the storage layer.
    """

    @property
    def key(self) -> str:
        """Full object key, prefix included."""
        ...

    @property
    def size(self) -> int:
        """Size of the object in bytes."""
        ...


@dataclass(frozen=True, slots=True)
class ExcludedEntry:
    """Excluded objects of one build that share an excluded entry."""

    path: str
    """Build-relative entry path; see `excluded_entry_path`."""

    objects: tuple[SizedObject, ...]
    """The objects under the entry, with their sizes, in key order."""

    @property
    def keys(self) -> tuple[str, ...]:
        """Full object keys under the entry, in key order."""
        return tuple(obj.key for obj in self.objects)

    @property
    def object_count(self) -> int:
        """Number of objects under the entry."""
        return len(self.objects)

    @property
    def total_size_bytes(self) -> int:
        """Combined size of the objects under the entry."""
        return sum(obj.size for obj in self.objects)


@dataclass(frozen=True, slots=True)
class BuildScrubMatch:
    """The excluded objects found under one build's storage prefix."""

    entries: tuple[ExcludedEntry, ...]
    """Excluded entries, in entry-path order. Empty when none matched."""

    listed_object_count: int
    """Number of objects the listing held under the prefix, excluded
    or not: the build's inventory as the store measured it."""

    listed_size_bytes: int
    """Combined size of :attr:`listed_object_count`."""

    @property
    def object_count(self) -> int:
        """Number of excluded objects across every entry."""
        return sum(entry.object_count for entry in self.entries)

    @property
    def total_size_bytes(self) -> int:
        """Combined size of the excluded objects across every entry."""
        return sum(entry.total_size_bytes for entry in self.entries)

    @property
    def objects(self) -> tuple[SizedObject, ...]:
        """Every excluded object with its size, entry by entry."""
        return tuple(obj for entry in self.entries for obj in entry.objects)

    @property
    def keys(self) -> tuple[str, ...]:
        """Every excluded object key, entry by entry."""
        return tuple(obj.key for obj in self.objects)


def excluded_entry_path(relative_path: str) -> str | None:
    """Return the excluded entry a build-relative object path falls under.

    The entry is the path up to and including its first segment named in
    `~docverse.excluded_paths.EXCLUDED_PATH_NAMES`, the same any-segment
    rule `~docverse.excluded_paths.is_excluded_path` applies. When that
    segment is a directory (anything follows it) the entry ends with
    ``/``, so ``.doctrees/environment.pickle`` falls under
    ``.doctrees/``; when it is the object itself, the entry is the whole
    path, as for ``.buildinfo``. A cache inside a nested sub-site keeps
    its parent segments (``v1/.doctrees/``), so the report says where in
    the tree it was found.

    Parameters
    ----------
    relative_path
        Object key with the build's storage prefix removed.

    Returns
    -------
    str or None
        The entry path, or `None` when the path is not excluded.
    """
    segments = relative_path.split("/")
    for index, segment in enumerate(segments):
        if segment in EXCLUDED_PATH_NAMES:
            entry = "/".join(segments[: index + 1])
            return entry if index == len(segments) - 1 else f"{entry}/"
    return None


def match_excluded_objects(
    objects: Iterable[SizedObject], *, storage_prefix: str
) -> BuildScrubMatch:
    """Select a build's excluded objects and group them by entry.

    Parameters
    ----------
    objects
        The build's objects, as listed under ``storage_prefix``. An
        object outside the prefix is ignored rather than trusted, and
        left out of the listed totals: its path relative to this build
        is undefined.
    storage_prefix
        The build's storage prefix, which is stripped from each key
        before the excluded-path rule is applied, so a cache name that
        happens to appear in the prefix itself never matches.

    Returns
    -------
    BuildScrubMatch
        The excluded objects grouped by `excluded_entry_path`, with the
        count and size of every object listed under the prefix.
    """
    grouped: dict[str, list[SizedObject]] = {}
    listed_object_count = 0
    listed_size_bytes = 0
    for obj in objects:
        if not obj.key.startswith(storage_prefix):
            continue
        listed_object_count += 1
        listed_size_bytes += obj.size
        entry = excluded_entry_path(obj.key[len(storage_prefix) :])
        if entry is not None:
            grouped.setdefault(entry, []).append(obj)
    return BuildScrubMatch(
        entries=tuple(
            ExcludedEntry(
                path=path,
                objects=tuple(sorted(matched, key=lambda obj: obj.key)),
            )
            for path, matched in sorted(grouped.items())
        ),
        listed_object_count=listed_object_count,
        listed_size_bytes=listed_size_bytes,
    )


def scrub_skip_reason(build: Build | None) -> str | None:
    """Say why a real scrub must leave a build's row alone, if it must.

    The scrub lists its candidate builds once, at the start, and then
    spends seconds to minutes on each. By the time it comes to delete a
    build's objects or write its row, the build may have been deleted,
    or another scrub of the same project may already have finished it.
    The worker asks this of a fresh read before it deletes anything and
    again, under the row lock, before it corrects the inventory, so a
    retired build is never written to and a build two scrubs raced over
    has its inventory corrected only once.

    Parameters
    ----------
    build
        The build as it stands now, or `None` if its row is gone.

    Returns
    -------
    str or None
        A short reason for the report, or `None` when the build is
        still completed, live and unstamped.
    """
    if build is None:
        return "the build no longer exists"
    if build.date_deleted is not None:
        return "the build was deleted during the scrub"
    if build.status is not BuildStatus.completed:
        return (
            f"the build's status is now {build.status.value!r}, not"
            f" {BuildStatus.completed.value!r}"
        )
    extras = build.annotations.model_extra if build.annotations else None
    if extras and SCRUBBED_ANNOTATION in extras:
        return "another scrub already scrubbed the build"
    return None
