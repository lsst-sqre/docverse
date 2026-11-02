"""Tests for the excluded-path scrub's matching and grouping rules."""

from __future__ import annotations

import pytest

from docverse_server.domain.excluded_path_scrub import (
    excluded_entry_path,
    match_excluded_objects,
)
from docverse_server.storage.objectstore import ObjectEntry

_PREFIX = "proj/__builds/01ABCDEF/"


@pytest.mark.parametrize(
    ("relative_path", "expected"),
    [
        (".doctrees/environment.pickle", ".doctrees/"),
        (".doctrees/sub/page.doctree", ".doctrees/"),
        (".buildinfo", ".buildinfo"),
        ("v1/.doctrees/index.doctree", "v1/.doctrees/"),
        ("v1/.buildinfo", "v1/.buildinfo"),
        ("a/.git/objects/.doctrees/x", "a/.git/"),
        ("index.html", None),
        ("_sources/index.rst.txt", None),
        ("docs/.well-known/x", None),
        ("foo.doctrees/x", None),
    ],
)
def test_excluded_entry_path(relative_path: str, expected: str | None) -> None:
    """The entry stops at the first excluded segment, ``/`` for a dir."""
    assert excluded_entry_path(relative_path) == expected


def test_match_groups_excluded_objects_by_entry() -> None:
    """Excluded keys group under their entry; served content is left out."""
    match = match_excluded_objects(
        [
            ObjectEntry(key=f"{_PREFIX}index.html", size=10),
            ObjectEntry(key=f"{_PREFIX}.doctrees/b.doctree", size=200),
            ObjectEntry(
                key=f"{_PREFIX}.doctrees/environment.pickle", size=900
            ),
            ObjectEntry(key=f"{_PREFIX}.buildinfo", size=7),
            ObjectEntry(key=f"{_PREFIX}v1/.doctrees/x.doctree", size=30),
        ],
        storage_prefix=_PREFIX,
    )

    assert [
        (entry.path, entry.object_count, entry.total_size_bytes)
        for entry in match.entries
    ] == [
        (".buildinfo", 1, 7),
        (".doctrees/", 2, 1100),
        ("v1/.doctrees/", 1, 30),
    ]
    assert match.entries[1].keys == (
        f"{_PREFIX}.doctrees/b.doctree",
        f"{_PREFIX}.doctrees/environment.pickle",
    )
    assert match.object_count == 4
    assert match.total_size_bytes == 1137
    assert f"{_PREFIX}index.html" not in match.keys


def test_match_ignores_cache_names_in_the_prefix_itself() -> None:
    """Only the path below the build prefix is tested against the list."""
    prefix = "proj/.git/__builds/01ABCDEF/"

    match = match_excluded_objects(
        [ObjectEntry(key=f"{prefix}index.html", size=10)],
        storage_prefix=prefix,
    )

    assert match.entries == ()
    assert match.object_count == 0
    assert match.total_size_bytes == 0


def test_match_ignores_objects_outside_the_prefix() -> None:
    """A key from another build is never attributed to this one."""
    match = match_excluded_objects(
        [ObjectEntry(key="proj/__builds/OTHER/.buildinfo", size=7)],
        storage_prefix=_PREFIX,
    )

    assert match.entries == ()
