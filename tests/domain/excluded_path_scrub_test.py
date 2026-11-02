"""Tests for the excluded-path scrub's matching and grouping rules."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from docverse.models import BuildStatus
from docverse_server.domain.build import Build
from docverse_server.domain.excluded_path_scrub import (
    SCRUBBED_ANNOTATION,
    excluded_entry_path,
    match_excluded_objects,
    scrub_skip_reason,
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


def test_match_measures_everything_listed_under_the_prefix() -> None:
    """The listed totals count served and excluded objects alike.

    They are the build's inventory as the store holds it, which a real
    scrub records less what it deleted. An object outside the prefix is
    no part of this build's inventory.
    """
    match = match_excluded_objects(
        [
            ObjectEntry(key=f"{_PREFIX}index.html", size=10),
            ObjectEntry(key=f"{_PREFIX}.buildinfo", size=7),
            ObjectEntry(key="proj/__builds/OTHER/index.html", size=500),
        ],
        storage_prefix=_PREFIX,
    )

    assert match.listed_object_count == 2
    assert match.listed_size_bytes == 17
    assert match.object_count == 1
    assert match.total_size_bytes == 7


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


def test_match_keeps_each_objects_size() -> None:
    """The match carries every excluded object's own size, in key order.

    A deletion that only partly succeeds can then subtract exactly the
    bytes it removed from the build's inventory.
    """
    match = match_excluded_objects(
        [
            ObjectEntry(key=f"{_PREFIX}index.html", size=10),
            ObjectEntry(key=f"{_PREFIX}.doctrees/b.doctree", size=200),
            ObjectEntry(key=f"{_PREFIX}.buildinfo", size=7),
            ObjectEntry(key=f"{_PREFIX}.doctrees/a.doctree", size=900),
        ],
        storage_prefix=_PREFIX,
    )

    assert [(obj.key, obj.size) for obj in match.objects] == [
        (f"{_PREFIX}.buildinfo", 7),
        (f"{_PREFIX}.doctrees/a.doctree", 900),
        (f"{_PREFIX}.doctrees/b.doctree", 200),
    ]


def _build(**overrides: Any) -> Build:
    fields: dict[str, Any] = {
        "id": 1,
        "public_id": 1,
        "project_id": 1,
        "git_ref": "main",
        "content_hash": "sha256:" + "0" * 64,
        "status": BuildStatus.completed,
        "staging_key": "__staging/x.tar.gz",
        "storage_prefix": _PREFIX,
        "uploader": "testuser",
        "date_created": datetime(2026, 11, 2, tzinfo=UTC),
    }
    fields.update(overrides)
    return Build.model_validate(fields)


def test_scrub_skip_reason_is_none_for_a_live_unscrubbed_build() -> None:
    """A completed, live build without the stamp is still the scrub's."""
    assert scrub_skip_reason(_build()) is None
    assert (
        scrub_skip_reason(_build(annotations={"commit_sha": "abc123"})) is None
    )


@pytest.mark.parametrize(
    ("build", "reason"),
    [
        (None, "no longer exists"),
        (
            _build(date_deleted=datetime(2026, 11, 2, tzinfo=UTC)),
            "was deleted",
        ),
        (_build(status=BuildStatus.superseded), "'superseded'"),
        (
            _build(
                annotations={
                    SCRUBBED_ANNOTATION: {"date_scrubbed": "2026-11-02"}
                }
            ),
            "already scrubbed",
        ),
    ],
)
def test_scrub_skip_reason_names_why_a_build_is_left_alone(
    build: Build | None, reason: str
) -> None:
    """A build retired or stamped since it was listed is skipped."""
    skip = scrub_skip_reason(build)

    assert skip is not None
    assert reason in skip
