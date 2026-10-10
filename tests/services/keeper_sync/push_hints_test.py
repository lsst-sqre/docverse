"""Tests for ``docverse_server.services.keeper_sync.push_hints``.

Pure-function tests with no DB or HTTP, on in-memory
``KeeperSyncState`` rows like ``scheduler_test.py``. The push processor
writes what :func:`stamp_pushed_ref` returns onto a project's state row,
so these tests pin the shape of the ``github_pushed_refs`` annotation as
well as the stamp, prune, cap, and window rules.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from docverse_server.services.keeper_sync.push_hints import (
    ANNOTATION_GITHUB_PUSHED_REFS,
    PUSHED_REFS_CAP,
    is_in_push_window,
    prune_pushed_refs,
    read_pushed_refs,
    stamp_pushed_ref,
)
from docverse_server.storage.keeper_sync import KeeperSyncState

_NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
_WINDOW = timedelta(hours=1)


def _project_state(
    *,
    annotations: dict[str, Any] | None = None,
    ltd_slug: str = "sqr-112",
) -> KeeperSyncState:
    """Build a project-resource ``KeeperSyncState`` row."""
    return KeeperSyncState(
        id=1,
        public_id=1,
        org_id=1,
        resource_type="project",
        ltd_id=None,
        ltd_slug=ltd_slug,
        docverse_id=7,
        annotations=annotations,
    )


def test_stamp_records_ref_with_iso_push_time() -> None:
    """A first stamp adds the ref to the map as an ISO-8601 string."""
    merged = stamp_pushed_ref(
        _project_state(), ref="tickets/DM-1", now=_NOW, window=_WINDOW
    )

    assert merged[ANNOTATION_GITHUB_PUSHED_REFS] == {
        "tickets/DM-1": _NOW.isoformat()
    }


def test_stamp_repeat_push_overwrites_the_time() -> None:
    """A second push to the same ref moves its time forward."""
    first = _NOW - timedelta(minutes=30)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {"main": first.isoformat()}
        }
    )

    merged = stamp_pushed_ref(state, ref="main", now=_NOW, window=_WINDOW)

    assert merged[ANNOTATION_GITHUB_PUSHED_REFS] == {"main": _NOW.isoformat()}


def test_stamp_keeps_other_refs_inside_the_window() -> None:
    """Stamping one ref leaves the project's other live stamps in place."""
    other = _NOW - timedelta(minutes=10)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {"main": other.isoformat()}
        }
    )

    merged = stamp_pushed_ref(state, ref="v1.0", now=_NOW, window=_WINDOW)

    assert merged[ANNOTATION_GITHUB_PUSHED_REFS] == {
        "main": other.isoformat(),
        "v1.0": _NOW.isoformat(),
    }


def test_stamp_prunes_refs_older_than_the_window() -> None:
    """A ref pushed a full window ago or earlier is dropped on stamp."""
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "expired": (_NOW - _WINDOW).isoformat(),
                "ancient": (_NOW - timedelta(days=3)).isoformat(),
                "live": (_NOW - _WINDOW + timedelta(seconds=1)).isoformat(),
            }
        }
    )

    merged = stamp_pushed_ref(state, ref="main", now=_NOW, window=_WINDOW)

    assert set(merged[ANNOTATION_GITHUB_PUSHED_REFS]) == {"live", "main"}


def test_stamp_caps_the_map_dropping_the_oldest() -> None:
    """A 21st ref drops the oldest push, keeping the cap's worth."""
    existing = {
        f"ref-{index:02d}": (_NOW - timedelta(minutes=50 - index)).isoformat()
        for index in range(PUSHED_REFS_CAP)
    }
    state = _project_state(
        annotations={ANNOTATION_GITHUB_PUSHED_REFS: existing}
    )

    merged = stamp_pushed_ref(state, ref="newest", now=_NOW, window=_WINDOW)

    stamped = merged[ANNOTATION_GITHUB_PUSHED_REFS]
    assert PUSHED_REFS_CAP == 20
    assert len(stamped) == PUSHED_REFS_CAP
    assert "ref-00" not in stamped
    assert "ref-01" in stamped
    assert stamped["newest"] == _NOW.isoformat()


def test_stamp_preserves_other_annotation_keys() -> None:
    """The tier crons' own annotation keys survive a stamp."""
    state = _project_state(
        annotations={
            "date_main_last_polled": "2026-10-09T11:55:00+00:00",
            "main_edition_url": "https://keeper.lsst.codes/editions/1",
        }
    )

    merged = stamp_pushed_ref(state, ref="main", now=_NOW, window=_WINDOW)

    assert merged["date_main_last_polled"] == "2026-10-09T11:55:00+00:00"
    assert merged["main_edition_url"] == (
        "https://keeper.lsst.codes/editions/1"
    )


def test_stamp_replaces_a_malformed_map() -> None:
    """A map that is not an object, or bad entries in it, are discarded."""
    not_a_map = _project_state(
        annotations={ANNOTATION_GITHUB_PUSHED_REFS: ["main"]}
    )
    bad_entries = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "garbled": "not a time",
                "number": 12,
                "": _NOW.isoformat(),
            }
        }
    )

    for state in (not_a_map, bad_entries):
        merged = stamp_pushed_ref(state, ref="main", now=_NOW, window=_WINDOW)
        assert merged[ANNOTATION_GITHUB_PUSHED_REFS] == {
            "main": _NOW.isoformat()
        }


def test_read_pushed_refs_parses_times() -> None:
    """The map reads back as aware datetimes; a naive time reads as UTC."""
    pushed = _NOW - timedelta(minutes=5)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "main": pushed.isoformat(),
                "naive": "2026-10-09T11:00:00",
            }
        }
    )

    assert read_pushed_refs(state) == {
        "main": pushed,
        "naive": datetime(2026, 10, 9, 11, 0, tzinfo=UTC),
    }


def test_read_pushed_refs_without_a_row_or_map_is_empty() -> None:
    """No row, no annotations, or no map all read as no stamps."""
    assert read_pushed_refs(None) == {}
    assert read_pushed_refs(_project_state()) == {}
    assert read_pushed_refs(_project_state(annotations={"x": 1})) == {}


def test_prune_pushed_refs_keeps_only_the_window() -> None:
    """Pruning keeps refs pushed less than a window ago."""
    refs = {
        "live": _NOW - timedelta(minutes=59),
        "edge": _NOW - _WINDOW,
    }

    assert prune_pushed_refs(refs, now=_NOW, window=_WINDOW) == {
        "live": refs["live"]
    }


def test_is_in_push_window() -> None:
    """A project is inside the window while any stamp is."""
    live = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "old": (_NOW - timedelta(hours=3)).isoformat(),
                "main": (_NOW - timedelta(minutes=59)).isoformat(),
            }
        }
    )
    expired = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "main": (_NOW - _WINDOW).isoformat()
            }
        }
    )

    assert is_in_push_window(live, now=_NOW, window=_WINDOW)
    assert not is_in_push_window(expired, now=_NOW, window=_WINDOW)
    assert not is_in_push_window(_project_state(), now=_NOW, window=_WINDOW)
    assert not is_in_push_window(None, now=_NOW, window=_WINDOW)
