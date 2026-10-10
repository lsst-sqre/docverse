"""Tests for ``docverse_server.services.keeper_sync.push_hints``.

Pure-function tests with no DB or HTTP, on in-memory
``KeeperSyncState`` rows like ``scheduler_test.py``. The push processor
writes what :func:`stamp_pushed_ref` returns onto a project's state row,
and ``tier_main`` what :func:`settle_pushed_refs` returns, so these
tests pin the shape of the ``github_pushed_refs`` annotation as well as
the stamp, prune, cap, window, and settle rules, and the rebuild check
``tier_main`` runs on each pushed ref's edition.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from docverse_server.services.keeper_sync.push_hints import (
    ANNOTATION_GITHUB_PUSHED_REFS,
    PUSHED_REFS_CAP,
    is_in_push_window,
    ltd_rebuilt_since_sync,
    prune_pushed_refs,
    read_pushed_refs,
    settle_pushed_refs,
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


def _edition_state(
    *,
    date_rebuilt_seen: datetime | None = None,
    date_last_synced: datetime | None = None,
) -> KeeperSyncState:
    """Build an edition-resource ``KeeperSyncState`` row."""
    return KeeperSyncState(
        id=2,
        public_id=2,
        org_id=1,
        resource_type="edition",
        ltd_id=42,
        ltd_slug="tickets-DM-1",
        docverse_id=8,
        date_rebuilt_seen=date_rebuilt_seen,
        date_last_synced=date_last_synced,
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


def test_settle_clears_a_ref_whose_stamp_has_not_moved() -> None:
    """A ref cleared at the push time the visit read is dropped."""
    pushed = _NOW - timedelta(minutes=20)
    other = _NOW - timedelta(minutes=5)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "tickets/DM-1": pushed.isoformat(),
                "main": other.isoformat(),
            }
        }
    )

    settled = settle_pushed_refs(
        state, cleared={"tickets/DM-1": pushed}, now=_NOW, window=_WINDOW
    )

    assert settled[ANNOTATION_GITHUB_PUSHED_REFS] == {
        "main": other.isoformat()
    }


def test_settle_keeps_a_ref_pushed_again_since_the_visit() -> None:
    """A ref re-stamped after the visit read it keeps its newer stamp."""
    seen = _NOW - timedelta(minutes=20)
    again = _NOW - timedelta(seconds=10)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {"tickets/DM-1": again.isoformat()}
        }
    )

    settled = settle_pushed_refs(
        state, cleared={"tickets/DM-1": seen}, now=_NOW, window=_WINDOW
    )

    assert settled[ANNOTATION_GITHUB_PUSHED_REFS] == {
        "tickets/DM-1": again.isoformat()
    }


def test_settle_prunes_expired_refs() -> None:
    """Settling drops every ref whose window has passed."""
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "expired": (_NOW - _WINDOW).isoformat(),
                "live": (_NOW - timedelta(minutes=5)).isoformat(),
            }
        }
    )

    settled = settle_pushed_refs(state, cleared={}, now=_NOW, window=_WINDOW)

    assert set(settled[ANNOTATION_GITHUB_PUSHED_REFS]) == {"live"}


def test_settle_preserves_other_annotation_keys() -> None:
    """The tier crons' own annotation keys survive settling."""
    state = _project_state(
        annotations={
            "date_main_last_polled": "2026-10-09T11:55:00+00:00",
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "main": (_NOW - _WINDOW).isoformat()
            },
        }
    )

    settled = settle_pushed_refs(state, cleared={}, now=_NOW, window=_WINDOW)

    assert settled == {
        "date_main_last_polled": "2026-10-09T11:55:00+00:00",
        ANNOTATION_GITHUB_PUSHED_REFS: {},
    }


def test_ltd_rebuilt_since_sync_compares_with_the_rebuild_seen() -> None:
    """A rebuild newer than the one Docverse last saw is a change."""
    seen = _NOW - timedelta(minutes=30)
    state = _edition_state(date_rebuilt_seen=seen, date_last_synced=_NOW)

    assert ltd_rebuilt_since_sync(state, ltd_date_rebuilt=_NOW)
    assert not ltd_rebuilt_since_sync(state, ltd_date_rebuilt=seen)
    assert not ltd_rebuilt_since_sync(
        state, ltd_date_rebuilt=seen - timedelta(seconds=1)
    )


def test_ltd_rebuilt_since_sync_falls_back_to_the_last_sync() -> None:
    """With no rebuild recorded, the last sync time is the reference."""
    synced = _NOW - timedelta(minutes=30)
    state = _edition_state(date_last_synced=synced)

    assert ltd_rebuilt_since_sync(state, ltd_date_rebuilt=_NOW)
    assert not ltd_rebuilt_since_sync(
        state, ltd_date_rebuilt=synced - timedelta(minutes=1)
    )


def test_ltd_rebuilt_since_sync_edge_cases() -> None:
    """An unbuilt LTD edition never changes; an unsynced row always does."""
    assert not ltd_rebuilt_since_sync(
        _edition_state(date_rebuilt_seen=_NOW), ltd_date_rebuilt=None
    )
    assert not ltd_rebuilt_since_sync(_edition_state(), ltd_date_rebuilt=None)
    assert ltd_rebuilt_since_sync(_edition_state(), ltd_date_rebuilt=_NOW)
