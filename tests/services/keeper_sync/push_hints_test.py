"""Tests for ``docverse_server.services.keeper_sync.push_hints``.

Pure-function tests with no DB or HTTP, on in-memory
``KeeperSyncState`` rows like ``scheduler_test.py``. The push processor
writes what :func:`stamp_pushed_ref` returns onto a project's state row,
and ``tier_main`` what :func:`settle_pushed_refs` returns, so these
tests pin the shape of the ``github_pushed_refs`` and
``github_pushed_refs_enqueued`` annotations as well as the stamp, prune,
cap, window, and settle rules, how a closed window is reported, and the
rebuild check ``tier_main`` runs on each pushed ref's edition.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from docverse_server.services.keeper_sync.push_hints import (
    ANNOTATION_GITHUB_PUSHED_REFS,
    ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED,
    PUSHED_REFS_CAP,
    PushCheckOutcome,
    closed_window_outcome,
    is_in_push_window,
    ltd_rebuilt_since_sync,
    prune_pushed_refs,
    read_pushed_refs,
    read_pushed_refs_enqueued,
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
    date_tombstoned: datetime | None = None,
    tombstone_reason: str | None = None,
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
        date_tombstoned=date_tombstoned,
        tombstone_reason=tombstone_reason,
    )


def test_stamp_records_ref_with_iso_push_time() -> None:
    """A first stamp adds the ref to the map as an ISO-8601 string."""
    merged = stamp_pushed_ref(_project_state(), ref="tickets/DM-1", now=_NOW)

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

    merged = stamp_pushed_ref(state, ref="main", now=_NOW)

    assert merged[ANNOTATION_GITHUB_PUSHED_REFS] == {"main": _NOW.isoformat()}


def test_stamp_keeps_other_refs_inside_the_window() -> None:
    """Stamping one ref leaves the project's other live stamps in place."""
    other = _NOW - timedelta(minutes=10)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {"main": other.isoformat()}
        }
    )

    merged = stamp_pushed_ref(state, ref="v1.0", now=_NOW)

    assert merged[ANNOTATION_GITHUB_PUSHED_REFS] == {
        "main": other.isoformat(),
        "v1.0": _NOW.isoformat(),
    }


def test_stamp_leaves_expired_refs_for_tier_main() -> None:
    """A ref whose window has passed survives a stamp of another ref.

    Pruning is ``tier_main``'s, which reports each ref it prunes as
    ``expired``; a stamp that dropped it first would lose that report.
    """
    expired = _NOW - _WINDOW
    ancient = _NOW - timedelta(days=3)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "expired": expired.isoformat(),
                "ancient": ancient.isoformat(),
            }
        }
    )

    merged = stamp_pushed_ref(state, ref="main", now=_NOW)

    assert merged[ANNOTATION_GITHUB_PUSHED_REFS] == {
        "expired": expired.isoformat(),
        "ancient": ancient.isoformat(),
        "main": _NOW.isoformat(),
    }


def test_stamp_caps_the_map_dropping_the_oldest() -> None:
    """A 21st ref drops the oldest push, keeping the cap's worth."""
    existing = {
        f"ref-{index:02d}": (_NOW - timedelta(minutes=50 - index)).isoformat()
        for index in range(PUSHED_REFS_CAP)
    }
    state = _project_state(
        annotations={ANNOTATION_GITHUB_PUSHED_REFS: existing}
    )

    merged = stamp_pushed_ref(state, ref="newest", now=_NOW)

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

    merged = stamp_pushed_ref(state, ref="main", now=_NOW)

    assert merged["date_main_last_polled"] == "2026-10-09T11:55:00+00:00"
    assert merged["main_edition_url"] == (
        "https://keeper.lsst.codes/editions/1"
    )


def test_stamp_carries_the_enqueue_map_over() -> None:
    """A push leaves ``tier_main``'s enqueue record as it found it.

    The enqueue map is ``tier_main``'s to write and prune; a repeated
    push restarts the ref's window, and the enqueue on record, now older
    than the push, no longer counts for it (see
    :func:`closed_window_outcome`).
    """
    enqueues = {"main": (_NOW - timedelta(minutes=5)).isoformat()}
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "main": (_NOW - timedelta(minutes=20)).isoformat()
            },
            ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED: enqueues,
        }
    )

    merged = stamp_pushed_ref(state, ref="main", now=_NOW)

    assert merged[ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED] == enqueues


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
        merged = stamp_pushed_ref(state, ref="main", now=_NOW)
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


def test_read_pushed_refs_enqueued_parses_times() -> None:
    """The enqueue map reads back as aware datetimes, like the stamps."""
    enqueued = _NOW - timedelta(minutes=2)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED: {
                "main": enqueued.isoformat(),
                "naive": "2026-10-09T11:00:00",
            }
        }
    )

    assert read_pushed_refs_enqueued(state) == {
        "main": enqueued,
        "naive": datetime(2026, 10, 9, 11, 0, tzinfo=UTC),
    }


def test_read_pushed_refs_enqueued_is_defensive() -> None:
    """A missing or malformed enqueue map reads as never enqueued.

    Rows written before the map existed carry no key, and the map
    round-trips through JSONB, so a non-object map or a bad entry is
    skipped rather than raised on.
    """
    assert read_pushed_refs_enqueued(None) == {}
    assert read_pushed_refs_enqueued(_project_state()) == {}
    assert read_pushed_refs_enqueued(_project_state(annotations={})) == {}
    not_a_map = _project_state(
        annotations={ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED: ["main"]}
    )
    assert read_pushed_refs_enqueued(not_a_map) == {}
    bad_entries = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED: {
                "garbled": "not a time",
                "number": 12,
                "": _NOW.isoformat(),
                "main": _NOW.isoformat(),
            }
        }
    )
    assert read_pushed_refs_enqueued(bad_entries) == {"main": _NOW}


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


def test_settle_keeps_an_enqueued_ref_until_its_window_passes() -> None:
    """A ref whose sync was enqueued keeps its stamp for the whole window.

    ``tier_main`` settles the row after each visit, including the one
    that enqueued the ref's sync. The ref stays stamped, so later ticks
    check it again and catch a second LTD rebuild inside the window; the
    first settle after the window has passed prunes it.
    """
    pushed = _NOW - timedelta(minutes=20)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {"tickets/DM-1": pushed.isoformat()}
        }
    )

    inside = settle_pushed_refs(
        state, enqueued={"tickets/DM-1": _NOW}, now=_NOW, window=_WINDOW
    )
    after = settle_pushed_refs(
        state, enqueued={}, now=pushed + _WINDOW, window=_WINDOW
    )

    assert inside[ANNOTATION_GITHUB_PUSHED_REFS] == {
        "tickets/DM-1": pushed.isoformat()
    }
    assert after[ANNOTATION_GITHUB_PUSHED_REFS] == {}


def test_settle_judges_a_ref_by_its_fresh_stamp() -> None:
    """A ref pushed again since the visit read it restarts its window.

    The visit read a stamp whose window had passed, but a push stamped
    the ref again before the settling write re-read the row: the newer
    push is what counts, so the ref is kept rather than pruned.
    """
    again = _NOW - timedelta(seconds=10)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {"tickets/DM-1": again.isoformat()}
        }
    )

    settled = settle_pushed_refs(state, enqueued={}, now=_NOW, window=_WINDOW)

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

    settled = settle_pushed_refs(state, enqueued={}, now=_NOW, window=_WINDOW)

    assert set(settled[ANNOTATION_GITHUB_PUSHED_REFS]) == {"live"}


def test_settle_records_each_enqueued_ref() -> None:
    """A ref whose sync the visit enqueued records that enqueue's time.

    Written as an ISO-8601 string beside the ref's stamp, overwriting an
    earlier enqueue of the same ref, so the map holds the ref's last
    enqueue; a ref the visit did not enqueue keeps what it had.
    """
    pushed = _NOW - timedelta(minutes=20)
    earlier = _NOW - timedelta(minutes=10)
    enqueued_at = _NOW + timedelta(seconds=2)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "tickets/DM-1": pushed.isoformat(),
                "tickets/DM-2": pushed.isoformat(),
                "tickets/DM-3": pushed.isoformat(),
            },
            ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED: {
                "tickets/DM-1": earlier.isoformat(),
                "tickets/DM-3": earlier.isoformat(),
            },
        }
    )

    settled = settle_pushed_refs(
        state,
        enqueued={"tickets/DM-1": enqueued_at, "tickets/DM-2": enqueued_at},
        now=_NOW,
        window=_WINDOW,
    )

    assert settled[ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED] == {
        "tickets/DM-1": enqueued_at.isoformat(),
        "tickets/DM-2": enqueued_at.isoformat(),
        "tickets/DM-3": earlier.isoformat(),
    }
    assert settled[ANNOTATION_GITHUB_PUSHED_REFS] == {
        "tickets/DM-1": pushed.isoformat(),
        "tickets/DM-2": pushed.isoformat(),
        "tickets/DM-3": pushed.isoformat(),
    }


def test_settle_prunes_an_enqueue_with_its_stamp() -> None:
    """A ref's enqueue goes when its stamp does.

    The expired ref's enqueue is pruned with its stamp, and an enqueue
    for a ref with no stamp left on the row (the push processor's cap
    dropped it) is dropped too, so the map only ever describes refs
    still stamped.
    """
    expired = _NOW - _WINDOW
    live = _NOW - timedelta(minutes=5)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                "expired": expired.isoformat(),
                "live": live.isoformat(),
            },
            ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED: {
                "expired": (expired + timedelta(minutes=5)).isoformat(),
                "live": (live + timedelta(minutes=1)).isoformat(),
                "unstamped": live.isoformat(),
            },
        }
    )

    settled = settle_pushed_refs(state, enqueued={}, now=_NOW, window=_WINDOW)

    assert settled[ANNOTATION_GITHUB_PUSHED_REFS] == {"live": live.isoformat()}
    assert settled[ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED] == {
        "live": (live + timedelta(minutes=1)).isoformat()
    }


def test_settle_caps_the_enqueue_map_keeping_the_latest() -> None:
    """The enqueue map holds at most the cap's worth of refs.

    The push processor caps the stamps, but a row can carry more (a
    hand-edited one, say); the enqueue map keeps the latest enqueues.
    """
    count = PUSHED_REFS_CAP + 5
    pushed = _NOW - timedelta(minutes=50)
    state = _project_state(
        annotations={
            ANNOTATION_GITHUB_PUSHED_REFS: {
                f"ref-{index:02d}": pushed.isoformat()
                for index in range(count)
            },
            ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED: {
                f"ref-{index:02d}": (
                    pushed + timedelta(minutes=index)
                ).isoformat()
                for index in range(count - 1)
            },
        }
    )

    settled = settle_pushed_refs(
        state,
        enqueued={f"ref-{count - 1:02d}": _NOW},
        now=_NOW,
        window=_WINDOW,
    )

    enqueues = settled[ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED]
    assert len(enqueues) == PUSHED_REFS_CAP
    assert enqueues[f"ref-{count - 1:02d}"] == _NOW.isoformat()
    assert "ref-04" not in enqueues
    assert "ref-05" in enqueues


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

    settled = settle_pushed_refs(state, enqueued={}, now=_NOW, window=_WINDOW)

    assert settled == {
        "date_main_last_polled": "2026-10-09T11:55:00+00:00",
        ANNOTATION_GITHUB_PUSHED_REFS: {},
        ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED: {},
    }


def test_closed_window_with_an_enqueue_is_completed() -> None:
    """A ref that enqueued its sync in its window ends ``completed``.

    The enqueue counts when it came at or after the push the stamp
    records, so it was made for that push's window.
    """
    pushed = _NOW - _WINDOW - timedelta(minutes=5)

    for enqueued_at in (pushed, pushed + timedelta(minutes=12)):
        outcome = closed_window_outcome(
            pushed_at=pushed, enqueued_at=enqueued_at
        )
        assert outcome is PushCheckOutcome.completed


def test_closed_window_without_an_enqueue_is_expired() -> None:
    """A ref that never enqueued in its window ends ``expired``."""
    pushed = _NOW - _WINDOW - timedelta(minutes=5)

    outcome = closed_window_outcome(pushed_at=pushed, enqueued_at=None)

    assert outcome is PushCheckOutcome.expired


def test_closed_window_ignores_an_enqueue_before_the_push() -> None:
    """An enqueue from an earlier push's window does not count.

    A ref synced for one push, then pushed again (the stamp restarting
    its window) with no sync since, still ends ``expired``: the enqueue
    on record predates the push the stamp now holds.
    """
    pushed = _NOW - _WINDOW - timedelta(minutes=5)

    outcome = closed_window_outcome(
        pushed_at=pushed, enqueued_at=pushed - timedelta(seconds=1)
    )

    assert outcome is PushCheckOutcome.expired


def test_completed_neither_enqueues_nor_keeps_the_stamp() -> None:
    """``completed`` ends a window like ``expired``: no sync, stamp pruned."""
    assert not PushCheckOutcome.completed.enqueues
    assert PushCheckOutcome.completed.prunes
    assert PushCheckOutcome.expired.prunes
    assert not any(
        outcome.prunes
        for outcome in PushCheckOutcome
        if outcome
        not in {PushCheckOutcome.completed, PushCheckOutcome.expired}
    )


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


def test_ltd_rebuilt_since_sync_falls_back_to_the_tombstone_date() -> None:
    """A tombstoned row with no rebuild recorded compares with its veto.

    A ``lifecycle_preemptive`` tombstone was written before keeper-sync
    imported the edition, so it records no rebuild and no sync; only a
    rebuild after the tombstone is new to it (PRD #830). Without the
    fallback every such row would read as never synced, and so rebuilt,
    on every push to its ref.
    """
    tombstoned = _NOW - timedelta(days=2)
    state = _edition_state(
        date_tombstoned=tombstoned, tombstone_reason="lifecycle_preemptive"
    )

    assert ltd_rebuilt_since_sync(state, ltd_date_rebuilt=_NOW)
    assert not ltd_rebuilt_since_sync(
        state, ltd_date_rebuilt=tombstoned - timedelta(days=30)
    )


def test_ltd_rebuilt_since_sync_edge_cases() -> None:
    """An unbuilt LTD edition never changes; an unsynced row always does."""
    assert not ltd_rebuilt_since_sync(
        _edition_state(date_rebuilt_seen=_NOW), ltd_date_rebuilt=None
    )
    assert not ltd_rebuilt_since_sync(_edition_state(), ltd_date_rebuilt=None)
    assert ltd_rebuilt_since_sync(_edition_state(), ltd_date_rebuilt=_NOW)
