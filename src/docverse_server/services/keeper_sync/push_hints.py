"""Push hints: the refs GitHub says a keeper-synced project was pushed to.

A GitHub ``push`` cannot sync anything by itself: the repository's own
CI builds the docs and uploads them to LTD Keeper afterwards. It is a
*hint* that LTD is about to change, and keeper-sync answers it with a
bounded window of targeted polling (PRD #803). The hint lives on the
project's ``keeper_sync_state`` row, in the ``github_pushed_refs``
annotation: a map from each pushed ref — normalized, so ``main`` or
``v1.0`` rather than ``refs/heads/main`` — to the ISO-8601 time of its
latest push.

The rules for that map are factored out here as pure functions, in the
style of :mod:`~docverse_server.services.keeper_sync.scheduler`, so the
push processor that writes it and the tier crons that read it agree on
one shape, and so the rules can be unit-tested on in-memory
:class:`~docverse_server.storage.keeper_sync.KeeperSyncState` rows:

- a stamp records a ref's push time, and a repeated push to the same ref
  overwrites it, extending the window;
- a ref is inside the window while less than ``window`` has passed since
  its push; a stamp leaves the rest in place, for ``tier_main`` to prune;
- the map holds at most :data:`PUSHED_REFS_CAP` refs, dropping the
  oldest pushes first, so a repository that pushes many refs at once
  cannot grow the row without bound;
- ``tier_main`` visits each stamped ref on every tick until its window
  passes, including after a visit that enqueued the ref's sync, so a ref
  can enqueue more than once in its window: LTD can rebuild it once per
  CI run of several pushes, or create its edition before the first
  upload lands and rebuild it after. ``tier_main`` settles the map after
  each visit, pruning only the refs whose window has passed
  (:func:`settle_pushed_refs`);
- ``tier_main`` records each ref's last enqueue beside its stamp, in the
  ``github_pushed_refs_enqueued`` annotation, which is pruned with the
  stamp and capped like it. A ref whose window has passed is reported
  as ``completed`` when it enqueued in that window and as ``expired``
  when it never did (:func:`closed_window_outcome`).

Nothing here does I/O: the callers read and write the row.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from docverse_server.storage.keeper_sync import KeeperSyncState

__all__ = [
    "ANNOTATION_GITHUB_PUSHED_REFS",
    "ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED",
    "PUSHED_REFS_CAP",
    "PushCheckOutcome",
    "closed_window_outcome",
    "is_in_push_window",
    "ltd_rebuilt_since_sync",
    "prune_pushed_refs",
    "read_pushed_refs",
    "read_pushed_refs_enqueued",
    "settle_pushed_refs",
    "stamp_pushed_ref",
]

#: ``keeper_sync_state.annotations`` key on a project-resource state row
#: holding the push hints: a JSON object mapping each normalized ref to
#: the ISO-8601 time of its latest push. Written by the keeper-sync push
#: processor through :func:`stamp_pushed_ref`, and settled by
#: ``tier_main`` through :func:`settle_pushed_refs`.
ANNOTATION_GITHUB_PUSHED_REFS = "github_pushed_refs"

#: ``keeper_sync_state.annotations`` key on a project-resource state row
#: recording, for each stamped ref, the ISO-8601 time of the last sync
#: ``tier_main`` enqueued for it. Written and pruned with the ref's stamp
#: by :func:`settle_pushed_refs`, and read when the ref's window passes
#: to tell a push that produced a sync (``completed``) from one that
#: never did (``expired``). A row without the key has never enqueued.
ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED = "github_pushed_refs_enqueued"


class PushCheckOutcome(StrEnum):
    """What ``tier_main``'s visit to one stamped ref found.

    Logged as ``outcome`` on each visit. :attr:`new_edition` and
    :attr:`rebuilt` are the outcomes that enqueue the project's sync
    (:attr:`enqueues`). Every outcome but :attr:`completed` and
    :attr:`expired` leaves the ref's stamp for the next tick, including
    one whose sync was enqueued: the ref is checked on every tick until
    its window passes, and a later rebuild inside the window enqueues
    the sync again. Once the window has passed the ref is pruned
    (:attr:`prunes`), as :attr:`completed` when it enqueued in that
    window and :attr:`expired` when it never did
    (:func:`closed_window_outcome`).
    """

    new_edition = "new_edition"
    """No synced edition tracks the ref yet, and LTD lists one
    keeper-sync has not seen that tracks it: the edition LTD created for
    the push's CI upload."""

    rebuilt = "rebuilt"
    """LTD rebuilt the edition tracking the ref since keeper-sync last
    synced it."""

    unchanged = "unchanged"
    """The edition tracking the ref is as keeper-sync last synced it: the
    push's CI has not uploaded yet."""

    completed = "completed"
    """The ref's window passed after it enqueued the project's sync at
    least once in that window; its stamp is pruned."""

    expired = "expired"
    """The ref's window passed without it enqueueing the project's sync:
    the push never produced a sync. Its stamp is pruned."""

    not_found = "not_found"
    """No synced edition tracks the ref, and LTD lists no edition
    keeper-sync has not seen that tracks it: LTD has not created the
    ref's edition yet."""

    error = "error"
    """LTD failed to answer; the stamp is kept for the next tick."""

    @property
    def enqueues(self) -> bool:
        """Whether this outcome calls for the project's sync."""
        return self in {PushCheckOutcome.new_edition, PushCheckOutcome.rebuilt}

    @property
    def prunes(self) -> bool:
        """Whether the ref's window has passed, so its stamp is pruned."""
        return self in {PushCheckOutcome.completed, PushCheckOutcome.expired}


#: Most refs one project's push hints hold. A stamp that would leave
#: more drops the oldest pushes first. Twenty is far above the handful
#: of branches a documentation repository has in flight at once, and
#: bounds the row against a repository that pushes many tags in one go.
PUSHED_REFS_CAP = 20


def read_pushed_refs(state: KeeperSyncState | None) -> dict[str, datetime]:
    """Return a project's stamped refs and the time of each one's push.

    Read defensively, as the annotation round-trips through JSONB: a
    missing or non-object map reads as empty, and an entry whose time
    is not an ISO-8601 string (or a ``datetime``, as an in-memory row
    may carry) is skipped rather than raised on. A time without an
    offset is read as UTC, so it can be compared with an aware clock.

    Parameters
    ----------
    state
        The project's ``keeper_sync_state`` row, or ``None`` when the
        project has none.

    Returns
    -------
    dict of str to datetime
        Every well-formed stamp, keyed by normalized ref. Includes refs
        whose window has passed; see :func:`prune_pushed_refs`.
    """
    return _read_ref_times(state, ANNOTATION_GITHUB_PUSHED_REFS)


def read_pushed_refs_enqueued(
    state: KeeperSyncState | None,
) -> dict[str, datetime]:
    """Return when ``tier_main`` last enqueued each stamped ref's sync.

    Read as defensively as :func:`read_pushed_refs`: a missing or
    non-object map reads as empty, so a row written before the map
    existed reads as no ref ever enqueued, and a malformed entry is
    skipped rather than raised on.

    Parameters
    ----------
    state
        The project's ``keeper_sync_state`` row, or ``None`` when the
        project has none.

    Returns
    -------
    dict of str to datetime
        Every well-formed entry of
        :data:`ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED`, keyed by
        normalized ref.
    """
    return _read_ref_times(state, ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED)


def prune_pushed_refs(
    refs: Mapping[str, datetime], *, now: datetime, window: timedelta
) -> dict[str, datetime]:
    """Return the refs still inside the push window at ``now``.

    A ref is inside the window while less than ``window`` has passed
    since its push; one pushed exactly ``window`` ago has expired.

    Parameters
    ----------
    refs
        Stamped refs and their push times, as from
        :func:`read_pushed_refs`.
    now
        The current time.
    window
        How long a push keeps its ref on the fast path.
    """
    return {
        ref: pushed_at
        for ref, pushed_at in refs.items()
        if now - pushed_at < window
    }


def stamp_pushed_ref(
    state: KeeperSyncState, *, ref: str, now: datetime
) -> dict[str, Any]:
    """Return a project's annotations with a push to ``ref`` stamped.

    The ref's push time becomes ``now``, overwriting any earlier stamp
    for it. The map is capped at :data:`PUSHED_REFS_CAP`, keeping the
    most recent pushes. Refs whose window has passed are left in place
    for ``tier_main``, which prunes each one on its next visit and
    reports it as ``completed`` or ``expired``
    (:func:`settle_pushed_refs`): pruning them here would drop that
    report whenever another ref of the repository is pushed first. They
    are the oldest pushes, so the cap drops them before any live ref.
    Every other annotation key on the row, ``tier_main``'s
    :data:`ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED` included, is carried
    over unchanged, so the caller can write the result back whole.

    Parameters
    ----------
    state
        The project's ``keeper_sync_state`` row as last read.
    ref
        The pushed ref, normalized (``main``, not ``refs/heads/main``).
    now
        The time of the push.

    Returns
    -------
    dict of str to Any
        The row's annotations to write back, with
        :data:`ANNOTATION_GITHUB_PUSHED_REFS` replaced.
    """
    refs = read_pushed_refs(state)
    refs[ref] = now
    prior = state.annotations or {}
    return {
        **prior,
        ANNOTATION_GITHUB_PUSHED_REFS: _as_iso(_newest(refs)),
    }


def is_in_push_window(
    state: KeeperSyncState | None, *, now: datetime, window: timedelta
) -> bool:
    """Report whether any of a project's pushed refs is inside the window.

    Parameters
    ----------
    state
        The project's ``keeper_sync_state`` row, or ``None`` when the
        project has none.
    now
        The current time.
    window
        How long a push keeps its ref on the fast path.
    """
    return bool(
        prune_pushed_refs(read_pushed_refs(state), now=now, window=window)
    )


def settle_pushed_refs(
    state: KeeperSyncState,
    *,
    enqueued: Mapping[str, datetime],
    now: datetime,
    window: timedelta,
) -> dict[str, Any]:
    """Return a project's annotations with a ``tier_main`` visit settled.

    ``tier_main`` reads the stamps, visits each ref, and then writes the
    map back from a fresh read of the row, which a push may have stamped
    again in between. Only the refs whose window has passed by the fresh
    read are pruned: a ref keeps its stamp until then whatever its visit
    found, including a visit that enqueued its sync, so a later LTD
    rebuild of the ref inside the window is caught too. A newer push to a
    ref is judged by its newer stamp, which restarts the window.

    The enqueues the visit made are recorded in
    :data:`ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED`, each overwriting the
    ref's earlier entry. The map is pruned with the stamps, so it keeps
    an entry only for a ref still stamped once the expired refs are gone
    (a ref the push processor's cap dropped loses its entry here too),
    and is capped at :data:`PUSHED_REFS_CAP`, keeping the latest
    enqueues. Every other annotation key on the row is carried over
    unchanged.

    Parameters
    ----------
    state
        The project's ``keeper_sync_state`` row, freshly read.
    enqueued
        The refs whose sync the visit enqueued, each with the time of
        that enqueue; empty when it enqueued none.
    now
        The time of the visit.
    window
        How long a push keeps its ref on the fast path.

    Returns
    -------
    dict of str to Any
        The row's annotations to write back, with
        :data:`ANNOTATION_GITHUB_PUSHED_REFS` and
        :data:`ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED` replaced.
    """
    kept = prune_pushed_refs(read_pushed_refs(state), now=now, window=window)
    enqueue_times = {
        ref: enqueued_at
        for ref, enqueued_at in {
            **read_pushed_refs_enqueued(state),
            **enqueued,
        }.items()
        if ref in kept
    }
    prior = state.annotations or {}
    return {
        **prior,
        ANNOTATION_GITHUB_PUSHED_REFS: _as_iso(kept),
        ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED: _as_iso(
            _newest(enqueue_times)
        ),
    }


def closed_window_outcome(
    *, pushed_at: datetime, enqueued_at: datetime | None
) -> PushCheckOutcome:
    """Return how a ref whose window has passed is reported.

    :attr:`~PushCheckOutcome.completed` when ``tier_main`` enqueued the
    ref's sync in the window of the push its stamp records, that is at
    or after that push, and :attr:`~PushCheckOutcome.expired` otherwise.
    An enqueue from before the push was made for an earlier push's
    window, which a later push to the ref restarted, so it does not
    count for this one.

    Parameters
    ----------
    pushed_at
        The ref's push time, from its stamp.
    enqueued_at
        The ref's last enqueue, from
        :data:`ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED`, or ``None`` when
        none is recorded.
    """
    if enqueued_at is not None and enqueued_at >= pushed_at:
        return PushCheckOutcome.completed
    return PushCheckOutcome.expired


def ltd_rebuilt_since_sync(
    state: KeeperSyncState, *, ltd_date_rebuilt: datetime | None
) -> bool:
    """Report whether LTD rebuilt an edition since keeper-sync synced it.

    The rebuild check ``tier_main`` runs on the edition tracking a
    pushed ref. LTD's ``date_rebuilt`` is compared with the
    ``date_rebuilt_seen`` the edition's state row recorded at its last
    sync or, when the row recorded none, with its ``date_last_synced``
    — or, on a tombstoned row, its ``date_tombstoned``: a
    ``lifecycle_preemptive`` tombstone vetoed the edition before any
    sync, so only a rebuild after the veto is new to it, the same
    baseline keeper-sync's re-evaluation of the tombstone reads (PRD
    #830). An LTD edition that has never been rebuilt has nothing new
    to sync, and a row that records none of these times has never been
    synced, so any rebuild is new to it.

    Parameters
    ----------
    state
        The edition's ``keeper_sync_state`` row.
    ltd_date_rebuilt
        The LTD edition's ``date_rebuilt``.
    """
    if ltd_date_rebuilt is None:
        return False
    reference = state.date_rebuilt_seen or (
        state.date_tombstoned
        if state.date_tombstoned is not None
        else state.date_last_synced
    )
    if reference is None:
        return True
    return ltd_date_rebuilt > reference


def _newest(times: Mapping[str, datetime]) -> dict[str, datetime]:
    """Return the :data:`PUSHED_REFS_CAP` latest entries, latest first.

    Ties on time are broken by ref, so the cut is deterministic.
    """
    return dict(
        sorted(
            times.items(), key=lambda item: (item[1], item[0]), reverse=True
        )[:PUSHED_REFS_CAP]
    )


def _as_iso(times: Mapping[str, datetime]) -> dict[str, str]:
    """Return a ref-to-time map as it is stored: ISO-8601 strings."""
    return {ref: at.isoformat() for ref, at in times.items()}


def _read_ref_times(
    state: KeeperSyncState | None, key: str
) -> dict[str, datetime]:
    """Return the ref-to-time map under annotation ``key``, read defensively.

    A missing or non-object map reads as empty, and an entry whose ref is
    not a non-empty string or whose time does not parse is skipped.
    """
    if state is None or state.annotations is None:
        return {}
    raw = state.annotations.get(key)
    if not isinstance(raw, Mapping):
        return {}
    times: dict[str, datetime] = {}
    for ref, value in raw.items():
        parsed = _parse_push_time(value)
        if isinstance(ref, str) and ref and parsed is not None:
            times[ref] = parsed
    return times


def _parse_push_time(value: object) -> datetime | None:
    """Return a map entry's time as an aware datetime, or ``None``."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
