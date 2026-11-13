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
  stamp and so bounded by the stamps' cap: the time of the enqueue, and
  the LTD ``date_rebuilt`` it was for (:class:`PushedRefEnqueue`). A ref
  whose window has passed is reported as ``completed`` when
  ``tier_main`` enqueued its sync at or after its latest push and as
  ``expired`` when it never did (:func:`closed_window_outcome`). Both
  say what ``tier_main`` did for the ref, not whether a given upload was
  synced;
- a ref that reads ``rebuilt`` or ``new_edition`` again for the rebuild
  its last enqueue was for — a sync that completed or failed without
  recording that rebuild — is enqueued again only once
  :data:`PUSHED_REF_REENQUEUE_BACKOFF` has passed since that enqueue,
  while a new rebuild of the ref is enqueued at once
  (:func:`pushed_ref_enqueue_due`).

Nothing here does I/O: the callers read and write the row.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from docverse_server.storage.keeper_sync import KeeperSyncState

__all__ = [
    "ANNOTATION_GITHUB_PUSHED_REFS",
    "ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED",
    "PUSHED_REFS_CAP",
    "PUSHED_REF_REENQUEUE_BACKOFF",
    "PushCheckOutcome",
    "PushedRefEnqueue",
    "closed_window_outcome",
    "is_in_push_window",
    "ltd_rebuilt_since_sync",
    "prune_pushed_refs",
    "pushed_ref_enqueue_due",
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
#: recording, for each stamped ref, the last sync ``tier_main`` enqueued
#: for it: a JSON object mapping the ref to ``date_enqueued``, the
#: ISO-8601 time of the enqueue, and ``date_rebuilt``, the LTD
#: ``date_rebuilt`` the enqueue was for, or null when it was for none
#: (:class:`PushedRefEnqueue`). Written and pruned with the ref's stamp by
#: :func:`settle_pushed_refs`. Read on each visit to hold a repeated
#: enqueue for the same rebuild to :data:`PUSHED_REF_REENQUEUE_BACKOFF`
#: (:func:`pushed_ref_enqueue_due`), and when the ref's window passes to
#: tell a window in which ``tier_main`` enqueued the ref's sync
#: (``completed``) from one in which it never did (``expired``). A row
#: without the key has never enqueued.
ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED = "github_pushed_refs_enqueued"

#: How long ``tier_main`` waits after enqueueing a pushed ref's sync
#: before it enqueues it again for the same LTD rebuild. A sync that
#: completes or fails without recording the rebuild on the edition's
#: state row — its build copy failing, or the live-refs read of a
#: lifecycle tombstone's review failing on GitHub — leaves the ref
#: reading ``rebuilt`` on every tick; without the backoff each tick of
#: the window would enqueue the project's whole sync again for it. A new
#: rebuild of the ref is not held back. Fifteen minutes is three
#: ``tier_main`` ticks, so a rebuild enqueues at most four times in the
#: default one-hour window. A tick compares its own start time with the
#: enqueue, made a little after the start of an earlier tick, so on the
#: five-minute cadence the tick 15 minutes on usually falls a few
#: seconds short and the re-enqueue comes on the tick after, 20 minutes
#: on.
PUSHED_REF_REENQUEUE_BACKOFF = timedelta(minutes=15)

_DATE_ENQUEUED = "date_enqueued"
"""Key of an enqueue's time in one entry of the enqueue map."""

_DATE_REBUILT = "date_rebuilt"
"""Key of the LTD rebuild an enqueue was for in one entry of the map."""


@dataclass(frozen=True, slots=True)
class PushedRefEnqueue:
    """``tier_main``'s last enqueue of the project's sync for one pushed ref.

    One entry of :data:`ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED`.
    """

    date_enqueued: datetime
    """When ``tier_main`` enqueued the sync."""

    date_rebuilt: datetime | None
    """The LTD ``date_rebuilt`` of the ref's edition the enqueue was for.

    ``None`` for an enqueue for no rebuild: a ``new_edition``, whose
    check compares no rebuild, or a ``rebuilt`` edition LTD has never
    rebuilt. Also ``None`` when :attr:`rebuild_known` is false.
    """

    rebuild_known: bool = True
    """Whether the entry records the rebuild the enqueue was for.

    False for an entry holding only an enqueue time — a bare ISO-8601
    string, as the map held before it recorded rebuilds, or an object
    without ``date_rebuilt`` — which :func:`pushed_ref_enqueue_due`
    never holds back, having nothing to compare.
    """


class PushCheckOutcome(StrEnum):
    """What ``tier_main``'s visit to one stamped ref found.

    Logged as ``outcome`` on each visit. :attr:`new_edition` and
    :attr:`rebuilt` are the outcomes that enqueue the project's sync
    (:attr:`enqueues`), unless the ref's last enqueue was for the same
    rebuild too recently (:func:`pushed_ref_enqueue_due`). Every
    outcome but :attr:`completed` and
    :attr:`expired` leaves the ref's stamp for the next tick, including
    one whose sync was enqueued: the ref is checked on every tick until
    its window passes, and a later rebuild inside the window enqueues
    the sync again. Once the window has passed the ref is pruned
    (:attr:`prunes`), as :attr:`completed` when ``tier_main`` enqueued
    its sync at or after its latest push and :attr:`expired` when it
    never did (:func:`closed_window_outcome`).
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
    """The ref's window passed, and ``tier_main`` enqueued the project's
    sync for it at least once at or after its latest push; its stamp is
    pruned. The enqueue may have been for an earlier push's upload that
    a tick caught only after the latest push."""

    expired = "expired"
    """The ref's window passed, and ``tier_main`` never enqueued the
    project's sync for it in that window; its stamp is pruned. Another
    sync may still have imported the push's upload: one that already
    held the project's slot, one enqueued for another ref or by the
    ``main`` check, or ``tier_other``'s."""

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
) -> dict[str, PushedRefEnqueue]:
    """Return ``tier_main``'s last enqueue of each stamped ref's sync.

    Read as defensively as :func:`read_pushed_refs`: a missing or
    non-object map reads as empty, so a row written before the map
    existed reads as no ref ever enqueued, and a malformed entry — one
    whose ``date_enqueued`` is missing or does not parse, or whose
    ``date_rebuilt`` is neither null nor a time — is skipped rather than
    raised on, reading as never enqueued. An entry holding only an
    enqueue time, a bare ISO-8601 string or an object without
    ``date_rebuilt``, reads as an enqueue whose rebuild is unknown
    (:attr:`PushedRefEnqueue.rebuild_known`).

    Parameters
    ----------
    state
        The project's ``keeper_sync_state`` row, or ``None`` when the
        project has none.

    Returns
    -------
    dict of str to PushedRefEnqueue
        Every well-formed entry of
        :data:`ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED`, keyed by
        normalized ref.
    """
    enqueues: dict[str, PushedRefEnqueue] = {}
    for ref, value in _read_ref_map(
        state, ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED
    ).items():
        enqueue = _parse_enqueue(value)
        if enqueue is not None:
            enqueues[ref] = enqueue
    return enqueues


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
    enqueued: Mapping[str, PushedRefEnqueue],
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
    ref's earlier entry, with the rebuild it was for. The map is pruned
    with the stamps, so it keeps an entry only for a ref still stamped
    once the expired refs are gone (a ref the push processor's cap
    dropped loses its entry here too). It has no cap of its own: holding
    at most one entry per stamp written back, it is bounded by the
    stamps, and every ref still stamped keeps its entry, so a ref that
    did enqueue is reported ``completed`` when its window passes. An
    entry the visit left alone is written back as read, one whose
    rebuild is unknown without ``date_rebuilt``. Every other annotation
    key on the row is carried over unchanged.

    Parameters
    ----------
    state
        The project's ``keeper_sync_state`` row, freshly read.
    enqueued
        The refs whose sync the visit enqueued, each with that enqueue;
        empty when it enqueued none.
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
    enqueues = {
        **read_pushed_refs_enqueued(state),
        **enqueued,
    }
    prior = state.annotations or {}
    return {
        **prior,
        ANNOTATION_GITHUB_PUSHED_REFS: _as_iso(kept),
        ANNOTATION_GITHUB_PUSHED_REFS_ENQUEUED: {
            ref: _enqueue_as_json(enqueue)
            for ref, enqueue in enqueues.items()
            if ref in kept
        },
    }


def pushed_ref_enqueue_due(
    last: PushedRefEnqueue | None,
    *,
    date_rebuilt: datetime | None,
    now: datetime,
) -> bool:
    """Report whether a ref that calls for its sync may enqueue it now.

    The rule ``tier_main`` applies to a ref whose visit found
    ``rebuilt`` or ``new_edition``. It is enqueued at once when it has
    no enqueue on record, when the one on record does not say which
    rebuild it was for, or when LTD's ``date_rebuilt`` now differs from
    the rebuild that enqueue was for: LTD rebuilt the edition again, or
    the last enqueue was for a build-less ``new_edition`` and the
    edition now has a rebuild. When it is the same rebuild — a null one
    included, as for a ``new_edition`` found again — the ref is enqueued
    again only once :data:`PUSHED_REF_REENQUEUE_BACKOFF` has passed since
    the last enqueue, so a sync that completes or fails without
    recording the rebuild is not re-run on every tick.

    Parameters
    ----------
    last
        The ref's last enqueue, from :func:`read_pushed_refs_enqueued`,
        or ``None`` when none is on record.
    date_rebuilt
        The LTD ``date_rebuilt`` the visit compared: the ref's edition's
        for ``rebuilt``, and ``None`` for ``new_edition``.
    now
        The time of the visit.
    """
    if last is None or not last.rebuild_known:
        return True
    if last.date_rebuilt != date_rebuilt:
        return True
    return now - last.date_enqueued >= PUSHED_REF_REENQUEUE_BACKOFF


def closed_window_outcome(
    *, pushed_at: datetime, enqueued_at: datetime | None
) -> PushCheckOutcome:
    """Return how a ref whose window has passed is reported.

    :attr:`~PushCheckOutcome.completed` when ``tier_main`` enqueued the
    ref's sync in the window of the push its stamp records, that is at
    or after that push, and :attr:`~PushCheckOutcome.expired` otherwise.
    An enqueue from before the push was made in an earlier push's
    window, which a later push to the ref restarted, so it does not
    count for this one.

    Only the times are compared, so the outcome says what ``tier_main``
    did for the ref, not which upload it synced: an enqueue at or after
    the push counts even when it was for an earlier push's upload that a
    tick caught late, so a push whose own CI never uploads can still be
    ``completed``. And ``tier_main`` records only the enqueues its own
    visits made, so a ref whose upload another sync imported first —
    one that already held the project's slot, one enqueued for another
    ref or by the ``main`` check, or ``tier_other``'s — is ``expired``
    unless a later rebuild in the window was enqueued for it.

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


def _enqueue_as_json(enqueue: PushedRefEnqueue) -> dict[str, str | None]:
    """Return one enqueue as the enqueue map stores it.

    An enqueue whose rebuild is unknown is written without
    ``date_rebuilt``, so it reads back as unknown rather than as an
    enqueue for no rebuild.
    """
    stored: dict[str, str | None] = {
        _DATE_ENQUEUED: enqueue.date_enqueued.isoformat()
    }
    if enqueue.rebuild_known:
        stored[_DATE_REBUILT] = (
            enqueue.date_rebuilt.isoformat()
            if enqueue.date_rebuilt is not None
            else None
        )
    return stored


def _read_ref_map(state: KeeperSyncState | None, key: str) -> dict[str, Any]:
    """Return the ref-keyed map under annotation ``key``, read defensively.

    A missing or non-object map reads as empty, and an entry whose ref is
    not a non-empty string is skipped; the values are left to the caller.
    """
    if state is None or state.annotations is None:
        return {}
    raw = state.annotations.get(key)
    if not isinstance(raw, Mapping):
        return {}
    return {
        ref: value
        for ref, value in raw.items()
        if isinstance(ref, str) and ref
    }


def _read_ref_times(
    state: KeeperSyncState | None, key: str
) -> dict[str, datetime]:
    """Return the ref-to-time map under annotation ``key``, read defensively.

    A missing or non-object map reads as empty, and an entry whose ref is
    not a non-empty string or whose time does not parse is skipped.
    """
    times: dict[str, datetime] = {}
    for ref, value in _read_ref_map(state, key).items():
        parsed = _parse_push_time(value)
        if parsed is not None:
            times[ref] = parsed
    return times


def _parse_enqueue(value: object) -> PushedRefEnqueue | None:
    """Return one entry of the enqueue map, or ``None`` if it is malformed.

    A bare time is an enqueue whose rebuild is unknown; an object needs a
    ``date_enqueued`` time, and a ``date_rebuilt`` that is null or a
    time, or none at all for an unknown rebuild.
    """
    if not isinstance(value, Mapping):
        date_enqueued = _parse_push_time(value)
        if date_enqueued is None:
            return None
        return PushedRefEnqueue(
            date_enqueued=date_enqueued, date_rebuilt=None, rebuild_known=False
        )
    date_enqueued = _parse_push_time(value.get(_DATE_ENQUEUED))
    if date_enqueued is None:
        return None
    if _DATE_REBUILT not in value:
        return PushedRefEnqueue(
            date_enqueued=date_enqueued, date_rebuilt=None, rebuild_known=False
        )
    raw_rebuilt = value[_DATE_REBUILT]
    if raw_rebuilt is None:
        return PushedRefEnqueue(date_enqueued=date_enqueued, date_rebuilt=None)
    date_rebuilt = _parse_push_time(raw_rebuilt)
    if date_rebuilt is None:
        return None
    return PushedRefEnqueue(
        date_enqueued=date_enqueued, date_rebuilt=date_rebuilt
    )


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
