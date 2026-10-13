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
- ``tier_main`` visits each stamped ref, and settles the map afterwards:
  a ref whose sync it enqueued is cleared, unless a newer push has
  stamped it again since, and the refs whose window has passed are
  pruned (:func:`settle_pushed_refs`), each reported as ``expired``.

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
    "PUSHED_REFS_CAP",
    "PushCheckOutcome",
    "is_in_push_window",
    "ltd_rebuilt_since_sync",
    "prune_pushed_refs",
    "read_pushed_refs",
    "settle_pushed_refs",
    "stamp_pushed_ref",
]

#: ``keeper_sync_state.annotations`` key on a project-resource state row
#: holding the push hints: a JSON object mapping each normalized ref to
#: the ISO-8601 time of its latest push. Written by the keeper-sync push
#: processor through :func:`stamp_pushed_ref`, and settled by
#: ``tier_main`` through :func:`settle_pushed_refs`.
ANNOTATION_GITHUB_PUSHED_REFS = "github_pushed_refs"


class PushCheckOutcome(StrEnum):
    """What ``tier_main``'s visit to one stamped ref found.

    Logged as ``outcome`` on each visit. :attr:`new_edition` and
    :attr:`rebuilt` are the outcomes that enqueue the project's sync
    (:attr:`enqueues`); a ref whose sync was enqueued is cleared from
    the map, and every other outcome but :attr:`expired` leaves its
    stamp for the next tick.
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

    expired = "expired"
    """The ref's window passed without a sync; its stamp is pruned."""

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
    if state is None or state.annotations is None:
        return {}
    raw = state.annotations.get(ANNOTATION_GITHUB_PUSHED_REFS)
    if not isinstance(raw, Mapping):
        return {}
    refs: dict[str, datetime] = {}
    for ref, value in raw.items():
        pushed_at = _parse_push_time(value)
        if isinstance(ref, str) and ref and pushed_at is not None:
            refs[ref] = pushed_at
    return refs


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
    reports it as ``expired`` (:func:`settle_pushed_refs`): pruning them
    here would drop that report whenever another ref of the repository
    is pushed first. They are the oldest pushes, so the cap drops them
    before any live ref. Every other annotation key on the row is
    carried over unchanged, so the caller can write the result back
    whole.

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
    newest = sorted(
        refs.items(), key=lambda item: (item[1], item[0]), reverse=True
    )[:PUSHED_REFS_CAP]
    prior = state.annotations or {}
    return {
        **prior,
        ANNOTATION_GITHUB_PUSHED_REFS: {
            stamped_ref: pushed_at.isoformat()
            for stamped_ref, pushed_at in newest
        },
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
    cleared: Mapping[str, datetime],
    now: datetime,
    window: timedelta,
) -> dict[str, Any]:
    """Return a project's annotations with a ``tier_main`` visit settled.

    ``tier_main`` reads the stamps, visits each ref, and then writes the
    map back from a fresh read of the row, which a push may have stamped
    again in between. A ref in ``cleared`` is dropped only while its
    stamp is still the push time the visit read: a newer push to it is
    a newer hint, and keeps its stamp for the next tick. Refs whose
    window has passed are pruned. Every other annotation key on the row
    is carried over unchanged.

    Parameters
    ----------
    state
        The project's ``keeper_sync_state`` row, freshly read.
    cleared
        The refs whose sync the visit enqueued, each with the push time
        the visit read for it.
    now
        The time of the visit.
    window
        How long a push keeps its ref on the fast path.

    Returns
    -------
    dict of str to Any
        The row's annotations to write back, with
        :data:`ANNOTATION_GITHUB_PUSHED_REFS` replaced.
    """
    refs = {
        ref: pushed_at
        for ref, pushed_at in read_pushed_refs(state).items()
        if cleared.get(ref) != pushed_at
    }
    kept = prune_pushed_refs(refs, now=now, window=window)
    prior = state.annotations or {}
    return {
        **prior,
        ANNOTATION_GITHUB_PUSHED_REFS: {
            ref: pushed_at.isoformat() for ref, pushed_at in kept.items()
        },
    }


def ltd_rebuilt_since_sync(
    state: KeeperSyncState, *, ltd_date_rebuilt: datetime | None
) -> bool:
    """Report whether LTD rebuilt an edition since keeper-sync synced it.

    The rebuild check ``tier_main`` runs on the edition tracking a
    pushed ref. LTD's ``date_rebuilt`` is compared with the
    ``date_rebuilt_seen`` the edition's state row recorded at its last
    sync or, when the row recorded none, with its ``date_last_synced``.
    An LTD edition that has never been rebuilt has nothing new to sync,
    and a row that records neither time has never been synced, so any
    rebuild is new to it.

    Parameters
    ----------
    state
        The edition's ``keeper_sync_state`` row.
    ltd_date_rebuilt
        The LTD edition's ``date_rebuilt``.
    """
    if ltd_date_rebuilt is None:
        return False
    reference = state.date_rebuilt_seen or state.date_last_synced
    if reference is None:
        return True
    return ltd_date_rebuilt > reference


def _parse_push_time(value: object) -> datetime | None:
    """Return a stamp's push time as an aware datetime, or ``None``."""
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
