"""Pure planners for keeper-sync tier-cron worker functions.

The three steady-state tier crons in
``docverse.worker.functions.keeper_sync`` (``keeper_sync_tier_main``,
``_discovery``, ``_other``) decide on every tick which projects to
refresh. The decisions are factored out as pure functions here so they
can be unit-tested with in-memory ``KeeperSyncState`` instances and so
the cron orchestration stays focused on I/O.

The planners take only the local ``keeper_sync_state`` snapshot and
the LTD-side view they need; they emit Booleans, never SQL or HTTP.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from docverse.storage.keeper_sync import KeeperSyncState

__all__ = [
    "TIER_OTHER_REFRESH_THRESHOLD",
    "is_unknown_resource",
    "should_refresh_main_edition",
    "should_refresh_other_edition",
]

#: How long a non-``main`` edition's local state may lag LTD before
#: ``keeper_sync_tier_other`` re-enqueues a refresh. One hour matches
#: PRD #275 user story 10's branch-edition staleness SLO. Exposed as a
#: module constant so the worker function and the unit tests pull from
#: the same source of truth.
TIER_OTHER_REFRESH_THRESHOLD = timedelta(hours=1)


def should_refresh_main_edition(
    *,
    state: KeeperSyncState | None,
    ltd_date_rebuilt: datetime | None,
) -> bool:
    """Decide whether tier_main should re-enqueue a project.

    Three positive cases:

    * ``state`` is ``None`` — we have never recorded the main edition.
    * ``state.docverse_id`` is ``None`` — the row is a placeholder, the
      project never finished an initial sync.
    * ``ltd_date_rebuilt`` is strictly newer than the recorded
      ``state.date_rebuilt_seen`` — LTD has rebuilt since our last
      pass.

    A ``None`` ``ltd_date_rebuilt`` means LTD has never rebuilt the
    edition, so there is nothing to chase. State whose
    ``date_rebuilt_seen`` is somehow ahead of LTD is left alone (most
    likely a clock skew; the safe move is to wait for LTD to catch
    up).
    """
    if state is None:
        return True
    if state.docverse_id is None:
        return True
    if ltd_date_rebuilt is None:
        return False
    if state.date_rebuilt_seen is None:
        return True
    return ltd_date_rebuilt > state.date_rebuilt_seen


def is_unknown_resource(state: KeeperSyncState | None) -> bool:
    """Return ``True`` when discovery has never seen the LTD resource.

    Trivially equivalent to ``state is None``. Lifted as a named
    function so the tier_discovery cron's call site reads as the rule
    it implements ("enqueue for resources we don't know about yet")
    rather than an ad-hoc nullness check, and so its contract is
    locked by the unit tests in ``scheduler_test.py``.
    """
    return state is None


def should_refresh_other_edition(
    *,
    state: KeeperSyncState,
    now: datetime,
    threshold: timedelta = TIER_OTHER_REFRESH_THRESHOLD,
) -> bool:
    """Decide whether tier_other should re-enqueue a non-main edition.

    A row whose ``date_last_synced`` is ``None`` is treated as stale
    (the next sync should record it). Otherwise the row is stale once
    its age (``now - date_last_synced``) reaches the configured
    ``threshold``; the comparison is ``>=`` so an edition synced exactly
    one threshold-period ago re-enters the refresh set on this tick
    instead of slipping to the next one.

    Callers pass ``now`` explicitly so tests can fix the wall clock.
    """
    if state.date_last_synced is None:
        return True
    return (now - state.date_last_synced) >= threshold
