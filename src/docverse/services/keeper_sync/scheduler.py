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
    "ANNOTATION_DATE_MAIN_LAST_POLLED",
    "TIER_MAIN_DORMANT_INTERVAL",
    "TIER_MAIN_HOT_WINDOW",
    "TIER_OTHER_REFRESH_THRESHOLD",
    "is_unknown_resource",
    "should_poll_main_for_project",
    "should_refresh_main_edition",
    "should_refresh_other_edition",
]

#: How long a non-``main`` edition's local state may lag LTD before
#: ``keeper_sync_tier_other`` re-enqueues a refresh. One hour matches
#: PRD #275 user story 10's branch-edition staleness SLO. Exposed as a
#: module constant so the worker function and the unit tests pull from
#: the same source of truth.
TIER_OTHER_REFRESH_THRESHOLD = timedelta(hours=1)

#: Window after a project's last observed LTD ``main`` rebuild during
#: which ``keeper_sync_tier_main`` always polls on its 5-minute cadence.
#: Set to two weeks so any project rebuilt in the recent past stays on
#: the hot SLO; production LTD has ~1500 projects but the long tail
#: hasn't rebuilt in months, and polling them every 5 min sustained
#: ~5 RPS to LTD purely on this cron.
TIER_MAIN_HOT_WINDOW = timedelta(days=14)

#: Maximum interval between LTD fetches for a dormant project (one whose
#: last observed ``date_rebuilt`` is older than ``TIER_MAIN_HOT_WINDOW``).
#: Once-a-day is enough to discover late rebuilds without contributing
#: meaningful load to LTD; shorter would re-create the long-tail
#: pressure this planner exists to bound.
TIER_MAIN_DORMANT_INTERVAL = timedelta(hours=24)

#: ``keeper_sync_state.annotations`` key on a project-resource state row
#: holding the ISO-8601 timestamp of the last LTD fetch issued by
#: ``keeper_sync_tier_main`` for the project. Both the planner here and
#: ``_tier_main_for_org`` reference the same string; lifting it to a
#: module constant keeps writers and readers in lockstep.
ANNOTATION_DATE_MAIN_LAST_POLLED = "date_main_last_polled"


def should_poll_main_for_project(
    *,
    state: KeeperSyncState | None,
    now: datetime,
    hot_window: timedelta = TIER_MAIN_HOT_WINDOW,
    dormant_interval: timedelta = TIER_MAIN_DORMANT_INTERVAL,
) -> bool:
    """Decide whether tier_main should fetch LTD for a project this tick.

    Splits the in-scope project list into hot and dormant cohorts so
    the long tail of never-rebuilt projects does not pin tier_main to
    1500 LTD fetches every 5 min (PRD-scale).

    Rules, in order:

    1. ``state is None`` — no project state row at all; this is the
       initial-discovery path. Poll.
    2. ``state.date_rebuilt_seen is None`` — we have a row but
       ``_tier_main_for_org`` has never recorded LTD's ``main`` rebuild
       timestamp on it (typical for a project that's only been touched
       by the operator-driven backfill). Poll so the next tick has a
       date to gate on.
    3. ``now - state.date_rebuilt_seen < hot_window`` — the project's
       ``main`` edition rebuilt recently enough to count as hot. Always
       poll on the hot cadence.
    4. Otherwise (dormant): consult
       ``state.annotations[ANNOTATION_DATE_MAIN_LAST_POLLED]``. If
       absent, malformed, or older than ``dormant_interval``, poll.
       Otherwise skip — the project is rate-limited to one LTD fetch
       per ``dormant_interval``.

    The hot-window comparison is strict ``<``; an exactly-at-window
    state row falls through to the dormant gate so the rate-limit
    contract on hot↔dormant boundary projects is one rule, not two.
    """
    if state is None:
        return True
    if state.date_rebuilt_seen is None:
        return True
    if (now - state.date_rebuilt_seen) < hot_window:
        return True
    annotations = state.annotations or {}
    last_polled_raw = annotations.get(ANNOTATION_DATE_MAIN_LAST_POLLED)
    last_polled = _parse_annotation_datetime(last_polled_raw)
    if last_polled is None:
        return True
    return (now - last_polled) >= dormant_interval


def _parse_annotation_datetime(raw: object) -> datetime | None:
    """Return a datetime from a JSONB annotation value, or ``None``.

    Annotation values round-trip through JSONB, which has no native
    datetime type — they're stored as ISO-8601 strings. Pydantic's
    in-memory representation can carry a real ``datetime`` (e.g. from a
    direct upsert in tests). Accept both shapes; treat anything else
    as missing so a malformed annotation re-polls instead of wedging
    the planner.
    """
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None
    return None


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
