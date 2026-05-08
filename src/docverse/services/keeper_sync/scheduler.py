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
from enum import StrEnum

from docverse.storage.keeper_sync import KeeperSyncState

__all__ = [
    "ANNOTATION_DATE_DISCOVERY_LAST_POLLED",
    "ANNOTATION_DATE_MAIN_LAST_POLLED",
    "ANNOTATION_DATE_OTHER_LAST_POLLED",
    "TIER_DISCOVERY_DORMANT_INTERVAL",
    "TIER_DISCOVERY_HOT_WINDOW",
    "TIER_MAIN_DORMANT_INTERVAL",
    "TIER_MAIN_HOT_WINDOW",
    "TIER_OTHER_DORMANT_INTERVAL",
    "TIER_OTHER_HOT_WINDOW",
    "TIER_OTHER_REFRESH_THRESHOLD",
    "Tier",
    "is_unknown_resource",
    "should_poll_for_tier",
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

#: Window after a project's last observed LTD ``main`` rebuild during
#: which ``keeper_sync_tier_discovery`` polls on its 30-minute cadence.
#: Mirrors ``TIER_MAIN_HOT_WINDOW`` because the dormancy signal is the
#: same ``date_rebuilt_seen`` predicate: a project whose ``main`` has
#: rebuilt recently is the same project whose branch editions are most
#: likely to have been added.
TIER_DISCOVERY_HOT_WINDOW = timedelta(days=14)

#: Maximum interval between LTD fetches for a dormant project on the
#: ``tier_discovery`` cadence. Once-a-day matches ``TIER_MAIN_DORMANT_
#: INTERVAL`` — the long-tail load-shed budget is shared across the
#: three reconciliation tiers. Once a dormant project rebuilds, the
#: next ``tier_main`` visit refreshes ``date_rebuilt_seen`` and the
#: planner naturally re-classifies it as hot for all three tiers.
TIER_DISCOVERY_DORMANT_INTERVAL = timedelta(hours=24)

#: Window after a project's last observed LTD ``main`` rebuild during
#: which ``keeper_sync_tier_other`` polls on its hourly cadence. Same
#: 14-day rationale as the other two tiers: ``date_rebuilt_seen`` is
#: shared dormancy state, so the hot/dormant cohorts agree across the
#: three planners.
TIER_OTHER_HOT_WINDOW = timedelta(days=14)

#: Maximum interval between LTD fetches for a dormant project on the
#: ``tier_other`` cadence. Once-a-day, matching the discovery and main
#: tiers; see :data:`TIER_DISCOVERY_DORMANT_INTERVAL` for the rationale
#: shared across all three tiers.
TIER_OTHER_DORMANT_INTERVAL = timedelta(hours=24)

#: ``keeper_sync_state.annotations`` key on a project-resource state row
#: holding the ISO-8601 timestamp of the last LTD fetch issued by
#: ``keeper_sync_tier_main`` for the project. Both the planner here and
#: ``_tier_main_for_org`` reference the same string; lifting it to a
#: module constant keeps writers and readers in lockstep.
ANNOTATION_DATE_MAIN_LAST_POLLED = "date_main_last_polled"

#: Companion to :data:`ANNOTATION_DATE_MAIN_LAST_POLLED` for the
#: discovery tier. ``_tier_discovery_for_org`` writes this stamp on
#: every polled visit; :func:`should_poll_for_tier` (with
#: ``tier=Tier.discovery``) reads it to clamp dormant projects to one
#: pass per ``TIER_DISCOVERY_DORMANT_INTERVAL``.
ANNOTATION_DATE_DISCOVERY_LAST_POLLED = "date_discovery_last_polled"

#: Companion to :data:`ANNOTATION_DATE_MAIN_LAST_POLLED` for the
#: ``tier_other`` cron. ``_tier_other_for_org`` writes this stamp on
#: every polled visit; :func:`should_poll_for_tier` (with
#: ``tier=Tier.other``) reads it to clamp dormant projects to one
#: pass per ``TIER_OTHER_DORMANT_INTERVAL``.
ANNOTATION_DATE_OTHER_LAST_POLLED = "date_other_last_polled"


class Tier(StrEnum):
    """Identifier for the three steady-state reconciliation tiers.

    Threaded through :func:`should_poll_for_tier` so a single planner
    body covers ``tier_main`` (5 min cadence), ``tier_discovery`` (30
    min), and ``tier_other`` (hourly). The string values double as a
    structlog key for cross-tier observability.
    """

    main = "main"
    discovery = "discovery"
    other = "other"


_TIER_ANNOTATION_KEYS: dict[Tier, str] = {
    Tier.main: ANNOTATION_DATE_MAIN_LAST_POLLED,
    Tier.discovery: ANNOTATION_DATE_DISCOVERY_LAST_POLLED,
    Tier.other: ANNOTATION_DATE_OTHER_LAST_POLLED,
}


def should_poll_for_tier(
    *,
    state: KeeperSyncState | None,
    now: datetime,
    tier: Tier,
    hot_window: timedelta,
    dormant_interval: timedelta,
) -> bool:
    """Decide whether a tier-cron should fetch LTD for a project this tick.

    Generalises :func:`should_poll_main_for_project` to all three
    steady-state tiers. The dormancy signal (``state.date_rebuilt_seen``)
    is shared across tiers — ``tier_main`` writes it on every successful
    poll and the other two tiers read it — so a project that goes hot
    on tier_main automatically goes hot on tier_discovery and
    tier_other within the same hot window. The per-tier rate-limit
    annotation (``date_<tier>_last_polled``) is independent so each
    tier clamps its own dormant cadence without interfering.

    Rules, in order:

    1. ``state is None`` — no project state row at all; this is the
       cold-start path. Poll.
    2. ``state.date_rebuilt_seen is None`` — we have a row but no
       observed LTD ``main`` rebuild yet. Poll so the next tick has a
       date to gate on.
    3. ``now - state.date_rebuilt_seen < hot_window`` — the project is
       hot. Always poll on the tier's fast cadence.
    4. Otherwise (dormant): consult the per-tier last-polled
       annotation. If absent, malformed, or older than
       ``dormant_interval``, poll. Otherwise skip — the project is
       rate-limited to one LTD fetch per ``dormant_interval``.

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
    last_polled_raw = annotations.get(_TIER_ANNOTATION_KEYS[tier])
    last_polled = _parse_annotation_datetime(last_polled_raw)
    if last_polled is None:
        return True
    return (now - last_polled) >= dormant_interval


def should_poll_main_for_project(
    *,
    state: KeeperSyncState | None,
    now: datetime,
    hot_window: timedelta = TIER_MAIN_HOT_WINDOW,
    dormant_interval: timedelta = TIER_MAIN_DORMANT_INTERVAL,
) -> bool:
    """Decide whether tier_main should fetch LTD for a project this tick.

    Thin wrapper around :func:`should_poll_for_tier` with
    ``tier=Tier.main`` baked in and tier_main's defaults applied.
    Preserves the original call shape for ``_tier_main_for_org`` and
    its tests; new callers should prefer :func:`should_poll_for_tier`
    with an explicit tier so the call site documents which tier's
    rate-limit annotation is in play.
    """
    return should_poll_for_tier(
        state=state,
        now=now,
        tier=Tier.main,
        hot_window=hot_window,
        dormant_interval=dormant_interval,
    )


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
