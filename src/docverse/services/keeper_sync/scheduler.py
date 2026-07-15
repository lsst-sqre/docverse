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

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Literal

from docverse.storage.keeper_sync import KeeperSyncState

__all__ = [
    "ANNOTATION_DATE_DISCOVERY_LAST_POLLED",
    "ANNOTATION_DATE_MAIN_LAST_POLLED",
    "ANNOTATION_DATE_OTHER_LAST_POLLED",
    "TIER_DISCOVERY_CRON_INTERVAL",
    "TIER_DISCOVERY_DORMANT_INTERVAL",
    "TIER_DISCOVERY_DORMANT_JITTER",
    "TIER_DISCOVERY_HOT_WINDOW",
    "TIER_MAIN_CRON_INTERVAL",
    "TIER_MAIN_DORMANT_INTERVAL",
    "TIER_MAIN_DORMANT_JITTER",
    "TIER_MAIN_HOT_WINDOW",
    "TIER_OTHER_CRON_INTERVAL",
    "TIER_OTHER_DORMANT_INTERVAL",
    "TIER_OTHER_DORMANT_JITTER",
    "TIER_OTHER_HOT_WINDOW",
    "TIER_OTHER_REFRESH_THRESHOLD",
    "Tier",
    "TierCohort",
    "TierStatus",
    "explain_tier_status",
    "is_unknown_resource",
    "next_cron_tick_at_or_after",
    "should_poll_for_tier",
    "should_poll_main_for_project",
    "should_refresh_main_edition",
    "should_refresh_other_edition",
    "stable_hash_fraction",
]


#: Tier-cohort labels surfaced to operators by :func:`explain_tier_status`.
#: ``hot`` mirrors the planner's rules 2 and 3 (no recorded rebuild yet, or
#: rebuilt within ``hot_window``); ``dormant`` mirrors rule 4 (older than
#: ``hot_window``); ``unseen`` is the explainer-only label for "no state row
#: exists" — the planner returns True (poll) for that case but the operator
#: cohort is "we have never observed this project on this tier".
TierCohort = Literal["hot", "dormant", "unseen"]

#: How long a non-``main`` edition's local state may lag LTD before
#: ``keeper_sync_tier_other`` re-enqueues a refresh. One hour matches
#: PRD #275 user story 10's branch-edition staleness SLO. Exposed as a
#: module constant so the worker function and the unit tests pull from
#: the same source of truth.
TIER_OTHER_REFRESH_THRESHOLD = timedelta(hours=1)

#: Wall-clock cadence at which ``keeper_sync_tier_main`` fires. Matches
#: PRD #275 user story 10's ~5-minute SLO for the user-visible ``main``
#: edition. The single source of truth for the ``cron(...)``
#: declaration in :mod:`docverse.worker.main` and for the cron-tick
#: surfaced by :func:`explain_tier_status` for non-dormant cohorts.
TIER_MAIN_CRON_INTERVAL = timedelta(minutes=5)

#: Wall-clock cadence at which ``keeper_sync_tier_discovery`` fires.
#: Bounded discovery of LTD resources without a ``keeper_sync_state``
#: row; 30 minutes leaves room for the ``tier_main`` fan-out without
#: doubling up on its work.
TIER_DISCOVERY_CRON_INTERVAL = timedelta(minutes=30)

#: Wall-clock cadence at which ``keeper_sync_tier_other`` fires. Hourly
#: catches non-``main`` editions whose state has aged past
#: :data:`TIER_OTHER_REFRESH_THRESHOLD`; the SLO is also hourly so the
#: cron and the threshold are intentionally identical.
TIER_OTHER_CRON_INTERVAL = timedelta(hours=1)

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

#: Random-but-stable spread added to ``TIER_MAIN_DORMANT_INTERVAL`` per
#: project so the dormant cohort does not synchronise on the cron tick
#: that lands exactly one ``dormant_interval`` after a load shed event.
#: Equal to ``TIER_MAIN_DORMANT_INTERVAL`` so the average dormant wait
#: is 1.5x the interval but no two projects with the same
#: ``date_main_last_polled`` ever come due on the same tick: the slug-
#: keyed jitter spreads them uniformly across the 24 h window.
TIER_MAIN_DORMANT_JITTER = timedelta(hours=24)

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

#: Random-but-stable spread added to ``TIER_DISCOVERY_DORMANT_INTERVAL``
#: per project. Same rationale as :data:`TIER_MAIN_DORMANT_JITTER`:
#: equal to the dormant interval so dormant-cohort polls are spread
#: uniformly across a 24 h window instead of synchronising on the
#: anniversary of a deploy or load shed.
TIER_DISCOVERY_DORMANT_JITTER = timedelta(hours=24)

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

#: Random-but-stable spread added to ``TIER_OTHER_DORMANT_INTERVAL`` per
#: project. Same rationale as :data:`TIER_MAIN_DORMANT_JITTER`: equal
#: to the dormant interval so the long tail is uniformly visited across
#: the 24 h window rather than in a periodic burst.
TIER_OTHER_DORMANT_JITTER = timedelta(hours=24)

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
    jitter_window: timedelta = timedelta(0),
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
       annotation. If absent, malformed, or older than the effective
       dormant interval, poll. Otherwise skip.

    ``jitter_window`` (default zero) widens the rule-4 effective
    interval to ``dormant_interval + (stable_hash_fraction(ltd_slug) *
    jitter_window)`` so the dormant cohort does not all become due on
    the same tick after a deploy or load shed. Only the dormant gate
    is jittered; rules 1-3 are unaffected so the hot-cohort SLO is
    preserved.
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
    effective_interval = dormant_interval + (
        stable_hash_fraction(state.ltd_slug) * jitter_window
    )
    return (now - last_polled) >= effective_interval


def next_cron_tick_at_or_after(now: datetime, interval: timedelta) -> datetime:
    """Round ``now`` up to the next cron-tick boundary.

    The tier crons fire on wall-clock minute boundaries anchored on UTC
    midnight (arq's ``cron(..., minute={...})`` takes a set of wall-
    clock minutes within each hour). Given a cadence ``interval``, this
    returns the earliest ``datetime`` ``>= now`` that lies on an
    interval boundary measured from midnight of ``now``'s date.

    Pure; no I/O, no globals. Used by :func:`explain_tier_status` to
    surface a deterministic next-poll time for the hot, unseen, and
    dormant-without-last-polled cohorts where there is no per-project
    rate-limit gate — the next cron tick is what polls. ``now`` already
    on a boundary returns ``now`` itself (``at_or_after``).
    """
    anchor = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = now - anchor
    n_floor = elapsed // interval
    candidate = anchor + n_floor * interval
    if candidate == now:
        return now
    return anchor + (n_floor + 1) * interval


def stable_hash_fraction(slug: str) -> float:
    """Return a deterministic ``[0, 1)`` real for ``slug``.

    Uses sha256 (not Python's built-in ``hash()``, which is salted per
    interpreter run) so callers get the same value across processes
    and across worker restarts. The first 8 bytes of the digest are
    interpreted as a big-endian uint64 and divided by ``2**64``, so
    the output is uniformly distributed over ``[0, 1)`` for any
    well-mixed input distribution. Pure: no I/O, no global state.
    """
    digest = hashlib.sha256(slug.encode()).digest()
    return int.from_bytes(digest[:8], "big") / (1 << 64)


def should_poll_main_for_project(
    *,
    state: KeeperSyncState | None,
    now: datetime,
    hot_window: timedelta = TIER_MAIN_HOT_WINDOW,
    dormant_interval: timedelta = TIER_MAIN_DORMANT_INTERVAL,
    jitter_window: timedelta = TIER_MAIN_DORMANT_JITTER,
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
        jitter_window=jitter_window,
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


@dataclass(frozen=True)
class TierStatus:
    """Operator-readable explanation of one tier's planner decision.

    Surfaced to admins by ``GET /orgs/{org}/keeper-sync/projects/
    {ltd_slug}`` so an operator can answer "why isn't this project
    syncing?" without grep'ing worker logs. Lives next to the gate
    planners because the cohort labels and ``next_due_at`` math must
    stay byte-identical to :func:`should_poll_for_tier`'s rules — if
    the gate skips a project, the explainer must agree on why.

    ``cohort`` is the qualitative label; see :data:`TierCohort` for the
    rules. ``last_polled_at`` is the parsed annotation if present and
    well-formed, else ``None``. ``next_due_at`` is the wall-clock
    timestamp at which the planner will next greenlight a poll: for
    hot, unseen, and dormant-without-last-polled cohorts that's the
    next cron tick (the only thing gating them); for dormant cohorts
    with a recorded last-polled it's ``last_polled + effective
    interval``. The value is the next *poll* time, not necessarily an
    enqueue time — change-detection and mutex gates downstream may
    still suppress the actual enqueue.
    """

    cohort: TierCohort
    last_polled_at: datetime | None
    next_due_at: datetime | None


def explain_tier_status(
    state: KeeperSyncState | None,
    now: datetime,
    *,
    tier: Tier,
    hot_window: timedelta,
    dormant_interval: timedelta,
    cron_interval: timedelta,
    jitter_window: timedelta = timedelta(0),
) -> TierStatus:
    """Explain the planner's tier-cron decision for one project state row.

    Pure read-side mirror of :func:`should_poll_for_tier`: the gate's
    rules and the explainer's labels share the same constants and
    annotation-key resolver so they cannot drift. Used by the org-
    admin GET endpoint to project tier-cron decisions into a small,
    serialisable shape.

    Mapping from gate rules to explainer cohorts:

    1. ``state is None`` → ``cohort='unseen'``: the cron has never
       observed this project. ``next_due_at`` is the next cron tick
       — the next tier-cron firing is what polls.
    2. ``state.date_rebuilt_seen is None`` → ``cohort='hot'``: cold-
       start row. The gate polls every tick until tier_main writes a
       rebuild signal, so the cohort is hot for the operator's
       purposes. ``next_due_at`` is the next cron tick.
    3. ``now - state.date_rebuilt_seen < hot_window`` → ``cohort=
       'hot'``. ``next_due_at`` is the next cron tick.
    4. Dormant without a per-tier last-polled annotation →
       ``cohort='dormant'``. ``next_due_at`` is the next cron tick
       — the gate's "missing annotation" branch polls on first sight.
    5. Dormant with a parsed last-polled annotation → ``cohort=
       'dormant'``. ``next_due_at`` is ``last_polled + (dormant_
       interval + stable_hash_fraction(slug) * jitter_window)`` —
       the calendar-gated deadline.

    ``next_due_at`` is the next *poll* time, not necessarily an
    enqueue time: a hot poll may still no-op via change-detection or
    mutex gates downstream of the planner.
    """
    if state is None:
        return TierStatus(
            cohort="unseen",
            last_polled_at=None,
            next_due_at=next_cron_tick_at_or_after(now, cron_interval),
        )
    annotations = state.annotations or {}
    last_polled = _parse_annotation_datetime(
        annotations.get(_TIER_ANNOTATION_KEYS[tier])
    )
    if state.date_rebuilt_seen is None:
        return TierStatus(
            cohort="hot",
            last_polled_at=last_polled,
            next_due_at=next_cron_tick_at_or_after(now, cron_interval),
        )
    if (now - state.date_rebuilt_seen) < hot_window:
        return TierStatus(
            cohort="hot",
            last_polled_at=last_polled,
            next_due_at=next_cron_tick_at_or_after(now, cron_interval),
        )
    if last_polled is None:
        return TierStatus(
            cohort="dormant",
            last_polled_at=None,
            next_due_at=next_cron_tick_at_or_after(now, cron_interval),
        )
    effective_interval = dormant_interval + (
        stable_hash_fraction(state.ltd_slug) * jitter_window
    )
    return TierStatus(
        cohort="dormant",
        last_polled_at=last_polled,
        next_due_at=last_polled + effective_interval,
    )
