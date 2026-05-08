"""Tests for ``docverse.services.keeper_sync.scheduler``.

Pure-function tests with no DB or HTTP. Each tier-cron function in
``worker/functions/keeper_sync.py`` calls one of these planners on
every candidate state row to decide whether to enqueue a refresh; the
unit tests here lock the threshold and the missing/stale/up-to-date
contracts so a future change has to update both the rule and the
test.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from docverse.services.keeper_sync.scheduler import (
    ANNOTATION_DATE_DISCOVERY_LAST_POLLED,
    ANNOTATION_DATE_MAIN_LAST_POLLED,
    ANNOTATION_DATE_OTHER_LAST_POLLED,
    TIER_DISCOVERY_DORMANT_INTERVAL,
    TIER_DISCOVERY_DORMANT_JITTER,
    TIER_DISCOVERY_HOT_WINDOW,
    TIER_MAIN_DORMANT_INTERVAL,
    TIER_MAIN_DORMANT_JITTER,
    TIER_MAIN_HOT_WINDOW,
    TIER_OTHER_DORMANT_INTERVAL,
    TIER_OTHER_DORMANT_JITTER,
    TIER_OTHER_HOT_WINDOW,
    TIER_OTHER_REFRESH_THRESHOLD,
    Tier,
    is_unknown_resource,
    should_poll_for_tier,
    should_poll_main_for_project,
    should_refresh_main_edition,
    should_refresh_other_edition,
    stable_hash_fraction,
)
from docverse.storage.keeper_sync import KeeperSyncState


def _state(
    *,
    docverse_id: int | None = 7,
    date_last_synced: datetime | None = None,
    date_rebuilt_seen: datetime | None = None,
    annotations: dict[str, Any] | None = None,
    resource_type: str = "edition",
    ltd_id: int | None = 42,
    ltd_slug: str = "main",
) -> KeeperSyncState:
    """Build a ``KeeperSyncState`` with sane defaults for assertions."""
    return KeeperSyncState(
        id=1,
        org_id=1,
        resource_type=resource_type,
        ltd_id=ltd_id,
        ltd_slug=ltd_slug,
        docverse_id=docverse_id,
        date_last_synced=date_last_synced,
        date_rebuilt_seen=date_rebuilt_seen,
        annotations=annotations,
    )


# ---------------------------------------------------------------------------
# should_refresh_main_edition
# ---------------------------------------------------------------------------


def test_should_refresh_main_when_state_missing() -> None:
    """No state row at all means tier_main has never seen this edition."""
    assert should_refresh_main_edition(
        state=None,
        ltd_date_rebuilt=datetime(2026, 5, 7, tzinfo=UTC),
    )


def test_should_refresh_main_when_docverse_id_missing() -> None:
    """A state row without ``docverse_id`` is a placeholder; resync."""
    state = _state(docverse_id=None, date_rebuilt_seen=None)
    assert should_refresh_main_edition(
        state=state,
        ltd_date_rebuilt=datetime(2026, 5, 7, tzinfo=UTC),
    )


def test_should_refresh_main_when_ltd_date_rebuilt_is_newer() -> None:
    """LTD has rebuilt since the last sync; tier_main re-enqueues."""
    state = _state(
        date_rebuilt_seen=datetime(2026, 5, 7, 10, tzinfo=UTC),
    )
    assert should_refresh_main_edition(
        state=state,
        ltd_date_rebuilt=datetime(2026, 5, 7, 11, tzinfo=UTC),
    )


def test_should_not_refresh_main_when_dates_match() -> None:
    """Same ``date_rebuilt`` on both sides — nothing to do."""
    same = datetime(2026, 5, 7, 10, tzinfo=UTC)
    state = _state(date_rebuilt_seen=same)
    assert not should_refresh_main_edition(state=state, ltd_date_rebuilt=same)


def test_should_not_refresh_main_when_state_is_newer() -> None:
    """Defensively: if state is somehow ahead of LTD, don't re-enqueue.

    A clock skew between LTD and Docverse hosts could land us here; the
    safe choice is to wait for LTD to advance past our recorded value.
    """
    state = _state(
        date_rebuilt_seen=datetime(2026, 5, 7, 12, tzinfo=UTC),
    )
    assert not should_refresh_main_edition(
        state=state,
        ltd_date_rebuilt=datetime(2026, 5, 7, 11, tzinfo=UTC),
    )


def test_should_not_refresh_main_when_ltd_never_rebuilt() -> None:
    """LTD reports ``date_rebuilt=None`` — nothing to chase."""
    state = _state(date_rebuilt_seen=None)
    assert not should_refresh_main_edition(state=state, ltd_date_rebuilt=None)


def test_should_refresh_main_when_state_has_no_rebuilt_seen() -> None:
    """LTD has a rebuild but state has never recorded one — resync."""
    state = _state(date_rebuilt_seen=None)
    assert should_refresh_main_edition(
        state=state,
        ltd_date_rebuilt=datetime(2026, 5, 7, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# is_unknown_resource
# ---------------------------------------------------------------------------


def test_is_unknown_resource_for_missing_state() -> None:
    """tier_discovery enqueues for resources without a state row."""
    assert is_unknown_resource(None)


def test_is_unknown_resource_skips_existing_state() -> None:
    """A state row, even with no ``docverse_id``, has already been seen."""
    assert not is_unknown_resource(_state())
    assert not is_unknown_resource(_state(docverse_id=None))


# ---------------------------------------------------------------------------
# should_refresh_other_edition
# ---------------------------------------------------------------------------


def test_should_refresh_other_when_never_synced() -> None:
    """A state row with no ``date_last_synced`` is by definition stale."""
    state = _state(date_last_synced=None)
    assert should_refresh_other_edition(
        state=state, now=datetime(2026, 5, 7, tzinfo=UTC)
    )


def test_should_refresh_other_when_past_threshold() -> None:
    """``date_last_synced`` older than the threshold triggers a refresh."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _state(date_last_synced=now - timedelta(hours=2))
    assert should_refresh_other_edition(state=state, now=now)


def test_should_not_refresh_other_when_within_threshold() -> None:
    """Just-synced editions don't get re-enqueued on every cron tick."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _state(date_last_synced=now - timedelta(minutes=10))
    assert not should_refresh_other_edition(state=state, now=now)


def test_should_refresh_other_at_threshold_exactly() -> None:
    """Exactly-at-threshold counts as stale (>= comparison).

    The cron interval rounds to seconds anyway, so a strictly-greater
    comparison would skip refreshes that landed exactly on the boundary
    until the next tick.
    """
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _state(date_last_synced=now - TIER_OTHER_REFRESH_THRESHOLD)
    assert should_refresh_other_edition(state=state, now=now)


def test_tier_other_refresh_threshold_matches_user_story() -> None:
    """The threshold is one hour, per user story 10's branch SLO.

    Locking the constant here so a future change has to come back and
    re-acknowledge the SLO it's drifting away from.
    """
    assert timedelta(hours=1) == TIER_OTHER_REFRESH_THRESHOLD


def test_should_refresh_other_respects_caller_threshold() -> None:
    """Callers may override the threshold (e.g. tests, follow-up tiers)."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _state(date_last_synced=now - timedelta(minutes=30))
    # Default threshold is 1h, so 30m old is fresh — but a 15m caller
    # threshold flips that decision.
    assert should_refresh_other_edition(
        state=state, now=now, threshold=timedelta(minutes=15)
    )


# ---------------------------------------------------------------------------
# should_poll_main_for_project
# ---------------------------------------------------------------------------


def _project_state(
    *,
    date_rebuilt_seen: datetime | None = None,
    annotations: dict[str, Any] | None = None,
) -> KeeperSyncState:
    """Build a project-resource state row with the given dormancy fields."""
    return _state(
        resource_type="project",
        ltd_id=None,
        ltd_slug="pipelines",
        date_rebuilt_seen=date_rebuilt_seen,
        annotations=annotations,
    )


def test_should_poll_main_when_state_missing() -> None:
    """No project state row at all — initial discovery, always poll."""
    assert should_poll_main_for_project(
        state=None, now=datetime(2026, 5, 7, tzinfo=UTC)
    )


def test_should_poll_main_when_date_rebuilt_seen_missing() -> None:
    """Project row exists but has never recorded a rebuild — poll.

    Common when a project was first synced via the operator-driven
    backfill (which doesn't touch the project row's ``date_rebuilt_
    seen``); the next tier_main tick must poll so dormancy gating has
    a date to gate on going forward.
    """
    state = _project_state(date_rebuilt_seen=None)
    assert should_poll_main_for_project(
        state=state, now=datetime(2026, 5, 7, tzinfo=UTC)
    )


def test_should_poll_main_when_hot_inside_window() -> None:
    """Rebuilt within the hot window — always poll on the 5-min cadence."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(date_rebuilt_seen=now - timedelta(days=7))
    assert should_poll_main_for_project(state=state, now=now)


def test_should_not_poll_main_at_hot_window_boundary_when_dormant_recent() -> (
    None
):
    """Exactly-at-hot-window falls through to the dormant rate-limiter.

    The strict ``<`` on the hot comparison means a rebuild ``date_re-
    built_seen`` exactly ``hot_window`` ago is dormant; with a recent
    last-polled annotation the planner skips. This locks the boundary
    handling so future drift requires updating both the rule and this
    test.
    """
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(
        date_rebuilt_seen=now - TIER_MAIN_HOT_WINDOW,
        annotations={
            ANNOTATION_DATE_MAIN_LAST_POLLED: (
                now - timedelta(minutes=10)
            ).isoformat()
        },
    )
    assert not should_poll_main_for_project(state=state, now=now)


def test_should_poll_main_when_dormant_and_never_polled() -> None:
    """Dormant project with no last-polled annotation — poll on first sight."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(date_rebuilt_seen=now - timedelta(days=30))
    assert should_poll_main_for_project(state=state, now=now)


def test_should_not_poll_main_when_dormant_and_recently_polled() -> None:
    """Dormant project polled within ``dormant_interval`` — skip.

    The whole point: clamps a dormant project to ≤ 1 LTD fetch per
    ``dormant_interval`` rather than firing every 5 min like a hot
    project.
    """
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(
        date_rebuilt_seen=now - timedelta(days=30),
        annotations={
            ANNOTATION_DATE_MAIN_LAST_POLLED: (
                now - timedelta(hours=1)
            ).isoformat()
        },
    )
    assert not should_poll_main_for_project(state=state, now=now)


def test_should_poll_main_when_dormant_and_stale_polled() -> None:
    """Dormant project whose last poll predates the dormant interval.

    ``jitter_window=timedelta(0)`` pins the effective dormant
    interval to ``TIER_MAIN_DORMANT_INTERVAL`` exactly so this test
    locks the bare-interval rule independent of how the slug-keyed
    jitter would otherwise spread the boundary; jitter behavior is
    covered by its own test below.
    """
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(
        date_rebuilt_seen=now - timedelta(days=30),
        annotations={
            ANNOTATION_DATE_MAIN_LAST_POLLED: (
                now - timedelta(hours=25)
            ).isoformat()
        },
    )
    assert should_poll_main_for_project(
        state=state, now=now, jitter_window=timedelta(0)
    )


def test_should_poll_main_at_dormant_interval_boundary_exactly() -> None:
    """Dormant + last-polled exactly at the interval — poll (>= comparison).

    The ``>=`` mirrors ``should_refresh_other_edition``'s boundary
    handling so a poll that landed exactly one ``dormant_interval`` ago
    re-enters the polled set on this tick instead of slipping by one
    cron period. ``jitter_window=timedelta(0)`` keeps the assertion on
    the un-jittered boundary; the jittered boundary is necessarily
    slug-dependent and is locked by the dedicated jitter tests below.
    """
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(
        date_rebuilt_seen=now - timedelta(days=30),
        annotations={
            ANNOTATION_DATE_MAIN_LAST_POLLED: (
                now - TIER_MAIN_DORMANT_INTERVAL
            ).isoformat()
        },
    )
    assert should_poll_main_for_project(
        state=state, now=now, jitter_window=timedelta(0)
    )


def test_should_poll_main_when_annotation_malformed() -> None:
    """A garbled annotation value re-polls (and rewrites) on the next tick."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(
        date_rebuilt_seen=now - timedelta(days=30),
        annotations={ANNOTATION_DATE_MAIN_LAST_POLLED: "not-a-datetime"},
    )
    assert should_poll_main_for_project(state=state, now=now)


def test_tier_main_constants_match_documented_cadence() -> None:
    """Hot window is 14 days; dormant interval is 24 hours.

    Locking the constants so a future re-tune comes back and re-
    acknowledges the SLO (hot SLO is the user-visible 5-min cadence;
    the long-tail load-shed budget is the 24-h dormant ceiling).
    """
    assert timedelta(days=14) == TIER_MAIN_HOT_WINDOW
    assert timedelta(hours=24) == TIER_MAIN_DORMANT_INTERVAL


def test_should_poll_main_accepts_callable_overrides() -> None:
    """Callers may override window/interval (used by tests today)."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(date_rebuilt_seen=now - timedelta(days=20))
    # 20-day-old rebuild is dormant under the default 14-day window,
    # but a 30-day caller override flips it back to hot.
    assert should_poll_main_for_project(
        state=state, now=now, hot_window=timedelta(days=30)
    )


# ---------------------------------------------------------------------------
# should_poll_for_tier (parametric, covers main/discovery/other)
# ---------------------------------------------------------------------------


_TIER_PARAMS = [
    pytest.param(
        Tier.main,
        ANNOTATION_DATE_MAIN_LAST_POLLED,
        TIER_MAIN_HOT_WINDOW,
        TIER_MAIN_DORMANT_INTERVAL,
        id="main",
    ),
    pytest.param(
        Tier.discovery,
        ANNOTATION_DATE_DISCOVERY_LAST_POLLED,
        TIER_DISCOVERY_HOT_WINDOW,
        TIER_DISCOVERY_DORMANT_INTERVAL,
        id="discovery",
    ),
    pytest.param(
        Tier.other,
        ANNOTATION_DATE_OTHER_LAST_POLLED,
        TIER_OTHER_HOT_WINDOW,
        TIER_OTHER_DORMANT_INTERVAL,
        id="other",
    ),
]


@pytest.mark.parametrize(
    ("tier", "annotation_key", "hot_window", "dormant_interval"),
    _TIER_PARAMS,
)
def test_should_poll_for_tier_when_state_missing(
    tier: Tier,
    annotation_key: str,
    hot_window: timedelta,
    dormant_interval: timedelta,
) -> None:
    """No state row at all — cold-start, always poll regardless of tier."""
    assert should_poll_for_tier(
        state=None,
        now=datetime(2026, 5, 7, tzinfo=UTC),
        tier=tier,
        hot_window=hot_window,
        dormant_interval=dormant_interval,
    )


@pytest.mark.parametrize(
    ("tier", "annotation_key", "hot_window", "dormant_interval"),
    _TIER_PARAMS,
)
def test_should_poll_for_tier_when_date_rebuilt_seen_missing(
    tier: Tier,
    annotation_key: str,
    hot_window: timedelta,
    dormant_interval: timedelta,
) -> None:
    """Project row exists but no recorded rebuild — poll on every tier.

    ``tier_main`` is the only writer of ``date_rebuilt_seen``; until it
    has run for a project, ``tier_discovery`` / ``tier_other`` cannot
    distinguish hot from dormant. The safe default is poll so the next
    tick has a date to gate on, even if that means a few extra LTD
    requests for cold-start projects.
    """
    state = _project_state(date_rebuilt_seen=None)
    assert should_poll_for_tier(
        state=state,
        now=datetime(2026, 5, 7, tzinfo=UTC),
        tier=tier,
        hot_window=hot_window,
        dormant_interval=dormant_interval,
    )


@pytest.mark.parametrize(
    ("tier", "annotation_key", "hot_window", "dormant_interval"),
    _TIER_PARAMS,
)
def test_should_poll_for_tier_when_hot_inside_window(
    tier: Tier,
    annotation_key: str,
    hot_window: timedelta,
    dormant_interval: timedelta,
) -> None:
    """Rebuilt within the hot window — every tier polls on its fast cadence."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(date_rebuilt_seen=now - timedelta(days=7))
    assert should_poll_for_tier(
        state=state,
        now=now,
        tier=tier,
        hot_window=hot_window,
        dormant_interval=dormant_interval,
    )


@pytest.mark.parametrize(
    ("tier", "annotation_key", "hot_window", "dormant_interval"),
    _TIER_PARAMS,
)
def test_should_poll_for_tier_dormant_and_never_polled(
    tier: Tier,
    annotation_key: str,
    hot_window: timedelta,
    dormant_interval: timedelta,
) -> None:
    """Dormant project with no last-polled annotation — poll on first sight."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(date_rebuilt_seen=now - timedelta(days=30))
    assert should_poll_for_tier(
        state=state,
        now=now,
        tier=tier,
        hot_window=hot_window,
        dormant_interval=dormant_interval,
    )


@pytest.mark.parametrize(
    ("tier", "annotation_key", "hot_window", "dormant_interval"),
    _TIER_PARAMS,
)
def test_should_not_poll_for_tier_dormant_and_recently_polled(
    tier: Tier,
    annotation_key: str,
    hot_window: timedelta,
    dormant_interval: timedelta,
) -> None:
    """Dormant project polled within ``dormant_interval`` — skip every tier."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(
        date_rebuilt_seen=now - timedelta(days=30),
        annotations={annotation_key: (now - timedelta(hours=1)).isoformat()},
    )
    assert not should_poll_for_tier(
        state=state,
        now=now,
        tier=tier,
        hot_window=hot_window,
        dormant_interval=dormant_interval,
    )


@pytest.mark.parametrize(
    ("tier", "annotation_key", "hot_window", "dormant_interval"),
    _TIER_PARAMS,
)
def test_should_poll_for_tier_dormant_at_interval_boundary(
    tier: Tier,
    annotation_key: str,
    hot_window: timedelta,
    dormant_interval: timedelta,
) -> None:
    """Dormant + last-polled exactly at the interval — poll (>= comparison)."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(
        date_rebuilt_seen=now - timedelta(days=30),
        annotations={annotation_key: (now - dormant_interval).isoformat()},
    )
    assert should_poll_for_tier(
        state=state,
        now=now,
        tier=tier,
        hot_window=hot_window,
        dormant_interval=dormant_interval,
    )


def test_should_poll_for_tier_uses_per_tier_annotation_key() -> None:
    """Each tier's last-polled stamp is independent.

    The dormancy gate is per-tier: a recent ``tier_main`` poll must
    not silence ``tier_discovery``. Concretely, a state row carrying
    only ``date_main_last_polled`` is dormant for tier_discovery (no
    matching annotation -> rule 4 returns "missing" -> poll), and
    vice versa. Locks the tier-independence contract.
    """
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(
        date_rebuilt_seen=now - timedelta(days=30),
        annotations={
            ANNOTATION_DATE_MAIN_LAST_POLLED: (
                now - timedelta(hours=1)
            ).isoformat()
        },
    )
    # tier_main has its annotation — skip.
    assert not should_poll_for_tier(
        state=state,
        now=now,
        tier=Tier.main,
        hot_window=TIER_MAIN_HOT_WINDOW,
        dormant_interval=TIER_MAIN_DORMANT_INTERVAL,
    )
    # tier_discovery has no annotation of its own — poll.
    assert should_poll_for_tier(
        state=state,
        now=now,
        tier=Tier.discovery,
        hot_window=TIER_DISCOVERY_HOT_WINDOW,
        dormant_interval=TIER_DISCOVERY_DORMANT_INTERVAL,
    )
    # tier_other has no annotation of its own — poll.
    assert should_poll_for_tier(
        state=state,
        now=now,
        tier=Tier.other,
        hot_window=TIER_OTHER_HOT_WINDOW,
        dormant_interval=TIER_OTHER_DORMANT_INTERVAL,
    )


@pytest.mark.parametrize(
    ("tier", "annotation_key", "hot_window", "dormant_interval"),
    _TIER_PARAMS,
)
def test_should_poll_for_tier_when_annotation_malformed(
    tier: Tier,
    annotation_key: str,
    hot_window: timedelta,
    dormant_interval: timedelta,
) -> None:
    """A garbled annotation re-polls (and rewrites) on the next tick."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(
        date_rebuilt_seen=now - timedelta(days=30),
        annotations={annotation_key: "not-a-datetime"},
    )
    assert should_poll_for_tier(
        state=state,
        now=now,
        tier=tier,
        hot_window=hot_window,
        dormant_interval=dormant_interval,
    )


def test_tier_discovery_constants_match_documented_cadence() -> None:
    """Hot window 14 d, dormant interval 24 h, mirroring tier_main.

    Locking the constants so a future re-tune comes back and re-
    acknowledges the SLO and load-shed budget shared across the three
    tier crons.
    """
    assert timedelta(days=14) == TIER_DISCOVERY_HOT_WINDOW
    assert timedelta(hours=24) == TIER_DISCOVERY_DORMANT_INTERVAL


def test_tier_other_constants_match_documented_cadence() -> None:
    """Hot window 14 d, dormant interval 24 h, mirroring tier_main."""
    assert timedelta(days=14) == TIER_OTHER_HOT_WINDOW
    assert timedelta(hours=24) == TIER_OTHER_DORMANT_INTERVAL


def test_tier_annotation_keys_are_distinct() -> None:
    """Three tiers, three distinct annotation keys.

    A copy-paste typo that re-used ``date_main_last_polled`` for one of
    the new tiers would silently break the rate-limit independence
    asserted by ``test_should_poll_for_tier_uses_per_tier_annotation_
    key``; the explicit-distinct check makes the failure mode
    immediate.
    """
    keys = {
        ANNOTATION_DATE_MAIN_LAST_POLLED,
        ANNOTATION_DATE_DISCOVERY_LAST_POLLED,
        ANNOTATION_DATE_OTHER_LAST_POLLED,
    }
    assert len(keys) == 3


# ---------------------------------------------------------------------------
# stable_hash_fraction + jittered dormant interval
# ---------------------------------------------------------------------------


_HASH_FIXTURE_SLUGS = [
    "pipelines",
    "ldm-503",
    "sqr-112",
    "dm-54794",
    "u-jsick-feature",
    "afw",
    "validate-drp",
    "sims-maf",
    "ts-mtdome",
    "ap-association",
    "rubin-system-engineering",
    "obs-base",
    "skymap",
    "meas-algorithms",
    "geom",
    "scarlet",
    "ndarray",
    "ip-isr",
    "shared-utils",
    "ts-utils",
]


def test_stable_hash_fraction_is_deterministic() -> None:
    """Same input → same output across calls.

    The whole point of using sha256 over Python's salted ``hash()`` is
    that the value survives across processes and worker restarts.
    Calling twice must yield the same result so the planner stamp
    pre-deploy and the planner read post-deploy agree on whether a
    project is due.
    """
    for slug in _HASH_FIXTURE_SLUGS:
        first = stable_hash_fraction(slug)
        second = stable_hash_fraction(slug)
        assert first == second


def test_stable_hash_fraction_is_in_unit_interval() -> None:
    """Output is in ``[0, 1)`` for any input."""
    for slug in _HASH_FIXTURE_SLUGS:
        value = stable_hash_fraction(slug)
        assert 0.0 <= value < 1.0


def test_stable_hash_fraction_distinguishes_slugs() -> None:
    """Different slugs → distinct fractions, so jitter spreads them.

    With sha256 the collision probability across a few hundred slugs
    is vanishingly small; the fixture set here is well under that
    bound, so any duplicate would be a real bug (e.g. ``hash()`` slipping
    in or a digest-truncation off-by-one) rather than statistical fluke.
    """
    fractions = {stable_hash_fraction(slug) for slug in _HASH_FIXTURE_SLUGS}
    assert len(fractions) == len(_HASH_FIXTURE_SLUGS)


def test_stable_hash_fraction_is_uniformly_distributed() -> None:
    """Mean and bucket counts on the fixture set match a uniform draw.

    A uniform distribution has mean ≈ 0.5 and quartile counts ≈ N/4.
    With 20 fixture slugs this is a coarse check, but it would catch a
    digest truncation bug (e.g. taking 4 bytes instead of 8 — values
    would still be in [0, 1) but mean would shift) or a scaling bug.
    """
    fractions = [stable_hash_fraction(slug) for slug in _HASH_FIXTURE_SLUGS]
    mean = sum(fractions) / len(fractions)
    # 20 samples → 95% CI on mean is roughly 0.5 ± 0.13 (for U[0,1)
    # variance 1/12). Loosen to ±0.2 so the test is not flaky on a
    # particular fixture set; a mean outside this window means the
    # distribution is not uniform.
    assert 0.3 < mean < 0.7
    # Quartile bucket counts: roughly N/4 ± a few each.
    buckets = [0, 0, 0, 0]
    for f in fractions:
        buckets[min(int(f * 4), 3)] += 1
    # No bucket should be empty or hold more than half the samples on
    # 20 draws from a well-mixed uniform distribution.
    assert all(b > 0 for b in buckets)
    assert all(b < len(fractions) // 2 for b in buckets)


def test_should_poll_for_tier_jitter_does_not_affect_hot_path() -> None:
    """Hot projects are unaffected by jitter (regression).

    Jitter only widens the dormant-due gate. A hot project (rule 3)
    short-circuits before any jitter math is consulted, so its 5-min
    SLO is preserved. Locking this contract here so a future move of
    the jitter math out of rule 4 fails immediately rather than
    silently delaying hot-cohort polls.
    """
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    # Hot: rebuilt one day ago, well inside the 14-day hot window.
    state = _project_state(date_rebuilt_seen=now - timedelta(days=1))
    # Even with a maximal jitter window (10x dormant_interval) and a
    # last_polled annotation that would otherwise gate the project,
    # the hot rule wins.
    assert should_poll_for_tier(
        state=state,
        now=now,
        tier=Tier.main,
        hot_window=TIER_MAIN_HOT_WINDOW,
        dormant_interval=TIER_MAIN_DORMANT_INTERVAL,
        jitter_window=timedelta(days=10),
    )


def test_should_poll_for_tier_jitter_extends_dormant_interval() -> None:
    """Two slugs with the same ``last_polled`` come due at distinct ``now``.

    The acceptance criterion: with ``jitter_window > 0``, slug A's
    effective interval differs from slug B's, so the threshold ``now``
    at which each becomes dormant-due differs by ``delta_fraction *
    jitter_window``. Demonstrated by picking two slugs whose hash
    fractions differ enough to land on opposite sides of a chosen
    ``now`` instant.
    """
    # ``due-proj`` hashes to ≈ 0.062 and ``skip-proj`` to ≈ 0.877 (locked
    # by ``test_stable_hash_fraction_is_deterministic`` via sha256).
    # With a 24h jitter window the effective intervals are ≈ 25.5h
    # vs ≈ 45.0h, so a ``now`` exactly 30h after ``last_polled`` polls
    # the first and skips the second.
    fast_slug = "due-proj"
    slow_slug = "skip-proj"
    assert stable_hash_fraction(fast_slug) < stable_hash_fraction(slow_slug)

    last_polled = datetime(2026, 5, 7, 0, tzinfo=UTC)
    dormant_interval = timedelta(hours=24)
    jitter_window = timedelta(hours=24)
    # 30h after last_polled — past slug A's effective interval, before
    # slug B's.
    now = last_polled + timedelta(hours=30)

    annotations = {ANNOTATION_DATE_MAIN_LAST_POLLED: last_polled.isoformat()}
    fast_state = _state(
        resource_type="project",
        ltd_id=None,
        ltd_slug=fast_slug,
        date_rebuilt_seen=now - timedelta(days=30),
        annotations=annotations,
    )
    slow_state = _state(
        resource_type="project",
        ltd_id=None,
        ltd_slug=slow_slug,
        date_rebuilt_seen=now - timedelta(days=30),
        annotations=annotations,
    )

    assert should_poll_for_tier(
        state=fast_state,
        now=now,
        tier=Tier.main,
        hot_window=TIER_MAIN_HOT_WINDOW,
        dormant_interval=dormant_interval,
        jitter_window=jitter_window,
    )
    assert not should_poll_for_tier(
        state=slow_state,
        now=now,
        tier=Tier.main,
        hot_window=TIER_MAIN_HOT_WINDOW,
        dormant_interval=dormant_interval,
        jitter_window=jitter_window,
    )


def test_should_poll_for_tier_jitter_zero_matches_unjittered() -> None:
    """``jitter_window=timedelta(0)`` is the no-op identity.

    Locks the default-zero invariant so a refactor that flips the
    default to the per-tier jitter constant fails this test rather
    than silently changing the dormant boundary on every existing
    caller.
    """
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    last_polled = now - timedelta(hours=24)
    state = _state(
        resource_type="project",
        ltd_id=None,
        ltd_slug="some-busy-slug",
        date_rebuilt_seen=now - timedelta(days=30),
        annotations={
            ANNOTATION_DATE_MAIN_LAST_POLLED: last_polled.isoformat()
        },
    )
    # Without jitter the planner uses ``>=`` boundary semantics so a
    # poll exactly one ``dormant_interval`` ago re-polls.
    assert should_poll_for_tier(
        state=state,
        now=now,
        tier=Tier.main,
        hot_window=TIER_MAIN_HOT_WINDOW,
        dormant_interval=TIER_MAIN_DORMANT_INTERVAL,
        jitter_window=timedelta(0),
    )


def test_tier_dormant_jitter_constants_match_documented_cadence() -> None:
    """Each tier's jitter window equals its dormant interval.

    Picking ``jitter == dormant_interval`` doubles the worst-case wait
    but guarantees no two projects ever come due on the same tick — the
    long tail is uniformly distributed across the 24 h window. Lock
    the constants so a future re-tune comes back and re-acknowledges
    the spread / latency tradeoff.
    """
    assert timedelta(hours=24) == TIER_MAIN_DORMANT_JITTER
    assert timedelta(hours=24) == TIER_DISCOVERY_DORMANT_JITTER
    assert timedelta(hours=24) == TIER_OTHER_DORMANT_JITTER
