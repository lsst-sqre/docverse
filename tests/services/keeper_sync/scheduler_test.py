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

from docverse.services.keeper_sync.scheduler import (
    ANNOTATION_DATE_MAIN_LAST_POLLED,
    TIER_MAIN_DORMANT_INTERVAL,
    TIER_MAIN_HOT_WINDOW,
    TIER_OTHER_REFRESH_THRESHOLD,
    is_unknown_resource,
    should_poll_main_for_project,
    should_refresh_main_edition,
    should_refresh_other_edition,
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
    """Dormant project whose last poll predates the dormant interval."""
    now = datetime(2026, 5, 7, 12, tzinfo=UTC)
    state = _project_state(
        date_rebuilt_seen=now - timedelta(days=30),
        annotations={
            ANNOTATION_DATE_MAIN_LAST_POLLED: (
                now - timedelta(hours=25)
            ).isoformat()
        },
    )
    assert should_poll_main_for_project(state=state, now=now)


def test_should_poll_main_at_dormant_interval_boundary_exactly() -> None:
    """Dormant + last-polled exactly at the interval — poll (>= comparison).

    The ``>=`` mirrors ``should_refresh_other_edition``'s boundary
    handling so a poll that landed exactly one ``dormant_interval`` ago
    re-enters the polled set on this tick instead of slipping by one
    cron period.
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
    assert should_poll_main_for_project(state=state, now=now)


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
