"""Tests for ``docverse_server.config.Configuration``.

Smoke-tests the keeper-sync timeout knobs that were introduced for
the run-finalisation guarantees on ``KeeperSyncWorkerSettings``. Both
defaults and env-var overrides matter: test/staging environments
need to drive the values way down (e.g. ``KEEPER_SYNC_JOB_TIMEOUT_SECONDS=30``)
to surface stuck-worker behaviour quickly, while production needs a
1-hour job timeout and a reaper threshold that sits just above it.

The reaper threshold is *derived* from the job timeout rather than
carrying an independent literal: the keeper-sync functions run with
``max_tries=1``, so arq has already cancelled any job that reaches
its timeout and a row still ``in_progress`` past that point is
definitively dead. The derivation keeps the pair in lockstep when an
operator drives the job timeout down.

Also covers the two memory-shaping knobs that together set the sync
worker's worst-case resident size — ``keeper_sync_max_jobs`` and
``keeper_sync_copy_concurrency`` — plus the sibling ``max_jobs``
settings for the other two arq pools. Their defaults are pinned
because the Phalanx memory limits are sized against exactly that
product.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from docverse_server.config import (
    EDITION_RECONCILE_REAPER_MARGIN_SECONDS,
    KEEPER_SYNC_REAPER_MARGIN_SECONDS,
    Configuration,
)
from docverse_server.services.keeper_sync.copier import (
    DEFAULT_COPY_CONCURRENCY,
)

#: Cadence gap of the ``keeper_sync_reaper`` cron
#: (``cron(minute={0, 30})``), the worst-case extra detection latency
#: on top of the threshold.
_REAPER_CRON_GAP_SECONDS = 1800


def test_keeper_sync_timeout_defaults() -> None:
    """Documented defaults: 60 min job timeout, derived reaper threshold."""
    config = Configuration()
    assert config.keeper_sync_job_timeout_seconds == 3600
    assert config.keeper_sync_reaper_threshold_seconds == (
        3600 + KEEPER_SYNC_REAPER_MARGIN_SECONDS
    )
    # Explicitly not the old 6 h literal: arq cancelled the job at the
    # 1 h timeout, so waiting another five hours parks the project
    # behind the partial unique index for nothing.
    assert config.keeper_sync_reaper_threshold_seconds != 21600


def test_keeper_sync_reaper_threshold_follows_job_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lowering the job timeout alone drags the derived threshold down."""
    monkeypatch.setenv("DOCVERSE_KEEPER_SYNC_JOB_TIMEOUT_SECONDS", "30")
    config = Configuration()
    assert config.keeper_sync_job_timeout_seconds == 30
    assert config.keeper_sync_reaper_threshold_seconds == (
        30 + KEEPER_SYNC_REAPER_MARGIN_SECONDS
    )


def test_keeper_sync_timeout_env_var_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both knobs are env-var overridable under the ``DOCVERSE_`` prefix.

    Test/staging needs to drive these way down (seconds, not hours)
    to verify stuck-run handling end-to-end, and an explicit threshold
    must win over the derivation.
    """
    monkeypatch.setenv("DOCVERSE_KEEPER_SYNC_JOB_TIMEOUT_SECONDS", "30")
    monkeypatch.setenv("DOCVERSE_KEEPER_SYNC_REAPER_THRESHOLD_SECONDS", "120")
    config = Configuration()
    assert config.keeper_sync_job_timeout_seconds == 30
    assert config.keeper_sync_reaper_threshold_seconds == 120


def test_keeper_sync_reaper_threshold_override_beats_derivation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit threshold wins even when the job timeout is default."""
    monkeypatch.setenv(
        "DOCVERSE_KEEPER_SYNC_REAPER_THRESHOLD_SECONDS", "21600"
    )
    config = Configuration()
    assert config.keeper_sync_job_timeout_seconds == 3600
    assert config.keeper_sync_reaper_threshold_seconds == 21600


def test_keeper_sync_reaper_margin_clears_one_cron_gap() -> None:
    """The margin absorbs at least one 30-minute reaper cron gap.

    ``keeper_sync_reaper`` runs on ``cron(minute={0, 30})``, so a row
    is detected at worst one gap after it crosses the threshold. A
    margin below the gap would leave no headroom for a job that is
    finalising just as its timeout lands.
    """
    assert KEEPER_SYNC_REAPER_MARGIN_SECONDS >= _REAPER_CRON_GAP_SECONDS
    config = Configuration()
    assert config.keeper_sync_reaper_threshold_seconds > (
        config.keeper_sync_job_timeout_seconds + _REAPER_CRON_GAP_SECONDS - 1
    )


def test_keeper_sync_copy_concurrency_default() -> None:
    """The copier fan-out bound defaults to the copier's own fallback.

    ``BuildContentCopier`` keeps ``DEFAULT_COPY_CONCURRENCY`` for
    direct construction (tests build it without a factory), so the
    config default must track it rather than carry an independent
    literal that can silently drift.
    """
    config = Configuration()
    assert config.keeper_sync_copy_concurrency == DEFAULT_COPY_CONCURRENCY
    assert config.keeper_sync_copy_concurrency == 8


def test_keeper_sync_copy_concurrency_env_var_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fan-out bound is env-overridable under the prefix."""
    monkeypatch.setenv("DOCVERSE_KEEPER_SYNC_COPY_CONCURRENCY", "3")
    config = Configuration()
    assert config.keeper_sync_copy_concurrency == 3


def test_pool_max_jobs_defaults_preserve_arq_behaviour() -> None:
    """All three pools default to arq's own 10-job concurrency.

    The settings exist to make the bound visible and controllable, not
    to change it — the sync worker's memory limit is sized against the
    stock ``max_jobs`` x ``keeper_sync_copy_concurrency`` product.
    """
    config = Configuration()
    assert config.arq_max_jobs == 10
    assert config.keeper_sync_max_jobs == 10
    assert config.maintenance_max_jobs == 10


def test_pool_max_jobs_env_var_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each pool's concurrency is independently env-overridable."""
    monkeypatch.setenv("DOCVERSE_ARQ_MAX_JOBS", "20")
    monkeypatch.setenv("DOCVERSE_KEEPER_SYNC_MAX_JOBS", "4")
    monkeypatch.setenv("DOCVERSE_MAINTENANCE_MAX_JOBS", "2")
    config = Configuration()
    assert config.arq_max_jobs == 20
    assert config.keeper_sync_max_jobs == 4
    assert config.maintenance_max_jobs == 2


def test_sync_worker_buffered_body_budget_is_the_documented_product() -> None:
    """The two sync knobs multiply out to the sized-against 80 bodies.

    ``keeper_sync_project`` jobs each run their own copier pool, so the
    worker's peak buffered-body count — the term the Phalanx memory
    limit is sized against — is the product of the two. Pinning it here
    means a future default bump has to move this number deliberately.
    """
    config = Configuration()
    assert (
        config.keeper_sync_max_jobs * config.keeper_sync_copy_concurrency
    ) == 80


def test_publish_edition_job_timeout_default() -> None:
    """The publish budget is 30 min — well above arq's 300 s default.

    ``publish_edition`` waits on the ``EDITION_UPDATE`` advisory lock,
    then on the per-hostname purge coalescer, then on the purger's
    rate-limit backoff, so arq's implicit default was far too tight to
    be a meaningful ceiling on any of them.
    """
    config = Configuration()
    assert config.publish_edition_job_timeout_seconds == 1800


def test_publish_edition_job_timeout_env_var_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The publish budget is env-overridable under the prefix."""
    monkeypatch.setenv("DOCVERSE_PUBLISH_EDITION_JOB_TIMEOUT_SECONDS", "45")
    config = Configuration()
    assert config.publish_edition_job_timeout_seconds == 45


def test_other_reaper_thresholds_unchanged() -> None:
    """Keeper-sync and edition_reconcile derive; these five keep literals.

    The two derived thresholds each back a job whose own timeout bounds
    it (``keeper_sync_job_timeout_seconds`` and the shared
    ``maintenance_job_timeout_seconds``). The five below backstop jobs
    with no such pairing, so they stay flat literals.
    """
    config = Configuration()
    assert config.lifecycle_reaper_threshold_seconds == 21600
    assert config.dashboard_build_reaper_threshold_seconds == 1800
    assert config.publish_edition_reaper_threshold_seconds == 14400
    assert config.build_processing_reaper_threshold_seconds == 28800
    assert config.dashboard_sync_reaper_threshold_seconds == 21600


def test_purgatory_cleanup_defaults() -> None:
    """The sweep ships off, capped, and on its own cron slot.

    ``purgatory_cleanup_enabled`` defaults false because the job deletes
    object-store content permanently: it goes on per environment, after
    the operator has looked at what the first tick would reclaim, rather
    than the moment the image lands. The rest of the defaults are the
    shape every maintenance job has — a per-job cap, a daily UTC slot
    staggered off the other crons, and a reaper threshold matching its
    siblings'.
    """
    config = Configuration()
    assert config.purgatory_cleanup_enabled is False
    assert config.purgatory_cleanup_max_builds_per_job == 500
    assert config.purgatory_cleanup_cron_hour == 3
    assert config.purgatory_cleanup_cron_minute == 23
    assert config.purgatory_cleanup_reaper_threshold_seconds == 21600


def test_purgatory_cleanup_env_var_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every knob is env-overridable under the ``DOCVERSE_`` prefix.

    Phalanx sets all five from the chart's ``config.maintenance``
    values, and roundtable-dev needs to drive the cron and the cap to
    something a person can watch inside one sitting.
    """
    monkeypatch.setenv("DOCVERSE_PURGATORY_CLEANUP_ENABLED", "true")
    monkeypatch.setenv("DOCVERSE_PURGATORY_CLEANUP_MAX_BUILDS_PER_JOB", "5")
    monkeypatch.setenv("DOCVERSE_PURGATORY_CLEANUP_CRON_HOUR", "11")
    monkeypatch.setenv("DOCVERSE_PURGATORY_CLEANUP_CRON_MINUTE", "7")
    monkeypatch.setenv(
        "DOCVERSE_PURGATORY_CLEANUP_REAPER_THRESHOLD_SECONDS", "60"
    )
    config = Configuration()
    assert config.purgatory_cleanup_enabled is True
    assert config.purgatory_cleanup_max_builds_per_job == 5
    assert config.purgatory_cleanup_cron_hour == 11
    assert config.purgatory_cleanup_cron_minute == 7
    assert config.purgatory_cleanup_reaper_threshold_seconds == 60


def test_purgatory_cleanup_cap_refuses_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cap of zero would leave the job queued and doing nothing.

    An operator reaching for "pause the sweep" wants the feature flag;
    a cap of 0 would instead run a job per org every night that plans
    an empty work list and completes, which looks like success while
    the backlog grows.
    """
    monkeypatch.setenv("DOCVERSE_PURGATORY_CLEANUP_MAX_BUILDS_PER_JOB", "0")
    with pytest.raises(ValidationError):
        Configuration()


def test_edition_reconcile_defaults() -> None:
    """The reconciler ships on and capped, unlike the purgatory sweep.

    The two flags look alike and default opposite ways on purpose.
    ``purgatory_cleanup_enabled`` is off because that sweep deletes
    object-store content permanently, so an operator has to opt each
    environment in. Reconciliation only ever *enqueues* a publish of the
    build an edition already points at, so the worst a wrong tick can do
    is republish something that was already correct — leaving it off
    would mean every environment silently keeps the drift the loop
    exists to repair.

    The reaper threshold carries no literal of its own: like
    keeper-sync's it derives from the timeout that bounds the job it
    backstops, so it cannot be left behind when an operator moves that
    timeout.
    """
    config = Configuration()
    assert config.edition_reconcile_enabled is True
    assert config.edition_reconcile_max_actions_per_job == 100
    assert config.edition_reconcile_reaper_threshold_seconds == (
        config.maintenance_job_timeout_seconds
        + EDITION_RECONCILE_REAPER_MARGIN_SECONDS
    )
    assert config.edition_reconcile_reaper_threshold_seconds == 5400


def test_edition_reconcile_reaper_threshold_follows_maintenance_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Moving the shared maintenance timeout drags the threshold with it.

    ``maintenance_job_timeout_seconds`` is shared by every function on
    the pool, so an operator raising it for one of them (the purgatory
    sweep, say) used to leave the reconcile reaper's flat 3600 s exactly
    at — and then below — the timeout, which let ``fail_silent_jobs``
    reap a tick arq was still running.
    """
    monkeypatch.setenv("DOCVERSE_MAINTENANCE_JOB_TIMEOUT_SECONDS", "7200")
    config = Configuration()
    assert config.maintenance_job_timeout_seconds == 7200
    assert config.edition_reconcile_reaper_threshold_seconds == (
        7200 + EDITION_RECONCILE_REAPER_MARGIN_SECONDS
    )


def test_edition_reconcile_env_var_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both knobs are env-overridable under the ``DOCVERSE_`` prefix.

    Phalanx sets them from the chart's ``config.maintenance`` values, and
    an operator watching a badly drifted org needs to be able to pull the
    flag without a redeploy of a new image.
    """
    monkeypatch.setenv("DOCVERSE_EDITION_RECONCILE_ENABLED", "false")
    monkeypatch.setenv("DOCVERSE_EDITION_RECONCILE_MAX_ACTIONS_PER_JOB", "7")
    monkeypatch.setenv("DOCVERSE_MAINTENANCE_JOB_TIMEOUT_SECONDS", "30")
    monkeypatch.setenv(
        "DOCVERSE_EDITION_RECONCILE_REAPER_THRESHOLD_SECONDS", "45"
    )
    config = Configuration()
    assert config.edition_reconcile_enabled is False
    assert config.edition_reconcile_max_actions_per_job == 7
    assert config.edition_reconcile_reaper_threshold_seconds == 45


def test_edition_reconcile_reaper_threshold_override_must_clear_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An override at or below the maintenance timeout is refused.

    A threshold that does not strictly clear the timeout lets the reaper
    fail a tick arq has not cancelled yet. That drops the row from
    ``idx_queue_jobs_edition_reconcile_active_uq``, so the next
    dispatcher tick mints a second reconciler for the same org, both
    enqueue publishes for the unapplied tail of the first plan, and the
    first job's later ``complete()`` raises ``InvalidJobStateError``. The
    old flat 3600 s default sat exactly on that boundary, which is the
    value pinned here.
    """
    monkeypatch.setenv(
        "DOCVERSE_EDITION_RECONCILE_REAPER_THRESHOLD_SECONDS", "3600"
    )
    with pytest.raises(ValidationError) as excinfo:
        Configuration()
    message = str(excinfo.value)
    assert "edition_reconcile_reaper_threshold_seconds" in message
    assert "maintenance_job_timeout_seconds" in message


def test_edition_reconcile_reaper_threshold_override_below_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The floor is the timeout itself, not merely a positive number.

    Non-prod environments reach for seconds-long thresholds to watch the
    reaper fire; doing that here means driving the maintenance timeout
    down too, rather than leaving the pair inverted.
    """
    monkeypatch.setenv(
        "DOCVERSE_EDITION_RECONCILE_REAPER_THRESHOLD_SECONDS", "45"
    )
    with pytest.raises(ValidationError):
        Configuration()


def test_edition_reconcile_cap_refuses_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cap of zero would plan repairs every tick and apply none.

    An operator reaching for "stop reconciling" wants
    ``edition_reconcile_enabled``; a cap of 0 would instead run a job per
    org that reads every edition, reports the drift it found, and fixes
    none of it — a tick that looks healthy while nothing converges.
    """
    monkeypatch.setenv("DOCVERSE_EDITION_RECONCILE_MAX_ACTIONS_PER_JOB", "0")
    with pytest.raises(ValidationError):
        Configuration()


def test_edition_reconcile_reaper_threshold_is_tighter_than_siblings() -> None:
    """The reconcile reaper's window is sized to its own cadence.

    Every other maintenance-pool reaper backstops a daily or
    operator-triggered job and can afford a six-hour window. This one
    backstops a loop that ticks twice an hour, and the wedged row holds
    the per-org mutex, so a sibling-sized threshold would cost an
    organization twelve consecutive reconciliation passes before the
    backstop fired.
    """
    config = Configuration()
    assert config.edition_reconcile_reaper_threshold_seconds < (
        config.purgatory_cleanup_reaper_threshold_seconds
    )
    assert config.edition_reconcile_reaper_threshold_seconds < (
        config.publish_edition_reaper_threshold_seconds
    )


def test_publish_edition_reaper_description_points_at_reconcile() -> None:
    """The publish reaper's knob no longer overstates what a reap does.

    Reaping a ``publish_edition`` row only fails the ``queue_jobs`` row:
    the edition and its ``edition_build_history`` pair stay in
    ``publishing``, which is exactly the signal ``edition_reconcile``
    (PRD #612) reads to re-drive the pair. The old wording promised the
    opposite — that the reap kept an edition out of ``publishing`` — so
    an operator reading it would have expected the pair to self-clear
    and would not have known to look at the reconciler. Pinned as a test
    because the description is the operator-facing documentation of the
    knob, published straight into the settings reference.
    """
    description = Configuration.model_fields[
        "publish_edition_reaper_threshold_seconds"
    ].description
    assert description is not None
    assert "edition_reconcile" in description
    assert "does not sit in" not in description
