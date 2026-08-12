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

from docverse_server.config import (
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


def test_other_reaper_thresholds_unchanged() -> None:
    """Only keeper-sync derives; the other five reapers keep literals."""
    config = Configuration()
    assert config.lifecycle_reaper_threshold_seconds == 21600
    assert config.dashboard_build_reaper_threshold_seconds == 1800
    assert config.publish_edition_reaper_threshold_seconds == 14400
    assert config.build_processing_reaper_threshold_seconds == 28800
    assert config.dashboard_sync_reaper_threshold_seconds == 21600
