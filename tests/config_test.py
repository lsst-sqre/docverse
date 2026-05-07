"""Tests for ``docverse.config.Configuration``.

Smoke-tests the keeper-sync timeout knobs that were introduced for
the run-finalisation guarantees on ``KeeperSyncWorkerSettings``. Both
defaults and env-var overrides matter: test/staging environments
need to drive the values way down (e.g. ``KEEPER_SYNC_JOB_TIMEOUT_SECONDS=30``)
to surface stuck-worker behaviour quickly, while production needs
the documented 1-hour / 6-hour defaults.
"""

from __future__ import annotations

import pytest

from docverse.config import Configuration


def test_keeper_sync_timeout_defaults() -> None:
    """Documented defaults: 60 min job timeout, 6 h reaper threshold."""
    config = Configuration()
    assert config.keeper_sync_job_timeout_seconds == 3600
    assert config.keeper_sync_reaper_threshold_seconds == 21600


def test_keeper_sync_timeout_env_var_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both knobs are env-var overridable under the ``DOCVERSE_`` prefix.

    Test/staging needs to drive these way down (seconds, not hours)
    to verify stuck-run handling end-to-end.
    """
    monkeypatch.setenv("DOCVERSE_KEEPER_SYNC_JOB_TIMEOUT_SECONDS", "30")
    monkeypatch.setenv("DOCVERSE_KEEPER_SYNC_REAPER_THRESHOLD_SECONDS", "120")
    config = Configuration()
    assert config.keeper_sync_job_timeout_seconds == 30
    assert config.keeper_sync_reaper_threshold_seconds == 120
