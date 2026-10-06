"""Tests for the sync worker's keeper-sync time-ladder startup log.

The keeper-sync slice budget, job timeout and reaper threshold read as
one ladder (PRD #765), and the reaper threshold is capped at the
timeout plus margin however high an operator sets it. The sync worker
logs the ladder it runs with at startup, and warns once when the cap
rewrote an explicit threshold, so an operator whose Phalanx value no
longer applies finds out from the pod log rather than from a reaper
firing sooner than expected.

The full ``_startup`` needs Redis and a GitHub App, so these drive the
seam, :func:`log_keeper_sync_time_ladder`, against a
:class:`Configuration` read from the environment, and check the
keeper-sync pool's ``on_startup`` calls it with ``_startup`` stubbed.
"""

from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any
from unittest.mock import AsyncMock

import pytest
import structlog
from structlog.testing import capture_logs

from docverse_server.config import Configuration
from docverse_server.worker import main as worker_main
from docverse_server.worker.main import (
    log_keeper_sync_time_ladder,
    startup_keeper_sync,
)

_LADDER_EVENT = "Keeper-sync time ladder"


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse_server.worker")  # type: ignore[no-any-return]


def _warnings(
    captured: list[MutableMapping[str, Any]],
) -> list[MutableMapping[str, Any]]:
    return [entry for entry in captured if entry["log_level"] == "warning"]


def test_ladder_logs_the_derived_values() -> None:
    """At stock settings the log reports 3000 < 3600 < 5400, no warning."""
    settings = Configuration()
    with capture_logs() as captured:
        log_keeper_sync_time_ladder(settings, logger=_logger())
    ladder = [entry for entry in captured if entry["event"] == _LADDER_EVENT]
    assert len(ladder) == 1
    assert ladder[0]["log_level"] == "info"
    assert ladder[0]["slice_budget_seconds"] == 3000
    assert ladder[0]["job_timeout_seconds"] == 3600
    assert ladder[0]["reaper_threshold_seconds"] == 5400
    assert _warnings(captured) == []


def test_capped_reaper_threshold_warns_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The prod Phalanx pin of 21600 s logs one warning naming both values."""
    monkeypatch.setenv(
        "DOCVERSE_KEEPER_SYNC_REAPER_THRESHOLD_SECONDS", "21600"
    )
    settings = Configuration()
    with capture_logs() as captured:
        log_keeper_sync_time_ladder(settings, logger=_logger())
    warnings = _warnings(captured)
    assert len(warnings) == 1
    assert warnings[0]["requested_seconds"] == 21600
    assert warnings[0]["effective_seconds"] == 5400


def test_reaper_threshold_below_cap_does_not_warn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit threshold under the cap applies as is, silently."""
    monkeypatch.setenv("DOCVERSE_KEEPER_SYNC_REAPER_THRESHOLD_SECONDS", "4000")
    settings = Configuration()
    with capture_logs() as captured:
        log_keeper_sync_time_ladder(settings, logger=_logger())
    assert _warnings(captured) == []
    ladder = [entry for entry in captured if entry["event"] == _LADDER_EVENT]
    assert ladder[0]["reaper_threshold_seconds"] == 4000


@pytest.mark.asyncio
async def test_keeper_sync_startup_logs_the_ladder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sync pool's ``on_startup`` logs the ladder and the cap warning.

    ``keeper_sync_reaper`` runs on this pool, so its log is where an
    operator looks for the threshold it applies.
    """
    monkeypatch.setenv(
        "DOCVERSE_KEEPER_SYNC_REAPER_THRESHOLD_SECONDS", "21600"
    )
    monkeypatch.setattr(worker_main, "config", Configuration())
    monkeypatch.setattr(worker_main, "_startup", AsyncMock())
    with capture_logs() as captured:
        await startup_keeper_sync({})
    assert [e["event"] for e in captured if e["event"] == _LADDER_EVENT] == [
        _LADDER_EVENT
    ]
    assert len(_warnings(captured)) == 1
