"""Tests for the memory sampler's wiring into the arq worker (PRD #753).

The full ``_startup`` needs Redis and a GitHub App, so these drive the
seam it calls, :func:`start_worker_memory_sampler`, against a
:class:`Configuration` read from the ``DOCVERSE_MEMORY_DIAGNOSTICS_*``
environment, and then the real :func:`shutdown`. All three pools share
``_startup``, which hands the seam the same ``component`` label it tags
Sentry with, so here each pool's label is handed to the seam directly.
"""

from __future__ import annotations

import asyncio
import tracemalloc
from collections.abc import Iterator, MutableMapping
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
import structlog
from safir.dependencies.db_session import db_session_dependency
from structlog.testing import capture_logs

from docverse_server.config import Configuration
from docverse_server.sentry import DocverseSentryComponent
from docverse_server.worker.main import shutdown, start_worker_memory_sampler

_MEMORY_DIAGNOSTICS_ENV = (
    "DOCVERSE_MEMORY_DIAGNOSTICS_ENABLED",
    "DOCVERSE_MEMORY_DIAGNOSTICS_INTERVAL_SECONDS",
    "DOCVERSE_MEMORY_DIAGNOSTICS_TRACEMALLOC_ENABLED",
    "DOCVERSE_MEMORY_DIAGNOSTICS_TRACEMALLOC_FRAMES",
    "DOCVERSE_MEMORY_DIAGNOSTICS_TOP_N",
)


@pytest.fixture(autouse=True)
def _stop_tracemalloc() -> Iterator[None]:
    """Leave the worker process untraced whatever the test did."""
    yield
    if tracemalloc.is_tracing():
        tracemalloc.stop()


@pytest.fixture
def memory_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Start each test with every memory-diagnostics variable unset."""
    for name in _MEMORY_DIAGNOSTICS_ENV:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse_server.worker")  # type: ignore[no-any-return]


def _events(
    captured: list[MutableMapping[str, Any]], event: str
) -> list[MutableMapping[str, Any]]:
    return [entry for entry in captured if entry["event"] == event]


def _sampler_tasks() -> list[asyncio.Task[Any]]:
    """Return the sampler tasks still running on this event loop."""
    return [
        task
        for task in asyncio.all_tasks()
        if task.get_name().startswith("memory-sampler-") and not task.done()
    ]


async def _wait_for_sample(
    captured: list[MutableMapping[str, Any]],
) -> MutableMapping[str, Any]:
    """Wait for the first ``"Memory sample"`` line; fail after 5 s."""
    async with asyncio.timeout(5):
        while True:
            if samples := _events(captured, "Memory sample"):
                return samples[0]
            await asyncio.sleep(0.01)


async def _shutdown(
    ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the real ``shutdown`` on a ctx holding only what it closes.

    The database dependency is process-global and shared with the rest
    of the suite, so its close is stubbed out.
    """
    monkeypatch.setattr(db_session_dependency, "aclose", AsyncMock())
    ctx["http_client"] = httpx.AsyncClient()
    ctx["copy_http_client"] = httpx.AsyncClient()
    await shutdown(ctx)


@pytest.mark.asyncio
async def test_worker_starts_no_sampler_by_default(
    memory_env: pytest.MonkeyPatch,
) -> None:
    """With the settings unset, startup runs no sampler and logs nothing."""
    ctx: dict[str, Any] = {}
    with capture_logs() as captured:
        await start_worker_memory_sampler(
            ctx,
            settings=Configuration(),
            component="worker",
            logger=_logger(),
        )
        await asyncio.sleep(0.05)
        tasks = _sampler_tasks()
        await _shutdown(ctx, memory_env)

    assert "memory_sampler" not in ctx
    assert tasks == []
    assert not [e for e in captured if str(e["event"]).startswith("Memory")]
    assert not tracemalloc.is_tracing()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "component", ["worker", "worker-keeper-sync", "worker-maintenance"]
)
async def test_worker_sampler_logs_samples_until_shutdown(
    memory_env: pytest.MonkeyPatch, component: DocverseSentryComponent
) -> None:
    """Enabled, each pool samples under its own label until ``shutdown``."""
    memory_env.setenv("DOCVERSE_MEMORY_DIAGNOSTICS_ENABLED", "true")
    memory_env.setenv("DOCVERSE_MEMORY_DIAGNOSTICS_INTERVAL_SECONDS", "1")
    ctx: dict[str, Any] = {}
    with capture_logs() as captured:
        await start_worker_memory_sampler(
            ctx,
            settings=Configuration(),
            component=component,
            logger=_logger(),
        )
        [task] = _sampler_tasks()
        try:
            sample = await _wait_for_sample(captured)
        finally:
            await _shutdown(ctx, memory_env)

    [enabled] = _events(captured, "Memory diagnostics enabled")
    assert enabled["component"] == component
    assert enabled["interval_seconds"] == 1
    assert enabled["tracemalloc_enabled"] is False
    assert sample["component"] == component
    assert "rss_peak_bytes" in sample
    assert task.done()
    assert _sampler_tasks() == []
    assert "memory_sampler" not in ctx


@pytest.mark.asyncio
async def test_worker_tracemalloc_alone_warns_and_traces_nothing(
    memory_env: pytest.MonkeyPatch,
) -> None:
    """Tracemalloc without the sampler switch is refused with a warning."""
    memory_env.setenv(
        "DOCVERSE_MEMORY_DIAGNOSTICS_TRACEMALLOC_ENABLED", "true"
    )
    ctx: dict[str, Any] = {}
    with capture_logs() as captured:
        await start_worker_memory_sampler(
            ctx,
            settings=Configuration(),
            component="worker-keeper-sync",
            logger=_logger(),
        )
        tasks = _sampler_tasks()

    [warning] = [e for e in captured if e["log_level"] == "warning"]
    assert warning["event"] == (
        "Memory diagnostics disabled; ignoring tracemalloc setting"
    )
    assert warning["component"] == "worker-keeper-sync"
    assert not tracemalloc.is_tracing()
    assert tasks == []
    assert "memory_sampler" not in ctx
