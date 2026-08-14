"""Tests for the arq workers' database connection-pool sizing.

Every worker pool runs ``max_jobs`` jobs concurrently, and the jobs that
take a ``LockService`` advisory lock hold a *dedicated*
``engine.connect()`` for the lock's lifetime on top of their own
``AsyncSession`` connection. SQLAlchemy's stock ``pool_size=5``,
``max_overflow=10`` gives 15 connections total, which a ten-job pool
exhausts as soon as its jobs start pairing a lock connection with a
session connection: the losers block for ``pool_timeout`` and then
raise, and ``sync_project``'s per-edition ``except Exception`` turns
each into an edition failure that can trip the systemic-abort breaker.
These tests pin the derivation that replaces the implicit default.
"""

from __future__ import annotations

from typing import Any

import pytest
from safir.dependencies.db_session import db_session_dependency

from docverse_server import worker
from docverse_server.config import Configuration
from docverse_server.worker.main import (
    DB_CONNECTIONS_PER_JOB,
    DB_POOL_HEADROOM,
    initialize_worker_db_pool,
    startup_default,
    startup_keeper_sync,
    startup_maintenance,
    worker_db_pool_sizing,
)

_config = Configuration()

#: SQLAlchemy's own defaults, which every worker inherited before the
#: sizing below was made explicit.
_SQLALCHEMY_DEFAULT_TOTAL = 5 + 10


def test_pool_sizing_budgets_two_connections_per_concurrent_job() -> None:
    """``pool_size`` covers a lock connection *and* a session per job.

    ``publish_edition``, ``sync_build``, and ``_ensure_aggregate_edition``
    all hold both at once, so a pool that budgets one connection per job
    deadlocks itself at full concurrency.
    """
    sizing = worker_db_pool_sizing(10)
    assert sizing.pool_size == 10 * DB_CONNECTIONS_PER_JOB + DB_POOL_HEADROOM


def test_pool_sizing_leaves_headroom_above_the_per_job_budget() -> None:
    """Overflow is reserved for cron and reaper jobs.

    Cron-driven work (the tier polls, the reapers, the queue-stats
    gauge) runs alongside the ``max_jobs`` budget rather than inside it,
    so the surge allowance must not be zero.
    """
    sizing = worker_db_pool_sizing(10)
    assert sizing.max_overflow == DB_POOL_HEADROOM


def test_pool_sizing_clears_the_sqlalchemy_default_at_stock_max_jobs() -> None:
    """The stock ten-job pool no longer fits in SQLAlchemy's 15 slots."""
    sizing = worker_db_pool_sizing(_config.keeper_sync_max_jobs)
    total = sizing.pool_size + sizing.max_overflow
    assert total > _SQLALCHEMY_DEFAULT_TOTAL


@pytest.mark.asyncio
async def test_initialize_worker_db_pool_passes_explicit_sizing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The engine is built with explicit pool arguments, not defaults."""
    recorded: dict[str, Any] = {}

    async def _initialize(url: Any, password: Any, /, **kwargs: Any) -> None:
        recorded["url"] = url
        recorded["password"] = password
        recorded.update(kwargs)

    monkeypatch.setattr(db_session_dependency, "initialize", _initialize)

    await initialize_worker_db_pool(max_jobs=4)

    expected = worker_db_pool_sizing(4)
    assert recorded["pool_size"] == expected.pool_size
    assert recorded["max_overflow"] == expected.max_overflow


@pytest.mark.asyncio
async def test_each_worker_startup_sizes_the_pool_from_its_own_max_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each pool derives its sizing from the ``max_jobs`` it declares.

    The three pools share one ``_startup`` body, so the per-worker
    concurrency has to travel with the ``component`` / ``queue_name``
    tags rather than being read from a single config field.
    """
    recorded: list[int] = []

    async def _startup(ctx: dict[str, Any], **kwargs: Any) -> None:
        _ = ctx
        recorded.append(kwargs["max_jobs"])

    monkeypatch.setattr(worker.main, "_startup", _startup)

    await startup_default({})
    await startup_keeper_sync({})
    await startup_maintenance({})

    assert recorded == [
        _config.arq_max_jobs,
        _config.keeper_sync_max_jobs,
        _config.maintenance_max_jobs,
    ]
