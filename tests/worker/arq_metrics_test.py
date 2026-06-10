"""Tests for the generic Safir arq-queue metrics wiring (SQR-112).

Each of the three worker pools (default, keeper-sync, maintenance)
publishes ``arq_job_run`` for every job it runs (via ``on_job_start``)
and ``arq_queue_stats`` for its own queue (via a per-pool cron). These
tests drive both hooks against a ``MockEventManager`` so they assert the
events fire with the right queue name without a live Redis or Kafka.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from arq.cron import CronJob
from safir.metrics import EventManager, MockEventPublisher
from safir.metrics.arq import ARQ_EVENTS_CONTEXT_KEY, initialize_arq_metrics

from docverse.config import Configuration
from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.worker.main import (
    KeeperSyncWorkerSettings,
    MaintenanceWorkerSettings,
    WorkerSettings,
    publish_queue_stats_cron,
)
from docverse.worker.queues import MAINTENANCE_QUEUE_NAME

_config = Configuration()

# Five-minute resolution is ample for the Sasquatch product-analytics
# queue-depth gauge (deliberately distinct from operational telemetry)
# and limits the per-tick Redis pool that Safir's helper opens.
_QUEUE_STATS_MINUTES = set(range(0, 60, 5))

# (WorkerSettings class, the queue name that pool binds to). The class
# is typed ``Any`` so mypy does not flag the arbitrary ``on_job_start`` /
# ``cron_jobs`` attribute access across the three parametrized pools.
_POOLS: list[tuple[Any, str]] = [
    (WorkerSettings, _config.arq_queue_name),
    (KeeperSyncWorkerSettings, KEEPER_SYNC_QUEUE_NAME),
    (MaintenanceWorkerSettings, MAINTENANCE_QUEUE_NAME),
]

_POOL_IDS = ["default", "keeper_sync", "maintenance"]


async def _make_arq_metrics_ctx() -> tuple[EventManager, dict[str, Any]]:
    """Build a started ``MockEventManager`` and a ctx with arq publishers.

    Mirrors what production's ``_startup`` does: build the event manager
    and run ``initialize_arq_metrics`` so ``ctx[ARQ_EVENTS_CONTEXT_KEY]``
    carries the generic arq publishers the hooks read back.
    """
    manager = _config.metrics.make_manager()
    await manager.initialize()
    ctx: dict[str, Any] = {}
    await initialize_arq_metrics(manager, ctx)
    return manager, ctx


def _underlying(coroutine: Any) -> Any:
    """Peel the one ``instrument_arq_task`` layer off a registered task.

    :func:`docverse.sentry.instrument_arq_task` wraps with
    :func:`functools.wraps`, exposing the original coroutine via
    ``__wrapped__`` — so identity comparisons can target the unwrapped
    ``publish_queue_stats_cron``.
    """
    return getattr(coroutine, "__wrapped__", coroutine)


def _queue_stats_cron(settings_cls: Any) -> CronJob:
    crons = [
        job
        for job in getattr(settings_cls, "cron_jobs", [])
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is publish_queue_stats_cron
    ]
    assert len(crons) == 1
    return crons[0]


@pytest.mark.parametrize(
    ("settings_cls", "expected_queue"), _POOLS, ids=_POOL_IDS
)
@pytest.mark.asyncio
async def test_on_job_start_publishes_arq_job_run(
    settings_cls: Any, expected_queue: str
) -> None:
    """Every job run on a pool publishes ``arq_job_run`` for its queue."""
    manager, ctx = await _make_arq_metrics_ctx()
    # arq sets ``score`` to the epoch-millisecond instant the job should
    # ideally have started; five seconds in the past yields a positive
    # ``time_in_queue``.
    ideal_start = datetime.now(tz=UTC) - timedelta(seconds=5)
    ctx["score"] = int(ideal_start.timestamp() * 1000)

    await settings_cls.on_job_start(ctx)

    publisher = ctx[ARQ_EVENTS_CONTEXT_KEY].arq_queue_job_event
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.queue == expected_queue
    assert event.time_in_queue >= timedelta(seconds=4)

    await manager.aclose()


@pytest.mark.parametrize(
    "settings_cls",
    [WorkerSettings, KeeperSyncWorkerSettings, MaintenanceWorkerSettings],
    ids=_POOL_IDS,
)
def test_pool_registers_one_queue_stats_cron(settings_cls: Any) -> None:
    """Each pool registers exactly one five-minute queue-stats cron."""
    cron_job = _queue_stats_cron(settings_cls)
    assert cron_job.minute == _QUEUE_STATS_MINUTES


@pytest.mark.parametrize(
    ("settings_cls", "expected_queue"), _POOLS, ids=_POOL_IDS
)
@pytest.mark.asyncio
async def test_queue_stats_cron_publishes_arq_queue_stats(
    settings_cls: Any,
    expected_queue: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The queue-stats cron publishes ``arq_queue_stats`` with the depth."""
    manager, ctx = await _make_arq_metrics_ctx()
    # The cron reads the pool's queue name from ctx, exactly as
    # ``_startup`` sets it per pool. ``redis_settings`` is opaque here:
    # Safir's ``publish_queue_stats`` only forwards it to ``create_pool``,
    # which the test mocks out.
    ctx["queue_name"] = expected_queue
    ctx["redis_settings"] = object()

    class _FakeRedis:
        async def zcard(self, _queue: str) -> int:
            return 7

    async def _fake_create_pool(settings: Any) -> _FakeRedis:
        return _FakeRedis()

    monkeypatch.setattr("safir.metrics.arq.create_pool", _fake_create_pool)

    cron_job = _queue_stats_cron(settings_cls)
    await cron_job.coroutine(ctx)

    publisher = ctx[ARQ_EVENTS_CONTEXT_KEY].arq_queue_stats
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.queue == expected_queue
    assert event.num_queued == 7

    await manager.aclose()
