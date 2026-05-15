"""Tests for the dual-queue ``WorkerSettings`` shape in ``worker.main``."""

from __future__ import annotations

from typing import Any

from arq.cron import CronJob
from arq.worker import Function

from docverse.config import Configuration
from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.worker.functions import (
    keeper_sync_project,
    keeper_sync_reaper,
    keeper_sync_run_discovery,
)
from docverse.worker.main import (
    KeeperSyncWorkerSettings,
    WorkerSettings,
    shutdown,
    startup_default,
    startup_keeper_sync,
)

_config = Configuration()


def _underlying(coroutine: Any) -> Any:
    """Return the raw function under any ``instrument_arq_task`` wrap.

    Every task on the production WorkerSettings is wrapped with
    :func:`docverse.sentry.instrument_arq_task`, which uses
    :func:`functools.wraps` and therefore exposes the original
    coroutine via ``__wrapped__``. This helper peels exactly one
    layer (or returns the value as-is if it is already raw) so the
    registration-shape assertions in this module can compare against
    the unwrapped function imported from ``worker.functions``.
    """
    return getattr(coroutine, "__wrapped__", coroutine)


def _function_by_coroutine(coro: object) -> Function:
    for entry in KeeperSyncWorkerSettings.functions:
        if (
            isinstance(entry, Function)
            and _underlying(entry.coroutine) is coro
        ):
            return entry
    msg = f"No registered Function wraps {coro!r}"
    raise AssertionError(msg)


def test_keeper_sync_worker_settings_uses_dedicated_queue() -> None:
    """The sync queue is isolated from the default arq queue."""
    assert KeeperSyncWorkerSettings.queue_name == KEEPER_SYNC_QUEUE_NAME
    assert KeeperSyncWorkerSettings.queue_name != WorkerSettings.queue_name


def test_keeper_sync_worker_settings_registers_keeper_sync_functions() -> None:
    """Both keeper_sync functions are registered on the dedicated queue.

    They are wrapped with :func:`arq.func` so the dedicated queue can
    enforce a per-job timeout and disable arq's default 5-attempt
    retry behaviour — failure must surface promptly so the existing
    per-function ``except Exception`` blocks can route to
    ``queue_job_store.fail()`` and the parent run still finalises.
    """
    discovery = _function_by_coroutine(keeper_sync_run_discovery)
    project = _function_by_coroutine(keeper_sync_project)
    expected_timeout = float(_config.keeper_sync_job_timeout_seconds)
    assert discovery.timeout_s == expected_timeout
    assert project.timeout_s == expected_timeout
    assert discovery.max_tries == 1
    assert project.max_tries == 1


def test_default_worker_does_not_register_keeper_sync_functions() -> None:
    """The default queue stays free of keeper-sync work."""
    default_underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in WorkerSettings.functions
    }
    assert keeper_sync_run_discovery not in default_underlying
    assert keeper_sync_project not in default_underlying
    assert keeper_sync_reaper not in default_underlying


def test_keeper_sync_worker_settings_share_lifecycle_hooks() -> None:
    """Each queue has its own ``on_startup`` (per-component Sentry tag).

    Both wrappers funnel through the same ``_startup`` body so the
    factory-builder shape stays uniform across the two queues; the only
    intentional divergence is the Sentry ``component`` tag (``worker``
    vs. ``worker-keeper-sync``). ``on_shutdown`` is fully shared.
    """
    assert WorkerSettings.on_startup is startup_default
    assert KeeperSyncWorkerSettings.on_startup is startup_keeper_sync
    assert KeeperSyncWorkerSettings.on_startup is not WorkerSettings.on_startup
    assert KeeperSyncWorkerSettings.on_shutdown is shutdown
    assert KeeperSyncWorkerSettings.on_shutdown is WorkerSettings.on_shutdown


def test_keeper_sync_worker_registers_reaper_cron() -> None:
    """The reaper runs on a 30-minute cron on the dedicated queue.

    Frequent enough that test/staging environments running with low
    thresholds still see prompt finalisation, infrequent enough that
    production with the 6 h threshold rarely sees no-work-to-do log
    spam.
    """
    cron_jobs = list(getattr(KeeperSyncWorkerSettings, "cron_jobs", []))
    reaper_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is keeper_sync_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {0, 30}


def test_default_worker_has_no_keeper_sync_cron() -> None:
    """The default queue does not run the reaper."""
    cron_jobs = list(getattr(WorkerSettings, "cron_jobs", []) or [])
    coroutines = {
        _underlying(job.coroutine)
        for job in cron_jobs
        if isinstance(job, CronJob)
    }
    assert keeper_sync_reaper not in coroutines
