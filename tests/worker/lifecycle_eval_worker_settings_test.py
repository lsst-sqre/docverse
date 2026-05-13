"""Tests for the ``LifecycleEvalWorkerSettings`` arq config.

Pins the contract operators rely on: the lifecycle queue is isolated
from both the default queue and the keeper-sync queue, the dispatcher
fires on an hourly cron, and the per-org worker is registered on the
same dedicated pool so the dispatcher's enqueues actually run.
"""

from __future__ import annotations

from arq.cron import CronJob
from arq.worker import Function

from docverse.worker.functions import lifecycle_eval, lifecycle_eval_dispatcher
from docverse.worker.functions.lifecycle_eval_dispatcher import (
    LIFECYCLE_EVAL_QUEUE_NAME,
)
from docverse.worker.main import (
    KeeperSyncWorkerSettings,
    LifecycleEvalWorkerSettings,
    WorkerSettings,
    shutdown,
    startup,
)


def _function_by_coroutine(coro: object) -> Function:
    for entry in LifecycleEvalWorkerSettings.functions:
        if isinstance(entry, Function) and entry.coroutine is coro:
            return entry
    msg = f"No registered Function wraps {coro!r}"
    raise AssertionError(msg)


def test_lifecycle_eval_worker_settings_uses_dedicated_queue() -> None:
    """The lifecycle queue is isolated from default and sync queues."""
    assert LifecycleEvalWorkerSettings.queue_name == LIFECYCLE_EVAL_QUEUE_NAME
    assert LifecycleEvalWorkerSettings.queue_name != WorkerSettings.queue_name
    assert (
        LifecycleEvalWorkerSettings.queue_name
        != KeeperSyncWorkerSettings.queue_name
    )


def test_lifecycle_eval_worker_settings_registers_functions() -> None:
    """Dispatcher and per-org worker are both registered, single-attempt."""
    dispatcher = _function_by_coroutine(lifecycle_eval_dispatcher)
    per_org = _function_by_coroutine(lifecycle_eval)
    assert dispatcher.max_tries == 1
    assert per_org.max_tries == 1


def test_lifecycle_eval_worker_settings_share_lifecycle_hooks() -> None:
    """All three WorkerSettings classes share startup/shutdown."""
    assert LifecycleEvalWorkerSettings.on_startup is startup
    assert LifecycleEvalWorkerSettings.on_shutdown is shutdown


def test_lifecycle_eval_dispatcher_runs_hourly() -> None:
    """The dispatcher cron fires at minute 0 of every hour."""
    cron_jobs = list(getattr(LifecycleEvalWorkerSettings, "cron_jobs", []))
    dispatcher_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and job.coroutine is lifecycle_eval_dispatcher
    ]
    assert len(dispatcher_crons) == 1
    assert dispatcher_crons[0].minute == {0}


def test_default_worker_does_not_register_lifecycle_functions() -> None:
    """The default queue stays free of lifecycle work."""
    default_coros: set[object] = set()
    for entry in WorkerSettings.functions:
        if isinstance(entry, Function):
            default_coros.add(entry.coroutine)
    assert lifecycle_eval not in WorkerSettings.functions
    assert lifecycle_eval_dispatcher not in WorkerSettings.functions
    assert lifecycle_eval not in default_coros
    assert lifecycle_eval_dispatcher not in default_coros
