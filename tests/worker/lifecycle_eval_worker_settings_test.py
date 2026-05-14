"""Tests for the ``LifecycleEvalWorkerSettings`` arq config.

Pins the contract operators rely on: the lifecycle queue is isolated
from both the default queue and the keeper-sync queue, the dispatcher
fires on an hourly cron, and the per-org worker is registered on the
same dedicated pool so the dispatcher's enqueues actually run.
"""

from __future__ import annotations

from arq.cron import CronJob
from arq.worker import Function

from docverse.config import Configuration
from docverse.worker.functions import (
    lifecycle_eval,
    lifecycle_eval_dispatcher,
    lifecycle_reaper,
)
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

_config = Configuration()


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
    """Dispatcher and per-org worker are both registered, single-attempt.

    Both are wrapped with :func:`arq.func` carrying the configured
    per-job ``timeout`` so a runaway evaluator or wedged dispatcher
    tick is cancelled by arq long before the cron-driven
    ``lifecycle_reaper`` window — the timeout is the first
    durability backstop and the reaper is the second.
    """
    dispatcher = _function_by_coroutine(lifecycle_eval_dispatcher)
    per_org = _function_by_coroutine(lifecycle_eval)
    expected_timeout = float(_config.lifecycle_eval_job_timeout_seconds)
    assert dispatcher.timeout_s == expected_timeout
    assert per_org.timeout_s == expected_timeout
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


def test_lifecycle_reaper_registered_as_function() -> None:
    """The reaper is registered as a plain coroutine on the lifecycle pool.

    Unlike the dispatcher and per-org worker, the reaper is not wrapped
    in :func:`arq.func` — it is a cron-only backstop with no per-job
    timeout knob and arq's default retry policy is irrelevant since
    each tick is self-contained. Mirrors how ``keeper_sync_reaper`` is
    registered on :class:`KeeperSyncWorkerSettings`.
    """
    assert lifecycle_reaper in LifecycleEvalWorkerSettings.functions


def test_lifecycle_reaper_runs_every_thirty_minutes() -> None:
    """The reaper cron fires on minute 0 and 30 of every hour.

    Frequent enough that test/staging environments running with a low
    ``lifecycle_reaper_threshold_seconds`` see prompt finalisation,
    infrequent enough that production with the 6 h default rarely
    sees no-work-to-do log spam.
    """
    cron_jobs = list(getattr(LifecycleEvalWorkerSettings, "cron_jobs", []))
    reaper_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob) and job.coroutine is lifecycle_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {0, 30}


def test_default_worker_does_not_register_lifecycle_functions() -> None:
    """The default queue stays free of lifecycle work."""
    default_coros: set[object] = set()
    for entry in WorkerSettings.functions:
        if isinstance(entry, Function):
            default_coros.add(entry.coroutine)
    assert lifecycle_eval not in WorkerSettings.functions
    assert lifecycle_eval_dispatcher not in WorkerSettings.functions
    assert lifecycle_reaper not in WorkerSettings.functions
    assert lifecycle_eval not in default_coros
    assert lifecycle_eval_dispatcher not in default_coros
    assert lifecycle_reaper not in default_coros


def test_default_worker_has_no_lifecycle_cron() -> None:
    """The default queue does not run the lifecycle dispatcher or reaper."""
    cron_jobs = list(getattr(WorkerSettings, "cron_jobs", []) or [])
    coroutines = {
        job.coroutine for job in cron_jobs if isinstance(job, CronJob)
    }
    assert lifecycle_eval_dispatcher not in coroutines
    assert lifecycle_reaper not in coroutines


def test_keeper_sync_worker_does_not_register_lifecycle_functions() -> None:
    """Lifecycle work does not leak onto the keeper-sync queue.

    The PRD specifies a third pool precisely so that a slow rule pass
    cannot delay keeper_sync_project; registering lifecycle functions
    on KeeperSyncWorkerSettings would defeat that isolation.
    """
    sync_coros: set[object] = set()
    for entry in KeeperSyncWorkerSettings.functions:
        if isinstance(entry, Function):
            sync_coros.add(entry.coroutine)
    assert lifecycle_eval not in sync_coros
    assert lifecycle_eval_dispatcher not in sync_coros
    assert lifecycle_reaper not in sync_coros
    assert lifecycle_eval not in KeeperSyncWorkerSettings.functions
    assert lifecycle_eval_dispatcher not in KeeperSyncWorkerSettings.functions
    assert lifecycle_reaper not in KeeperSyncWorkerSettings.functions
