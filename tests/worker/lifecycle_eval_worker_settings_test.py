"""Tests for the ``LifecycleEvalWorkerSettings`` arq config.

Pins the contract operators rely on: the lifecycle queue is isolated
from both the default queue and the keeper-sync queue, the dispatcher
fires on an hourly cron, and the per-org worker is registered on the
same dedicated pool so the dispatcher's enqueues actually run.
"""

from __future__ import annotations

from typing import Any

from arq.cron import CronJob
from arq.worker import Function

from docverse.config import Configuration
from docverse.worker.functions import (
    build_processing_reaper,
    dashboard_build_reaper,
    dashboard_sync_reaper,
    lifecycle_eval,
    lifecycle_eval_dispatcher,
    lifecycle_reaper,
    publish_edition_reaper,
)
from docverse.worker.functions.lifecycle_eval_dispatcher import (
    LIFECYCLE_EVAL_QUEUE_NAME,
)
from docverse.worker.main import (
    KeeperSyncWorkerSettings,
    LifecycleEvalWorkerSettings,
    WorkerSettings,
    shutdown,
    startup_lifecycle_eval,
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
    for entry in LifecycleEvalWorkerSettings.functions:
        if (
            isinstance(entry, Function)
            and _underlying(entry.coroutine) is coro
        ):
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
    """Lifecycle queue has its own ``on_startup`` (per-component Sentry tag).

    The lifecycle wrapper funnels through the same ``_startup`` body as
    the default and keeper-sync queues so the factory-builder shape stays
    uniform; the only intentional divergence is the Sentry ``component``
    tag (``worker-lifecycle-eval``). ``on_shutdown`` is fully shared.
    """
    assert LifecycleEvalWorkerSettings.on_startup is startup_lifecycle_eval
    assert (
        LifecycleEvalWorkerSettings.on_startup is not WorkerSettings.on_startup
    )
    assert LifecycleEvalWorkerSettings.on_shutdown is shutdown


def test_lifecycle_eval_dispatcher_runs_hourly() -> None:
    """The dispatcher cron fires at minute 0 of every hour."""
    cron_jobs = list(getattr(LifecycleEvalWorkerSettings, "cron_jobs", []))
    dispatcher_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is lifecycle_eval_dispatcher
    ]
    assert len(dispatcher_crons) == 1
    assert dispatcher_crons[0].minute == {0}


def test_lifecycle_reaper_registered_as_function() -> None:
    """The reaper is registered as a plain coroutine on the lifecycle pool.

    Unlike the dispatcher and per-org worker, the reaper is not wrapped
    in :func:`arq.func` — it is a cron-only backstop with no per-job
    timeout knob and arq's default retry policy is irrelevant since
    each tick is self-contained. Mirrors how ``keeper_sync_reaper`` is
    registered on :class:`KeeperSyncWorkerSettings`. After
    :func:`docverse.sentry.instrument_arq_task` wraps it, the entry is
    no longer ``is lifecycle_reaper``; the test unwraps every entry to
    recover the underlying coroutine.
    """
    underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in LifecycleEvalWorkerSettings.functions
    }
    assert lifecycle_reaper in underlying


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
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is lifecycle_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {0, 30}


def test_dashboard_build_reaper_registered_as_function() -> None:
    """``dashboard_build_reaper`` is registered on the lifecycle pool.

    Per PRD #367 §"Pool placement" the four main-pool kind reapers
    register on the existing :class:`LifecycleEvalWorkerSettings` arq
    pool so reaper sweeps never compete with build processing or
    user-triggered dashboard rebuilds for worker capacity.
    """
    underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in LifecycleEvalWorkerSettings.functions
    }
    assert dashboard_build_reaper in underlying


def test_dashboard_build_reaper_runs_every_fifteen_minutes() -> None:
    """The dashboard_build reaper cron fires on minute 0, 15, 30 and 45.

    ``dashboard_build`` is the only main-pool kind whose wedge is
    directly user-visible (the 409 on ``POST /dashboard/rebuild``),
    so PRD #367 picks a tighter 15-minute cadence — worst-case
    wall-clock recovery time stays under ~45 minutes from the moment
    a worker silently dies.
    """
    cron_jobs = list(getattr(LifecycleEvalWorkerSettings, "cron_jobs", []))
    reaper_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is dashboard_build_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {0, 15, 30, 45}


def test_default_worker_does_not_register_dashboard_build_reaper() -> None:
    """The default queue stays free of the dashboard_build reaper.

    The reaper lives exclusively on the lifecycle pool so cron-driven
    maintenance work never contends with the operator-triggered
    ``dashboard_build`` job itself on the default pool.
    """
    default_underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in WorkerSettings.functions
    }
    assert dashboard_build_reaper not in default_underlying

    cron_jobs = list(getattr(WorkerSettings, "cron_jobs", []) or [])
    coroutines = {
        _underlying(job.coroutine)
        for job in cron_jobs
        if isinstance(job, CronJob)
    }
    assert dashboard_build_reaper not in coroutines


def test_publish_edition_reaper_registered_as_function() -> None:
    """``publish_edition_reaper`` is registered on the lifecycle pool.

    Per PRD #367 §"Pool placement" the four main-pool kind reapers
    register on the existing :class:`LifecycleEvalWorkerSettings` arq
    pool so reaper sweeps never compete with build processing or
    user-triggered ``publish_edition`` jobs for worker capacity.
    """
    underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in LifecycleEvalWorkerSettings.functions
    }
    assert publish_edition_reaper in underlying


def test_publish_edition_reaper_runs_every_thirty_minutes() -> None:
    """The publish_edition reaper cron fires on minute 0 and 30.

    A stuck ``publish_edition`` is not directly user-visible (no
    operator-facing 409), so PRD #367 picks the standard 30-minute
    cadence shared with the lifecycle and keeper-sync reapers rather
    than the dashboard_build reaper's tighter 15-minute schedule.
    """
    cron_jobs = list(getattr(LifecycleEvalWorkerSettings, "cron_jobs", []))
    reaper_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is publish_edition_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {0, 30}


def test_default_worker_does_not_register_publish_edition_reaper() -> None:
    """The default queue stays free of the publish_edition reaper.

    The reaper lives exclusively on the lifecycle pool so cron-driven
    maintenance work never contends with the operator-triggered
    ``publish_edition`` job itself on the default pool.
    """
    default_underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in WorkerSettings.functions
    }
    assert publish_edition_reaper not in default_underlying

    cron_jobs = list(getattr(WorkerSettings, "cron_jobs", []) or [])
    coroutines = {
        _underlying(job.coroutine)
        for job in cron_jobs
        if isinstance(job, CronJob)
    }
    assert publish_edition_reaper not in coroutines


def test_build_processing_reaper_registered_as_function() -> None:
    """``build_processing_reaper`` is registered on the lifecycle pool.

    Per PRD #367 §"Pool placement" the four main-pool kind reapers
    register on the existing :class:`LifecycleEvalWorkerSettings` arq
    pool so reaper sweeps never compete with build processing or
    user-triggered ``build_processing`` jobs for worker capacity.
    """
    underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in LifecycleEvalWorkerSettings.functions
    }
    assert build_processing_reaper in underlying


def test_build_processing_reaper_runs_every_thirty_minutes() -> None:
    """The build_processing reaper cron fires on minute 0 and 30.

    A stuck ``build_processing`` is not directly user-visible (no
    operator-facing 409), so PRD #367 picks the standard 30-minute
    cadence shared with the lifecycle and keeper-sync reapers rather
    than the dashboard_build reaper's tighter 15-minute schedule.
    The 8-hour threshold is what keeps real multi-hour uploads safe
    from false reaping — cadence is independent of threshold.
    """
    cron_jobs = list(getattr(LifecycleEvalWorkerSettings, "cron_jobs", []))
    reaper_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is build_processing_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {0, 30}


def test_default_worker_does_not_register_build_processing_reaper() -> None:
    """The default queue stays free of the build_processing reaper.

    The reaper lives exclusively on the lifecycle pool so cron-driven
    maintenance work never contends with the operator-triggered
    ``build_processing`` job itself on the default pool.
    """
    default_underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in WorkerSettings.functions
    }
    assert build_processing_reaper not in default_underlying

    cron_jobs = list(getattr(WorkerSettings, "cron_jobs", []) or [])
    coroutines = {
        _underlying(job.coroutine)
        for job in cron_jobs
        if isinstance(job, CronJob)
    }
    assert build_processing_reaper not in coroutines


def test_dashboard_sync_reaper_registered_as_function() -> None:
    """``dashboard_sync_reaper`` is registered on the lifecycle pool.

    Per PRD #367 §"Pool placement" the four main-pool kind reapers
    register on the existing :class:`LifecycleEvalWorkerSettings` arq
    pool so reaper sweeps never compete with build processing or
    user-triggered ``dashboard_sync`` jobs for worker capacity.
    """
    underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in LifecycleEvalWorkerSettings.functions
    }
    assert dashboard_sync_reaper in underlying


def test_dashboard_sync_reaper_runs_every_thirty_minutes() -> None:
    """The dashboard_sync reaper cron fires on minute 0 and 30.

    A stuck ``dashboard_sync`` is not directly user-visible (no
    operator-facing 409), so PRD #367 picks the standard 30-minute
    cadence shared with the lifecycle and keeper-sync reapers rather
    than the dashboard_build reaper's tighter 15-minute schedule.
    The 6-hour threshold is what gives an operator-triggered GitHub
    fetch + fanout room — cadence is independent of threshold.
    """
    cron_jobs = list(getattr(LifecycleEvalWorkerSettings, "cron_jobs", []))
    reaper_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is dashboard_sync_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {0, 30}


def test_default_worker_does_not_register_dashboard_sync_reaper() -> None:
    """The default queue stays free of the dashboard_sync reaper.

    The reaper lives exclusively on the lifecycle pool so cron-driven
    maintenance work never contends with the operator-triggered
    ``dashboard_sync`` job itself on the default pool.
    """
    default_underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in WorkerSettings.functions
    }
    assert dashboard_sync_reaper not in default_underlying

    cron_jobs = list(getattr(WorkerSettings, "cron_jobs", []) or [])
    coroutines = {
        _underlying(job.coroutine)
        for job in cron_jobs
        if isinstance(job, CronJob)
    }
    assert dashboard_sync_reaper not in coroutines


def test_default_worker_does_not_register_lifecycle_functions() -> None:
    """The default queue stays free of lifecycle work."""
    default_underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in WorkerSettings.functions
    }
    assert lifecycle_eval not in default_underlying
    assert lifecycle_eval_dispatcher not in default_underlying
    assert lifecycle_reaper not in default_underlying


def test_default_worker_has_no_lifecycle_cron() -> None:
    """The default queue does not run the lifecycle dispatcher or reaper."""
    cron_jobs = list(getattr(WorkerSettings, "cron_jobs", []) or [])
    coroutines = {
        _underlying(job.coroutine)
        for job in cron_jobs
        if isinstance(job, CronJob)
    }
    assert lifecycle_eval_dispatcher not in coroutines
    assert lifecycle_reaper not in coroutines


def test_keeper_sync_worker_does_not_register_lifecycle_functions() -> None:
    """Lifecycle work does not leak onto the keeper-sync queue.

    The PRD specifies a third pool precisely so that a slow rule pass
    cannot delay keeper_sync_project; registering lifecycle functions
    on KeeperSyncWorkerSettings would defeat that isolation.
    """
    sync_underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in KeeperSyncWorkerSettings.functions
    }
    assert lifecycle_eval not in sync_underlying
    assert lifecycle_eval_dispatcher not in sync_underlying
    assert lifecycle_reaper not in sync_underlying
