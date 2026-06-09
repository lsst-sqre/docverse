"""Tests for the ``MaintenanceWorkerSettings`` arq config.

Pins the contract operators rely on: the maintenance queue is isolated
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
    project_github_resolve,
    publish_edition_reaper,
)
from docverse.worker.main import (
    KeeperSyncWorkerSettings,
    MaintenanceWorkerSettings,
    WorkerSettings,
    shutdown,
    startup_maintenance,
)
from docverse.worker.queues import MAINTENANCE_QUEUE_NAME

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
    for entry in MaintenanceWorkerSettings.functions:
        if (
            isinstance(entry, Function)
            and _underlying(entry.coroutine) is coro
        ):
            return entry
    msg = f"No registered Function wraps {coro!r}"
    raise AssertionError(msg)


def test_maintenance_worker_settings_uses_dedicated_queue() -> None:
    """The maintenance queue is isolated from default and sync queues."""
    assert MaintenanceWorkerSettings.queue_name == MAINTENANCE_QUEUE_NAME
    assert MaintenanceWorkerSettings.queue_name != WorkerSettings.queue_name
    assert (
        MaintenanceWorkerSettings.queue_name
        != KeeperSyncWorkerSettings.queue_name
    )


def test_maintenance_worker_settings_registers_functions() -> None:
    """Dispatcher and per-org worker are both registered, single-attempt.

    Both are wrapped with :func:`arq.func` carrying the configured
    per-job ``timeout`` so a runaway evaluator or wedged dispatcher
    tick is cancelled by arq long before the cron-driven
    ``lifecycle_reaper`` window — the timeout is the first
    durability backstop and the reaper is the second.
    """
    dispatcher = _function_by_coroutine(lifecycle_eval_dispatcher)
    per_org = _function_by_coroutine(lifecycle_eval)
    expected_timeout = float(_config.maintenance_job_timeout_seconds)
    assert dispatcher.timeout_s == expected_timeout
    assert per_org.timeout_s == expected_timeout
    assert dispatcher.max_tries == 1
    assert per_org.max_tries == 1


def test_maintenance_worker_settings_share_lifecycle_hooks() -> None:
    """Maintenance queue has its own ``on_startup`` (per-component Sentry tag).

    The maintenance wrapper funnels through the same ``_startup`` body as
    the default and keeper-sync queues so the factory-builder shape stays
    uniform; the only intentional divergence is the Sentry ``component``
    tag (``worker-maintenance``). ``on_shutdown`` is fully shared.
    """
    assert MaintenanceWorkerSettings.on_startup is startup_maintenance
    assert (
        MaintenanceWorkerSettings.on_startup is not WorkerSettings.on_startup
    )
    assert MaintenanceWorkerSettings.on_shutdown is shutdown


def test_lifecycle_eval_dispatcher_runs_hourly() -> None:
    """The dispatcher cron fires at minute 0 of every hour."""
    cron_jobs = list(getattr(MaintenanceWorkerSettings, "cron_jobs", []))
    dispatcher_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is lifecycle_eval_dispatcher
    ]
    assert len(dispatcher_crons) == 1
    assert dispatcher_crons[0].minute == {0}


def test_lifecycle_reaper_registered_as_function() -> None:
    """The reaper is registered as a plain coroutine on the maintenance pool.

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
        for entry in MaintenanceWorkerSettings.functions
    }
    assert lifecycle_reaper in underlying


def test_lifecycle_reaper_runs_every_thirty_minutes() -> None:
    """The reaper cron fires on minute 0 and 30 of every hour.

    Frequent enough that test/staging environments running with a low
    ``lifecycle_reaper_threshold_seconds`` see prompt finalisation,
    infrequent enough that production with the 6 h default rarely
    sees no-work-to-do log spam. The ``lifecycle_reaper`` keeps the
    canonical ``{0, 30}`` slot; the other reapers on the maintenance
    pool are staggered off it so a horizontally scaled pool never
    fires two reapers on the same minute.
    """
    cron_jobs = list(getattr(MaintenanceWorkerSettings, "cron_jobs", []))
    reaper_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is lifecycle_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {0, 30}


def test_dashboard_build_reaper_registered_as_function() -> None:
    """``dashboard_build_reaper`` is registered on the maintenance pool.

    Per PRD #367 §"Pool placement" the four main-pool kind reapers
    register on the existing :class:`MaintenanceWorkerSettings` arq
    pool so reaper sweeps never compete with build processing or
    user-triggered dashboard rebuilds for worker capacity.
    """
    underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in MaintenanceWorkerSettings.functions
    }
    assert dashboard_build_reaper in underlying


def test_dashboard_build_reaper_runs_every_fifteen_minutes() -> None:
    """The dashboard_build reaper cron fires every 15 minutes.

    ``dashboard_build`` is the only main-pool kind whose wedge is
    directly user-visible (the 409 on ``POST /dashboard/rebuild``),
    so PRD #367 picks a tighter 15-minute cadence — worst-case
    wall-clock recovery time stays under ~45 minutes from the moment
    a worker silently dies. The slots are offset off the canonical
    quarter-hours (``{3, 18, 33, 48}``) so this reaper never
    co-fires with the maintenance pool's other reapers.
    """
    cron_jobs = list(getattr(MaintenanceWorkerSettings, "cron_jobs", []))
    reaper_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is dashboard_build_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {3, 18, 33, 48}


def test_default_worker_does_not_register_dashboard_build_reaper() -> None:
    """The default queue stays free of the dashboard_build reaper.

    The reaper lives exclusively on the maintenance pool so cron-driven
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
    """``publish_edition_reaper`` is registered on the maintenance pool.

    Per PRD #367 §"Pool placement" the four main-pool kind reapers
    register on the existing :class:`MaintenanceWorkerSettings` arq
    pool so reaper sweeps never compete with build processing or
    user-triggered ``publish_edition`` jobs for worker capacity.
    """
    underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in MaintenanceWorkerSettings.functions
    }
    assert publish_edition_reaper in underlying


def test_publish_edition_reaper_runs_every_thirty_minutes() -> None:
    """The publish_edition reaper fires twice an hour, staggered off lifecycle.

    A stuck ``publish_edition`` is not directly user-visible (no
    operator-facing 409), so PRD #367 picks a 30-minute cadence
    rather than the dashboard_build reaper's tighter 15-minute
    schedule. The slot is offset off the lifecycle reaper's
    ``{0, 30}`` so the two reapers never query ``queue_jobs`` at
    the same instant on a horizontally scaled maintenance pool.
    """
    cron_jobs = list(getattr(MaintenanceWorkerSettings, "cron_jobs", []))
    reaper_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is publish_edition_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {6, 36}


def test_default_worker_does_not_register_publish_edition_reaper() -> None:
    """The default queue stays free of the publish_edition reaper.

    The reaper lives exclusively on the maintenance pool so cron-driven
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
    """``build_processing_reaper`` is registered on the maintenance pool.

    Per PRD #367 §"Pool placement" the four main-pool kind reapers
    register on the existing :class:`MaintenanceWorkerSettings` arq
    pool so reaper sweeps never compete with build processing or
    user-triggered ``build_processing`` jobs for worker capacity.
    """
    underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in MaintenanceWorkerSettings.functions
    }
    assert build_processing_reaper in underlying


def test_build_processing_reaper_runs_every_thirty_minutes() -> None:
    """The build_processing reaper fires twice an hour, off lifecycle's slot.

    A stuck ``build_processing`` is not directly user-visible (no
    operator-facing 409), so PRD #367 picks a 30-minute cadence
    rather than the dashboard_build reaper's tighter 15-minute
    schedule. The slot is offset off the lifecycle reaper's
    ``{0, 30}`` so the two reapers never query ``queue_jobs`` at
    the same instant on a horizontally scaled maintenance pool. The
    8-hour threshold is what keeps real multi-hour uploads safe
    from false reaping — cadence is independent of threshold.
    """
    cron_jobs = list(getattr(MaintenanceWorkerSettings, "cron_jobs", []))
    reaper_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is build_processing_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {12, 42}


def test_default_worker_does_not_register_build_processing_reaper() -> None:
    """The default queue stays free of the build_processing reaper.

    The reaper lives exclusively on the maintenance pool so cron-driven
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
    """``dashboard_sync_reaper`` is registered on the maintenance pool.

    Per PRD #367 §"Pool placement" the four main-pool kind reapers
    register on the existing :class:`MaintenanceWorkerSettings` arq
    pool so reaper sweeps never compete with build processing or
    user-triggered ``dashboard_sync`` jobs for worker capacity.
    """
    underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in MaintenanceWorkerSettings.functions
    }
    assert dashboard_sync_reaper in underlying


def test_dashboard_sync_reaper_runs_every_thirty_minutes() -> None:
    """The dashboard_sync reaper fires twice an hour, staggered off lifecycle.

    A stuck ``dashboard_sync`` is not directly user-visible (no
    operator-facing 409), so PRD #367 picks a 30-minute cadence
    rather than the dashboard_build reaper's tighter 15-minute
    schedule. The slot is offset off the lifecycle reaper's
    ``{0, 30}`` so the two reapers never query ``queue_jobs`` at
    the same instant on a horizontally scaled maintenance pool. The
    6-hour threshold is what gives an operator-triggered GitHub
    fetch + fanout room — cadence is independent of threshold.
    """
    cron_jobs = list(getattr(MaintenanceWorkerSettings, "cron_jobs", []))
    reaper_crons = [
        job
        for job in cron_jobs
        if isinstance(job, CronJob)
        and _underlying(job.coroutine) is dashboard_sync_reaper
    ]
    assert len(reaper_crons) == 1
    assert reaper_crons[0].minute == {24, 54}


def test_maintenance_pool_reaper_crons_are_staggered() -> None:
    """No two reapers on the maintenance pool share a firing minute.

    Today's single-worker maintenance pool runs cron jobs sequentially,
    so co-fires are harmless. The maintenance pool is explicitly
    designed to scale horizontally, however, and on a multi-worker
    pool every reaper that fires on the same minute would race for
    the same Postgres connection-pool slots and ``queue_jobs`` heap
    pages. The disjoint-minute invariant is what keeps that load
    spread across the hour; this test pins it so a future cadence
    tweak cannot silently re-collapse the schedule.

    The ``git_ref_audit_discovery`` cron at ``minute=17`` is the
    canonical precedent (see ``worker/main.py``).
    """
    reaper_coros = {
        lifecycle_reaper,
        publish_edition_reaper,
        build_processing_reaper,
        dashboard_sync_reaper,
        dashboard_build_reaper,
    }
    cron_jobs = list(getattr(MaintenanceWorkerSettings, "cron_jobs", []))
    seen: dict[int, str] = {}
    for job in cron_jobs:
        if not isinstance(job, CronJob):
            continue
        coro = _underlying(job.coroutine)
        if coro not in reaper_coros:
            continue
        # ``CronJob.minute`` is typed ``set[int] | int`` (arq accepts
        # either form); every maintenance-pool reaper uses the set form,
        # but normalise here so the invariant test does not have to
        # care about that shape.
        raw = job.minute
        if raw is None:
            continue
        minutes = raw if isinstance(raw, set) else {raw}
        for minute in minutes:
            assert minute not in seen, (
                f"minute {minute} is shared by {seen[minute]!r} and "
                f"{coro.__name__!r} — reaper crons on the maintenance "
                "pool must be staggered"
            )
            seen[minute] = coro.__name__


def test_default_worker_does_not_register_dashboard_sync_reaper() -> None:
    """The default queue stays free of the dashboard_sync reaper.

    The reaper lives exclusively on the maintenance pool so cron-driven
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


def test_project_github_resolve_registered_on_maintenance_pool() -> None:
    """``project_github_resolve`` is registered on the maintenance pool.

    PRD #419 moves the opportunistic GitHub-id resolve off the default
    publishing queue and onto the maintenance pool: its work is not
    time-sensitive and must not contend with the live publishing flow.
    """
    underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in MaintenanceWorkerSettings.functions
    }
    assert project_github_resolve in underlying


def test_default_worker_does_not_register_project_github_resolve() -> None:
    """The default queue no longer registers ``project_github_resolve``.

    PRD #419 relocates the resolve onto the maintenance pool, so the
    default publishing pool must not also register it — otherwise a
    default-pool worker could pick the job up and defeat the isolation
    the move exists to provide.
    """
    default_underlying = {
        _underlying(entry.coroutine if isinstance(entry, Function) else entry)
        for entry in WorkerSettings.functions
    }
    assert project_github_resolve not in default_underlying


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
