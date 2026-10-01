"""Every ``queue_jobs``-backed arq function records its own cancellation.

arq's per-job timeout and a worker shutdown both cancel the job's task,
and the ``CancelledError`` bypasses each worker function's ``except
Exception`` branch, so a function that does not wrap its body in
:func:`~docverse_server.worker.functions._cancellation.record_cancellation`
(or, for one that only hands rows to arq,
:func:`~docverse_server.worker.functions._cancellation.record_handoff_cancellation`)
strands its row for a reaper (PRD #765, #699). Those helpers are context
managers inside each body, so a function that uses one also carries the
:func:`~docverse_server.worker.functions._cancellation.cancellation_recorded`
marker, which these tests read off what the three ``WorkerSettings``
classes actually register with arq.

Adding a worker function means classifying it here: either it owns (or
hands off) ``queue_jobs`` rows and goes in :data:`RECORDS_CANCELLATION`,
or it is listed in :data:`DELIBERATELY_UNWRAPPED` with the reason it
is left unwrapped.
"""

from __future__ import annotations

import inspect
import sys
from collections.abc import Callable, Collection, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import pytest
from arq.cron import CronJob
from arq.worker import Function

from docverse_server.worker.functions._cancellation import (
    CANCELLATION_RECORDED_ATTR,
)
from docverse_server.worker.main import (
    KeeperSyncWorkerSettings,
    MaintenanceWorkerSettings,
    WorkerSettings,
)

RECORDS_CANCELLATION = frozenset(
    {
        # Default pool.
        "build_processing",
        "dashboard_build",
        "dashboard_sync",
        "publish_edition",
        # Keeper-sync pool; the tier crons hold no row of their own and
        # use the hand-off helper for the child rows they create.
        "keeper_sync_project",
        "keeper_sync_run_discovery",
        "keeper_sync_tier_discovery",
        "keeper_sync_tier_main",
        "keeper_sync_tier_other",
        # Maintenance pool; ``project_github_resolve`` uses the hand-off
        # helper for the ``dashboard_sync`` rows it creates.
        "edition_reconcile",
        "git_ref_audit",
        "lifecycle_eval",
        "project_github_resolve",
        "purgatory_cleanup",
    }
)
"""The functions PRD #765 scope item 1 wraps in a cancellation helper."""

_REAPER = (
    "Reaper: sweeps other jobs' stale rows and holds none of its own; a"
    " cancel rolls back its uncommitted sweep, which the next tick repeats."
)
_DISPATCHER = (
    "Dispatcher: holds no queue_jobs row of its own; a per-org row a"
    " cancel strands before it reaches arq is an orphan for the matching"
    " reaper's orphan sweep (PRD #765 leaves the dispatchers unwrapped)."
)

DELIBERATELY_UNWRAPPED: Mapping[str, str] = {
    "ping": "Health check; touches no queue_jobs row.",
    "publish_queue_stats_cron": (
        "Publishes queue-depth gauges from Redis; touches no queue_jobs row."
    ),
    "inventory_census": (
        "Publishes the resource_inventory gauge; touches no queue_jobs row."
    ),
    "keeper_sync_reaper": _REAPER,
    "lifecycle_reaper": _REAPER,
    "build_processing_reaper": _REAPER,
    "dashboard_build_reaper": _REAPER,
    "dashboard_sync_reaper": _REAPER,
    "edition_reconcile_reaper": _REAPER,
    "publish_edition_reaper": _REAPER,
    "purgatory_cleanup_reaper": _REAPER,
    "edition_reconcile_dispatcher": _DISPATCHER,
    "git_ref_audit_discovery": _DISPATCHER,
    "lifecycle_eval_dispatcher": _DISPATCHER,
    "purgatory_cleanup_dispatcher": _DISPATCHER,
}
"""Registered functions that are not wrapped, each with the reason."""

_SETTINGS = (
    WorkerSettings,
    KeeperSyncWorkerSettings,
    MaintenanceWorkerSettings,
)


@dataclass(frozen=True)
class _Registration:
    """One coroutine a ``WorkerSettings`` class hands to arq."""

    name: str
    """The worker function's own name."""

    where: str
    """Which settings class and list registered it."""

    coroutine: Callable[..., Any]
    """What arq calls: ``instrument_arq_task``'s wrapper, which copies
    the marker over from the function with :func:`functools.wraps`."""


def _registered() -> Iterator[_Registration]:
    """Yield every coroutine the three settings classes register.

    Covers each class's ``functions`` list and its ``cron_jobs``, which
    arq registers alongside them; a function on both lists is yielded
    once for each, as each is a separately instrumented wrapper.
    """
    for settings in _SETTINGS:
        for entry in settings.functions:
            # Each list mixes ``func(...)`` wrappers with bare coroutines.
            coroutine = (
                entry.coroutine if isinstance(entry, Function) else entry
            )
            assert callable(coroutine)
            yield _Registration(
                name=inspect.unwrap(coroutine).__name__,
                where=f"{settings.__name__}.functions",
                coroutine=coroutine,
            )
        for cron_job in settings.cron_jobs:
            assert isinstance(cron_job, CronJob)
            yield _Registration(
                name=inspect.unwrap(cron_job.coroutine).__name__,
                where=f"{settings.__name__}.cron_jobs",
                coroutine=cron_job.coroutine,
            )


_REGISTERED = list(_registered())


def _params(names: Collection[str]) -> list[Any]:
    """Parametrize over the registrations of ``names``, one id each."""
    return [
        pytest.param(entry, id=f"{entry.where}:{entry.name}")
        for entry in _REGISTERED
        if entry.name in names
    ]


def test_every_registered_function_is_classified() -> None:
    """Each registered function is wrapped or listed with a reason.

    A new worker function fails this until it is classified, and a
    classification for a function no settings class registers any more
    fails it too, so neither list can drift from ``worker/main.py``.
    """
    names = {entry.name for entry in _REGISTERED}
    unwrapped = set(DELIBERATELY_UNWRAPPED)
    classified = RECORDS_CANCELLATION | unwrapped
    assert RECORDS_CANCELLATION & unwrapped == set(), "classified both ways"
    assert names - classified == set(), "registered but not classified"
    assert classified - names == set(), "classified but not registered"


@pytest.mark.parametrize("entry", _params(RECORDS_CANCELLATION))
def test_queue_job_function_records_its_cancellation(
    entry: _Registration,
) -> None:
    """A wrapped function carries the marker on what arq registered.

    The marker is applied by
    :func:`~docverse_server.worker.functions._cancellation.cancellation_recorded`
    alongside the helper inside the body, and the function's module must
    actually import one of the helpers.
    """
    marker = getattr(entry.coroutine, CANCELLATION_RECORDED_ATTR, False)
    assert marker is True, entry.name
    module = sys.modules[inspect.unwrap(entry.coroutine).__module__]
    assert hasattr(module, "record_cancellation") or hasattr(
        module, "record_handoff_cancellation"
    ), module.__name__


@pytest.mark.parametrize("entry", _params(DELIBERATELY_UNWRAPPED.keys()))
def test_unwrapped_function_carries_no_marker(entry: _Registration) -> None:
    """A function listed as unwrapped really is unmarked.

    Once one gains a wrap it belongs in :data:`RECORDS_CANCELLATION`,
    where losing the wrap again would be caught.
    """
    assert not hasattr(entry.coroutine, CANCELLATION_RECORDED_ATTR), entry.name
