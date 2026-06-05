"""Tests for ``instrument_arq_task`` and its WorkerSettings wiring.

arq has no built-in Sentry integration, so
:func:`docverse.sentry.instrument_arq_task` is the seam that gives
every captured event from a worker function the task name as its
``transaction`` (instead of arq's default ``"unknown arq task"``
fallback). These tests pin three contracts:

1. ``functools.wraps`` propagates the wrapped function's ``__name__``,
   ``__qualname__``, and ``__module__`` -- arq's :func:`arq.func` /
   default registration both key on ``__qualname__``, so a regression
   here would silently rename every task in Redis.

2. A captured Sentry event from a wrapped task carries ``transaction =
   <fn name>`` and ``tags["arq.job_id"] = ctx["job_id"]`` so an
   operator can grep alerts by task and cross-reference against pod
   logs.

3. The wrapper is a no-op for the SDK when Sentry is uninitialised --
   i.e. ``nox -s test`` runs without :envvar:`SENTRY_DSN` do not blow
   up just because the wrapper opened a transaction.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
import sentry_sdk
from arq import func
from arq.typing import WorkerCoroutine
from safir.testing.sentry import (
    TestTransport,
    capture_events_fixture,
    sentry_init_fixture,
)

from docverse.sentry import initialize_sentry, instrument_arq_task
from docverse.worker.main import (
    KeeperSyncWorkerSettings,
    MaintenanceWorkerSettings,
    WorkerSettings,
    startup_maintenance,
)


def _patch_sentry_init_with_test_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force every ``sentry_sdk.init`` to use ``TestTransport``.

    Mirrors the helper in ``tests/sentry_test.py`` -- the production
    ``initialize_sentry`` wrapper does not expose a transport hook, so
    the patch redirects at the SDK boundary rather than asking the
    wrapper to expose a testing-only knob.
    """
    real_init = sentry_sdk.init

    def init_with_test_transport(*args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("transport", TestTransport())
        return real_init(*args, **kwargs)

    monkeypatch.setattr(sentry_sdk, "init", init_with_test_transport)


async def _example_task(ctx: dict[str, Any], payload: dict[str, Any]) -> str:
    """Return a marker echo; signature mirrors the production tasks."""
    return f"{ctx['job_id']}:{payload['marker']}"


def test_instrument_arq_task_preserves_function_identity() -> None:
    """``functools.wraps`` leaves arq's registration key unchanged.

    :func:`arq.func` keys on ``coroutine.__qualname__`` (see
    ``arq.worker.func``), and the in-process function table the worker
    uses on dispatch keys on the same name. Renaming the wrapped
    function would break every existing ``QueueBackend.enqueue(job_type=
    "<fn name>", ...)`` call site -- including the cross-references
    from :mod:`docverse.services.publish_enqueue` and
    :mod:`docverse.services.dashboard.enqueue` -- so this contract is
    load-bearing.
    """
    wrapped = instrument_arq_task(_example_task)
    # ``WorkerCoroutine`` only declares ``__qualname__``, but
    # ``functools.wraps`` propagates ``__module__`` too. Both are
    # used by :func:`arq.func` when building the ``Function`` registration
    # entry, so the test pins both.
    assert wrapped.__qualname__ == _example_task.__qualname__
    assert wrapped.__module__ == _example_task.__module__


def test_instrument_arq_task_keeps_arq_func_registration_name() -> None:
    """``arq.func`` registers the wrapped task under the original name.

    Direct end-to-end check that the ``functools.wraps`` contract above
    survives :func:`arq.func`'s ``coroutine.__qualname__`` fallback,
    which is the actual registration path used by the production
    :class:`KeeperSyncWorkerSettings` / :class:`MaintenanceWorkerSettings`
    classes.
    """
    registered = func(instrument_arq_task(_example_task))
    assert registered.name == _example_task.__qualname__


@pytest.mark.asyncio
async def test_instrument_arq_task_sets_transaction_and_job_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A captured event from a wrapped task gets ``transaction`` and tags.

    The alert that motivated this seam (DOCVERSE-4 on the production
    Sentry org) showed ``culprit: "unknown arq task"`` and ``transaction
    = "unknown arq task"`` because arq has no SDK integration to set the
    transaction name. The wrapper opens a ``queue.task.arq``
    transaction named after the function, so any error captured inside
    it inherits both ``event["transaction"] == fn.__name__`` and the
    ``arq.job_id`` / ``arq.job_try`` tags. Asserting on both fields
    locks the operator-facing contract -- alerts filterable by task,
    cross-referenceable to pod logs by job_id.
    """
    monkeypatch.setenv("SENTRY_DSN", "https://test@example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "test")
    _patch_sentry_init_with_test_transport(monkeypatch)

    boom_message = "intentional task failure"

    async def boom_task(ctx: dict[str, Any]) -> None:
        raise RuntimeError(boom_message)

    wrapped = instrument_arq_task(boom_task)

    with sentry_init_fixture():
        initialize_sentry(component="worker")
        captured = capture_events_fixture(monkeypatch)()

        ctx: dict[str, Any] = {"job_id": "abc123", "job_try": 2}
        with pytest.raises(RuntimeError, match=boom_message):
            await wrapped(ctx)

    assert len(captured.errors) == 1
    event = captured.errors[0]
    assert event["transaction"] == "boom_task"
    assert event["tags"]["arq.job_id"] == "abc123"
    assert event["tags"]["arq.job_try"] == "2"


@pytest.mark.asyncio
async def test_instrument_arq_task_is_noop_when_sentry_uninitialised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without ``SENTRY_DSN``, the wrapper still runs the task cleanly.

    Pins the contract that ``nox -s test`` and local-dev arq workers --
    which deliberately leave Sentry uninitialised via
    :func:`safir.sentry.should_enable_sentry` -- still execute wrapped
    tasks. ``sentry_sdk.start_transaction`` returns a ``NoOpSpan`` when
    no client is bound, and ``isolation_scope`` is always safe; the
    wrapper must rely on both of those to not need a ``should_enable_sentry``
    short-circuit of its own.
    """
    monkeypatch.delenv("SENTRY_DSN", raising=False)

    async def task(ctx: dict[str, Any]) -> str:
        return f"ran:{ctx['job_id']}"

    wrapped = instrument_arq_task(task)

    with sentry_init_fixture():
        initialize_sentry(component="worker")
        assert sentry_sdk.is_initialized() is False
        result = await wrapped({"job_id": "xyz"})

    assert result == "ran:xyz"


def test_worker_settings_registers_tasks_under_original_names() -> None:
    """Every WorkerSettings entry registers under its raw function name.

    Backstop against a future regression where someone adds a task or
    cron job to :class:`WorkerSettings`,
    :class:`KeeperSyncWorkerSettings`, or
    :class:`MaintenanceWorkerSettings` and forgets to wrap it with
    :func:`instrument_arq_task` (which would leave it producing
    ``transaction: "unknown arq task"`` alerts) -- or wraps it with
    something that breaks :func:`functools.wraps` and silently renames
    every Redis-side ``job_type``.

    The assertion is: for every entry in ``functions`` and
    ``cron_jobs``, the registered name matches the underlying
    coroutine's ``__qualname__``, AND every registered coroutine has
    been routed through :func:`instrument_arq_task` (detected via the
    ``__wrapped__`` attribute that :func:`functools.wraps` sets).
    """
    settings_classes = (
        WorkerSettings,
        KeeperSyncWorkerSettings,
        MaintenanceWorkerSettings,
    )
    for settings in settings_classes:
        # ``functions`` may hold raw coroutines or ``arq.Function``
        # instances (the latter when wrapped with ``arq.func`` for
        # per-job ``timeout`` / ``max_tries``); arq's own ``Worker``
        # normalises both via ``map(func, functions)``, so the test
        # mirrors that to inspect every entry uniformly. ``cron_jobs``
        # entries are already ``CronJob`` instances and expose
        # ``.coroutine`` / ``.name`` directly. The ``cast`` is needed
        # because the list-literal type widens to ``object`` once both
        # shapes are mixed in one list.
        functions = [
            func(cast("WorkerCoroutine", f)) for f in settings.functions
        ]
        cron_jobs = list(getattr(settings, "cron_jobs", None) or [])
        for entry in (*functions, *cron_jobs):
            coroutine = entry.coroutine
            # ``CronJob.name`` is ``"cron:" + coroutine.__qualname__``,
            # so only enforce the rename-detector against the suffix.
            registered_suffix = entry.name.removeprefix("cron:")
            assert coroutine.__qualname__ == registered_suffix, (
                f"{settings.__name__}: registered name {entry.name!r}"
                f" drifted from coroutine name {coroutine.__qualname__!r}"
            )
            assert hasattr(coroutine, "__wrapped__"), (
                f"{settings.__name__}: task {entry.name!r} is not wrapped"
                " with instrument_arq_task"
            )


@pytest.mark.asyncio
async def test_startup_maintenance_uses_worker_maintenance_component(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The maintenance startup tags Sentry events with ``worker-maintenance``.

    ``MaintenanceWorkerSettings.on_startup`` funnels through the shared
    ``_startup`` body (DB / Redis init) that every pool reuses; the only
    intentional divergence is the Sentry ``component`` tag that lets the
    maintenance pool's events be filtered apart from the default and
    keeper-sync pools. Rather than stand up the full worker environment,
    patch ``_startup`` and assert the maintenance wrapper forwards
    ``component="worker-maintenance"`` — the one value that
    distinguishes this pool.
    """
    captured: dict[str, Any] = {}

    async def fake_startup(ctx: dict[str, Any], *, component: str) -> None:
        captured["component"] = component

    monkeypatch.setattr("docverse.worker.main._startup", fake_startup)
    await startup_maintenance({})
    assert captured["component"] == "worker-maintenance"
