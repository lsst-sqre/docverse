"""Tests for the default-queue ``WorkerSettings`` registration shape.

``publish_edition`` is the one default-pool job that deliberately
sleeps: it queues on the process-wide CDN purge coalescer and can then
sit in the purger's rate-limit backoff. Registered bare it inherited
arq's 300 s default timeout, which is neither derived from those waits
nor visible to an operator. These tests pin the explicit budget.
"""

from __future__ import annotations

from typing import Any

from arq.worker import Function

from docverse_server.config import Configuration
from docverse_server.storage._http_retry import MAX_BACKOFF_SECONDS
from docverse_server.storage.cdncachepurger._cloudflare import (
    _MAX_ATTEMPTS as _PURGE_MAX_ATTEMPTS,
)
from docverse_server.worker.functions import publish_edition
from docverse_server.worker.main import WorkerSettings

_config = Configuration()


def _underlying(coroutine: Any) -> Any:
    """Return the raw function under any ``instrument_arq_task`` wrap.

    Every task on the production WorkerSettings is wrapped with
    :func:`docverse_server.sentry.instrument_arq_task`, which uses
    :func:`functools.wraps` and therefore exposes the original
    coroutine via ``__wrapped__``. This helper peels exactly one layer
    (or returns the value as-is if it is already raw) so the
    registration-shape assertions in this module can compare against
    the unwrapped function imported from ``worker.functions``.
    """
    return getattr(coroutine, "__wrapped__", coroutine)


def _function_by_coroutine(coro: object) -> Function:
    for entry in WorkerSettings.functions:
        if (
            isinstance(entry, Function)
            and _underlying(entry.coroutine) is coro
        ):
            return entry
    msg = f"No registered Function wraps {coro!r}"
    raise AssertionError(msg)


def test_publish_edition_declares_an_explicit_timeout() -> None:
    """The publish job carries its own budget, not arq's 300 s default."""
    entry = _function_by_coroutine(publish_edition)
    assert entry.timeout_s == float(
        _config.publish_edition_job_timeout_seconds
    )


def test_publish_edition_timeout_clears_the_purge_backoff() -> None:
    """The budget covers the coalescer wait plus a full purge backoff.

    Worst case for one publish is the coalescer's throttle interval
    followed by four purge attempts spaced by the clamped ``Retry-After``
    ceiling. Anything at or below that would cancel healthy publishes
    that are merely riding out a Cloudflare 429.
    """
    worst_case_purge = _config.cdn_purge_min_interval_seconds + (
        (_PURGE_MAX_ATTEMPTS - 1) * MAX_BACKOFF_SECONDS
    )
    assert _config.publish_edition_job_timeout_seconds > worst_case_purge


def test_publish_edition_timeout_sits_below_its_reaper_threshold() -> None:
    """The job is cancelled long before the reaper would fail its row.

    The reaper is the backstop for a worker that died without arq ever
    firing a timeout, so a budget above the threshold would invert the
    two and let the reaper fail jobs that are still running.
    """
    assert (
        _config.publish_edition_job_timeout_seconds
        < _config.publish_edition_reaper_threshold_seconds
    )


def test_publish_edition_keeps_arqs_retry_policy() -> None:
    """Adding a timeout must not silently change the retry behaviour.

    Unlike the keeper-sync and maintenance pools, ``publish_edition``
    handles its own failures (``except Exception`` → ``queue_job_store``
    ``fail()`` → ``return "failed"``), so it never relied on
    ``max_tries=1`` to surface them promptly. Leaving ``max_tries``
    unset keeps the worker-level default this job already had.
    """
    entry = _function_by_coroutine(publish_edition)
    assert entry.max_tries is None
