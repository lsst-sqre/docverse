"""Record an arq cancellation on the ``queue_jobs`` row it interrupted.

arq (0.28) runs every job in its own task under ``asyncio.wait_for``.
Its per-function ``timeout`` cancels that task, and so does a worker
shutdown (the SIGTERM of a rolling deploy), so either way the job
function sees :exc:`asyncio.CancelledError` at whatever it was awaiting.
That is a :exc:`BaseException`: the ``except Exception`` branch every
``queue_jobs``-backed worker function uses to fail its row never runs,
the row stays ``in_progress``, and — for a keeper-sync run child — the
parent run cannot finalise until a reaper notices hours later (#699).

:func:`record_cancellation` closes that gap. A worker function wraps the
part of its body that holds an ``in_progress`` row in it; on a cancel
the helper fails the row itself, from a fresh database session, rolls
up the parent run, and re-raises so arq still records the job as failed
(PRD #765). An OOM kill gives the process no chance to run any of this,
so those rows are still the reapers' to fail.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal

import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.keeper_sync_run import KeeperSyncRunWithActivity
from docverse_server.domain.queue import QueueJob
from docverse_server.factory import Factory
from docverse_server.sentry import capture_warning
from docverse_server.services.keeper_sync_finalisation import (
    maybe_finalise_run,
    publish_run_completed,
)
from docverse_server.storage.queue_job_store import QueueJobStore

__all__ = [
    "JOB_TIMEOUT_MESSAGE",
    "TIMEOUT_REASON_SLACK",
    "CancellationReason",
    "ProgressReporter",
    "RunFinaliser",
    "keeper_sync_run_finaliser",
    "record_cancellation",
]

type CancellationReason = Literal["job_timeout", "worker_shutdown"]
"""Why arq cancelled a job, as recorded in ``errors["reason"]``."""

type ProgressReporter = Callable[[], Mapping[str, Any]]
"""Zero-argument callable returning how far a job got.

Read at cancel time and merged into the failed row's ``progress``.
"""

type RunFinaliser = Callable[
    [Factory], Awaitable[KeeperSyncRunWithActivity | None]
]
"""Rolls up the run a cancelled job belongs to.

Called with a :class:`~docverse_server.factory.Factory` bound to the
helper's fresh session, inside the transaction that failed the row, so
it sees the row as failed. Returns the run when this call drove it
terminal (a ``keeper_sync_run_completed`` metric is then published for
it), or ``None`` otherwise.
"""

JOB_TIMEOUT_MESSAGE = "Queue job cancelled at its arq timeout"
"""Title of the Sentry event a ``job_timeout`` cancel captures.

Constant so every timeout groups under one Sentry issue; the job's kind
and queue are tags and its public id is in the event's context (see
:func:`~docverse_server.sentry.capture_warning`).
"""

TIMEOUT_REASON_SLACK = timedelta(seconds=5)
"""How far short of the pool timeout a cancel still reads as a timeout.

arq starts a job's timeout clock when it creates the job's task, but the
row's ``date_started`` is only stamped once the job has checked out a
database connection and committed its pickup, a few milliseconds (or,
on a saturated pool, a few seconds) later. Measured from
``date_started``, a genuine timeout therefore lands *just under* the
pool timeout. The slack absorbs that pickup latency so a real timeout is
never recorded as a shutdown; the cost is that a shutdown inside the
last few seconds of a job's allowance is recorded as a timeout. Clock
skew needs no slack: the elapsed time is measured entirely on the
database's clock (:meth:`QueueJobStore.get_elapsed_since_start`).
"""


@dataclass(frozen=True, slots=True)
class _RecordedCancellation:
    """What the helper wrote on the row of a cancelled job."""

    job: QueueJob
    reason: CancellationReason
    elapsed_seconds: float | None
    timeout_seconds: float
    progress: Mapping[str, Any]


def keeper_sync_run_finaliser(run_id: int | None) -> RunFinaliser | None:
    """Build the cancel-path finaliser for a keeper-sync run child.

    Returns ``None`` for a job with no run attribution (a tier-cron
    ``keeper_sync_project``), so the helper touches no run at all;
    otherwise a :data:`RunFinaliser` running :func:`maybe_finalise_run`
    for ``run_id`` — the roll-up the job's own ``except Exception``
    branch runs.
    """
    if run_id is None:
        return None

    async def finalise(factory: Factory) -> KeeperSyncRunWithActivity | None:
        return await maybe_finalise_run(
            run_store=factory.create_keeper_sync_run_store(), run_id=run_id
        )

    return finalise


@asynccontextmanager
async def record_cancellation(
    ctx: dict[str, Any],
    *,
    queue_job_id: int,
    timeout_seconds: float,
    logger: structlog.stdlib.BoundLogger,
    progress: ProgressReporter | None = None,
    finalise_run: RunFinaliser | None = None,
) -> AsyncIterator[None]:
    """Fail ``queue_job_id``'s row if the wrapped body is cancelled.

    Wrap the part of a worker function's body during which it holds the
    ``in_progress`` row, from just after its ``start_if_queued`` pickup
    commits to its return. Nothing happens unless the body raises
    :exc:`asyncio.CancelledError`; then the helper:

    1. Opens a *fresh* database session through
       ``db_session_dependency`` rather than reusing the job's own, which
       may be mid-rollback from the very cancel being recorded.
    2. In one transaction, fails the row — if it is still active — with
       ``errors`` of ``{"type": "CancelledError", "reason": ...,
       "message": ..., "elapsed_seconds": ..., "timeout_seconds": ...}``,
       merges ``progress()`` into its ``progress``, and calls
       ``finalise_run``. ``reason`` is ``"job_timeout"`` when the time
       since the row's ``date_started`` has reached ``timeout_seconds``
       (less :data:`TIMEOUT_REASON_SLACK`), otherwise
       ``"worker_shutdown"``. A row that already went terminal — a
       reaper beat the cancel to it, or the job completed its row before
       a best-effort tail step was cancelled — keeps the status it
       earned, and nothing else happens.
    3. Publishes ``keeper_sync_run_completed`` when the finaliser drove
       a run terminal, after that transaction commits.
    4. Logs one ``warning`` line, and captures a Sentry event only for a
       ``job_timeout``.
    5. Re-raises the ``CancelledError`` so arq records the job failed.

    The cleanup never raises: a failure anywhere in steps 1-4 is logged
    with its traceback and the original cancel still propagates, leaving
    the row to the reaper.

    Parameters
    ----------
    ctx
        The arq job context; its ``factory_builder`` builds the stores
        the fresh session uses, and its ``events`` (when set) publish the
        run-completed metric.
    queue_job_id
        Internal id of the ``queue_jobs`` row the body holds.
    timeout_seconds
        The per-job timeout of the pool the function runs on, which the
        elapsed time is compared against to infer ``reason``.
    logger
        The job's bound logger.
    progress
        How far the job got, read at cancel time; ``None`` records none.
    finalise_run
        Rolls up the run the job belongs to (see
        :func:`keeper_sync_run_finaliser`); ``None`` for a job outside
        any run.
    """
    try:
        yield
    except asyncio.CancelledError:
        try:
            recorded = await _record(
                ctx,
                queue_job_id=queue_job_id,
                timeout=timedelta(seconds=timeout_seconds),
                logger=logger,
                progress=progress,
                finalise_run=finalise_run,
            )
            if recorded is not None:
                _report(recorded, logger=logger)
        except Exception:
            # Never let the cleanup's own failure replace the cancel: arq
            # must still see the ``CancelledError``, and the reaper fails
            # the row this could not.
            logger.exception("Failed to record the queue job's cancellation")
        raise


async def _record(
    ctx: dict[str, Any],
    *,
    queue_job_id: int,
    timeout: timedelta,
    logger: structlog.stdlib.BoundLogger,
    progress: ProgressReporter | None,
    finalise_run: RunFinaliser | None,
) -> _RecordedCancellation | None:
    """Fail the cancelled job's row and roll up its run, from a fresh session.

    Returns ``None`` when the row had already gone terminal, which
    leaves it — and its run — exactly as they were.
    """
    recorded: _RecordedCancellation | None = None
    async for session in db_session_dependency():
        factory: Factory = ctx["factory_builder"](
            session=session, logger=logger
        )
        completion: KeeperSyncRunWithActivity | None = None
        async with session.begin():
            recorded = await _fail_cancelled_job(
                factory.create_queue_job_store(),
                queue_job_id=queue_job_id,
                timeout=timeout,
                progress=progress,
            )
            if recorded is not None and finalise_run is not None:
                completion = await finalise_run(factory)
        await publish_run_completed(
            events=ctx.get("events"),
            session=session,
            org_store=factory.create_org_store(),
            completion=completion,
            logger=logger,
        )
    return recorded


async def _fail_cancelled_job(
    queue_job_store: QueueJobStore,
    *,
    queue_job_id: int,
    timeout: timedelta,
    progress: ProgressReporter | None,
) -> _RecordedCancellation | None:
    """Fail an active row with the cancellation payload and progress.

    Runs inside the caller's transaction. The fail goes through
    :meth:`QueueJobStore.fail_if_active`, which locks the row before
    reading its status, so a terminal row is never overwritten; the
    progress merge follows only once the fail has landed.
    """
    elapsed = await queue_job_store.get_elapsed_since_start(queue_job_id)
    reason: CancellationReason = (
        "job_timeout"
        if elapsed is not None and elapsed + TIMEOUT_REASON_SLACK >= timeout
        else "worker_shutdown"
    )
    elapsed_seconds = (
        round(elapsed.total_seconds(), 1) if elapsed is not None else None
    )
    timeout_seconds = timeout.total_seconds()
    failed = await queue_job_store.fail_if_active(
        queue_job_id,
        errors={
            "type": "CancelledError",
            "reason": reason,
            "message": _describe(reason, timeout=timeout),
            "elapsed_seconds": elapsed_seconds,
            "timeout_seconds": timeout_seconds,
        },
    )
    if failed is None:
        return None
    snapshot = dict(progress()) if progress is not None else {}
    if snapshot:
        failed = await queue_job_store.update_progress(queue_job_id, snapshot)
    return _RecordedCancellation(
        job=failed,
        reason=reason,
        elapsed_seconds=elapsed_seconds,
        timeout_seconds=timeout_seconds,
        progress=snapshot,
    )


def _report(
    recorded: _RecordedCancellation,
    *,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Log a recorded cancellation, and page Sentry when it was a timeout.

    One ``warning`` line either way. Only a ``job_timeout`` reaches
    Sentry: a ``worker_shutdown`` cancel happens on every rolling deploy
    and the tier crons re-drive whatever it interrupted, while a job
    that ran out its pool timeout is the signal an operator needs.
    """
    job = recorded.job
    job_public_id = serialize_base32_id(job.public_id)
    logger.warning(
        "Queue job cancelled",
        queue_job_id=job_public_id,
        queue_job_kind=job.kind.value,
        reason=recorded.reason,
        elapsed_seconds=recorded.elapsed_seconds,
        timeout_seconds=recorded.timeout_seconds,
        progress=dict(recorded.progress),
    )
    if recorded.reason != "job_timeout":
        return
    tags = {"job_function": job.kind.value}
    if job.backend_queue_name is not None:
        tags["queue_name"] = job.backend_queue_name
    capture_warning(
        JOB_TIMEOUT_MESSAGE,
        tags=tags,
        contexts={
            "queue_job_cancellation": {
                "job_public_id": job_public_id,
                "job_function": job.kind.value,
                "queue_name": job.backend_queue_name,
                "reason": recorded.reason,
                "elapsed_seconds": recorded.elapsed_seconds,
                "timeout_seconds": recorded.timeout_seconds,
                "progress": dict(recorded.progress),
            }
        },
    )


def _describe(reason: CancellationReason, *, timeout: timedelta) -> str:
    """Render the ``errors["message"]`` for a cancelled job."""
    timeout_seconds = int(timeout.total_seconds())
    if reason == "job_timeout":
        return f"arq cancelled the job at its {timeout_seconds} s timeout"
    return (
        "arq cancelled the job before its"
        f" {timeout_seconds} s timeout; the worker shut down"
    )
