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

:func:`record_handoff_cancellation` is its sibling for the other window
a cancel can strand a row in: between a job committing a *child* row
and handing that child's job to arq. The tier crons and
``project_github_resolve`` hold no row of their own, only these; a
cancel there leaves the child ``queued`` with no ``backend_job_id`` — an
orphan the reapers' orphan sweeps fail only once it has idled past their
window, holding any active-job mutex it occupies until then. The helper
fails it at once.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
)
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
    fail_run_for_lost_discovery,
    maybe_finalise_run,
    publish_run_completed,
)
from docverse_server.storage.queue_job_store import QueueJobStore

__all__ = [
    "ARQ_DEFAULT_JOB_TIMEOUT_SECONDS",
    "JOB_TIMEOUT_MESSAGE",
    "TIMEOUT_REASON_SLACK",
    "CancellationReason",
    "ProgressReporter",
    "RunFinaliser",
    "discovery_run_finaliser",
    "keeper_sync_run_finaliser",
    "record_cancellation",
    "record_handoff_cancellation",
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
"""Closes out what a cancelled job owned besides its row.

Usually that is the keeper-sync run the job belongs to, rolled up as the
job's own error path would; for a job outside any run it is whatever
else that error path marks failed — ``build_processing``'s build,
``dashboard_sync``'s binding.

Called with a :class:`~docverse_server.factory.Factory` bound to the
helper's fresh session, inside the transaction that failed the row, so
it sees the row as failed. Returns the run when this call drove it
terminal (a ``keeper_sync_run_completed`` metric is then published for
it), or ``None`` otherwise.
"""

ARQ_DEFAULT_JOB_TIMEOUT_SECONDS = 300
"""The per-job timeout arq applies to a function that sets none, in seconds.

arq's ``Worker`` defaults ``job_timeout`` to this, and none of the three
``WorkerSettings`` classes overrides it, so it is the timeout every
function registered without its own ``func(..., timeout=...)`` — and
every ``cron(...)`` job, whose timeout defaults to the worker's — runs
under: ``build_processing``, ``dashboard_build`` and ``dashboard_sync``
on the default pool, the keeper-sync tier crons, and
``project_github_resolve``. Those functions pass it to
:func:`record_cancellation` or :func:`record_handoff_cancellation` so
the ``reason`` is inferred against the timeout arq actually enforces on
them, not their pool's longest one.
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


def discovery_run_finaliser(run_id: int) -> RunFinaliser:
    """Build the cancel-path finaliser for a ``keeper_sync_run_discovery``.

    A cancelled discovery has fanned out at most part of its run, so
    child counters cannot describe the outcome: the run goes ``failed``
    through :func:`fail_run_for_lost_discovery`, the verdict discovery's
    own ``except`` branch and the reaper's abandoned-discovery sweep
    reach for the same loss. Children it had already enqueued run on,
    and their roll-ups then find the run terminal and leave it be.

    The finaliser returns ``None`` whatever it did, so no
    ``keeper_sync_run_completed`` metric is published — matching the
    discovery-failure branch, which publishes none either.
    """

    async def finalise(factory: Factory) -> KeeperSyncRunWithActivity | None:
        await fail_run_for_lost_discovery(
            run_store=factory.create_keeper_sync_run_store(), run_id=run_id
        )
        return None

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
    reason = _infer_reason(elapsed, timeout=timeout)
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


def _infer_reason(
    elapsed: timedelta | None, *, timeout: timedelta
) -> CancellationReason:
    """Tell arq's timeout from a worker shutdown by how long the job ran.

    ``elapsed`` within :data:`TIMEOUT_REASON_SLACK` of ``timeout`` (or
    past it) is the timeout; anything shorter, or an unknown running
    time, is a shutdown.
    """
    if elapsed is not None and elapsed + TIMEOUT_REASON_SLACK >= timeout:
        return "job_timeout"
    return "worker_shutdown"


def _describe(reason: CancellationReason, *, timeout: timedelta) -> str:
    """Render the ``errors["message"]`` for a cancelled job."""
    timeout_seconds = int(timeout.total_seconds())
    if reason == "job_timeout":
        return f"arq cancelled the job at its {timeout_seconds} s timeout"
    return (
        "arq cancelled the job before its"
        f" {timeout_seconds} s timeout; the worker shut down"
    )


@dataclass(frozen=True, slots=True)
class _RecordedHandoffCancellation:
    """What the hand-off helper failed after its creator was cancelled."""

    orphans: list[QueueJob]
    job_function: str
    reason: CancellationReason
    elapsed_seconds: float
    timeout_seconds: float


@asynccontextmanager
async def record_handoff_cancellation(
    ctx: dict[str, Any],
    *,
    queue_job_ids: Callable[[], Iterable[int]],
    job_function: str,
    started: float,
    timeout_seconds: float,
    logger: structlog.stdlib.BoundLogger,
    finalise_run: RunFinaliser | None = None,
) -> AsyncIterator[None]:
    """Fail the child rows a cancel stranded before they reached arq.

    Wrap the commit-then-enqueue hand-off of a job that creates
    ``queue_jobs`` rows for *other* jobs without holding a row of its
    own. Nothing happens unless the body raises
    :exc:`asyncio.CancelledError`; then the helper reads
    ``queue_job_ids()`` — at cancel time, so a row created inside the
    block is included — and, from a fresh database session and in one
    transaction, fails each that
    :meth:`~docverse_server.storage.queue_job_store.QueueJobStore.fail_if_undispatched`
    finds still ``queued`` with no ``backend_job_id``. Their ``errors``
    carry the :func:`record_cancellation` keys plus ``job_function``,
    the creator whose cancel orphaned them. A row already stamped with
    its backend job id, or whose creating transaction the cancel rolled
    back, is left alone. A cancel that lands while the enqueue itself is
    in flight cannot tell whether arq took the job; failing the row then
    costs at most that one job, which finds its row terminal at pickup
    and skips, and the creator's next run re-drives it.

    When the stranded rows belong to a keeper-sync run, ``finalise_run``
    rolls that run up in the same transaction, once anything was failed
    — a ``keeper_sync_project`` continuation is run-attributed, and with
    its predecessor already completed no other job would ever finalise
    the run — and ``keeper_sync_run_completed`` is published when that
    drives it terminal.

    The ``reason`` is inferred from ``started``, a :func:`time.monotonic`
    reading taken when the creator began: an orphaned row was never
    picked up, so it has no ``date_started`` to measure from. One
    ``warning`` line is logged when anything was failed, a Sentry event
    is captured for a ``job_timeout``, and the ``CancelledError`` is
    re-raised. As with :func:`record_cancellation`, a failure in the
    cleanup itself is logged and the cancel still propagates, leaving the
    rows to the orphan sweeps.

    Parameters
    ----------
    ctx
        The arq job context; its ``factory_builder`` builds the store the
        fresh session uses.
    queue_job_ids
        Returns the internal ids of the rows the hand-off may have
        stranded, such as a dispatcher's
        :meth:`~docverse_server.services.queue_dispatch.QueueDispatcher.undispatched`.
    job_function
        The creator's arq function name, recorded on the failed rows and
        used as the Sentry ``job_function`` tag.
    started
        :func:`time.monotonic` when the creator's arq job began.
    timeout_seconds
        The creator's arq per-job timeout.
    logger
        The creator's bound logger.
    finalise_run
        Rolls up the run the stranded rows belong to (see
        :func:`keeper_sync_run_finaliser`); ``None`` for rows outside any
        run.
    """
    try:
        yield
    except asyncio.CancelledError:
        try:
            recorded = await _record_handoff(
                ctx,
                queue_job_ids=queue_job_ids,
                job_function=job_function,
                elapsed=timedelta(seconds=time.monotonic() - started),
                timeout=timedelta(seconds=timeout_seconds),
                logger=logger,
                finalise_run=finalise_run,
            )
            if recorded is not None:
                _report_handoff(recorded, logger=logger)
        except Exception:
            logger.exception("Failed to record the queue job's cancellation")
        raise


async def _record_handoff(
    ctx: dict[str, Any],
    *,
    queue_job_ids: Callable[[], Iterable[int]],
    job_function: str,
    elapsed: timedelta,
    timeout: timedelta,
    logger: structlog.stdlib.BoundLogger,
    finalise_run: RunFinaliser | None,
) -> _RecordedHandoffCancellation | None:
    """Fail the stranded rows, and roll up their run, from a fresh session.

    Returns ``None`` when there was nothing to fail.
    """
    ids = list(dict.fromkeys(queue_job_ids()))
    if not ids:
        return None
    reason = _infer_reason(elapsed, timeout=timeout)
    elapsed_seconds = round(elapsed.total_seconds(), 1)
    timeout_seconds = timeout.total_seconds()
    errors = {
        "type": "CancelledError",
        "reason": reason,
        "message": _describe_handoff(
            reason, timeout=timeout, job_function=job_function
        ),
        "elapsed_seconds": elapsed_seconds,
        "timeout_seconds": timeout_seconds,
        "job_function": job_function,
    }
    orphans: list[QueueJob] = []
    async for session in db_session_dependency():
        factory: Factory = ctx["factory_builder"](
            session=session, logger=logger
        )
        queue_job_store = factory.create_queue_job_store()
        completion: KeeperSyncRunWithActivity | None = None
        async with session.begin():
            for queue_job_id in ids:
                failed = await queue_job_store.fail_if_undispatched(
                    queue_job_id, errors=errors
                )
                if failed is not None:
                    orphans.append(failed)
            if orphans and finalise_run is not None:
                completion = await finalise_run(factory)
        await publish_run_completed(
            events=ctx.get("events"),
            session=session,
            org_store=factory.create_org_store(),
            completion=completion,
            logger=logger,
        )
    if not orphans:
        return None
    return _RecordedHandoffCancellation(
        orphans=orphans,
        job_function=job_function,
        reason=reason,
        elapsed_seconds=elapsed_seconds,
        timeout_seconds=timeout_seconds,
    )


def _report_handoff(
    recorded: _RecordedHandoffCancellation,
    *,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Log the failed orphans, and page Sentry when it was a timeout."""
    orphan_ids = [
        serialize_base32_id(job.public_id) for job in recorded.orphans
    ]
    logger.warning(
        "Queue job cancelled",
        job_function=recorded.job_function,
        reason=recorded.reason,
        elapsed_seconds=recorded.elapsed_seconds,
        timeout_seconds=recorded.timeout_seconds,
        orphaned_queue_job_ids=orphan_ids,
        orphaned_queue_job_kinds=[job.kind.value for job in recorded.orphans],
    )
    if recorded.reason != "job_timeout":
        return
    capture_warning(
        JOB_TIMEOUT_MESSAGE,
        tags={"job_function": recorded.job_function},
        contexts={
            "queue_job_cancellation": {
                "job_function": recorded.job_function,
                "reason": recorded.reason,
                "elapsed_seconds": recorded.elapsed_seconds,
                "timeout_seconds": recorded.timeout_seconds,
                "orphaned_queue_job_ids": orphan_ids,
            }
        },
    )


def _describe_handoff(
    reason: CancellationReason, *, timeout: timedelta, job_function: str
) -> str:
    """Render the ``errors["message"]`` for a row orphaned by a cancel."""
    timeout_seconds = int(timeout.total_seconds())
    if reason == "job_timeout":
        return (
            f"arq cancelled {job_function} at its {timeout_seconds} s"
            " timeout before it handed this job to the queue"
        )
    return (
        f"arq cancelled {job_function} before it handed this job to the"
        " queue; the worker shut down"
    )
