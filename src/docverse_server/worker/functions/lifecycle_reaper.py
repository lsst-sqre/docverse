"""arq worker function for the ``lifecycle_reaper`` cron backstop.

Mirrors :func:`docverse_server.worker.functions.keeper_sync.keeper_sync_reaper`
for ``kind IN ('lifecycle_eval', 'git_ref_audit')`` rows. Per the
PRDs (SQR-112 §"Reaper" plus PRD #346 §"git_ref_audit worker
function"), one wedged per-org child must not block subsequent
dispatcher ticks for that org indefinitely in either subsystem. The
reaper sweeps stuck ``queue_jobs`` rows for both kinds in a single
transaction — three passes per kind covering the three ways a row
goes stuck: silent (``in_progress`` but the worker died), orphan
(``queued`` and never reached arq), and abandoned (``queued``,
reached arq, and arq then lost the job — PRD #538) — and triggers
the matching finaliser
(:func:`maybe_finalise_lifecycle_run` or
:func:`maybe_finalise_git_ref_audit_run`) for each distinct parent
run so an operator never sees either run stuck in ``in_progress``
forever. One reaper covering both kinds keeps the cron-job count
down and lets a single ``lifecycle_reaper_threshold_seconds``
operator knob govern the two subsystems together.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse_server.config import config
from docverse_server.services.git_ref_audit_finalisation import (
    maybe_finalise_git_ref_audit_run,
)
from docverse_server.services.lifecycle_finalisation import (
    maybe_finalise_lifecycle_run,
)

__all__ = ["lifecycle_reaper"]


# Window before a queued ``lifecycle_eval`` row with no ``backend_job_id``
# is treated as orphaned. Matches ``keeper_sync.py``'s
# :data:`_ORPHAN_IDLE_WINDOW` (5 min) so the staleness checks across the
# two reapers stay aligned — long enough never to race a healthy
# concurrent dispatcher mid-fanout, short enough to free a stuck mutex
# on the next reaper tick.
_ORPHAN_IDLE_WINDOW = timedelta(minutes=5)


async def lifecycle_reaper(ctx: dict[str, Any]) -> str:
    """Cron-driven backstop that finalises silently-stuck lifecycle_eval rows.

    Mirrors
    :func:`docverse_server.worker.functions.keeper_sync.keeper_sync_reaper`'s
    silent / orphan split for the lifecycle_eval subsystem. arq's
    per-function ``timeout`` covers the common case (a job actually runs
    past the timeout and arq cancels it), but a worker pod that's
    OOM-killed mid-job or a job that arq itself loses leaves a per-org
    ``queue_jobs`` row stuck in ``in_progress`` (or ``queued``, if the
    dispatcher crashed between SQL commit and arq enqueue) — and with
    it the parent ``lifecycle_eval_runs`` row, which can never finalise
    while ``pending_count > 0``. The per-org mutex
    ``idx_queue_jobs_lifecycle_eval_active_uq`` then blocks all
    subsequent dispatcher ticks for that org.

    Sweeps three populations per kind in one transaction:

    1. Silent rows
       (:meth:`QueueJobStore.fail_silent_lifecycle_eval_jobs`) —
       ``status='in_progress'`` past
       ``config.lifecycle_reaper_threshold_seconds`` (default 6 h,
       env-overridable for fast verification in non-prod).
    2. Orphan rows
       (:meth:`QueueJobStore.fail_orphaned_lifecycle_eval_jobs`) —
       ``status='queued'`` with ``backend_job_id IS NULL`` past
       :data:`_ORPHAN_IDLE_WINDOW` (5 min, matching the keeper-sync
       orphan window).
    3. Abandoned rows
       (:meth:`QueueJobStore.fail_abandoned_lifecycle_eval_jobs`) — the
       third loss mode PRD #538 identified: the row *did* reach arq and
       arq then lost the job, so neither the silent pass
       (``in_progress`` only) nor the orphan pass (``backend_job_id IS
       NULL`` only) can see it, while the per-org mutex keeps counting
       it as live work.

    The abandoned passes ask the queue backend whether arq still knows
    each candidate before failing it, so a job merely backed up behind a
    saturated maintenance pool is never cancelled. That is the reaper's
    only dependency beyond the stores; when the backend is unreachable
    those two passes abort for the tick (logging a warning and mutating
    nothing) while the silent and orphan passes proceed. They reuse the
    silent threshold rather than the short orphan window: a row that
    reached arq deserves the same benefit of the doubt a running job
    gets before being declared dead.

    After all sweeps run, :func:`maybe_finalise_lifecycle_run` is
    invoked once per distinct ``lifecycle_eval_run_id`` seen across the
    reaped rows so the parent aggregate row rolls to its terminal
    status. Returns a one-line status string for arq's result log; the
    structured ``logger.warning`` carries the detail when anything was
    reaped.
    """
    logger = structlog.get_logger("docverse_server.worker.lifecycle_reaper")
    threshold = timedelta(seconds=config.lifecycle_reaper_threshold_seconds)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_lifecycle_eval_run_store()
        audit_run_store = factory.create_git_ref_audit_run_store()
        # The abandoned sweeps ask arq whether it still knows each
        # candidate job, so the reaper now needs a queue backend (PRD
        # #538 §Summary, "Reaper dependency change").
        queue_backend = factory.create_queue_backend()

        async with session.begin():
            le_silent = await queue_job_store.fail_silent_lifecycle_eval_jobs(
                idle_after=threshold
            )
            le_orphan = (
                await queue_job_store.fail_orphaned_lifecycle_eval_jobs(
                    idle_after=_ORPHAN_IDLE_WINDOW
                )
            )
            le_abandoned = (
                await queue_job_store.fail_abandoned_lifecycle_eval_jobs(
                    idle_after=threshold, queue_backend=queue_backend
                )
            )
            audit_silent = (
                await queue_job_store.fail_silent_git_ref_audit_jobs(
                    idle_after=threshold
                )
            )
            audit_orphan = (
                await queue_job_store.fail_orphaned_git_ref_audit_jobs(
                    idle_after=_ORPHAN_IDLE_WINDOW
                )
            )
            audit_abandoned = (
                await queue_job_store.fail_abandoned_git_ref_audit_jobs(
                    idle_after=threshold, queue_backend=queue_backend
                )
            )
            le_run_ids = {
                qj.lifecycle_eval_run_id
                for qj in (*le_silent, *le_orphan, *le_abandoned)
            }
            for run_id in le_run_ids:
                if run_id is None:
                    continue
                await maybe_finalise_lifecycle_run(
                    run_store=run_store, run_id=run_id
                )
            audit_run_ids = {
                qj.git_ref_audit_run_id
                for qj in (*audit_silent, *audit_orphan, *audit_abandoned)
            }
            for run_id in audit_run_ids:
                if run_id is None:
                    continue
                await maybe_finalise_git_ref_audit_run(
                    run_store=audit_run_store, run_id=run_id
                )

        by_sweep = (
            ("lifecycle_eval_silent", le_silent),
            ("lifecycle_eval_orphan", le_orphan),
            ("lifecycle_eval_abandoned", le_abandoned),
            ("git_ref_audit_silent", audit_silent),
            ("git_ref_audit_orphan", audit_orphan),
            ("git_ref_audit_abandoned", audit_abandoned),
        )
        total_reaped = sum(len(jobs) for _, jobs in by_sweep)
        if total_reaped:
            logger.warning(
                "Reaped stuck lifecycle queue jobs",
                reaped_count=total_reaped,
                lifecycle_eval_silent_count=len(le_silent),
                lifecycle_eval_orphan_count=len(le_orphan),
                lifecycle_eval_abandoned_count=len(le_abandoned),
                git_ref_audit_silent_count=len(audit_silent),
                git_ref_audit_orphan_count=len(audit_orphan),
                git_ref_audit_abandoned_count=len(audit_abandoned),
                lifecycle_eval_run_ids=sorted(
                    r for r in le_run_ids if r is not None
                ),
                git_ref_audit_run_ids=sorted(
                    r for r in audit_run_ids if r is not None
                ),
                # Per-row detail so a postmortem can tell which sweep
                # claimed a row and cross-reference the arq job ID that
                # went missing, without querying the database.
                reaped_jobs=[
                    {
                        "public_id": qj.public_id,
                        "sweep": sweep,
                        "backend_job_id": qj.backend_job_id,
                    }
                    for sweep, jobs in by_sweep
                    for qj in jobs
                ],
            )
        else:
            logger.debug("No stuck lifecycle queue jobs to reap")
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)
