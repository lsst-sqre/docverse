"""arq worker function for the ``lifecycle_reaper`` cron backstop.

Mirrors :func:`docverse.worker.functions.keeper_sync.keeper_sync_reaper`
for ``kind IN ('lifecycle_eval', 'git_ref_audit')`` rows. Per the
PRDs (SQR-112 §"Reaper" plus PRD #346 §"git_ref_audit worker
function"), one wedged per-org child must not block subsequent
dispatcher ticks for that org indefinitely in either subsystem. The
reaper sweeps stuck ``queue_jobs`` rows for both kinds in a single
transaction and triggers the matching finaliser
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

from docverse.config import config
from docverse.services.git_ref_audit_finalisation import (
    maybe_finalise_git_ref_audit_run,
)
from docverse.services.lifecycle_finalisation import (
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

    Mirrors :func:`docverse.worker.functions.keeper_sync.keeper_sync_reaper`'s
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

    Sweeps two populations in one transaction:

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

    After both sweeps run, :func:`maybe_finalise_lifecycle_run` is
    invoked once per distinct ``lifecycle_eval_run_id`` seen across the
    reaped rows so the parent aggregate row rolls to its terminal
    status. Returns a one-line status string for arq's result log; the
    structured ``logger.warning`` carries the detail when anything was
    reaped.
    """
    logger = structlog.get_logger("docverse.worker.lifecycle_reaper")
    threshold = timedelta(seconds=config.lifecycle_reaper_threshold_seconds)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_lifecycle_eval_run_store()
        audit_run_store = factory.create_git_ref_audit_run_store()

        async with session.begin():
            le_silent = await queue_job_store.fail_silent_lifecycle_eval_jobs(
                idle_after=threshold
            )
            le_orphan = (
                await queue_job_store.fail_orphaned_lifecycle_eval_jobs(
                    idle_after=_ORPHAN_IDLE_WINDOW
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
            le_run_ids = {
                qj.lifecycle_eval_run_id for qj in (*le_silent, *le_orphan)
            }
            for run_id in le_run_ids:
                if run_id is None:
                    continue
                await maybe_finalise_lifecycle_run(
                    run_store=run_store, run_id=run_id
                )
            audit_run_ids = {
                qj.git_ref_audit_run_id
                for qj in (*audit_silent, *audit_orphan)
            }
            for run_id in audit_run_ids:
                if run_id is None:
                    continue
                await maybe_finalise_git_ref_audit_run(
                    run_store=audit_run_store, run_id=run_id
                )

        le_reaped = len(le_silent) + len(le_orphan)
        audit_reaped = len(audit_silent) + len(audit_orphan)
        total_reaped = le_reaped + audit_reaped
        if total_reaped:
            logger.warning(
                "Reaped stuck lifecycle queue jobs",
                reaped_count=total_reaped,
                lifecycle_eval_silent_count=len(le_silent),
                lifecycle_eval_orphan_count=len(le_orphan),
                git_ref_audit_silent_count=len(audit_silent),
                git_ref_audit_orphan_count=len(audit_orphan),
                lifecycle_eval_run_ids=sorted(
                    r for r in le_run_ids if r is not None
                ),
                git_ref_audit_run_ids=sorted(
                    r for r in audit_run_ids if r is not None
                ),
            )
        else:
            logger.debug("No stuck lifecycle queue jobs to reap")
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)
