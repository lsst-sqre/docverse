"""arq worker function for the ``dashboard_build_reaper`` cron backstop.

Mirrors :func:`docverse.worker.functions.lifecycle_reaper.lifecycle_reaper`
for ``kind='dashboard_build'`` rows. Per PRD #367 §"Reaper module shape"
this is the run-less variant: ``dashboard_build`` does not aggregate
into a parent run row, so the reaper only sweeps stuck ``queue_jobs``
rows and finalises nothing.

The user-visible motivation is the 409 on ``POST /dashboard/rebuild``
that an operator sees when a previous ``dashboard_build`` was lost by
arq: the partial unique index
``idx_queue_jobs_dashboard_build_active_uq`` keeps the wedged row in
view as long as it is ``queued`` or ``in_progress``. Reaping flips the
row to ``failed``, the partial index releases, and the next rebuild
can be enqueued.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse.config import config

__all__ = ["dashboard_build_reaper"]


# Window before a queued ``dashboard_build`` row with no
# ``backend_job_id`` is treated as orphaned. Matches the lifecycle and
# keeper-sync reapers' :data:`_ORPHAN_IDLE_WINDOW` (5 min) so the
# staleness checks across reapers stay aligned — long enough never to
# race a healthy concurrent enqueue, short enough to free the per-project
# mutex on the next reaper tick.
_ORPHAN_IDLE_WINDOW = timedelta(minutes=5)


async def dashboard_build_reaper(ctx: dict[str, Any]) -> str:
    """Cron-driven backstop that fails silently-stuck dashboard_build rows.

    Sweeps two populations in one transaction:

    1. Silent rows
       (:meth:`QueueJobStore.fail_silent_dashboard_build_jobs`) —
       ``status='in_progress'`` past
       ``config.dashboard_build_reaper_threshold_seconds`` (default 30
       min, env-overridable for fast verification in non-prod).
    2. Orphan rows
       (:meth:`QueueJobStore.fail_orphaned_dashboard_build_jobs`) —
       ``status='queued'`` with ``backend_job_id IS NULL`` past
       :data:`_ORPHAN_IDLE_WINDOW` (5 min).

    Returns a one-line status string for arq's result log; the
    structured ``logger.warning`` carries the detail when anything was
    reaped, and ``logger.debug`` keeps healthy ticks quiet.
    """
    logger = structlog.get_logger("docverse.worker.dashboard_build_reaper")
    threshold = timedelta(
        seconds=config.dashboard_build_reaper_threshold_seconds
    )

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()

        async with session.begin():
            silent = await queue_job_store.fail_silent_dashboard_build_jobs(
                idle_after=threshold
            )
            orphan = await queue_job_store.fail_orphaned_dashboard_build_jobs(
                idle_after=_ORPHAN_IDLE_WINDOW
            )

        reaped_count = len(silent) + len(orphan)
        if reaped_count:
            logger.warning(
                "Reaped stuck dashboard_build queue jobs",
                reaped_count=reaped_count,
                silent_count=len(silent),
                orphan_count=len(orphan),
                reaped_public_ids=sorted(
                    qj.public_id for qj in (*silent, *orphan)
                ),
            )
        else:
            logger.debug("No stuck dashboard_build queue jobs to reap")
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)
