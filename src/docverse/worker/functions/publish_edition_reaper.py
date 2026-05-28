"""arq worker function for the ``publish_edition_reaper`` cron backstop.

Mirrors :mod:`docverse.worker.functions.dashboard_build_reaper` for
``kind='publish_edition'`` rows. Per PRD #367 §"Reaper module shape"
this is the run-less variant: ``publish_edition`` does not aggregate
into a parent run row, so the reaper only sweeps stuck ``queue_jobs``
rows and finalises nothing.

Without reconciliation a wedged ``publish_edition`` leaves an edition
in ``publishing`` status that never reaches the CDN — invisible to
operators today but corrosive: the CDN silently stays behind the
edition's intended target build. Reaping flips the wedged row to
``failed`` so the publish state can be retried on the next operator
action.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse.config import config

__all__ = ["publish_edition_reaper"]


# Window before a queued ``publish_edition`` row with no
# ``backend_job_id`` is treated as orphaned. Matches the lifecycle and
# keeper-sync reapers' :data:`_ORPHAN_IDLE_WINDOW` (5 min) so the
# staleness checks across reapers stay aligned — long enough never to
# race a healthy concurrent enqueue, short enough to free the wedged
# row on the next reaper tick.
_ORPHAN_IDLE_WINDOW = timedelta(minutes=5)


async def publish_edition_reaper(ctx: dict[str, Any]) -> str:
    """Cron-driven backstop that fails silently-stuck publish_edition rows.

    Sweeps two populations in one transaction:

    1. Silent rows
       (:meth:`QueueJobStore.fail_silent_publish_edition_jobs`) —
       ``status='in_progress'`` past
       ``config.publish_edition_reaper_threshold_seconds`` (default
       4 h, env-overridable for fast verification in non-prod).
    2. Orphan rows
       (:meth:`QueueJobStore.fail_orphaned_publish_edition_jobs`) —
       ``status='queued'`` with ``backend_job_id IS NULL`` past
       :data:`_ORPHAN_IDLE_WINDOW` (5 min).

    Returns a one-line status string for arq's result log; the
    structured ``logger.warning`` carries the detail when anything was
    reaped, and ``logger.debug`` keeps healthy ticks quiet.
    """
    logger = structlog.get_logger("docverse.worker.publish_edition_reaper")
    threshold = timedelta(
        seconds=config.publish_edition_reaper_threshold_seconds
    )

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()

        async with session.begin():
            silent = await queue_job_store.fail_silent_publish_edition_jobs(
                idle_after=threshold
            )
            orphan = await queue_job_store.fail_orphaned_publish_edition_jobs(
                idle_after=_ORPHAN_IDLE_WINDOW
            )

        reaped_count = len(silent) + len(orphan)
        if reaped_count:
            logger.warning(
                "Reaped stuck publish_edition queue jobs",
                reaped_count=reaped_count,
                silent_count=len(silent),
                orphan_count=len(orphan),
                reaped_public_ids=sorted(
                    qj.public_id for qj in (*silent, *orphan)
                ),
            )
        else:
            logger.debug("No stuck publish_edition queue jobs to reap")
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)
