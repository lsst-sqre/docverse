"""arq worker function for the ``edition_reconcile`` per-org pass.

The dispatcher (task #615) writes one ``queue_jobs`` row per
organization with ``kind='edition_reconcile'`` and
``subject_label=org.slug``; this worker is the per-org body of that
fan-out. For one org it reads every edition's recorded publish state,
reads back what the org's CDN actually serves, and repairs each
disagreement — a lost or stalled publish goes back on the default queue,
a key that outlived its edition is deleted.

Two properties shape the job, and both come from what it does *not* do.

It never publishes anything itself. A republish is an enqueue, so the
ordinary publish path — with its edition lock, its deleted-build guard
and its own retries — remains the only thing that writes a pointer. That
is why the job can run with ``max_tries=1`` and no compensation logic: a
tick that dies halfway has applied a prefix of its plan and left the
rest for the next tick, which re-plans from current state rather than
resuming a stored one.

It also never *clears* a publish state. Nothing here writes ``failed``
or rolls a pair back; the only rows it touches are the ones
``enqueue_publish_for_edition`` writes on its behalf. So the worst a
buggy tick can do is enqueue a redundant publish, which the publish path
treats as an ordinary republish of the build the edition already points
at, or delete a key for an edition the database has already tombstoned.

The transaction shape follows :mod:`purgatory_cleanup`: the
late-delivery guard runs first and alone, the service owns its own short
read transaction, and the terminal transition is committed at the end.
"""

from __future__ import annotations

import traceback
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse_server.config import config
from docverse_server.exceptions import NotFoundError

__all__ = ["edition_reconcile"]


async def edition_reconcile(
    ctx: dict[str, Any], payload: dict[str, Any]
) -> str:
    """Converge one organization's editions with what its CDN serves.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``).
    payload
        Job payload with ``org_id``, ``org_slug``, and ``queue_job_id``
        (the per-org ``queue_jobs`` row the dispatcher created).

    Returns
    -------
    str
        ``"skipped"`` when the late-delivery guard refused the row,
        ``"completed"`` when every planned action was applied, and
        ``"completed_with_errors"`` when at least one action failed. A
        failure that makes the whole tick impossible marks the queue job
        ``failed`` and re-raises so arq logs the job as failed.
    """
    org_id: int = payload["org_id"]
    org_slug: str = payload["org_slug"]
    queue_job_id: int = payload["queue_job_id"]
    logger = structlog.get_logger(
        "docverse_server.worker.edition_reconcile"
    ).bind(org=org_slug)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()

        async with session.begin():
            # Late-delivery guard, ahead of every read: a reaper may
            # already have failed this row and released the org's mutex,
            # in which case another reconciler may be running right now.
            if await queue_job_store.start_if_queued(queue_job_id) is None:
                return "skipped"

        try:
            async with session.begin():
                org = await factory.create_org_store().get_by_id(org_id)
            if org is None:
                msg = f"Organization {org_id} not found"
                raise NotFoundError(msg)
            service = factory.create_edition_reconcile_service()
            outcome = await service.reconcile_org(
                org, limit=config.edition_reconcile_max_actions_per_job
            )
        except Exception as exc:
            logger.exception("Edition reconciliation failed for org")
            async with session.begin():
                await queue_job_store.fail(
                    queue_job_id,
                    errors={
                        "message": str(exc),
                        "type": type(exc).__name__,
                        "traceback": traceback.format_exc(),
                    },
                )
            raise

        progress = outcome.as_progress()
        async with session.begin():
            await queue_job_store.update_progress(queue_job_id, progress)
            await queue_job_store.complete(
                queue_job_id, has_errors=outcome.has_errors
            )
        # A tick that changed nothing is the steady state and must not
        # be noise; one that re-drove a publish or removed a stranded
        # key means something else lost work, which is worth an
        # operator's attention even though the loop has already repaired
        # it.
        if outcome.acted:
            logger.warning("Reconciled drifted editions", **progress)
        else:
            logger.debug("No edition drift to reconcile", **progress)
        return "completed_with_errors" if outcome.has_errors else "completed"

    msg = "No database session available"
    raise RuntimeError(msg)
