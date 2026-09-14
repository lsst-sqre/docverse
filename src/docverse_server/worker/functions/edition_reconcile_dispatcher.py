"""arq cron worker that fans out per-org ``edition_reconcile`` jobs.

Half-hourly cron entrypoint for the ``edition_reconcile`` loop (PRD
#612). On each firing the dispatcher:

1. Checks ``config.edition_reconcile_enabled`` before touching the
   database, so a disabled loop writes no ``queue_jobs`` row and
   enqueues no job. The cron stays registered either way, so an
   environment turns the loop off — or back on — by flipping the flag
   rather than by restarting a worker. Unlike the purgatory sweep's
   flag this one ships **true**: the loop's only action is to enqueue a
   publish of the build an edition already points at, so leaving it off
   preserves exactly the drift it exists to repair.

2. Creates one ``queue_jobs`` row per organization with
   ``kind='edition_reconcile'`` and ``subject_label=org.slug`` (the
   human-readable slug, never the internal id, matching every other
   per-org fan-out) through
   :meth:`~docverse_server.storage.queue_job_store.QueueJobStore.create_unless_active`.
   The per-org mutex index is the real enforcement; going through
   ``create_unless_active`` turns a lost race into a step-over instead
   of an ``IntegrityError`` that would abandon the remaining orgs.

3. Enqueues the arq jobs *after* those rows commit — so no worker can
   pick a row up before it exists — and writes each arq
   ``backend_job_id`` back. The orphan tail (row committed,
   ``backend_job_id IS NULL``) is what ``edition_reconcile_reaper``
   sweeps; the per-job arq ``timeout`` on ``MaintenanceWorkerSettings``
   is the first backstop and the reaper the second.

There is deliberately no pre-flight. ``purgatory_cleanup_dispatcher``
can name the orgs worth visiting with one cheap query because "has a
build past its retention" is a fact the database already holds; drift,
by definition, is state nobody has compared against the edge yet, so any
query that could name the drifted orgs would have to do the per-org
job's own work — including its CDN read-back. Every org therefore gets a
row every tick, and a healthy org's job is a couple of indexed reads
that plan nothing.

Like ``purgatory_cleanup`` there is no run table: the record of a tick
is the log line this function emits plus the per-org
``edition_reconcile_completed`` event each fanned-out job publishes.
Each per-org ``queue_jobs`` row already carries its own outcome in
``progress``, and there is no cross-org rollup to finalise.
"""

from __future__ import annotations

from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import JobKind
from docverse_server.config import config
from docverse_server.domain.organization import Organization
from docverse_server.domain.queue import QueueJob
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.queues import MAINTENANCE_QUEUE_NAME

__all__ = ["edition_reconcile_dispatcher"]


async def edition_reconcile_dispatcher(ctx: dict[str, Any]) -> str:
    """Fan out one ``edition_reconcile`` job per organization.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``, ``arq_queue``).

    Returns
    -------
    str
        ``"skipped"`` when the feature flag is off — no rows, no jobs —
        and ``"completed"`` otherwise, including the tick that finds no
        organizations at all.
    """
    logger = structlog.get_logger(
        "docverse_server.worker.edition_reconcile_dispatcher"
    )

    if not config.edition_reconcile_enabled:
        logger.debug(
            "Skipping edition_reconcile dispatcher tick: feature flag is off"
        )
        return "skipped"

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()

        async with session.begin():
            orgs = await factory.create_org_store().list_all()
        if not orgs:
            logger.info(
                "Edition_reconcile dispatcher tick completed with no orgs",
                orgs_enqueued=0,
            )
            return "completed"

        claimed = await _claim_queue_jobs(
            session=session, queue_job_store=queue_job_store, orgs=orgs
        )
        await _enqueue_arq_jobs(
            ctx=ctx,
            session=session,
            queue_job_store=queue_job_store,
            claimed=claimed,
            logger=logger,
        )
        logger.info(
            "Edition_reconcile dispatcher tick completed",
            orgs_enqueued=len(claimed),
            orgs_mutex_held=len(orgs) - len(claimed),
        )
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _claim_queue_jobs(
    *,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    orgs: list[Organization],
) -> list[tuple[Organization, QueueJob]]:
    """Insert one ``queue_jobs`` row per org, skipping held mutexes.

    All the inserts share one transaction so the rows are durable
    together before any of them is enqueued. An org whose per-org mutex
    is already held — a previous tick's job still running, or a
    concurrent tick on a horizontally scaled pool — yields ``None`` from
    ``create_unless_active`` and is simply left out of the returned
    pairs. Absorbing that inside a SAVEPOINT is what keeps one busy org
    from costing every org after it in the loop its pass.
    """
    claimed: list[tuple[Organization, QueueJob]] = []
    async with session.begin():
        for org in orgs:
            queue_job = await queue_job_store.create_unless_active(
                kind=JobKind.edition_reconcile,
                org_id=org.id,
                subject_label=org.slug,
            )
            if queue_job is not None:
                claimed.append((org, queue_job))
    return claimed


async def _enqueue_arq_jobs(
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    claimed: list[tuple[Organization, QueueJob]],
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Enqueue one arq job per committed ``queue_jobs`` row.

    The rows are already committed by :func:`_claim_queue_jobs`, so a
    crash between the arq enqueue and the ``backend_job_id`` write
    leaves an orphan-queued row that ``edition_reconcile_reaper``
    sweeps — the same orphan-tail contract every other fan-out on this
    pool carries.
    """
    arq_queue = ctx["arq_queue"]
    for org, queue_job in claimed:
        metadata = await arq_queue.enqueue(
            "edition_reconcile",
            _queue_name=MAINTENANCE_QUEUE_NAME,
            payload={
                "org_id": org.id,
                "org_slug": org.slug,
                "queue_job_id": queue_job.id,
            },
        )
        async with session.begin():
            await queue_job_store.set_backend_job_id(
                queue_job.id, metadata.id, queue_name=metadata.queue_name
            )
        logger.debug(
            "Enqueued edition_reconcile for org",
            org=org.slug,
            queue_job_id=queue_job.id,
        )
