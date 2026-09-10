"""arq cron worker that fans out per-org ``purgatory_cleanup`` jobs.

Daily cron entrypoint for the ``purgatory_cleanup`` sweep (PRD #596).
On each firing the dispatcher:

1. Checks ``config.purgatory_cleanup_enabled`` before touching the
   database. The sweep deletes object-store content permanently and
   irreversibly, so the flag has to mean *nothing happens* rather than
   *work is planned and then cancelled*: a disabled tick writes no
   ``queue_jobs`` row and enqueues no job. The cron stays registered
   either way, so an environment turns the sweep on by flipping the
   flag rather than by restarting a worker.

2. Runs a cheap pre-flight —
   :meth:`~docverse_server.storage.build_store.BuildStore.list_org_ids_with_purgeable_builds`
   — that names every org holding at least one build past *that org's*
   own retention. An org whose deleted builds are all still restorable,
   or already reclaimed, costs no ``queue_jobs`` row, no mutex slot,
   and no operator attention.

3. Creates one ``queue_jobs`` row per in-scope org with
   ``kind='purgatory_cleanup'`` and ``subject_label=org.slug`` (the
   human-readable slug, never the internal id, matching every other
   per-org fan-out) through
   :meth:`~docverse_server.storage.queue_job_store.QueueJobStore.create_unless_active`.
   The per-org mutex index is the real enforcement; going through
   ``create_unless_active`` turns a lost race into a step-over instead
   of an ``IntegrityError`` that would abandon the remaining orgs.

4. Enqueues the arq jobs *after* those rows commit — so no worker can
   pick a row up before it exists — and writes each arq
   ``backend_job_id`` back. The orphan tail (row committed,
   ``backend_job_id IS NULL``) is what ``purgatory_cleanup_reaper``
   sweeps; the per-job arq ``timeout`` on ``MaintenanceWorkerSettings``
   is the first backstop and the reaper the second.

Unlike ``lifecycle_eval`` and ``git_ref_audit`` there is no run table:
the record of a tick is the log line this function emits plus the
per-org ``purgatory_cleanup_completed`` event each fanned-out job
publishes. That is deliberate — the sweep has no cross-org rollup to
finalise, and each per-org job's ``queue_jobs`` row already carries its
own outcome.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import JobKind
from docverse_server.config import config
from docverse_server.domain.organization import Organization
from docverse_server.domain.queue import QueueJob
from docverse_server.factory import Factory
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.queues import MAINTENANCE_QUEUE_NAME

__all__ = ["purgatory_cleanup_dispatcher"]


async def purgatory_cleanup_dispatcher(ctx: dict[str, Any]) -> str:
    """Fan out one ``purgatory_cleanup`` job per org with expired builds.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``, ``arq_queue``).

    Returns
    -------
    str
        ``"skipped"`` when the feature flag is off — no rows, no jobs —
        and ``"completed"`` otherwise, including the tick that finds
        nothing to reclaim anywhere.
    """
    logger = structlog.get_logger(
        "docverse_server.worker.purgatory_cleanup_dispatcher"
    )

    if not config.purgatory_cleanup_enabled:
        logger.debug(
            "Skipping purgatory_cleanup dispatcher tick: feature flag is off"
        )
        return "skipped"

    # One clock for the whole tick: the pre-flight judges every org's
    # retention against this instant, and each per-org job re-reads it
    # for its own cutoff. Sharing it keeps the dispatcher's answer and
    # the worker's answer about the same build from disagreeing purely
    # because minutes passed between them.
    now = datetime.now(tz=UTC)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()

        in_scope, orgs_skipped = await _resolve_in_scope_orgs(
            session=session, factory=factory, now=now
        )
        if not in_scope:
            logger.info(
                "Purgatory_cleanup dispatcher tick completed with no "
                "in-scope orgs",
                orgs_enqueued=0,
                orgs_skipped=orgs_skipped,
            )
            return "completed"

        claimed = await _claim_queue_jobs(
            session=session, queue_job_store=queue_job_store, orgs=in_scope
        )
        await _enqueue_arq_jobs(
            ctx=ctx,
            session=session,
            queue_job_store=queue_job_store,
            claimed=claimed,
            logger=logger,
        )
        logger.info(
            "Purgatory_cleanup dispatcher tick completed",
            orgs_enqueued=len(claimed),
            orgs_skipped=orgs_skipped,
            orgs_mutex_held=len(in_scope) - len(claimed),
        )
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _resolve_in_scope_orgs(
    *, session: AsyncSession, factory: Factory, now: datetime
) -> tuple[list[Organization], int]:
    """Return ``(orgs_with_expired_builds, skipped_count)``.

    Two queries regardless of how many orgs exist: one read of every
    org — the dispatcher needs each in-scope org's slug for
    ``subject_label`` anyway — and one read of the org ids holding an
    expired build. The eligibility question is answered entirely in the
    second query, which adds each org's own ``purgatory_retention`` to
    each build's ``date_deleted``, because a single shared cutoff would
    either skip an org running a one-day window or burn a queue slot on
    one running ninety.
    """
    org_store = factory.create_org_store()
    build_store = factory.create_build_store()
    async with session.begin():
        orgs = await org_store.list_all()
        in_scope_ids = await build_store.list_org_ids_with_purgeable_builds(
            now=now
        )
    in_scope = [org for org in orgs if org.id in in_scope_ids]
    return in_scope, len(orgs) - len(in_scope)


async def _claim_queue_jobs(
    *,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    orgs: list[Organization],
) -> list[tuple[Organization, QueueJob]]:
    """Insert one ``queue_jobs`` row per org, skipping held mutexes.

    All the inserts share one transaction so the rows are durable
    together before any of them is enqueued. An org whose per-org mutex
    is already held — a previous night's job still running, or a
    concurrent tick on a horizontally scaled pool — yields ``None`` from
    ``create_unless_active`` and is simply left out of the returned
    pairs. Absorbing that inside a SAVEPOINT is what keeps one busy org
    from costing every org after it in the loop its sweep.
    """
    claimed: list[tuple[Organization, QueueJob]] = []
    async with session.begin():
        for org in orgs:
            queue_job = await queue_job_store.create_unless_active(
                kind=JobKind.purgatory_cleanup,
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
    leaves an orphan-queued row that ``purgatory_cleanup_reaper``
    sweeps — the same orphan-tail contract every other fan-out on this
    pool carries.
    """
    arq_queue = ctx["arq_queue"]
    for org, queue_job in claimed:
        metadata = await arq_queue.enqueue(
            "purgatory_cleanup",
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
            "Enqueued purgatory_cleanup for org",
            org=org.slug,
            queue_job_id=queue_job.id,
        )
