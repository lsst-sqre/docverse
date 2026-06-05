"""arq cron worker that fans out per-org ``lifecycle_eval`` jobs.

Hourly cron entrypoint for the ``lifecycle_eval`` periodic background
job. On each firing the dispatcher:

1. Runs a cheap pre-flight that finds orgs with any rule at org level
   OR any non-deleted project with non-empty rules. Orgs with no rules
   anywhere are skipped — no ``queue_jobs`` row, no per-org pass.

2. Inserts the ``lifecycle_eval_runs`` row, its ``summary`` JSONB
   (``orgs_enqueued`` / ``orgs_skipped``), the per-org ``queue_jobs``
   children, and — when there is at least one child — the
   ``pending → in_progress`` transition, **all in a single
   transaction**. Either every row is durable together or none of
   them are: there is no window in which a ``lifecycle_eval_runs``
   row can exist without its children, so the
   ``has_non_terminal_run`` pre-flight cannot get wedged by a
   partial fan-out. The partial-unique non-terminal index on
   ``lifecycle_eval_runs`` is the DB-level backstop against two
   ticks racing; the dispatcher pre-checks ``has_non_terminal_run``
   *and* catches the ``IntegrityError`` from the index so a slow
   prior tick surfaces as a clean ``"skipped"`` result rather than
   an ``IntegrityError`` traceback. The per-org mutex partial-unique
   index on ``queue_jobs`` keeps concurrent ticks from doubling up
   on a single org.

3. For the all-skipped case (zero in-scope orgs) the same
   transaction transitions the run straight to ``succeeded`` — no
   children exist to finalise the run later.

4. Enqueues the actual arq jobs outside the SQL transaction (so the
   ``queue_jobs`` rows commit before any worker can pick them up)
   and writes the arq ``backend_job_id`` back onto each row. The
   orphan-tail window — row committed, ``backend_job_id IS NULL`` —
   is the responsibility of ``lifecycle_reaper``, the second
   durability backstop. The per-job arq ``timeout`` configured on
   ``MaintenanceWorkerSettings`` is the first backstop (cancels a
   runaway evaluator long before the reaper window).
"""

from __future__ import annotations

from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import JobKind, LifecycleEvalRunStatus
from docverse.domain.lifecycle_eval_run import LifecycleEvalRun
from docverse.domain.organization import Organization
from docverse.domain.queue import QueueJob
from docverse.factory import Factory
from docverse.storage.lifecycle_eval_run_store import LifecycleEvalRunStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.queues import MAINTENANCE_QUEUE_NAME

__all__ = ["lifecycle_eval_dispatcher"]


async def lifecycle_eval_dispatcher(ctx: dict[str, Any]) -> str:
    """Fan out one ``lifecycle_eval`` job per in-scope org.

    Returns ``"completed"`` for a clean fan-out (including the
    all-skipped case, which still records a run row), ``"skipped"``
    when a prior non-terminal run is in flight.
    """
    logger = structlog.get_logger("docverse.worker.lifecycle_eval_dispatcher")

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        run_store = factory.create_lifecycle_eval_run_store()
        queue_job_store = factory.create_queue_job_store()

        async with session.begin():
            if await run_store.has_non_terminal_run():
                logger.info(
                    "Skipping lifecycle_eval dispatcher tick: "
                    "a prior run is still in flight"
                )
                return "skipped"

        in_scope, skipped_count = await _resolve_in_scope_orgs(
            session=session, factory=factory
        )
        creation = await _create_run_with_children(
            session=session,
            run_store=run_store,
            queue_job_store=queue_job_store,
            orgs=in_scope,
            orgs_skipped=skipped_count,
        )
        if creation is None:
            logger.info(
                "Skipping lifecycle_eval dispatcher tick: another tick "
                "won the create race"
            )
            return "skipped"
        run, queue_jobs = creation
        logger = logger.bind(lifecycle_eval_run_id=run.id)

        if not in_scope:
            logger.info(
                "Lifecycle_eval dispatcher tick completed with no in-scope "
                "orgs",
                orgs_skipped=skipped_count,
            )
            return "completed"

        await _enqueue_arq_jobs(
            ctx=ctx,
            session=session,
            queue_job_store=queue_job_store,
            run_id=run.id,
            orgs=in_scope,
            queue_jobs=queue_jobs,
            logger=logger,
        )
        logger.info(
            "Lifecycle_eval dispatcher tick completed",
            orgs_enqueued=len(in_scope),
            orgs_skipped=skipped_count,
        )
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _resolve_in_scope_orgs(
    *, session: AsyncSession, factory: Factory
) -> tuple[list[Organization], int]:
    """Return ``(in_scope_orgs, skipped_count)`` from a pre-flight read.

    An org is in-scope iff its own ``lifecycle_rules`` column is
    populated *or* it owns at least one non-deleted project whose
    ``lifecycle_rules`` column is populated. The two-query shape
    (one read of every org, one read of every org_id that has a
    rule-bearing project) keeps the dispatcher round-trip count
    constant in the number of orgs.
    """
    org_store = factory.create_org_store()
    project_store = factory.create_project_store()
    async with session.begin():
        orgs = await org_store.list_all()
        project_org_ids = (
            await project_store.list_org_ids_with_lifecycle_rules()
        )
    in_scope: list[Organization] = []
    skipped = 0
    for org in orgs:
        if org.lifecycle_rules is not None or org.id in project_org_ids:
            in_scope.append(org)
        else:
            skipped += 1
    return in_scope, skipped


async def _create_run_with_children(
    *,
    session: AsyncSession,
    run_store: LifecycleEvalRunStore,
    queue_job_store: QueueJobStore,
    orgs: list[Organization],
    orgs_skipped: int,
) -> tuple[LifecycleEvalRun, list[QueueJob]] | None:
    """Insert the run row, summary, all child ``queue_jobs`` atomically.

    Either every row commits together or none of them do, so the
    ``has_non_terminal_run`` pre-flight cannot get wedged by a
    partial fan-out: a committed run row is always accompanied by
    its full set of child ``queue_jobs`` rows (or, in the
    all-skipped case, transitions straight to ``succeeded``). A
    crash later — during the arq enqueue loop — degenerates to the
    existing orphan-queued case (``backend_job_id IS NULL``) that
    ``lifecycle_reaper`` already handles.

    Returns ``None`` if the partial-unique non-terminal index
    rejected the insert — another dispatcher tick raced past the
    pre-flight ``has_non_terminal_run`` check and won the create.
    The caller translates this into a clean ``"skipped"`` tick.
    Returns ``(run, queue_jobs)`` otherwise; ``queue_jobs`` is in
    the same order as ``orgs`` so the arq enqueue loop can pair
    them up.
    """
    try:
        async with session.begin():
            run = await run_store.create()
            await run_store.set_summary(
                run_id=run.id,
                summary={
                    "orgs_enqueued": len(orgs),
                    "orgs_skipped": orgs_skipped,
                },
            )
            queue_jobs: list[QueueJob] = []
            for org in orgs:
                queue_job = await queue_job_store.create(
                    kind=JobKind.lifecycle_eval,
                    org_id=org.id,
                    lifecycle_eval_run_id=run.id,
                    subject_label=org.slug,
                )
                queue_jobs.append(queue_job)
            if orgs:
                await run_store.transition_status(
                    run_id=run.id,
                    new_status=LifecycleEvalRunStatus.in_progress,
                )
            else:
                await run_store.transition_status(
                    run_id=run.id,
                    new_status=LifecycleEvalRunStatus.succeeded,
                )
    except IntegrityError:
        return None
    return run, queue_jobs


async def _enqueue_arq_jobs(  # noqa: PLR0913
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    run_id: int,
    orgs: list[Organization],
    queue_jobs: list[QueueJob],
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Enqueue one arq job per pre-created ``queue_jobs`` row.

    The ``queue_jobs`` rows are already committed by
    :func:`_create_run_with_children`, so a crash between the
    arq enqueue and the ``backend_job_id`` write leaves an
    orphan-queued row (``backend_job_id IS NULL``) the
    ``lifecycle_reaper`` will sweep — mirrors the orphan-tail
    contract documented on ``KeeperSyncWorkerSettings``.
    """
    arq_queue = ctx["arq_queue"]
    for org, queue_job in zip(orgs, queue_jobs, strict=True):
        metadata = await arq_queue.enqueue(
            "lifecycle_eval",
            _queue_name=MAINTENANCE_QUEUE_NAME,
            payload={
                "org_id": org.id,
                "org_slug": org.slug,
                "lifecycle_eval_run_id": run_id,
                "queue_job_id": queue_job.id,
            },
        )
        async with session.begin():
            await queue_job_store.set_backend_job_id(queue_job.id, metadata.id)
        logger.debug(
            "Enqueued lifecycle_eval for org",
            org=org.slug,
            queue_job_id=queue_job.id,
        )
