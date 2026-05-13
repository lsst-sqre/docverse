"""arq cron worker that fans out per-org ``lifecycle_eval`` jobs.

Hourly cron entrypoint for the ``lifecycle_eval`` periodic background
job. On each firing the dispatcher:

1. Runs a cheap pre-flight that finds orgs with any rule at org level
   OR any non-deleted project with non-empty rules. Orgs with no rules
   anywhere are skipped — no ``queue_jobs`` row, no per-org pass.

2. Creates one ``lifecycle_eval_runs`` row in ``pending`` status. The
   partial-unique non-terminal index on that table is the DB-level
   backstop against two ticks racing; the dispatcher pre-checks
   ``has_non_terminal_run`` *and* catches the ``IntegrityError`` from
   the index (the pre-check and the insert are in separate
   transactions, so two ticks can both clear the pre-check) so a
   slow prior tick surfaces as a clean ``"skipped"`` result rather
   than an ``IntegrityError`` traceback.

3. Inserts one ``queue_jobs`` row per in-scope org with
   ``kind='lifecycle_eval'``, ``subject_label=org.slug``, and
   ``lifecycle_eval_run_id`` set to the new run. The per-org mutex
   partial-unique index keeps concurrent ticks from doubling up.

4. Writes the ``summary`` JSONB on the run row with ``orgs_enqueued``
   and ``orgs_skipped`` counts, then transitions the run from
   ``pending`` to ``in_progress`` once the fan-out commits — or
   straight to ``succeeded`` when the all-skipped path leaves no
   children to finalise the run later.

5. Enqueues the actual arq jobs outside the SQL transaction (so the
   queue_jobs row commits before any worker can pick it up) and
   writes the arq backend job id back onto each row. The orphan-tail
   window — row committed, ``backend_job_id IS NULL`` — is the
   reaper's responsibility, mirroring ``keeper_sync_run_discovery``'s
   shape.
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
from docverse.factory import Factory
from docverse.storage.lifecycle_eval_run_store import LifecycleEvalRunStore

__all__ = ["LIFECYCLE_EVAL_QUEUE_NAME", "lifecycle_eval_dispatcher"]


LIFECYCLE_EVAL_QUEUE_NAME = "docverse:lifecycle-queue"
"""arq queue name dedicated to ``lifecycle_eval`` work.

The queue is isolated from the default ``docverse:queue`` and the
``docverse:sync-queue`` so a slow lifecycle pass cannot starve
``build_processing`` / ``publish_edition`` or keeper-sync jobs. The
PRD calls for "a new dedicated arq worker pool (the third pool,
alongside the default and keeper-sync pools)" — this constant is the
queue name those pools bind to.
"""


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
        run = await _create_run_with_summary(
            session=session,
            run_store=run_store,
            orgs_enqueued=len(in_scope),
            orgs_skipped=skipped_count,
        )
        if run is None:
            logger.info(
                "Skipping lifecycle_eval dispatcher tick: another tick "
                "won the create race"
            )
            return "skipped"
        logger = logger.bind(lifecycle_eval_run_id=run.id)

        if not in_scope:
            async with session.begin():
                await run_store.transition_status(
                    run_id=run.id,
                    new_status=LifecycleEvalRunStatus.succeeded,
                )
            logger.info(
                "Lifecycle_eval dispatcher tick completed with no in-scope "
                "orgs",
                orgs_skipped=skipped_count,
            )
            return "completed"

        await _enqueue_children(
            ctx=ctx,
            session=session,
            factory=factory,
            run_store=run_store,
            run_id=run.id,
            orgs=in_scope,
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


async def _create_run_with_summary(
    *,
    session: AsyncSession,
    run_store: LifecycleEvalRunStore,
    orgs_enqueued: int,
    orgs_skipped: int,
) -> LifecycleEvalRun | None:
    """Insert the run row in one transaction and write its summary.

    Split from the fan-out write so the run row is durable even if
    the per-child enqueue loop dies mid-fanout — the reaper can then
    finalise the partially-fanned-out run from the children that did
    commit.

    Returns ``None`` if the partial-unique non-terminal index rejected
    the insert — another dispatcher tick raced past the pre-flight
    ``has_non_terminal_run`` check and won the create. The caller
    translates this into a clean ``"skipped"`` tick.
    """
    try:
        async with session.begin():
            run = await run_store.create()
            await run_store.set_summary(
                run_id=run.id,
                summary={
                    "orgs_enqueued": orgs_enqueued,
                    "orgs_skipped": orgs_skipped,
                },
            )
    except IntegrityError:
        return None
    return run


async def _enqueue_children(  # noqa: PLR0913
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    factory: Factory,
    run_store: LifecycleEvalRunStore,
    run_id: int,
    orgs: list[Organization],
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Fan out one per-org ``queue_jobs`` row + arq enqueue per org.

    Each iteration commits the ``queue_jobs`` row *before* enqueuing
    the arq job so a crash between the SQL commit and ``arq_queue.
    enqueue`` leaves an orphan-tail row (``backend_job_id IS NULL``)
    the reaper can sweep — mirrors ``_enqueue_children`` in
    ``keeper_sync.py``. The run's ``pending → in_progress`` transition
    is atomic with the first successful child create.
    """
    arq_queue = ctx["arq_queue"]
    queue_job_store = factory.create_queue_job_store()
    for index, org in enumerate(orgs):
        async with session.begin():
            queue_job = await queue_job_store.create(
                kind=JobKind.lifecycle_eval,
                org_id=org.id,
                lifecycle_eval_run_id=run_id,
                subject_label=org.slug,
            )
            if index == 0:
                await run_store.transition_status(
                    run_id=run_id,
                    new_status=LifecycleEvalRunStatus.in_progress,
                )
        metadata = await arq_queue.enqueue(
            "lifecycle_eval",
            _queue_name=LIFECYCLE_EVAL_QUEUE_NAME,
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
