"""arq cron worker that fans out per-org ``git_ref_audit`` jobs.

Daily cron entrypoint for the ``git_ref_audit`` periodic safety-net.
On each firing the dispatcher:

1. Returns ``"skipped"`` immediately when
   ``config.git_ref_audit_enabled`` is False — the feature is
   Phalanx-gated and the cron stays registered so flipping the flag
   does not require a worker restart.

2. Runs a cheap pre-flight that finds the set of orgs with at least
   one non-deleted GitHub-bound project. Orgs whose every project
   points at a non-GitHub ``source_url`` (or has no source at all)
   are skipped — no ``queue_jobs`` row, no per-org pass.

3. Inserts the ``git_ref_audit_runs`` row, its ``summary`` JSONB
   (``orgs_enqueued`` / ``orgs_skipped``), the per-org ``queue_jobs``
   children, and — when there is at least one child — the
   ``pending → in_progress`` transition, **all in a single
   transaction**. Either every row is durable together or none of
   them are: there is no window in which a ``git_ref_audit_runs``
   row can exist without its children, so the
   ``has_non_terminal_run`` pre-flight cannot get wedged by a
   partial fan-out. The partial-unique non-terminal index on
   ``git_ref_audit_runs`` is the DB-level backstop against two
   ticks racing; the dispatcher pre-checks
   ``has_non_terminal_run`` *and* catches the ``IntegrityError``
   from the index so a slow prior tick surfaces as a clean
   ``"skipped"`` result. The per-org mutex partial-unique index on
   ``queue_jobs`` keeps concurrent ticks from doubling up on a
   single org.

4. For the all-skipped case (zero in-scope orgs) the same
   transaction transitions the run straight to ``succeeded`` — no
   children exist to finalise the run later.

5. Enqueues the actual arq jobs outside the SQL transaction (so the
   ``queue_jobs`` rows commit before any worker can pick them up)
   and writes the arq ``backend_job_id`` back onto each row. The
   orphan-tail window — row committed, ``backend_job_id IS NULL`` —
   is the responsibility of ``lifecycle_reaper``, which extends its
   sweep to ``kind='git_ref_audit'`` rows. The per-job arq
   ``timeout`` is the first backstop (cancels a runaway audit long
   before the reaper window).
"""

from __future__ import annotations

from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import GitRefAuditRunStatus, JobKind
from docverse.config import config
from docverse.domain.git_ref_audit_run import GitRefAuditRun
from docverse.domain.organization import Organization
from docverse.domain.queue import QueueJob
from docverse.factory import Factory
from docverse.storage.git_ref_audit_run_store import GitRefAuditRunStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.queues import MAINTENANCE_QUEUE_NAME

__all__ = ["git_ref_audit_discovery"]


async def git_ref_audit_discovery(ctx: dict[str, Any]) -> str:
    """Fan out one ``git_ref_audit`` job per in-scope org.

    Returns ``"skipped"`` when the feature flag is off or a prior
    non-terminal run is in flight. Otherwise returns ``"completed"``
    for a clean fan-out (including the all-skipped case, which still
    records a run row).
    """
    logger = structlog.get_logger("docverse.worker.git_ref_audit_discovery")

    if not config.git_ref_audit_enabled:
        logger.debug(
            "Skipping git_ref_audit discovery tick: feature flag is off"
        )
        return "skipped"

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        run_store = factory.create_git_ref_audit_run_store()
        queue_job_store = factory.create_queue_job_store()

        async with session.begin():
            if await run_store.has_non_terminal_run():
                logger.info(
                    "Skipping git_ref_audit discovery tick: "
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
                "Skipping git_ref_audit discovery tick: another tick "
                "won the create race"
            )
            return "skipped"
        run, queue_jobs = creation
        logger = logger.bind(git_ref_audit_run_id=run.id)

        if not in_scope:
            logger.info(
                "Git_ref_audit discovery tick completed with no in-scope orgs",
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
            "Git_ref_audit discovery tick completed",
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

    An org is in-scope iff it owns at least one non-deleted project
    whose ``(github_owner, github_repo)`` is populated. Two queries:
    one read of every org, one read of every org_id that has a
    GitHub-bound project. Keeps the dispatcher round-trip count
    constant in the number of orgs.
    """
    org_store = factory.create_org_store()
    project_store = factory.create_project_store()
    async with session.begin():
        orgs = await org_store.list_all()
        in_scope_ids = (
            await project_store.list_org_ids_with_github_bound_projects()
        )
    in_scope: list[Organization] = []
    skipped = 0
    for org in orgs:
        if org.id in in_scope_ids:
            in_scope.append(org)
        else:
            skipped += 1
    return in_scope, skipped


async def _create_run_with_children(
    *,
    session: AsyncSession,
    run_store: GitRefAuditRunStore,
    queue_job_store: QueueJobStore,
    orgs: list[Organization],
    orgs_skipped: int,
) -> tuple[GitRefAuditRun, list[QueueJob]] | None:
    """Insert the run row, summary, all child ``queue_jobs`` atomically.

    Mirrors :func:`docverse.worker.functions.lifecycle_eval_dispatcher
    ._create_run_with_children`. Either every row commits together or
    none of them do, so the ``has_non_terminal_run`` pre-flight cannot
    get wedged by a partial fan-out. The partial-unique non-terminal
    index on ``git_ref_audit_runs`` is the DB-level backstop against
    two ticks racing; an ``IntegrityError`` here is translated to a
    clean ``"skipped"`` tick by the caller.
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
                    kind=JobKind.git_ref_audit,
                    org_id=org.id,
                    git_ref_audit_run_id=run.id,
                    subject_label=org.slug,
                )
                queue_jobs.append(queue_job)
            if orgs:
                await run_store.transition_status(
                    run_id=run.id,
                    new_status=GitRefAuditRunStatus.in_progress,
                )
            else:
                await run_store.transition_status(
                    run_id=run.id,
                    new_status=GitRefAuditRunStatus.succeeded,
                )
    except IntegrityError:
        return None
    return run, queue_jobs


async def _enqueue_arq_jobs(
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
    :func:`_create_run_with_children`, so a crash between the arq
    enqueue and the ``backend_job_id`` write leaves an orphan-queued
    row (``backend_job_id IS NULL``) which ``lifecycle_reaper`` sweeps
    via :meth:`QueueJobStore.fail_orphaned_git_ref_audit_jobs`. The
    audit shares the maintenance worker pool's queue so audit and
    lifecycle-eval ticks can never crowd each other off the worker
    fleet.
    """
    arq_queue = ctx["arq_queue"]
    for org, queue_job in zip(orgs, queue_jobs, strict=True):
        metadata = await arq_queue.enqueue(
            "git_ref_audit",
            _queue_name=MAINTENANCE_QUEUE_NAME,
            payload={
                "org_id": org.id,
                "org_slug": org.slug,
                "git_ref_audit_run_id": run_id,
                "queue_job_id": queue_job.id,
            },
        )
        async with session.begin():
            await queue_job_store.set_backend_job_id(queue_job.id, metadata.id)
        logger.debug(
            "Enqueued git_ref_audit for org",
            org=org.slug,
            queue_job_id=queue_job.id,
        )
