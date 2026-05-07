"""arq worker functions for the LTD Keeper sync queue.

This module owns the ``docverse:sync-queue`` callable surface:

* ``keeper_sync_run_discovery`` — top-of-the-fanout job that loads the
  org's ``keeper_sync_config`` snapshot, intersects it with LTD's flat
  product list, and enqueues one ``keeper_sync_project`` per in-scope
  product. It transitions its run from ``pending`` → ``in_progress``
  atomically with the first child enqueue.

* ``keeper_sync_project`` — orchestrates one LTD product into Docverse
  by delegating to :class:`KeeperSyncService`. The worker bookends the
  service call with two short transactions that own the
  ``queue_jobs`` lifecycle (``start`` then ``complete`` / ``fail``) and
  recompute run finalisation; the service itself manages the
  state-row + Docverse-row commits inside its own ``session.begin()``
  blocks.
"""

from __future__ import annotations

import traceback
from datetime import timedelta
from typing import Any, Literal

import httpx
import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    JobKind,
    KeeperSyncConfig,
    KeeperSyncRunStatus,
)
from docverse.config import config
from docverse.exceptions import NotFoundError
from docverse.factory import Factory
from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse.storage.queue_job_store import QueueJobStore

# Window before a queued child with no ``backend_job_id`` is treated as
# orphaned by ``_reconcile_orphan_children``. Long enough to never race
# a healthy concurrent discovery worker that's mid-fanout, short enough
# to free a stuck run on the next discovery attempt.
_ORPHAN_IDLE_WINDOW = timedelta(minutes=5)

__all__ = [
    "keeper_sync_project",
    "keeper_sync_reaper",
    "keeper_sync_run_discovery",
]


async def keeper_sync_reaper(ctx: dict[str, Any]) -> str:
    """Cron-driven backstop that finalises silently-stuck keeper-sync runs.

    Mechanism #2 of the two-mechanism guarantee that a sync run always
    reaches a terminal state. arq's per-function ``timeout`` covers the
    common case (a job actually runs past the timeout and arq cancels
    it), but a worker pod that's OOM-killed mid-job or a job that arq
    itself loses leaves a child ``queue_jobs`` row stuck in
    ``in_progress`` forever — and with it the parent ``keeper_sync_runs``
    row, which can never finalise while ``pending_count > 0``.

    The reaper:

    1. Reads ``config.keeper_sync_reaper_threshold_seconds`` (default
       6 h, env-overridable so test/staging environments can drive it
       down to seconds for fast verification).
    2. Calls ``QueueJobStore.fail_silent_run_children`` to mark every
       keeper-sync child whose ``date_started`` is older than that
       threshold (and which has no ``date_completed``) as ``failed``.
    3. For each unique ``keeper_sync_run_id`` whose children were just
       reaped, calls ``_maybe_finalise_run`` so the parent run rolls up
       to ``partial_failure``.

    Wired as a cron job on ``KeeperSyncWorkerSettings.cron_jobs``
    (every 30 min). Returns a one-line status string for arq's result
    log; the structured ``logger.info`` carries the detail.
    """
    logger = structlog.get_logger("docverse.worker.keeper_sync_reaper")
    threshold = timedelta(seconds=config.keeper_sync_reaper_threshold_seconds)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_keeper_sync_run_store()

        async with session.begin():
            reaped = await queue_job_store.fail_silent_run_children(
                idle_after=threshold
            )
            run_ids = {qj.keeper_sync_run_id for qj in reaped}
            for run_id in run_ids:
                if run_id is None:
                    continue
                await _maybe_finalise_run(run_store=run_store, run_id=run_id)

        if reaped:
            logger.warning(
                "Reaped silent keeper-sync child queue jobs",
                reaped_count=len(reaped),
                run_ids=sorted(r for r in run_ids if r is not None),
            )
        else:
            logger.debug("No silent keeper-sync child queue jobs to reap")
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def keeper_sync_run_discovery(
    ctx: dict[str, Any], payload: dict[str, Any]
) -> str:
    """Fan out one ``keeper_sync_project`` job per in-scope LTD product.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``, ``http_client``,
        ``arq_queue``).
    payload
        Job payload with ``org_id``, ``org_slug``, ``run_id``, and
        ``queue_job_id`` (the discovery's own ``queue_jobs`` row, so
        the worker can transition it through queued → in_progress →
        completed/failed).

    Returns
    -------
    str
        ``"completed"`` on a clean fan-out (including the empty case)
        or ``"failed"`` if discovery itself errored before fan-out.
    """
    org_id: int = payload["org_id"]
    org_slug: str = payload["org_slug"]
    run_id: int = payload["run_id"]
    queue_job_id: int = payload["queue_job_id"]
    logger = structlog.get_logger(
        "docverse.worker.keeper_sync_run_discovery"
    ).bind(org=org_slug, run_id=run_id)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_keeper_sync_run_store()

        async with session.begin():
            await queue_job_store.start(queue_job_id)
            await _reconcile_orphan_children(
                queue_job_store=queue_job_store,
                run_id=run_id,
                logger=logger,
            )

        try:
            config = await _load_config_snapshot(
                session=session,
                factory=factory,
                org_slug=org_slug,
            )
            if not config.enabled:
                msg = (
                    f"Keeper sync is disabled for organization "
                    f"{org_slug!r}; aborting discovery"
                )
                raise RuntimeError(msg)  # noqa: TRY301

            ltd_slugs = await _fetch_ltd_product_slugs(
                factory=factory, config=config, logger=logger
            )
            in_scope = _filter_to_allowlist(ltd_slugs, config.project_slugs)
            logger.info(
                "Resolved keeper-sync run scope",
                ltd_count=len(ltd_slugs),
                in_scope_count=len(in_scope),
            )

            await _enqueue_children(
                ctx=ctx,
                session=session,
                queue_job_store=queue_job_store,
                run_store=run_store,
                org_id=org_id,
                org_slug=org_slug,
                run_id=run_id,
                ltd_base_url=str(config.ltd_base_url),
                ltd_slugs=in_scope,
                logger=logger,
            )

            async with session.begin():
                await queue_job_store.update_phase(
                    queue_job_id,
                    "complete",
                    progress={
                        "message": "Discovery complete",
                        "in_scope_count": len(in_scope),
                    },
                )
                await queue_job_store.complete(queue_job_id)
                # Empty fan-out: nothing for ``keeper_sync_project`` to
                # finalise on the run, so terminate it here.
                if not in_scope:
                    await run_store.transition_status(
                        run_id=run_id,
                        new_status=KeeperSyncRunStatus.succeeded,
                    )
            logger.info(
                "Keeper-sync discovery completed",
                in_scope_count=len(in_scope),
            )
        except Exception as exc:
            logger.exception("Keeper-sync discovery failed")
            async with session.begin():
                await queue_job_store.fail(
                    queue_job_id,
                    errors={
                        "message": str(exc),
                        "type": type(exc).__name__,
                        "traceback": traceback.format_exc(),
                    },
                )
                await run_store.transition_status(
                    run_id=run_id,
                    new_status=KeeperSyncRunStatus.failed,
                )
            return "failed"
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def keeper_sync_project(
    ctx: dict[str, Any], payload: dict[str, Any]
) -> str:
    """Sync one LTD product into Docverse via :class:`KeeperSyncService`.

    The worker brackets the service call with two short transactions:

    1. Mark the ``queue_jobs`` row ``in_progress``.
    2. Construct ``KeeperSyncService`` from the factory and invoke
       :meth:`KeeperSyncService.sync_project`. The service runs outside
       any outer ``session.begin()`` so it can manage its own commits
       across LTD HTTP, content copy, and Docverse-side row writes.
    3. On success, mark the queue job ``completed``; on a caught
       exception, mark it ``failed`` with structured error details and
       re-raise so arq records the job as failed. Both branches call
       :func:`_maybe_finalise_run` so a terminal child cannot leave the
       parent run stuck in ``in_progress``.
    """
    org_id: int = payload["org_id"]
    org_slug: str = payload["org_slug"]
    run_id: int = payload["run_id"]
    queue_job_id: int = payload["queue_job_id"]
    ltd_slug: str = payload["ltd_slug"]
    ltd_base_url: str = payload["ltd_base_url"]
    logger = structlog.get_logger("docverse.worker.keeper_sync_project").bind(
        org=org_slug, run_id=run_id, ltd_slug=ltd_slug
    )

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_keeper_sync_run_store()
        org_store = factory.create_org_store()

        async with session.begin():
            await queue_job_store.start(queue_job_id)
            org = await org_store.get_by_id(org_id)

        try:
            if org is None:
                msg = f"Organization {org_id} not found"
                raise RuntimeError(msg)  # noqa: TRY301
            publishing_store_label = org.publishing_store_label
            if publishing_store_label is None:
                msg = (
                    f"Org {org_id} has no publishing_store_label "
                    "configured; keeper-sync requires a publishing "
                    "object store"
                )
                raise RuntimeError(msg)  # noqa: TRY301

            service = factory.create_keeper_sync_service(
                org_id=org_id,
                service_label=publishing_store_label,
                ltd_base_url=ltd_base_url,
            )
            await service.sync_project(org_id=org_id, ltd_slug=ltd_slug)
        except Exception as exc:
            logger.exception("Keeper-sync project failed")
            async with session.begin():
                await queue_job_store.fail(
                    queue_job_id,
                    errors={
                        "message": str(exc),
                        "type": type(exc).__name__,
                        "traceback": traceback.format_exc(),
                    },
                )
                await _maybe_finalise_run(run_store=run_store, run_id=run_id)
            raise

        async with session.begin():
            await queue_job_store.complete(queue_job_id)
            await _maybe_finalise_run(run_store=run_store, run_id=run_id)
        logger.info("Keeper-sync project completed")
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _load_config_snapshot(
    *,
    session: AsyncSession,
    factory: Factory,
    org_slug: str,
) -> KeeperSyncConfig:
    """Snapshot the org's ``keeper_sync_config`` for the run.

    The snapshot is captured at job start so config edits made while a
    run is in flight (e.g. an expanded allowlist) do not retroactively
    widen its scope — the operator must POST a new run after the
    current one terminates.
    """
    async with session.begin():
        config_service = factory.create_keeper_sync_config_service()
        return await config_service.get(org_slug=org_slug)


async def _fetch_ltd_product_slugs(
    *,
    factory: Factory,
    config: KeeperSyncConfig,
    logger: structlog.stdlib.BoundLogger,
) -> list[str]:
    """Fetch every product slug visible on the configured LTD instance."""
    client = factory.create_ltd_products_client(
        base_url=str(config.ltd_base_url)
    )
    try:
        return await client.list_product_slugs()
    except httpx.HTTPError:
        logger.exception("Failed to fetch LTD product slugs")
        raise


def _filter_to_allowlist(
    ltd_slugs: list[str], allowlist: list[str] | Literal["*"]
) -> list[str]:
    """Intersect an LTD slug list with the org's configured allowlist.

    ``"*"`` is the wildcard — every LTD slug stays in scope. Otherwise
    ordering follows the LTD listing so successive runs against the
    same LTD instance fan out their children deterministically.
    """
    if allowlist == "*":
        return list(ltd_slugs)
    allowed = set(allowlist)
    return [slug for slug in ltd_slugs if slug in allowed]


async def _enqueue_children(  # noqa: PLR0913
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    run_store: KeeperSyncRunStore,
    org_id: int,
    org_slug: str,
    run_id: int,
    ltd_base_url: str,
    ltd_slugs: list[str],
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Fan out one child ``keeper_sync_project`` job per slug.

    Each iteration creates the ``queue_jobs`` row tagged with
    ``keeper_sync_run_id`` *first* — so a crash mid-fan-out leaves
    queued rows that progress aggregation can still see — then
    enqueues the arq job and writes the backend job ID back. The
    ``pending → in_progress`` run transition is atomic with the first
    child's queue-job row insert so any concurrent ``GET /runs/{id}``
    can never observe a run with children but still ``pending``.

    The order leaves an orphan tail: if the worker dies between the
    SQL commit and ``arq_queue.enqueue``, the row sits in ``queued``
    with ``backend_job_id IS NULL`` and no arq job will ever pick it
    up — pending forever, blocking finalisation. The next discovery
    attempt sweeps these rows via ``_reconcile_orphan_children`` once
    they age past ``_ORPHAN_IDLE_WINDOW``.
    """
    arq_queue = ctx["arq_queue"]
    for index, ltd_slug in enumerate(ltd_slugs):
        async with session.begin():
            queue_job = await queue_job_store.create(
                kind=JobKind.keeper_sync_project,
                org_id=org_id,
                keeper_sync_run_id=run_id,
            )
            if index == 0:
                await run_store.transition_status(
                    run_id=run_id,
                    new_status=KeeperSyncRunStatus.in_progress,
                )
        # arq enqueue lives outside the session so the SQL transaction
        # commits before redis sees a job id pointing at our row. See
        # the orphan-tail caveat in this function's docstring.
        metadata = await arq_queue.enqueue(
            "keeper_sync_project",
            _queue_name=KEEPER_SYNC_QUEUE_NAME,
            payload={
                "org_id": org_id,
                "org_slug": org_slug,
                "run_id": run_id,
                "queue_job_id": queue_job.id,
                "ltd_slug": ltd_slug,
                "ltd_base_url": ltd_base_url,
            },
        )
        async with session.begin():
            await queue_job_store.set_backend_job_id(queue_job.id, metadata.id)
        logger.debug(
            "Enqueued keeper_sync_project",
            ltd_slug=ltd_slug,
            queue_job_id=queue_job.id,
        )


async def _reconcile_orphan_children(
    *,
    queue_job_store: QueueJobStore,
    run_id: int,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Fail any orphan child rows left by a previous discovery attempt.

    ``_enqueue_children`` commits each child ``queue_jobs`` row before
    calling ``arq_queue.enqueue``, so a worker crash in that window
    leaves an orphan: ``status='queued'``, ``backend_job_id IS NULL``,
    no arq job ever scheduled. Without reconciliation the orphan
    counts toward ``pending_count`` forever and the run can never
    finalise. We sweep them at the top of each discovery attempt so a
    retried (or operator-replayed) discovery can finish cleanly.
    """
    failed = await queue_job_store.fail_orphaned_run_children(
        run_id=run_id, idle_after=_ORPHAN_IDLE_WINDOW
    )
    if failed:
        logger.warning(
            "Reconciled orphan keeper-sync child queue jobs",
            orphan_count=len(failed),
            orphan_ids=[job.id for job in failed],
        )


async def _maybe_finalise_run(
    *,
    run_store: KeeperSyncRunStore,
    run_id: int,
) -> None:
    """Transition the run to a terminal status once all children are terminal.

    Idempotent re-entry on the same terminal status is handled by
    ``transition_status``'s same-status fast path. The explicit terminal
    pre-check guards a different case: two child finalisers racing each
    other can compute *different* terminal statuses (e.g. one sees all
    children completed and picks ``succeeded`` just as another child's
    failure commits, so the second finaliser picks ``partial_failure``).
    Without the pre-check, the second caller would hit
    ``transition_status``'s terminal→terminal guard and raise
    ``InvalidJobStateError``, which would roll back the surrounding
    ``session.begin()`` in ``keeper_sync_project`` and undo that
    child's ``complete()``. We swallow the conflict here so the loser
    of the race exits cleanly and lets the winning terminal status
    stand.
    """
    counters = await run_store.aggregate_counters(run_id=run_id)
    if counters.total_count == 0 or counters.pending_count > 0:
        return
    new_status = (
        KeeperSyncRunStatus.partial_failure
        if counters.failed_count > 0
        else KeeperSyncRunStatus.succeeded
    )
    run = await run_store.get(run_id)
    if run is None:
        msg = f"Keeper sync run {run_id} not found"
        raise NotFoundError(msg)
    if run.status in {
        KeeperSyncRunStatus.succeeded,
        KeeperSyncRunStatus.partial_failure,
        KeeperSyncRunStatus.failed,
    }:
        return
    await run_store.transition_status(run_id=run_id, new_status=new_status)
