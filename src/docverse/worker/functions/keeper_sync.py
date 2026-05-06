"""arq worker functions for the LTD Keeper sync queue.

This module owns the ``docverse:sync-queue`` callable surface:

* ``keeper_sync_run_discovery`` — top-of-the-fanout job that loads the
  org's ``keeper_sync_config`` snapshot, intersects it with LTD's flat
  product list, and enqueues one ``keeper_sync_project`` per in-scope
  product. It transitions its run from ``pending`` → ``in_progress``
  atomically with the first child enqueue.

* ``keeper_sync_project`` — placeholder for the per-project sync,
  which is delivered by #287. The slice keeps the function callable
  end-to-end so child queue-job rows reach a terminal state and
  ``GET /runs/{id}`` counter aggregation has something to count.

The two together let an operator drive the full ``POST → GET``
lifecycle in tests without depending on the deeper
``KeeperSyncService`` from #287.
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

__all__ = ["keeper_sync_project", "keeper_sync_run_discovery"]


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
    """Stub per-project sync.

    Marks the queue job complete and recomputes run finalisation.
    Replaced wholesale by the deeper ``KeeperSyncService`` orchestration
    from #287; this slice keeps the call site live so an operator can
    drive ``POST /runs`` end-to-end and watch the aggregate counters
    move through ``GET /runs/{id}``.
    """
    org_slug: str = payload["org_slug"]
    run_id: int = payload["run_id"]
    queue_job_id: int = payload["queue_job_id"]
    ltd_slug: str = payload["ltd_slug"]
    logger = structlog.get_logger("docverse.worker.keeper_sync_project").bind(
        org=org_slug, run_id=run_id, ltd_slug=ltd_slug
    )

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_keeper_sync_run_store()

        async with session.begin():
            await queue_job_store.start(queue_job_id)
            await queue_job_store.complete(queue_job_id)
            await _maybe_finalise_run(
                run_store=run_store,
                run_id=run_id,
            )
        logger.info("Keeper-sync project stub completed")
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
