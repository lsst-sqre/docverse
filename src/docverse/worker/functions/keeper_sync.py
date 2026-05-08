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

* ``keeper_sync_tier_main`` / ``_tier_discovery`` / ``_tier_other`` —
  cron-driven steady-state reconcilers that enqueue ``keeper_sync_
  project`` children with no run attribution. See PRD #275 §"
  Reconciliation cadence (steady state, run-independent)".
"""

from __future__ import annotations

import traceback
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol

import httpx
import structlog
from safir.arq import ArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    JobKind,
    KeeperSyncConfig,
    KeeperSyncRunStatus,
)
from docverse.config import config
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.organization import Organization
from docverse.factory import Factory
from docverse.services.keeper_sync.scheduler import (
    ANNOTATION_DATE_DISCOVERY_LAST_POLLED,
    ANNOTATION_DATE_MAIN_LAST_POLLED,
    ANNOTATION_DATE_OTHER_LAST_POLLED,
    TIER_DISCOVERY_DORMANT_INTERVAL,
    TIER_DISCOVERY_HOT_WINDOW,
    TIER_OTHER_DORMANT_INTERVAL,
    TIER_OTHER_HOT_WINDOW,
    Tier,
    is_unknown_resource,
    should_poll_for_tier,
    should_poll_main_for_project,
    should_refresh_main_edition,
    should_refresh_other_edition,
)
from docverse.services.keeper_sync.service import ProjectSyncResult
from docverse.services.keeper_sync_finalisation import maybe_finalise_run
from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.services.publish_enqueue import enqueue_publish_for_edition
from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse.storage.ltd import (
    LtdClient,
    LtdClientError,
    LtdEdition,
    LtdNotFoundError,
)
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
    "keeper_sync_tier_discovery",
    "keeper_sync_tier_main",
    "keeper_sync_tier_other",
]

#: Slug LTD assigns to every product's primary edition. Tier_main owns
#: refreshes for this slug; tier_other explicitly skips it.
_LTD_MAIN_SLUG = "main"

#: ``keeper_sync_state.annotations`` key on a project-resource state row
#: holding the resolved LTD ``main`` edition's full ``self_url``. Owned
#: by ``_tier_main_for_org`` so subsequent ticks bypass the
#: ``GET /products/<slug>/editions/`` walk and go straight to
#: ``GET /editions/<id>``.
_MAIN_EDITION_URL_KEY = "main_edition_url"

#: Companion to :data:`_MAIN_EDITION_URL_KEY`: the integer LTD edition
#: id that ``main_edition_url`` resolves to. Stored alongside the URL
#: so log lines and future reverse lookups have the id without needing
#: to re-parse the URL.
_MAIN_EDITION_LTD_ID_KEY = "main_edition_ltd_id"


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
       reaped, calls ``maybe_finalise_run`` so the parent run rolls up
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
                await maybe_finalise_run(run_store=run_store, run_id=run_id)

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
    3. For each finalized ``(edition, build)`` pair the service
       returned, call
       :func:`docverse.services.publish_enqueue.enqueue_publish_for_edition`
       so the publish path runs the same way it does after a normal
       client upload — KV publish via ``EditionPublishingService.publish``
       and a cascaded ``dashboard_build`` enqueue. The publish
       ``QueueJob`` rows carry ``keeper_sync_run_id`` so they roll into
       the parent run's progress counters and ``date_last_activity``.
       Short-circuited builds (LTD ``date_rebuilt`` unchanged) are
       skipped — there's no new state to publish.
    4. On success, mark the queue job ``completed``; on a caught
       exception, mark it ``failed`` with structured error details and
       re-raise so arq records the job as failed. Both branches call
       :func:`maybe_finalise_run` so a terminal child cannot leave the
       parent run stuck in ``in_progress``.
    """
    org_id: int = payload["org_id"]
    org_slug: str = payload["org_slug"]
    # ``run_id`` is absent from tier-cron-enqueued payloads (the
    # continuous reconciliation loops attribute their work to no run);
    # see PRD #275 "Reconciliation cadence (steady state, run-
    # independent)". When ``None``, the worker skips the run-roll-up
    # call so a tier-cron job cannot accidentally finalise some
    # unrelated run.
    run_id: int | None = payload.get("run_id")
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
            sync_result = await service.sync_project(
                org_id=org_id, ltd_slug=ltd_slug
            )
            await _enqueue_publish_for_finalized_builds(
                factory=factory,
                session=session,
                queue_job_store=queue_job_store,
                org_id=org_id,
                run_id=run_id,
                sync_result=sync_result,
                logger=logger,
            )
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
                if run_id is not None:
                    await maybe_finalise_run(
                        run_store=run_store, run_id=run_id
                    )
            raise

        async with session.begin():
            await queue_job_store.complete(queue_job_id)
            if run_id is not None:
                await maybe_finalise_run(run_store=run_store, run_id=run_id)
        logger.info("Keeper-sync project completed")
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _enqueue_publish_for_finalized_builds(  # noqa: PLR0913
    *,
    factory: Factory,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    org_id: int,
    run_id: int | None,
    sync_result: ProjectSyncResult,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Run the publish path for every edition that needs publishing.

    Iterates ``sync_result.edition_outcomes`` and decides per-edition
    whether to invoke
    :func:`docverse.services.publish_enqueue.enqueue_publish_for_edition`
    so KV publish + dashboard rebuild fire just like they do after a
    normal client upload. Each helper call tags the publish ``QueueJob``
    row with ``keeper_sync_run_id`` so the publish jobs roll into the
    parent run's ``GET /runs/{id}/jobs`` listing and progress counters.

    Two branches:

    * **Freshly-synced build** (``build_outcome.short_circuited`` is
      ``False``). The sync just finalized a new build for the edition,
      so a publish must follow.
    * **Self-heal on short-circuit**. When sync short-circuits on
      unchanged LTD ``date_rebuilt`` we'd normally skip, but if the
      edition is sitting on a ``current_build_id`` with
      ``publish_status IS NULL`` it never made it through the publish
      path (e.g. the build pre-dates this enqueue logic landing, or a
      prior publish enqueue was lost). In that case we enqueue a
      catch-up publish using the edition's already-pointed-at build.
      Editions whose ``publish_status`` is already ``pending`` /
      ``published`` / ``failed`` are left alone — a stuck pending
      publish is the in-flight publisher's problem to resolve, not
      ours, and a successful or failed prior publish does not need
      re-running on every reconciliation tick.
    """
    edition_store = factory.create_edition_store()
    history_store = factory.create_edition_build_history_store()
    queue_backend = factory.create_queue_backend()
    project_slug = sync_result.docverse_project_slug
    project_id = sync_result.docverse_project_id

    for outcome in sync_result.edition_outcomes:
        build_outcome = outcome.build_outcome
        if build_outcome is None:
            continue

        if build_outcome.short_circuited:
            target = await _resolve_self_heal_target(
                session=session,
                edition_store=edition_store,
                project_id=project_id,
                edition_slug=outcome.docverse_slug,
            )
            if target is None:
                continue
            build_id, build_public_id = target
            log_phase = "self_heal"
        else:
            if (
                build_outcome.docverse_build_id is None
                or build_outcome.docverse_build_public_id is None
            ):
                continue
            build_id = build_outcome.docverse_build_id
            build_public_id = build_outcome.docverse_build_public_id
            log_phase = "synced"

        await enqueue_publish_for_edition(
            session=session,
            edition_store=edition_store,
            history_store=history_store,
            queue_job_store=queue_job_store,
            queue_backend=queue_backend,
            org_id=org_id,
            project_id=project_id,
            project_slug=project_slug,
            edition_id=outcome.docverse_edition_id,
            edition_slug=outcome.docverse_slug,
            build_id=build_id,
            build_public_id=build_public_id,
            keeper_sync_run_id=run_id,
        )
        logger.info(
            "Enqueued publish_edition for synced build",
            edition_slug=outcome.docverse_slug,
            build_id=build_id,
            phase=log_phase,
        )


async def _resolve_self_heal_target(
    *,
    session: AsyncSession,
    edition_store: Any,
    project_id: int,
    edition_slug: str,
) -> tuple[int, str] | None:
    """Return ``(build_id, build_public_id)`` if the edition needs catch-up.

    Returns ``None`` when the edition has no ``current_build_id``, when
    its ``publish_status`` is already set (so a publish has run, is in
    flight, or previously failed), or when the joined build public_id
    is missing for any reason. The read happens inside its own
    transaction so it does not interfere with
    ``enqueue_publish_for_edition``'s phased commits.
    """
    async with session.begin():
        edition = await edition_store.get_by_slug(
            project_id=project_id, slug=edition_slug
        )
    if edition is None:
        return None
    if edition.publish_status is not None:
        return None
    if edition.current_build_id is None:
        return None
    if edition.current_build_public_id is None:
        return None
    return edition.current_build_id, serialize_base32_id(
        edition.current_build_public_id
    )


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
                subject_label=ltd_slug,
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


async def keeper_sync_tier_main(ctx: dict[str, Any]) -> str:
    """Cron (every 5 min): refresh ``main`` editions whose LTD rebuilt.

    Walks every org with ``keeper_sync_config.enabled`` and intersects
    its allowlist with LTD's product list; for each in-scope project
    fetches the LTD ``main`` edition and consults the local
    ``keeper_sync_state`` row. The pure
    :func:`docverse.services.keeper_sync.scheduler.should_refresh_main_edition`
    decides whether LTD's ``date_rebuilt`` has advanced past
    ``state.date_rebuilt_seen``; when it has, the cron enqueues a
    ``keeper_sync_project`` child with ``keeper_sync_run_id`` left
    ``None`` so the steady-state pass does not pollute any operator-
    triggered run's progress aggregation.

    Per-org failures (LTD outage on one host, an unreadable config)
    are logged and skipped so the cron stays best-effort across all
    enabled orgs. Returns ``"completed"`` regardless of how many child
    enqueues fired.
    """
    logger = structlog.get_logger("docverse.worker.keeper_sync_tier_main")
    return await _run_tier(
        ctx=ctx, logger=logger, processor=_tier_main_for_org, tier_name="main"
    )


async def keeper_sync_tier_discovery(ctx: dict[str, Any]) -> str:
    """Cron (every 30 min): enqueue projects with unseen LTD resources.

    For each in-scope LTD project the cron checks the project-level
    ``keeper_sync_state`` row first; if missing it enqueues a
    ``keeper_sync_project`` straight away. Otherwise it lists the
    project's editions and asks
    :func:`docverse.services.keeper_sync.scheduler.is_unknown_resource`
    whether any edition lacks a state row. Discovery never enqueues
    twice for the same project on a single tick — one
    ``keeper_sync_project`` covers all of its editions.
    """
    logger = structlog.get_logger("docverse.worker.keeper_sync_tier_discovery")
    return await _run_tier(
        ctx=ctx,
        logger=logger,
        processor=_tier_discovery_for_org,
        tier_name="discovery",
    )


async def keeper_sync_tier_other(ctx: dict[str, Any]) -> str:
    """Cron (hourly): refresh non-``main`` editions older than the threshold.

    Walks each in-scope project's LTD editions and consults
    :func:`docverse.services.keeper_sync.scheduler.should_refresh_other_edition`
    against the local state row's ``date_last_synced``. The first
    stale non-``main`` edition for a project triggers one
    ``keeper_sync_project`` enqueue (which re-syncs every edition),
    so multiple stale editions do not produce duplicate children.
    Editions with no state row are left to ``tier_discovery`` so the
    two cron functions do not race for the same enqueue.
    """
    logger = structlog.get_logger("docverse.worker.keeper_sync_tier_other")
    return await _run_tier(
        ctx=ctx,
        logger=logger,
        processor=_tier_other_for_org,
        tier_name="other",
    )


async def _run_tier(
    *,
    ctx: dict[str, Any],
    logger: structlog.stdlib.BoundLogger,
    processor: TierOrgProcessor,
    tier_name: str,
) -> str:
    """Shared cron-tick driver: list enabled orgs, run a per-org processor.

    The per-org loop is wrapped in a broad ``except`` because the cron
    must keep visiting every enabled org even if one of them is mid-
    incident (LTD down, malformed config, transient DB error). The
    failure is logged with structured context for follow-up; the next
    tick will retry naturally.
    """
    enqueued_total = 0
    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        org_store = factory.create_org_store()
        async with session.begin():
            all_orgs = await org_store.list_all()
        candidates = [
            o
            for o in all_orgs
            if o.keeper_sync_config is not None
            and o.keeper_sync_config.enabled
        ]
        for org in candidates:
            try:
                enqueued_total += await processor(
                    ctx=ctx,
                    session=session,
                    factory=factory,
                    org=org,
                    logger=logger,
                )
            except Exception:
                logger.exception(
                    "Keeper-sync tier processor failed for org",
                    tier=tier_name,
                    org=org.slug,
                )
        logger.info(
            "Keeper-sync tier pass complete",
            tier=tier_name,
            candidates=len(candidates),
            enqueued=enqueued_total,
        )
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


class TierOrgProcessor(Protocol):
    """Per-org tier processor callable shared by ``_run_tier``.

    Each tier cron (``main`` / ``discovery`` / ``other``) supplies a
    function matching this signature; it returns the number of
    ``keeper_sync_project`` children it enqueued for the org.
    """

    async def __call__(
        self,
        *,
        ctx: dict[str, Any],
        session: AsyncSession,
        factory: Factory,
        org: Organization,
        logger: structlog.stdlib.BoundLogger,
    ) -> int: ...


async def _tier_main_for_org(
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    factory: Factory,
    org: Organization,
    logger: structlog.stdlib.BoundLogger,
) -> int:
    """Run one tier_main pass for a single enabled org.

    Uses :func:`should_poll_main_for_project` to skip dormant projects
    (those whose LTD ``main`` hasn't rebuilt within the hot window) on
    most ticks, capping their LTD load at one fetch per
    ``TIER_MAIN_DORMANT_INTERVAL`` instead of one per 5-minute cron
    tick. Hot projects continue to poll on the 5-min SLO.
    """
    config_snapshot = org.keeper_sync_config
    if config_snapshot is None:
        return 0
    in_scope = await _list_in_scope_slugs(
        factory=factory, config=config_snapshot
    )
    if not in_scope:
        return 0
    ltd_client = factory.create_ltd_client(
        base_url=str(config_snapshot.ltd_base_url)
    )
    state_store = factory.create_keeper_sync_state_store()
    queue_job_store = factory.create_queue_job_store()
    arq_queue = ctx["arq_queue"]
    now = datetime.now(tz=UTC)
    enqueued = 0
    for ltd_slug in in_scope:
        async with session.begin():
            project_state = await state_store.get(
                org_id=org.id,
                resource_type=ResourceType.project,
                ltd_slug=ltd_slug,
            )
        if not should_poll_main_for_project(state=project_state, now=now):
            continue
        try:
            main_edition = await _find_main_edition(
                ltd_client=ltd_client,
                state_store=state_store,
                session=session,
                org_id=org.id,
                ltd_slug=ltd_slug,
            )
        except LtdClientError:
            logger.exception(
                "Tier-main: failed to fetch main edition",
                org=org.slug,
                ltd_slug=ltd_slug,
            )
            # Mark the visit polled even on error — otherwise a flaky
            # LTD endpoint would defeat dormancy gating by re-polling
            # every 5 min for dormant projects.
            await _record_main_polled(
                session=session,
                state_store=state_store,
                org_id=org.id,
                ltd_slug=ltd_slug,
                now=now,
                main_edition=None,
            )
            continue
        # Refresh the cached pointer + rate-limit annotation on every
        # successful resolve. The merge-and-upsert handles the cold-
        # cache case (no prior annotations), the steady-state hit case
        # (re-write the same pointer), and the rare maintainer-rename
        # case (walk discovered a different ltd_id than was cached).
        await _record_main_polled(
            session=session,
            state_store=state_store,
            org_id=org.id,
            ltd_slug=ltd_slug,
            now=now,
            main_edition=main_edition,
        )
        if main_edition is None:
            continue
        async with session.begin():
            state = await state_store.get(
                org_id=org.id,
                resource_type=ResourceType.edition,
                ltd_id=main_edition.ltd_id,
            )
        if not should_refresh_main_edition(
            state=state, ltd_date_rebuilt=main_edition.date_rebuilt
        ):
            continue
        await _enqueue_tier_project_sync(
            session=session,
            queue_job_store=queue_job_store,
            arq_queue=arq_queue,
            org_id=org.id,
            org_slug=org.slug,
            ltd_slug=ltd_slug,
            ltd_base_url=str(config_snapshot.ltd_base_url),
        )
        enqueued += 1
    return enqueued


async def _tier_discovery_for_org(
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    factory: Factory,
    org: Organization,
    logger: structlog.stdlib.BoundLogger,
) -> int:
    """Run one tier_discovery pass for a single enabled org.

    Uses :func:`should_poll_for_tier` (with ``tier=Tier.discovery``) to
    skip dormant projects so the long tail does not pin the cron to
    ~1500 ``GET /products/<slug>/editions/`` calls every 30 min. Hot
    projects (LTD ``main`` rebuilt within ``TIER_DISCOVERY_HOT_WINDOW``)
    keep the 30-min cadence; dormant projects fall back to one pass per
    ``TIER_DISCOVERY_DORMANT_INTERVAL``.
    """
    config_snapshot = org.keeper_sync_config
    if config_snapshot is None:
        return 0
    in_scope = await _list_in_scope_slugs(
        factory=factory, config=config_snapshot
    )
    if not in_scope:
        return 0
    ltd_client = factory.create_ltd_client(
        base_url=str(config_snapshot.ltd_base_url)
    )
    state_store = factory.create_keeper_sync_state_store()
    queue_job_store = factory.create_queue_job_store()
    arq_queue = ctx["arq_queue"]
    now = datetime.now(tz=UTC)
    enqueued = 0
    for ltd_slug in in_scope:
        async with session.begin():
            project_state = await state_store.get(
                org_id=org.id,
                resource_type=ResourceType.project,
                ltd_slug=ltd_slug,
            )
        if not should_poll_for_tier(
            state=project_state,
            now=now,
            tier=Tier.discovery,
            hot_window=TIER_DISCOVERY_HOT_WINDOW,
            dormant_interval=TIER_DISCOVERY_DORMANT_INTERVAL,
        ):
            continue
        try:
            should_enqueue = await _project_needs_discovery(
                session=session,
                state_store=state_store,
                ltd_client=ltd_client,
                org_id=org.id,
                ltd_slug=ltd_slug,
                project_state=project_state,
            )
        except LtdClientError:
            logger.exception(
                "Tier-discovery: failed to inspect project editions",
                org=org.slug,
                ltd_slug=ltd_slug,
            )
            # Stamp the polled annotation even on error — otherwise a
            # flaky LTD endpoint defeats the dormancy rate-limiter.
            await _record_tier_polled(
                session=session,
                state_store=state_store,
                org_id=org.id,
                ltd_slug=ltd_slug,
                tier=Tier.discovery,
                now=now,
            )
            continue
        if should_enqueue:
            await _enqueue_tier_project_sync(
                session=session,
                queue_job_store=queue_job_store,
                arq_queue=arq_queue,
                org_id=org.id,
                org_slug=org.slug,
                ltd_slug=ltd_slug,
                ltd_base_url=str(config_snapshot.ltd_base_url),
            )
            enqueued += 1
        # Stamp the polled annotation regardless of enqueue so the
        # planner clamps a project to one LTD pass per dormant
        # interval; if we only stamped on enqueue, a fully-known
        # dormant project would re-poll (and re-list editions) on
        # every tick.
        await _record_tier_polled(
            session=session,
            state_store=state_store,
            org_id=org.id,
            ltd_slug=ltd_slug,
            tier=Tier.discovery,
            now=now,
        )
    return enqueued


async def _tier_other_for_org(
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    factory: Factory,
    org: Organization,
    logger: structlog.stdlib.BoundLogger,
) -> int:
    """Run one tier_other pass for a single enabled org.

    Uses :func:`should_poll_for_tier` (with ``tier=Tier.other``) to
    skip dormant projects before the per-project
    ``GET /products/<slug>/editions/`` listing, so a project whose
    branches haven't been touched in months stops driving an hourly
    LTD fetch. Hot and dormant-due projects continue to fetch the
    edition list and re-enqueue when state lags past
    :data:`TIER_OTHER_REFRESH_THRESHOLD`.
    """
    config_snapshot = org.keeper_sync_config
    if config_snapshot is None:
        return 0
    in_scope = await _list_in_scope_slugs(
        factory=factory, config=config_snapshot
    )
    if not in_scope:
        return 0
    ltd_client = factory.create_ltd_client(
        base_url=str(config_snapshot.ltd_base_url)
    )
    state_store = factory.create_keeper_sync_state_store()
    queue_job_store = factory.create_queue_job_store()
    arq_queue = ctx["arq_queue"]
    now = datetime.now(tz=UTC)
    enqueued = 0
    for ltd_slug in in_scope:
        async with session.begin():
            project_state = await state_store.get(
                org_id=org.id,
                resource_type=ResourceType.project,
                ltd_slug=ltd_slug,
            )
        if not should_poll_for_tier(
            state=project_state,
            now=now,
            tier=Tier.other,
            hot_window=TIER_OTHER_HOT_WINDOW,
            dormant_interval=TIER_OTHER_DORMANT_INTERVAL,
        ):
            continue
        try:
            ltd_editions = await ltd_client.list_editions_for_product(ltd_slug)
        except LtdClientError:
            logger.exception(
                "Tier-other: failed to fetch project editions",
                org=org.slug,
                ltd_slug=ltd_slug,
            )
            await _record_tier_polled(
                session=session,
                state_store=state_store,
                org_id=org.id,
                ltd_slug=ltd_slug,
                tier=Tier.other,
                now=now,
            )
            continue
        if await _has_stale_non_main_edition(
            session=session,
            state_store=state_store,
            org_id=org.id,
            ltd_editions=ltd_editions,
            now=now,
        ):
            await _enqueue_tier_project_sync(
                session=session,
                queue_job_store=queue_job_store,
                arq_queue=arq_queue,
                org_id=org.id,
                org_slug=org.slug,
                ltd_slug=ltd_slug,
                ltd_base_url=str(config_snapshot.ltd_base_url),
            )
            enqueued += 1
        await _record_tier_polled(
            session=session,
            state_store=state_store,
            org_id=org.id,
            ltd_slug=ltd_slug,
            tier=Tier.other,
            now=now,
        )
    return enqueued


async def _list_in_scope_slugs(
    *, factory: Factory, config: KeeperSyncConfig
) -> list[str]:
    """Fetch LTD's product list and intersect it with the org's allowlist.

    Wraps :class:`LtdProductsClient` so the three tier-cron processors
    share the same list+filter pattern that ``keeper_sync_run_
    discovery`` uses; lifting it here keeps the per-tier logic focused
    on its decision rule.
    """
    products_client = factory.create_ltd_products_client(
        base_url=str(config.ltd_base_url)
    )
    ltd_slugs = await products_client.list_product_slugs()
    return _filter_to_allowlist(ltd_slugs, config.project_slugs)


async def _find_main_edition(
    *,
    ltd_client: LtdClient,
    state_store: KeeperSyncStateStore,
    session: AsyncSession,
    org_id: int,
    ltd_slug: str,
) -> LtdEdition | None:
    """Locate the LTD ``main`` edition for ``ltd_slug``.

    Uses a per-project cache persisted on the project-resource state
    row's ``annotations`` (``main_edition_url`` / ``main_edition_ltd_id``)
    so the steady-state common case is one ``GET /editions/<id>`` per
    project per tier_main tick instead of the
    ``GET /products/<slug>/editions/`` listing plus an
    ``GET /editions/<id>`` per non-``main`` edition. With ~1500 in-
    scope LTD products each carrying many ticket-branch editions, the
    walk path was the dominant load on the LTD API; the cache reduces
    it to one HTTP call per project.

    Cache invalidation:

    * Cached fetch returns 404 (the edition was deleted on LTD) —
      discard the pointer and walk.
    * Cached fetch returns 200 but the slug is no longer ``"main"`` —
      a maintainer renamed the edition; discard the pointer and walk.

    The caller (:func:`_tier_main_for_org`) re-writes the cache
    annotations on every successful resolve, so the pointer self-heals
    in the rare case where the walk discovers a different ``ltd_id``
    than was cached.
    """
    cached_url = await _cached_main_edition_url(
        state_store=state_store,
        session=session,
        org_id=org_id,
        ltd_slug=ltd_slug,
    )
    if cached_url is not None:
        try:
            edition = await ltd_client.get_edition_by_url(cached_url)
        except LtdNotFoundError:
            # Stale pointer: edition was deleted on LTD. Fall through to
            # the walk so we can rediscover ``main`` and overwrite.
            pass
        else:
            if edition.slug == _LTD_MAIN_SLUG:
                return edition
    return await _walk_for_main_edition(
        ltd_client=ltd_client, product_slug=ltd_slug
    )


async def _walk_for_main_edition(
    *,
    ltd_client: LtdClient,
    product_slug: str,
) -> LtdEdition | None:
    """Walk LTD's edition URL list looking for ``slug == "main"``.

    LTD has no slug-keyed edition lookup — every edition lives at
    ``/editions/{integer_id}``. We pull the URL list (one cheap HTTP
    call) and walk it in reverse: LTD orders the list newest-first
    and the ``main`` edition is typically the first edition created
    for a product (so it sits at the *end* of the listing), so this
    loop terminates after one fetch in the common case. Returns
    ``None`` when no ``main`` slug is found, which counts as "no main
    edition to refresh" rather than an error.
    """
    edition_urls = await ltd_client.list_edition_urls_for_product(product_slug)
    for url in reversed(edition_urls):
        edition = await ltd_client.get_edition_by_url(url)
        if edition.slug == _LTD_MAIN_SLUG:
            return edition
    return None


async def _cached_main_edition_url(
    *,
    state_store: KeeperSyncStateStore,
    session: AsyncSession,
    org_id: int,
    ltd_slug: str,
) -> str | None:
    """Return the project's cached ``main`` edition URL, if any."""
    async with session.begin():
        project_state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug=ltd_slug,
        )
    if project_state is None or project_state.annotations is None:
        return None
    cached = project_state.annotations.get(_MAIN_EDITION_URL_KEY)
    return cached if isinstance(cached, str) else None


async def _record_main_polled(  # noqa: PLR0913
    *,
    session: AsyncSession,
    state_store: KeeperSyncStateStore,
    org_id: int,
    ltd_slug: str,
    now: datetime,
    main_edition: LtdEdition | None,
) -> None:
    """Persist a tier_main poll outcome on the project state row.

    Two responsibilities, intentionally combined into one upsert so a
    polled visit always lands as a single transaction:

    * **Rate-limit bookkeeping.** ``date_main_last_polled`` is set to
      ``now`` on every polled visit (success, miss, or LTD error) so
      the dormancy planner clamps a project to ≤ 1 LTD fetch per
      ``TIER_MAIN_DORMANT_INTERVAL``. Skipping this on errors would
      let a flaky LTD endpoint defeat the rate limiter.
    * **Cached pointer + ``date_rebuilt_seen``.** When ``main_edition``
      is non-``None`` we additionally rewrite ``main_edition_ltd_id`` /
      ``main_edition_url`` (so the next tick's
      :func:`_find_main_edition` skips the URL walk) and write
      ``date_rebuilt_seen`` on the project state row so the next
      tick's :func:`should_poll_main_for_project` can decide hot vs
      dormant from this same row.

    Existing unrelated annotation keys are preserved by merge — no
    other writers exist today on the project-resource state row's
    annotations, but the forward-compatible posture costs nothing and
    avoids a future drive-by writer being blindsided.
    """
    async with session.begin():
        existing = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug=ltd_slug,
        )
        prior = (
            existing.annotations
            if existing is not None and existing.annotations is not None
            else {}
        )
        merged: dict[str, Any] = {
            **prior,
            ANNOTATION_DATE_MAIN_LAST_POLLED: now.isoformat(),
        }
        date_rebuilt_for_upsert: datetime | None = None
        if main_edition is not None:
            merged[_MAIN_EDITION_LTD_ID_KEY] = main_edition.ltd_id
            merged[_MAIN_EDITION_URL_KEY] = str(main_edition.self_url)
            date_rebuilt_for_upsert = main_edition.date_rebuilt
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug=ltd_slug,
            annotations=merged,
            date_rebuilt_seen=date_rebuilt_for_upsert,
        )


async def _record_tier_polled(  # noqa: PLR0913
    *,
    session: AsyncSession,
    state_store: KeeperSyncStateStore,
    org_id: int,
    ltd_slug: str,
    tier: Tier,
    now: datetime,
) -> None:
    """Stamp ``date_<tier>_last_polled`` on the project state row.

    Used by ``_tier_discovery_for_org`` and ``_tier_other_for_org`` to
    clamp dormant projects to one LTD pass per tier-specific
    ``dormant_interval``. Read-modify-write inside one transaction so
    other writers' annotation keys (the cached ``main_edition_*`` /
    ``date_main_last_polled``) are preserved by merge.

    Unlike :func:`_record_main_polled`, this helper does *not* update
    ``date_rebuilt_seen``; ``tier_main`` is the only writer of that
    field and the discovery / other tiers must not pretend they have
    observed an LTD rebuild.
    """
    annotation_key = _TIER_POLLED_ANNOTATION_KEYS[tier]
    async with session.begin():
        existing = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug=ltd_slug,
        )
        prior = (
            existing.annotations
            if existing is not None and existing.annotations is not None
            else {}
        )
        merged: dict[str, Any] = {**prior, annotation_key: now.isoformat()}
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug=ltd_slug,
            annotations=merged,
        )


_TIER_POLLED_ANNOTATION_KEYS: dict[Tier, str] = {
    Tier.main: ANNOTATION_DATE_MAIN_LAST_POLLED,
    Tier.discovery: ANNOTATION_DATE_DISCOVERY_LAST_POLLED,
    Tier.other: ANNOTATION_DATE_OTHER_LAST_POLLED,
}


async def _project_needs_discovery(  # noqa: PLR0913
    *,
    session: AsyncSession,
    state_store: KeeperSyncStateStore,
    ltd_client: LtdClient,
    org_id: int,
    ltd_slug: str,
    project_state: Any,
) -> bool:
    """Return True when an in-scope project has any unseen LTD resource.

    The cheap check first: if the project itself has no state row,
    enqueue immediately and skip the per-edition walk. Otherwise read
    every edition state row for the org in one batched query, then
    walk LTD's edition list checking presence in the in-memory dict.
    With 1500 in-scope LTD projects and an average ~10 editions each,
    the batched read replaces ~15 000 round-trips per discovery tick
    with ~1500.

    ``project_state`` is the state row already fetched by the caller
    (so the dormancy planner and this helper share one read). Pass
    ``None`` for "no row exists yet"; the cheap-path short-circuit
    will return ``True`` without touching LTD.
    """
    if is_unknown_resource(project_state):
        return True
    ltd_editions = await ltd_client.list_editions_for_product(ltd_slug)
    async with session.begin():
        edition_states = await state_store.list_for_org(
            org_id=org_id, resource_type=ResourceType.edition
        )
    by_ltd_id = {s.ltd_id: s for s in edition_states if s.ltd_id is not None}
    for ltd_edition in ltd_editions:
        if is_unknown_resource(by_ltd_id.get(ltd_edition.ltd_id)):
            return True
    return False


async def _has_stale_non_main_edition(
    *,
    session: AsyncSession,
    state_store: KeeperSyncStateStore,
    org_id: int,
    ltd_editions: list[LtdEdition],
    now: datetime,
) -> bool:
    """Return True when any non-``main`` edition's state is past threshold.

    Editions without a state row are deliberately ignored — they are
    ``tier_discovery``'s job. This decoupling keeps the two cron
    functions' decisions independent so a single missing-state row
    cannot cause two tiers to enqueue for the same project on the
    same hour.

    The state-row read is one batched ``list_for_org`` scoped to the
    LTD ids the caller already lists, replacing N per-edition ``get``
    round-trips. Memory cost stays bounded because the result set is
    capped by LTD's edition count for the project.
    """
    non_main_ltd_ids = [
        e.ltd_id for e in ltd_editions if e.slug != _LTD_MAIN_SLUG
    ]
    if not non_main_ltd_ids:
        return False
    async with session.begin():
        states = await state_store.list_for_org(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_ids=non_main_ltd_ids,
        )
    return any(should_refresh_other_edition(state=s, now=now) for s in states)


async def _enqueue_tier_project_sync(  # noqa: PLR0913
    *,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    arq_queue: ArqQueue,
    org_id: int,
    org_slug: str,
    ltd_slug: str,
    ltd_base_url: str,
) -> None:
    """Enqueue one ``keeper_sync_project`` child without run attribution.

    Mirrors ``_enqueue_children``'s commit-then-enqueue split (so the
    ``queue_jobs`` row exists before the arq job and a crash window
    leaves a recoverable orphan rather than an arq job pointing at no
    DB row). The two distinguishing details:

    * ``keeper_sync_run_id`` is left ``None`` on the ``queue_jobs``
      row — tier-cron jobs are continuous reconciliation, not
      bounded operator runs, and must not pollute any run's progress
      aggregate.
    * The arq payload omits the ``run_id`` key. The receiving
      ``keeper_sync_project`` worker reads it via ``payload.get("
      run_id")`` and skips ``maybe_finalise_run`` when ``None``.
    """
    async with session.begin():
        queue_job = await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label=ltd_slug,
        )
    metadata = await arq_queue.enqueue(
        "keeper_sync_project",
        _queue_name=KEEPER_SYNC_QUEUE_NAME,
        payload={
            "org_id": org_id,
            "org_slug": org_slug,
            "queue_job_id": queue_job.id,
            "ltd_slug": ltd_slug,
            "ltd_base_url": ltd_base_url,
        },
    )
    async with session.begin():
        await queue_job_store.set_backend_job_id(queue_job.id, metadata.id)
