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
  blocks. The publish-enqueue path runs per-edition via an
  ``on_edition_synced`` callback so a partial-failure mid-sync still
  publishes everything that succeeded; a tail-end self-heal pass
  catches editions whose build was already imported but never made it
  through the publish path.

* ``keeper_sync_tier_main`` / ``_tier_discovery`` / ``_tier_other`` —
  cron-driven steady-state reconcilers that enqueue ``keeper_sync_
  project`` children with no run attribution. See PRD #275 §"
  Reconciliation cadence (steady state, run-independent)".
"""

from __future__ import annotations

import traceback
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol

import httpx
import sentry_sdk
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
from docverse.domain.keeper_sync_run import KeeperSyncRunWithActivity
from docverse.domain.organization import Organization
from docverse.factory import Factory
from docverse.services.keeper_sync.scheduler import (
    _TIER_ANNOTATION_KEYS,
    ANNOTATION_DATE_MAIN_LAST_POLLED,
    TIER_DISCOVERY_DORMANT_INTERVAL,
    TIER_DISCOVERY_DORMANT_JITTER,
    TIER_DISCOVERY_HOT_WINDOW,
    TIER_OTHER_DORMANT_INTERVAL,
    TIER_OTHER_DORMANT_JITTER,
    TIER_OTHER_HOT_WINDOW,
    Tier,
    is_unknown_resource,
    should_poll_for_tier,
    should_poll_main_for_project,
    should_refresh_main_edition,
    should_refresh_other_edition,
)
from docverse.services.keeper_sync.service import (
    EditionSyncOutcome,
    ProjectSyncResult,
)
from docverse.services.keeper_sync_finalisation import (
    maybe_finalise_run,
    publish_run_completed,
)
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
    """Cron-driven backstop that finalises silently-stuck keeper-sync rows.

    Mechanism #2 of the two-mechanism guarantee that a sync run always
    reaches a terminal state. arq's per-function ``timeout`` covers the
    common case (a job actually runs past the timeout and arq cancels
    it), but a worker pod that's OOM-killed mid-job or a job that arq
    itself loses leaves a child ``queue_jobs`` row stuck in
    ``in_progress`` forever — and with it the parent ``keeper_sync_runs``
    row, which can never finalise while ``pending_count > 0``.

    Tier-cron-enqueued ``keeper_sync_project`` jobs do not carry a
    ``keeper_sync_run_id`` so they have no run finalisation hook, but
    the same OOM / orphan windows wedge their per-subject
    :meth:`~QueueJobStore.has_active_for_subject` mutex. The reaper
    therefore sweeps three populations in one transaction:

    1. Run-attributed silent rows
       (:meth:`QueueJobStore.fail_silent_run_children`) — followed by
       :func:`maybe_finalise_run` per distinct run.
    2. Tier-cron silent rows
       (:meth:`QueueJobStore.fail_silent_tier_cron_jobs`) — frees the
       subject mutex so the next tier tick can re-enqueue.
    3. Tier-cron orphans
       (:meth:`QueueJobStore.fail_orphaned_tier_cron_jobs`) — same
       outcome for queued rows whose worker crashed between the SQL
       commit and ``arq_queue.enqueue``.

    Thresholds: the silent paths use
    ``config.keeper_sync_reaper_threshold_seconds`` (default 6 h,
    env-overridable so test/staging environments can drive it down to
    seconds for fast verification). The orphan path uses
    :data:`_ORPHAN_IDLE_WINDOW` (5 min) so the staleness check matches
    the existing discovery-side orphan sweep.

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
        org_store = factory.create_org_store()

        completions: list[KeeperSyncRunWithActivity] = []
        async with session.begin():
            reaped = await queue_job_store.fail_silent_run_children(
                idle_after=threshold
            )
            tier_silent = await queue_job_store.fail_silent_tier_cron_jobs(
                idle_after=threshold
            )
            tier_orphans = await queue_job_store.fail_orphaned_tier_cron_jobs(
                idle_after=_ORPHAN_IDLE_WINDOW
            )
            run_ids = {qj.keeper_sync_run_id for qj in reaped}
            for run_id in run_ids:
                if run_id is None:
                    continue
                completion = await maybe_finalise_run(
                    run_store=run_store, run_id=run_id
                )
                if completion is not None:
                    completions.append(completion)

        # Publish one keeper_sync_run_completed per run this sweep drove
        # terminal, after the finalisation transaction commits.
        events = ctx.get("events")
        for completion in completions:
            await publish_run_completed(
                events=events,
                session=session,
                org_store=org_store,
                completion=completion,
                logger=logger,
            )

        total_reaped = len(reaped) + len(tier_silent) + len(tier_orphans)
        if total_reaped:
            logger.warning(
                "Reaped stuck keeper-sync queue jobs",
                reaped_count=total_reaped,
                run_attributed_silent_count=len(reaped),
                tier_cron_silent_count=len(tier_silent),
                tier_cron_orphan_count=len(tier_orphans),
                run_ids=sorted(r for r in run_ids if r is not None),
            )
        else:
            logger.debug("No stuck keeper-sync queue jobs to reap")
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
            # Drop tombstoned project slugs from the fan-out so we do
            # not enqueue ``keeper_sync_project`` children that
            # ``sync_project`` would only short-circuit on its own
            # tombstone check (PRD #332 / user story 17). The empty-
            # fan-out finalisation path below covers the case where
            # tombstones consume the entire in-scope set.
            state_store = factory.create_keeper_sync_state_store()
            tombstoned_slugs = await _fetch_tombstoned_project_slugs(
                state_store=state_store, session=session, org_id=org_id
            )
            if tombstoned_slugs:
                in_scope = [s for s in in_scope if s not in tombstoned_slugs]
            logger.info(
                "Resolved keeper-sync run scope",
                ltd_count=len(ltd_slugs),
                in_scope_count=len(in_scope),
                tombstoned_count=len(tombstoned_slugs),
            )

            enqueued_count = await _enqueue_children(
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
                        "enqueued_count": enqueued_count,
                    },
                )
                await queue_job_store.complete(queue_job_id)
                # Empty fan-out OR all-skipped fan-out: no children
                # attributed to this run, so the parent will never
                # finalise on a child terminal. Terminate it here.
                if enqueued_count == 0:
                    await run_store.transition_status(
                        run_id=run_id,
                        new_status=KeeperSyncRunStatus.succeeded,
                    )
            logger.info(
                "Keeper-sync discovery completed",
                in_scope_count=len(in_scope),
            )
        except Exception as exc:
            sentry_sdk.capture_exception(exc)
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
       The worker passes an ``on_edition_synced`` callback that fires
       after each :meth:`KeeperSyncService.sync_edition` returns; for
       freshly-synced (non-short-circuited) builds the callback calls
       :func:`docverse.services.publish_enqueue.enqueue_publish_for_edition`
       immediately so the publish path runs the same way it does
       after a normal client upload — KV publish via
       ``EditionPublishingService.publish`` and a cascaded
       ``dashboard_build`` enqueue. The publish
       ``QueueJob`` rows carry ``keeper_sync_run_id`` so they roll into
       the parent run's progress counters and ``date_last_activity``.
       Running publish per-edition (rather than after the entire
       project sync returns) bounds the blast radius of a mid-sync
       failure to the edition that was being synced when the failure
       fired; editions 1..M-1 still get published.
    3. After the service returns, run the tail-end self-heal pass
       :func:`_self_heal_unpublished_editions` to catch editions whose
       short-circuited build is sitting on ``publish_status IS NULL``
       (e.g. they were imported before this enqueue logic landed). The
       freshly-synced branch is no longer needed here — it's handled
       by the per-edition callback.
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

            on_edition_synced = _build_on_edition_synced(
                factory=factory,
                session=session,
                queue_job_store=queue_job_store,
                org_id=org_id,
                run_id=run_id,
                logger=logger,
            )

            sync_result = await service.sync_project(
                org_id=org_id,
                ltd_slug=ltd_slug,
                on_edition_synced=on_edition_synced,
            )
            await _self_heal_unpublished_editions(
                factory=factory,
                session=session,
                queue_job_store=queue_job_store,
                org_id=org_id,
                run_id=run_id,
                sync_result=sync_result,
                logger=logger,
            )
        except Exception as exc:
            sentry_sdk.capture_exception(exc)
            logger.exception("Keeper-sync project failed")
            completion: KeeperSyncRunWithActivity | None = None
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
                    completion = await maybe_finalise_run(
                        run_store=run_store, run_id=run_id
                    )
            await publish_run_completed(
                events=ctx.get("events"),
                session=session,
                org_store=org_store,
                completion=completion,
                logger=logger,
            )
            raise

        completion = None
        async with session.begin():
            await queue_job_store.complete(queue_job_id)
            if run_id is not None:
                completion = await maybe_finalise_run(
                    run_store=run_store, run_id=run_id
                )
        await publish_run_completed(
            events=ctx.get("events"),
            session=session,
            org_store=org_store,
            completion=completion,
            logger=logger,
        )
        logger.info("Keeper-sync project completed")
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


def _build_on_edition_synced(  # noqa: PLR0913
    *,
    factory: Factory,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    org_id: int,
    run_id: int | None,
    logger: structlog.stdlib.BoundLogger,
) -> Callable[[EditionSyncOutcome], Awaitable[None]]:
    """Build the ``on_edition_synced`` callback for ``sync_project``.

    Lifting the closure out of ``keeper_sync_project``'s
    ``async for session in db_session_dependency():`` body sidesteps
    ruff B023 (the worker function does not actually iterate the
    generator more than once, but the closure-over-loop-var rule
    fires anyway).
    """

    async def callback(outcome: EditionSyncOutcome) -> None:
        await _enqueue_publish_for_synced_edition(
            factory=factory,
            session=session,
            queue_job_store=queue_job_store,
            org_id=org_id,
            run_id=run_id,
            outcome=outcome,
            logger=logger,
        )

    return callback


async def _enqueue_publish_for_synced_edition(  # noqa: PLR0913
    *,
    factory: Factory,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    org_id: int,
    run_id: int | None,
    outcome: EditionSyncOutcome,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Enqueue a publish for one freshly-synced edition's build.

    Runs as the ``on_edition_synced`` callback for
    :meth:`KeeperSyncService.sync_project`: each successful sync_edition
    return triggers an immediate publish enqueue so a partial-failure
    mid-project still publishes the editions that already succeeded.

    Skips when the build was short-circuited (LTD ``date_rebuilt``
    unchanged) — those editions are handled by
    :func:`_self_heal_unpublished_editions` on the tail-end pass when
    their ``publish_status`` is still ``NULL``. Skips when the build
    outcome is missing or carries no Docverse build id (a no-op edition
    or a convergence outcome that did not point at a publishable row).
    Skips when the edition outcome carries no Docverse edition id —
    a tombstoned ``keeper_sync_state`` row whose ``docverse_id`` is
    ``NULL`` short-circuited before the edition was ever imported.
    """
    build_outcome = outcome.build_outcome
    if build_outcome is None:
        return
    if build_outcome.short_circuited:
        return
    if (
        build_outcome.docverse_build_id is None
        or build_outcome.docverse_build_public_id is None
    ):
        return
    edition_id = outcome.docverse_edition_id
    if edition_id is None:
        return

    edition_store = factory.create_edition_store()
    history_store = factory.create_edition_build_history_store()
    queue_backend = factory.create_queue_backend()

    await enqueue_publish_for_edition(
        session=session,
        edition_store=edition_store,
        history_store=history_store,
        queue_job_store=queue_job_store,
        queue_backend=queue_backend,
        org_id=org_id,
        project_id=outcome.docverse_project_id,
        project_slug=outcome.docverse_project_slug,
        edition_id=edition_id,
        edition_slug=outcome.docverse_slug,
        build_id=build_outcome.docverse_build_id,
        build_public_id=build_outcome.docverse_build_public_id,
        keeper_sync_run_id=run_id,
    )
    logger.info(
        "Enqueued publish_edition for synced build",
        edition_slug=outcome.docverse_slug,
        build_id=build_outcome.docverse_build_id,
        phase="synced",
    )


async def _self_heal_unpublished_editions(  # noqa: PLR0913
    *,
    factory: Factory,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    org_id: int,
    run_id: int | None,
    sync_result: ProjectSyncResult,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Tail-end pass: publish short-circuited editions still unpublished.

    Iterates ``sync_result.edition_outcomes`` looking for editions whose
    sync short-circuited (``build_outcome.short_circuited`` is ``True``)
    *and* whose Docverse-side ``publish_status`` is still ``NULL``. The
    freshly-synced branch is now handled by
    :func:`_enqueue_publish_for_synced_edition` as an
    ``on_edition_synced`` callback — running that path here too would
    double-publish.

    A short-circuit + ``publish_status IS NULL`` edition can arise when:

    * The build pre-dates this enqueue logic landing (i.e. it was
      synced before the per-edition publish path existed).
    * A prior publish enqueue was lost (Phase B failure between the
      ``QueueJob`` insert and the arq enqueue).

    Editions whose ``publish_status`` is already ``pending`` /
    ``published`` / ``failed`` are left alone — a stuck pending publish
    is the in-flight publisher's problem to resolve, not ours, and a
    successful or failed prior publish does not need re-running on
    every reconciliation tick. The tail-end position keeps this pass
    cheap on the steady-state common case (almost every edition either
    short-circuited and is already published, or was freshly synced and
    just got published by the per-edition callback).
    """
    project_id = sync_result.docverse_project_id
    if project_id is None:
        # A tombstoned project short-circuit returned no edition
        # outcomes; nothing to self-heal.
        return
    edition_store = factory.create_edition_store()
    history_store = factory.create_edition_build_history_store()
    queue_backend = factory.create_queue_backend()
    project_slug = sync_result.docverse_project_slug

    for outcome in sync_result.edition_outcomes:
        build_outcome = outcome.build_outcome
        if build_outcome is None:
            continue
        if not build_outcome.short_circuited:
            continue
        edition_id = outcome.docverse_edition_id
        if edition_id is None:
            continue

        target = await _resolve_self_heal_target(
            session=session,
            edition_store=edition_store,
            project_id=project_id,
            edition_slug=outcome.docverse_slug,
        )
        if target is None:
            continue
        build_id, build_public_id = target

        await enqueue_publish_for_edition(
            session=session,
            edition_store=edition_store,
            history_store=history_store,
            queue_job_store=queue_job_store,
            queue_backend=queue_backend,
            org_id=org_id,
            project_id=project_id,
            project_slug=project_slug,
            edition_id=edition_id,
            edition_slug=outcome.docverse_slug,
            build_id=build_id,
            build_public_id=build_public_id,
            keeper_sync_run_id=run_id,
        )
        logger.info(
            "Enqueued publish_edition for synced build",
            edition_slug=outcome.docverse_slug,
            build_id=build_id,
            phase="self_heal",
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
    except httpx.HTTPError as exc:
        sentry_sdk.capture_exception(exc)
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


async def _fetch_tombstoned_project_slugs(
    *,
    state_store: KeeperSyncStateStore,
    session: AsyncSession,
    org_id: int,
) -> set[str]:
    """Return the LTD slugs of all tombstoned project state rows.

    The four discovery paths (``keeper_sync_run_discovery`` plus the
    three tier crons) call this once per pass and subtract the result
    from their in-scope slug list, so a ``keeper_sync_project`` child
    is never enqueued for a Docverse-side-vetoed project. Without the
    filter, ``sync_project`` would short-circuit on its own tombstone
    check (PRD #332 §"Sync-side skip checks") a few milliseconds later
    — same outcome, wasted queue + DB work. Issue #396 / user story 17.
    """
    async with session.begin():
        project_states = await state_store.list_for_org(
            org_id=org_id,
            resource_type=ResourceType.project,
            include_tombstoned=True,
        )
    return {
        s.ltd_slug for s in project_states if s.date_tombstoned is not None
    }


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
) -> int:
    """Fan out one child ``keeper_sync_project`` job per slug.

    Each iteration creates the ``queue_jobs`` row tagged with
    ``keeper_sync_run_id`` *first* — so a crash mid-fan-out leaves
    queued rows that progress aggregation can still see — then
    enqueues the arq job and writes the backend job ID back. The
    ``pending → in_progress`` run transition is atomic with the first
    successful child create so any concurrent ``GET /runs/{id}`` can
    never observe a run with children but still ``pending``.

    Per-slug mutual exclusion: before each create, the function
    pre-checks ``QueueJobStore.has_active_for_subject`` for the same
    ``(org_id, kind=keeper_sync_project, subject_label=ltd_slug)``.
    When an active row already exists (the typical case is a tier-
    cron-enqueued job that has not yet been picked up), the discovery
    skips the slug and logs at ``info``. The in-flight job stays
    unattributed (``keeper_sync_run_id IS NULL``); it will not count
    toward this run's ``total_count`` aggregate, so the run's progress
    counters can be smaller than the in-scope project list. Skipping
    prevents two concurrent ``keeper_sync_project`` jobs for the same
    slug from racing through ``_ensure_edition`` and losing the
    ``uq_editions_project_lower_slug`` race.

    The order leaves an orphan tail: if the worker dies between the
    SQL commit and ``arq_queue.enqueue``, the row sits in ``queued``
    with ``backend_job_id IS NULL`` and no arq job will ever pick it
    up — pending forever, blocking finalisation. The next discovery
    attempt sweeps these rows via ``_reconcile_orphan_children`` once
    they age past ``_ORPHAN_IDLE_WINDOW``.

    Returns the number of slugs that were enqueued (skipped slugs do
    not count). Callers use this to terminate a run whose entire
    fan-out was skipped, the same way an empty in-scope list does.
    """
    arq_queue = ctx["arq_queue"]
    enqueued = 0
    for ltd_slug in ltd_slugs:
        async with session.begin():
            if await queue_job_store.has_active_for_subject(
                org_id=org_id,
                kind=JobKind.keeper_sync_project,
                subject_label=ltd_slug,
            ):
                logger.info(
                    "Skipping keeper_sync_project enqueue: "
                    "an active job for this project already exists",
                    org=org_slug,
                    ltd_slug=ltd_slug,
                    source="run_discovery",
                )
                continue
            queue_job = await queue_job_store.create(
                kind=JobKind.keeper_sync_project,
                org_id=org_id,
                keeper_sync_run_id=run_id,
                subject_label=ltd_slug,
            )
            if enqueued == 0:
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
        enqueued += 1
        logger.debug(
            "Enqueued keeper_sync_project",
            ltd_slug=ltd_slug,
            queue_job_id=queue_job.id,
        )
    return enqueued


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
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
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
    # Drop tombstoned project slugs from the candidate set up front:
    # ``sync_project`` would only short-circuit on them a few
    # milliseconds later (issue #396 / PRD #332 user story 17).
    tombstoned_slugs = await _fetch_tombstoned_project_slugs(
        state_store=state_store, session=session, org_id=org.id
    )
    if tombstoned_slugs:
        in_scope = [s for s in in_scope if s not in tombstoned_slugs]
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
        except LtdClientError as exc:
            sentry_sdk.capture_exception(exc)
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
        if not await _tier_main_should_enqueue_edition(
            state_store=state_store,
            session=session,
            org_id=org.id,
            main_edition=main_edition,
        ):
            continue
        if await _enqueue_tier_project_sync(
            session=session,
            queue_job_store=queue_job_store,
            arq_queue=arq_queue,
            org_id=org.id,
            org_slug=org.slug,
            ltd_slug=ltd_slug,
            ltd_base_url=str(config_snapshot.ltd_base_url),
            logger=logger,
            tier="main",
        ):
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
    # Drop tombstoned project slugs up front (issue #396 / PRD #332
    # user story 17): ``sync_project`` would short-circuit on its own
    # tombstone check, so the enqueue is pure waste.
    tombstoned_slugs = await _fetch_tombstoned_project_slugs(
        state_store=state_store, session=session, org_id=org.id
    )
    if tombstoned_slugs:
        in_scope = [s for s in in_scope if s not in tombstoned_slugs]
    # Hoist the org-wide edition-state read out of the per-slug loop.
    # The previous shape called ``list_for_org`` from inside
    # ``_project_needs_discovery``, so a 1500-slug discovery tick
    # scanned the org's ~15 000 edition state rows 1500 times per
    # tick. The map is consulted in memory per slug.
    #
    # ``include_tombstoned=True`` keeps tombstoned edition rows in the
    # dict so :func:`is_unknown_resource` reads them as known
    # (non-``None``) and ``_project_needs_discovery`` does not fire
    # the "unseen LTD edition" enqueue branch on them. Without the
    # flag, a tombstoned edition is filtered out and reads as missing
    # — the very state the enqueue branch reacts to. Issue #396.
    async with session.begin():
        edition_states = await state_store.list_for_org(
            org_id=org.id,
            resource_type=ResourceType.edition,
            include_tombstoned=True,
        )
    edition_state_by_ltd_id = {
        s.ltd_id: s for s in edition_states if s.ltd_id is not None
    }
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
            jitter_window=TIER_DISCOVERY_DORMANT_JITTER,
        ):
            continue
        try:
            should_enqueue = await _project_needs_discovery(
                ltd_client=ltd_client,
                ltd_slug=ltd_slug,
                project_state=project_state,
                edition_state_by_ltd_id=edition_state_by_ltd_id,
            )
        except LtdClientError as exc:
            sentry_sdk.capture_exception(exc)
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
        if should_enqueue and await _enqueue_tier_project_sync(
            session=session,
            queue_job_store=queue_job_store,
            arq_queue=arq_queue,
            org_id=org.id,
            org_slug=org.slug,
            ltd_slug=ltd_slug,
            ltd_base_url=str(config_snapshot.ltd_base_url),
            logger=logger,
            tier="discovery",
        ):
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
    # Drop tombstoned project slugs up front (issue #396 / PRD #332
    # user story 17). ``_has_stale_non_main_edition``'s default
    # ``include_tombstoned=False`` already excludes tombstoned
    # editions from the staleness scan, so no edition-level filter is
    # needed here.
    tombstoned_slugs = await _fetch_tombstoned_project_slugs(
        state_store=state_store, session=session, org_id=org.id
    )
    if tombstoned_slugs:
        in_scope = [s for s in in_scope if s not in tombstoned_slugs]
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
            jitter_window=TIER_OTHER_DORMANT_JITTER,
        ):
            continue
        try:
            ltd_editions = await ltd_client.list_editions_for_product(ltd_slug)
        except LtdClientError as exc:
            sentry_sdk.capture_exception(exc)
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
        ) and await _enqueue_tier_project_sync(
            session=session,
            queue_job_store=queue_job_store,
            arq_queue=arq_queue,
            org_id=org.id,
            org_slug=org.slug,
            ltd_slug=ltd_slug,
            ltd_base_url=str(config_snapshot.ltd_base_url),
            logger=logger,
            tier="other",
        ):
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


async def _tier_main_should_enqueue_edition(
    *,
    state_store: KeeperSyncStateStore,
    session: AsyncSession,
    org_id: int,
    main_edition: LtdEdition,
) -> bool:
    """Return ``True`` iff a resolved main edition warrants an enqueue.

    Reads the matching edition state row (including tombstoned rows)
    and runs the two skip predicates the per-slug loop consults
    after :func:`_find_main_edition` succeeds:

    * Skip on tombstoned state row — ``sync_edition`` would only
      short-circuit on its own tombstone check (issue #396 / PRD #332
      user story 17).
    * Otherwise defer to :func:`should_refresh_main_edition` for the
      LTD ``date_rebuilt`` vs ``state.date_rebuilt_seen`` decision.

    Lifted out of :func:`_tier_main_for_org` to keep the per-slug
    loop's cyclomatic complexity under the project's ruff C901 ceiling.
    """
    async with session.begin():
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=main_edition.ltd_id,
            include_tombstoned=True,
        )
    if state is not None and state.date_tombstoned is not None:
        return False
    return should_refresh_main_edition(
        state=state, ltd_date_rebuilt=main_edition.date_rebuilt
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
    annotation_key = _TIER_ANNOTATION_KEYS[tier]
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


async def _project_needs_discovery(
    *,
    ltd_client: LtdClient,
    ltd_slug: str,
    project_state: Any,
    edition_state_by_ltd_id: dict[int, Any],
) -> bool:
    """Return True when an in-scope project has any unseen LTD resource.

    The cheap check first: if the project itself has no state row,
    enqueue immediately and skip the per-edition walk. Otherwise
    consult the pre-loaded org-wide edition-state map and walk LTD's
    edition list checking presence in memory. The caller hoists the
    ``list_for_org(resource_type=edition)`` read out of the per-slug
    loop and passes the resulting map in: with 1500 in-scope projects
    that flips ~1500 ``list_for_org`` round-trips per discovery tick
    into one.

    ``project_state`` is the state row already fetched by the caller
    (so the dormancy planner and this helper share one read). Pass
    ``None`` for "no row exists yet"; the cheap-path short-circuit
    will return ``True`` without touching LTD.
    """
    if is_unknown_resource(project_state):
        return True
    ltd_editions = await ltd_client.list_editions_for_product(ltd_slug)
    for ltd_edition in ltd_editions:
        if is_unknown_resource(
            edition_state_by_ltd_id.get(ltd_edition.ltd_id)
        ):
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
    logger: structlog.stdlib.BoundLogger,
    tier: str,
) -> bool:
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

    Per-slug mutual exclusion: pre-checks
    :meth:`docverse.storage.queue_job_store.QueueJobStore.has_active_for_subject`
    and skips on duplicate. Tier ticks overlap (a 5-min tier_main and
    a 30-min tier_discovery both fire on :00 / :30) and a previous
    tick's job may not have started yet; skipping prevents two
    concurrent ``keeper_sync_project`` jobs from racing through
    ``_ensure_edition`` and losing the
    ``uq_editions_project_lower_slug`` race. Returns ``True`` on
    enqueue, ``False`` on skip so the caller can update its
    ``enqueued`` counter accurately.
    """
    async with session.begin():
        if await queue_job_store.has_active_for_subject(
            org_id=org_id,
            kind=JobKind.keeper_sync_project,
            subject_label=ltd_slug,
        ):
            logger.info(
                "Skipping keeper_sync_project enqueue: "
                "an active job for this project already exists",
                org=org_slug,
                ltd_slug=ltd_slug,
                tier=tier,
            )
            return False
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
    return True
