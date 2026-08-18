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
  through the publish path. Per-edition failures the service isolated
  are recorded on the job's ``progress`` and leave its status
  ``completed_with_errors``; a whole-project failure — including the
  service's systemic-outage abort after too many consecutive edition
  failures — fails the job.

* ``keeper_sync_tier_main`` / ``_tier_discovery`` / ``_tier_other`` —
  cron-driven steady-state reconcilers that enqueue ``keeper_sync_
  project`` children with no run attribution. See PRD #275 §"
  Reconciliation cadence (steady state, run-independent)".
"""

from __future__ import annotations

import traceback
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol

import httpx
import sentry_sdk
import structlog
from safir.arq import ArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    JobKind,
    KeeperSyncConfig,
    KeeperSyncRunStatus,
    TrackingMode,
)
from docverse_server.config import config
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.edition import Edition
from docverse_server.domain.edition_build_history import EditionBuildHistory
from docverse_server.domain.keeper_sync_run import KeeperSyncRunWithActivity
from docverse_server.domain.organization import Organization
from docverse_server.factory import Factory
from docverse_server.services.keeper_sync.scheduler import (
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
from docverse_server.services.keeper_sync.service import (
    EditionSyncFailure,
    EditionSyncOutcome,
    ProjectSyncResult,
)
from docverse_server.services.keeper_sync_finalisation import (
    fail_run_for_lost_discovery,
    maybe_finalise_run,
    publish_run_completed,
)
from docverse_server.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse_server.services.publish_enqueue import (
    enqueue_publish_for_edition,
)
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
)
from docverse_server.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse_server.storage.ltd import (
    LtdClient,
    LtdClientError,
    LtdEdition,
    LtdNotFoundError,
)
from docverse_server.storage.queue_job_store import QueueJobStore

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

#: Cap on the number of per-edition failure detail entries written into
#: a ``keeper_sync_project`` job's ``progress`` JSONB (and into the
#: accompanying log line). ``edition_failure_count`` is always exact;
#: only the detail list is truncated, so a project whose entire release
#: history is unreadable — LTD's oldest uploads carry no public-read
#: object ACL — cannot write an unbounded blob into the job record.
_MAX_RECORDED_EDITION_FAILURES = 20

#: Tracking modes that identify a semver aggregate edition (``15`` /
#: ``15.2``). These rows are not LTD resources, so they never appear as
#: their own :class:`EditionSyncOutcome` and
#: :func:`_self_heal_unpublished_aggregates` has to find them by shape.
#: Kept in sync with
#: :func:`~docverse_server.domain.semver_aggregate.semver_aggregate_specs`,
#: the single source of the rows both the native and keeper-sync paths
#: create.
_AGGREGATE_TRACKING_MODES = frozenset(
    {TrackingMode.semver_major, TrackingMode.semver_minor}
)


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
    therefore sweeps these populations in one transaction:

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
    4. Tier-cron abandoned rows
       (:meth:`QueueJobStore.fail_abandoned_tier_cron_jobs`) — the
       third loss mode PRD #538 identified: the row *did* reach arq and
       arq then lost the job, so neither the silent pass
       (``in_progress`` only) nor the orphan pass
       (``backend_job_id IS NULL`` only) can see it.
    5. Run-attributed abandoned children
       (:meth:`QueueJobStore.fail_abandoned_run_children`) — the same
       loss mode under a run, folded into the same ``run_ids``
       finalisation pass as population 1 so an abandoned child stops
       blocking its parent run exactly like an orphaned one does.
    6. Abandoned run discoveries
       (:meth:`QueueJobStore.fail_abandoned_run_discovery`) — the run's
       own fan-out job, lost by arq before it enqueued anything. It is
       run-attributed like population 5 but is not a child, so it gets
       its own sweep, its own ``errors.message``, and
       :func:`fail_run_for_lost_discovery` rather than
       :func:`maybe_finalise_run`: with no children to aggregate, the
       run fails outright the way a worker-raised discovery failure
       fails it. Until that happens ``has_non_terminal_run`` 409-blocks
       every later run for the org.

    Populations 4, 5, and 6 ask the queue backend whether arq still knows
    each candidate before failing it, so a job merely backed up behind a
    saturated pool is never cancelled. That is the reaper's only
    dependency beyond the queue-job store; when the backend is
    unreachable those passes abort for the tick (logging a warning and
    mutating nothing) while the first three proceed.

    Thresholds: the silent paths use
    ``config.keeper_sync_reaper_threshold_seconds``, which defaults to
    ``config.keeper_sync_job_timeout_seconds`` plus
    :data:`~docverse_server.config.KEEPER_SYNC_REAPER_MARGIN_SECONDS`
    (5400 s at stock settings) and is env-overridable so test/staging
    environments can drive it down to seconds for fast verification.
    Deriving it keeps the wait just past the point where arq — running
    these functions with ``max_tries=1`` — has definitely cancelled the
    job, instead of parking the project behind its active-job mutex for
    hours after the row is known dead. The orphan path uses
    :data:`_ORPHAN_IDLE_WINDOW` (5 min) so the staleness check matches
    the existing discovery-side orphan sweep. The abandoned paths reuse
    the silent threshold rather than that short window: a row that
    reached arq deserves the same benefit of the doubt a running job
    gets before being declared dead.

    Wired as a cron job on ``KeeperSyncWorkerSettings.cron_jobs``
    (every 30 min). Returns a one-line status string for arq's result
    log; the structured ``logger.info`` carries the detail.
    """
    logger = structlog.get_logger("docverse_server.worker.keeper_sync_reaper")
    threshold = timedelta(seconds=config.keeper_sync_reaper_threshold_seconds)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_keeper_sync_run_store()
        org_store = factory.create_org_store()
        # The abandoned sweeps ask arq whether it still knows each
        # candidate job, so the reaper now needs a queue backend (PRD
        # #538 §Summary, "Reaper dependency change").
        queue_backend = factory.create_queue_backend()

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
            tier_abandoned = (
                await queue_job_store.fail_abandoned_tier_cron_jobs(
                    idle_after=threshold, queue_backend=queue_backend
                )
            )
            # No ``run_id``: the reaper has no run in hand, so it sweeps
            # every run-attributed row at once and reads the runs that
            # need finalising back off the reaped rows below.
            run_abandoned = await queue_job_store.fail_abandoned_run_children(
                idle_after=threshold, queue_backend=queue_backend
            )
            discovery_abandoned = (
                await queue_job_store.fail_abandoned_run_discovery(
                    idle_after=threshold, queue_backend=queue_backend
                )
            )
            # A lost discovery means the run never fanned out, so it
            # fails outright — the terminal status
            # ``keeper_sync_run_discovery``'s own except-branch writes —
            # instead of going through ``maybe_finalise_run``, which
            # would read the lone discovery row as a failed child and
            # settle on ``partial_failure``. Doing it before the
            # finalisation loop leaves those runs terminal, so the loop's
            # own terminal pre-check turns into a no-op for them.
            discovery_run_ids = {
                qj.keeper_sync_run_id for qj in discovery_abandoned
            }
            for run_id in discovery_run_ids:
                if run_id is None:
                    continue
                await fail_run_for_lost_discovery(
                    run_store=run_store, run_id=run_id
                )
            run_ids = {
                qj.keeper_sync_run_id
                for qj in (*reaped, *run_abandoned, *discovery_abandoned)
            }
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

        by_sweep = (
            ("run_attributed_silent", reaped),
            ("tier_cron_silent", tier_silent),
            ("tier_cron_orphan", tier_orphans),
            ("tier_cron_abandoned", tier_abandoned),
            ("run_attributed_abandoned", run_abandoned),
            ("run_discovery_abandoned", discovery_abandoned),
        )
        total_reaped = sum(len(jobs) for _, jobs in by_sweep)
        if total_reaped:
            logger.warning(
                "Reaped stuck keeper-sync queue jobs",
                reaped_count=total_reaped,
                run_attributed_silent_count=len(reaped),
                tier_cron_silent_count=len(tier_silent),
                tier_cron_orphan_count=len(tier_orphans),
                tier_cron_abandoned_count=len(tier_abandoned),
                run_attributed_abandoned_count=len(run_abandoned),
                run_discovery_abandoned_count=len(discovery_abandoned),
                run_ids=sorted(r for r in run_ids if r is not None),
                # Per-row detail so a postmortem can tell which sweep
                # claimed a row and cross-reference the arq job ID that
                # went missing, without querying the database.
                reaped_jobs=[
                    {
                        "public_id": qj.public_id,
                        "sweep": sweep,
                        "backend_job_id": qj.backend_job_id,
                    }
                    for sweep, jobs in by_sweep
                    for qj in jobs
                ],
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
        "docverse_server.worker.keeper_sync_run_discovery"
    ).bind(org=org_slug, run_id=run_id)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_keeper_sync_run_store()

        async with session.begin():
            # Late-delivery guard (PRD #538): a reaper may have already
            # failed this row — and rolled the run up with it — so the
            # discovery must not fan out a second time.
            if await queue_job_store.start_if_queued(queue_job_id) is None:
                return "skipped"
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
                raise RuntimeError(msg)

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
       :func:`docverse_server.services.publish_enqueue.enqueue_publish_for_edition`
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
    4. On success, mark the queue job ``completed`` (or
       ``completed_with_errors`` when the service isolated any
       per-edition failure); on a caught exception, mark it ``failed``
       with structured error details and re-raise so arq records the job
       as failed. Both branches call :func:`maybe_finalise_run` so a
       terminal child cannot leave the parent run stuck in
       ``in_progress``.

    :meth:`~KeeperSyncService.sync_project` gives each edition its own
    failure boundary, so an unreadable LTD build no longer reaches the
    outer ``except`` — it lands on
    :attr:`ProjectSyncResult.edition_failures` instead. Those runs still
    reach the end of the edition list rather than aborting, but they
    finish ``completed_with_errors``, with the skipped LTD editions
    recorded on the ``queue_jobs`` row's ``progress`` (see
    :func:`_edition_failure_progress`) and repeated in a ``warning`` log
    line; the parent run rolls up ``partial_failure``. A failure of the
    project as a whole — the LTD product fetch, the org lookup, the
    copier's destination store — still fails the job outright, as does
    either of the service's systemic-outage signals, which raise
    :exc:`~docverse_server.exceptions.KeeperSyncSystemicFailureError`
    out of ``sync_project``:
    :data:`~docverse_server.services.keeper_sync.service.MAX_CONSECUTIVE_EDITION_FAILURES`
    consecutive edition failures (a mid-run LTD or database outage fails
    the job rather than quietly reporting a 3-of-80 import as done), and
    a run that ends with failures and nothing imported at all (the same
    outage on a project too small to reach that threshold).
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
    logger = structlog.get_logger(
        "docverse_server.worker.keeper_sync_project"
    ).bind(org=org_slug, run_id=run_id, ltd_slug=ltd_slug)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_keeper_sync_run_store()
        org_store = factory.create_org_store()

        async with session.begin():
            # Late-delivery guard (PRD #538): a reaper may have already
            # failed this row and, for a run child, rolled the parent run
            # up on its behalf.
            if await queue_job_store.start_if_queued(queue_job_id) is None:
                return "skipped"
            org = await org_store.get_by_id(org_id)

        try:
            if org is None:
                msg = f"Organization {org_id} not found"
                raise RuntimeError(msg)
            publishing_store_label = org.publishing_store_label
            if publishing_store_label is None:
                msg = (
                    f"Org {org_id} has no publishing_store_label "
                    "configured; keeper-sync requires a publishing "
                    "object store"
                )
                raise RuntimeError(msg)

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

        edition_failures = sync_result.edition_failures
        completion = await _finalise_project_job(
            session=session,
            queue_job_store=queue_job_store,
            run_store=run_store,
            queue_job_id=queue_job_id,
            run_id=run_id,
            edition_failures=edition_failures,
        )
        await publish_run_completed(
            events=ctx.get("events"),
            session=session,
            org_store=org_store,
            completion=completion,
            logger=logger,
        )
        _log_project_completion(
            logger=logger, edition_failures=edition_failures
        )
        return "completed_with_errors" if edition_failures else "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _finalise_project_job(
    *,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    run_store: KeeperSyncRunStore,
    queue_job_id: int,
    run_id: int | None,
    edition_failures: Sequence[EditionSyncFailure],
) -> KeeperSyncRunWithActivity | None:
    """Close out a ``keeper_sync_project`` job that reached its end.

    Records any per-edition failures the service isolated on the job's
    ``progress`` and marks the job terminal in the *same* transaction
    that rolls the parent run, so the job record and its terminal
    status can never disagree. Returns whatever
    :func:`maybe_finalise_run` returned (always ``None`` for a
    tier-cron job, which carries no ``run_id``) for the caller to
    publish after the transaction commits.

    A job with isolated per-edition failures completes
    ``completed_with_errors`` rather than plain ``completed`` — the same
    ``complete(has_errors=...)`` signal the ``git_ref_audit`` worker
    uses for its own per-project isolation. Reaching the end of the
    edition loop is not the same as importing the project, and a
    partial import must not be indistinguishable from a clean one at
    the status level. The status carries up to the run for free:
    ``KeeperSyncRunStore.aggregate_activity`` buckets
    ``completed_with_errors`` into ``failed_count``, so
    :func:`maybe_finalise_run` rolls the parent run to the existing
    ``partial_failure`` status once every child is terminal. No new run
    status is needed.
    """
    async with session.begin():
        if edition_failures:
            await queue_job_store.update_progress(
                queue_job_id, _edition_failure_progress(edition_failures)
            )
        await queue_job_store.complete(
            queue_job_id, has_errors=bool(edition_failures)
        )
        if run_id is None:
            return None
        return await maybe_finalise_run(run_store=run_store, run_id=run_id)


def _log_project_completion(
    *,
    logger: structlog.stdlib.BoundLogger,
    edition_failures: Sequence[EditionSyncFailure],
) -> None:
    """Emit the project sync's terminal log line, partial or clean."""
    if not edition_failures:
        logger.info("Keeper-sync project completed")
        return
    logger.warning(
        "Keeper-sync project completed with edition failures",
        edition_failure_count=len(edition_failures),
        failed_ltd_edition_slugs=[
            failure.ltd_edition_slug
            for failure in edition_failures[:_MAX_RECORDED_EDITION_FAILURES]
        ],
    )


def _edition_failure_progress(
    failures: Sequence[EditionSyncFailure],
) -> dict[str, Any]:
    """Build the ``progress`` payload for a partially-synced project.

    The job finishes ``completed_with_errors`` rather than ``failed``:
    a permanently unreadable LTD build (an old upload with no
    public-read ACL) must not abort the project's import on every
    subsequent poll, but it must not read as a clean sync either. These
    per-edition entries carry the detail behind that status, so an
    operator reading ``GET /jobs/<id>`` can see exactly which LTD
    editions were skipped and why.
    """
    return {
        "message": (
            f"Project synced with {len(failures)} edition failure(s);"
            " the remaining editions synced normally"
        ),
        "edition_failure_count": len(failures),
        "edition_failures": [
            {
                "ltd_edition_id": failure.ltd_edition_id,
                "ltd_edition_slug": failure.ltd_edition_slug,
                "error_type": failure.error_type,
                "error_message": failure.error_message,
            }
            for failure in failures[:_MAX_RECORDED_EDITION_FAILURES]
        ],
    }


def _build_on_edition_synced(
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
        await _enqueue_publish_for_aggregates(
            factory=factory,
            session=session,
            queue_job_store=queue_job_store,
            org_id=org_id,
            run_id=run_id,
            outcome=outcome,
            logger=logger,
        )

    return callback


async def _enqueue_publish_for_synced_edition(
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


async def _enqueue_publish_for_aggregates(
    *,
    factory: Factory,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    org_id: int,
    run_id: int | None,
    outcome: EditionSyncOutcome,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Publish the semver aggregates the synced release just moved.

    The ``15`` / ``15.2`` editions keeper-sync backfills carry a current
    build but are not LTD resources, so they never appear as their own
    :class:`EditionSyncOutcome` — without this pass the dashboard would
    link to an unpublished aggregate.

    Runs regardless of ``build_outcome.short_circuited``: an aggregate
    can be created on a re-sync whose build short-circuited (the release
    was imported before this backfill existed). The service only emits
    an outcome when it actually created or advanced the row, so the
    steady state enqueues nothing.

    This is a one-shot enqueue and the only outcome-driven one an
    aggregate ever gets: the next sync skips the backfill outright
    (its ``aggregates_backfilled_build_id`` state marker already names
    this build) and would emit no outcome even if it ran, because the
    pointer is where it should be — nothing here fires again. Whatever
    this
    call loses — ``sync_project`` swallows this callback's exceptions,
    and a worker can die between the backfill's commit and this enqueue
    — is recovered from persistent state by
    :func:`_self_heal_unpublished_aggregates`.
    """
    if not outcome.aggregate_outcomes:
        return
    edition_store = factory.create_edition_store()
    history_store = factory.create_edition_build_history_store()
    queue_backend = factory.create_queue_backend()

    for aggregate in outcome.aggregate_outcomes:
        await enqueue_publish_for_edition(
            session=session,
            edition_store=edition_store,
            history_store=history_store,
            queue_job_store=queue_job_store,
            queue_backend=queue_backend,
            org_id=org_id,
            project_id=outcome.docverse_project_id,
            project_slug=outcome.docverse_project_slug,
            edition_id=aggregate.docverse_edition_id,
            edition_slug=aggregate.docverse_slug,
            build_id=aggregate.docverse_build_id,
            build_public_id=aggregate.docverse_build_public_id,
            keeper_sync_run_id=run_id,
        )
        logger.info(
            "Enqueued publish_edition for synced build",
            edition_slug=aggregate.docverse_slug,
            build_id=aggregate.docverse_build_id,
            phase="semver_aggregate",
        )


async def _self_heal_unpublished_editions(
    *,
    factory: Factory,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    org_id: int,
    run_id: int | None,
    sync_result: ProjectSyncResult,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Tail-end pass: publish editions whose build was never published.

    Two legs run here. The first iterates
    ``sync_result.edition_outcomes`` — the LTD-backed editions — and is
    described below. The second,
    :func:`_self_heal_unpublished_aggregates`, covers the semver
    aggregates, which are not LTD resources and therefore never appear
    in ``edition_outcomes`` at all; it heals from persistent state
    instead, and runs only on the runs
    :func:`_run_may_have_moved_aggregates` admits — reading that state
    means a full-project scan, which must not be on the steady-state
    poll's bill.

    Iterates ``sync_result.edition_outcomes`` looking for editions whose
    sync short-circuited (``build_outcome.short_circuited`` is ``True``)
    and whose current build has no publish on record, as
    :func:`_resolve_unpublished_build_target` decides. The freshly-synced
    branch is now handled by
    :func:`_enqueue_publish_for_synced_edition` as an
    ``on_edition_synced`` callback — running that path here too would
    double-publish.

    A short-circuited edition can be missing its build's publish when:

    * The build pre-dates this enqueue logic landing (i.e. it was
      synced before the per-edition publish path existed).
    * A prior publish enqueue was lost (Phase B failure between the
      ``QueueJob`` insert and the arq enqueue).
    * A convergence repoint moved the edition onto a build the
      short-circuit path never publishes: ``sync_build`` re-points the
      edition at the older completed build carrying the same content
      hash and returns ``short_circuited=True``, which
      :func:`_enqueue_publish_for_synced_edition` skips by design. This
      leg is the only path that can publish that pair.

    Pairs whose publish is already ``pending`` / ``published`` /
    ``failed`` are left alone — a stuck pending publish is the in-flight
    publisher's problem to resolve, not ours, and a successful or failed
    prior publish does not need re-running on every reconciliation tick.
    The tail-end position keeps this pass cheap on the steady-state
    common case (almost every edition either short-circuited and is
    already published, or was freshly synced and just got published by
    the per-edition callback).
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
            history_store=history_store,
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

    if _run_may_have_moved_aggregates(sync_result):
        await _self_heal_unpublished_aggregates(
            factory=factory,
            session=session,
            queue_job_store=queue_job_store,
            org_id=org_id,
            run_id=run_id,
            project_id=project_id,
            project_slug=project_slug,
            logger=logger,
        )


def _run_may_have_moved_aggregates(sync_result: ProjectSyncResult) -> bool:
    """Report whether this run could have moved a semver aggregate.

    The gate on :func:`_self_heal_unpublished_aggregates`, whose scan is
    the one part of a ``keeper_sync_project`` job that costs the same on
    a run that changed nothing as on a run that imported the whole
    project. A migrated project with 80 releases carries ~30 ``N`` /
    ``N.M`` rows, and the reconciliation tiers re-poll every project
    every few minutes forever — so an ungated pass is a permanent
    per-project tax paid for a result that is, in the steady state,
    always "nothing to do".

    Two signals open the gate, matching exactly the ways an aggregate's
    build pointer can end up unpublished in the first place:

    * **An aggregate outcome.** ``_backfill_semver_aggregates`` created
      or advanced a row this run, so the one publish enqueue it gets
      (from :func:`_enqueue_publish_for_aggregates`) is in flight — and
      may have been swallowed, which is the loss mode the self-heal
      exists for. The pass runs in the same job, so the recovery lands
      on the run that opened the hole.
    * **A build outcome that did not short-circuit.** An edition
      imported a fresh build, which is the only way the backfill can
      have run at all this poll — including the paths that lose their
      outcome, where ``_backfill_semver_aggregates`` raised (or the
      worker died) after committing an aggregate but before reporting
      it. Those emit no ``aggregate_outcomes`` by construction, so the
      build-level signal is what covers them.

    A run with neither signal imported no build and reconciled no
    aggregate, so anything it would find was already in place when the
    previous run ended — healed there, or left for the next run that
    moves something on the project, which for a project still receiving
    LTD builds is its next rebuild. The window this trades away is a
    project whose editions have *all* frozen and which lost an enqueue on
    the last run that touched them: its aggregate stays unpublished until
    something moves again. Scanning every project on every poll forever
    is too much to pay to close it.
    """
    for outcome in sync_result.edition_outcomes:
        if outcome.aggregate_outcomes:
            return True
        build_outcome = outcome.build_outcome
        if build_outcome is not None and not build_outcome.short_circuited:
            return True
    return False


async def _self_heal_unpublished_aggregates(
    *,
    factory: Factory,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    org_id: int,
    run_id: int | None,
    project_id: int,
    project_slug: str,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Tail-end pass: publish semver aggregates left pointing at nothing.

    The aggregates (``15`` / ``15.2``) get exactly one publish enqueue,
    from :func:`_enqueue_publish_for_aggregates` on the
    ``on_edition_synced`` path, and three things can swallow it:

    * The enqueue itself raises — ``sync_project`` deliberately absorbs
      ``on_edition_synced`` failures so one edition's callback cannot
      abort the project.
    * The worker dies after ``_backfill_semver_aggregates`` commits the
      repointed row but before the enqueue runs.
    * ``_backfill_semver_aggregates`` raises and ``sync_edition``
      absorbs it, returning an outcome with no ``aggregate_outcomes``
      even though an earlier spec in the loop already committed.

    Any of those leaves the aggregate row pointing at the release build
    with its KV pointer never written — a URL that serves 404 (or stale
    content) indefinitely, because no later pass recovers it: the LTD
    leg of :func:`_self_heal_unpublished_editions` iterates only
    ``edition_outcomes``, and a re-sync of an unchanged edition skips
    the backfill on its ``aggregates_backfilled_build_id`` marker (and
    ``_ensure_aggregate_edition`` would return ``None`` anyway once
    ``current_build_id`` already equals the build), so no outcome is
    emitted and nothing re-enqueues.

    Healing therefore reads persistent state rather than this run's
    in-memory outcomes: every aggregate-shaped edition on the project is
    a candidate, and :func:`_unpublished_build_target` decides which ones
    are genuinely unpublished. That covers all three loss modes,
    including the ones whose outcome never existed.

    *Which runs* look at that state is a separate question, answered by
    :func:`_run_may_have_moved_aggregates` — the caller's gate keeps this
    scan off the steady-state poll entirely.

    The scan itself is two queries in one transaction: the project's
    editions, then every candidate's ``edition_build_history`` row in a
    single batched lookup. The per-aggregate alternative
    (:func:`_resolve_unpublished_build_target`, still used by the LTD
    leg, which reaches its editions one at a time anyway) would open a
    transaction per row, and a migrated project carries one aggregate
    per release series.
    """
    edition_store = factory.create_edition_store()
    history_store = factory.create_edition_build_history_store()
    queue_backend = factory.create_queue_backend()

    aggregates: list[tuple[Edition, int]] = []
    async with session.begin():
        for edition in await edition_store.list_all_by_project(project_id):
            if edition.tracking_mode not in _AGGREGATE_TRACKING_MODES:
                continue
            current_build_id = edition.current_build_id
            if current_build_id is None:
                continue
            aggregates.append((edition, current_build_id))
        histories = await history_store.list_by_edition_build_pairs(
            [(edition.id, build_id) for edition, build_id in aggregates]
        )

    history_by_pair: dict[tuple[int, int], EditionBuildHistory] = {}
    for history in histories:
        history_by_pair.setdefault(
            (history.edition_id, history.build_id), history
        )

    for aggregate, current_build_id in aggregates:
        target = _unpublished_build_target(
            edition=aggregate,
            history=history_by_pair.get((aggregate.id, current_build_id)),
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
            edition_id=aggregate.id,
            edition_slug=aggregate.slug,
            build_id=build_id,
            build_public_id=build_public_id,
            keeper_sync_run_id=run_id,
        )
        logger.info(
            "Enqueued publish_edition for synced build",
            edition_slug=aggregate.slug,
            build_id=build_id,
            phase="aggregate_self_heal",
        )


def _unpublished_build_target(
    *,
    edition: Edition,
    history: EditionBuildHistory | None,
) -> tuple[int, str] | None:
    """Return ``(build_id, build_public_id)`` if the pair needs a publish.

    The single "is this edition's current build unpublished?" rule,
    shared by both legs of :func:`_self_heal_unpublished_editions`. It
    takes *history* — the ``edition_build_history`` row for the
    ``(edition, current_build)`` pair, or ``None`` when there is none —
    rather than loading it, so the aggregate leg can supply rows from
    one batched query while the LTD leg loads them one at a time via
    :func:`_resolve_unpublished_build_target`.

    "Unpublished" is decided per ``(edition, current_build)`` pair via the
    ``edition_build_history`` row rather than the edition's own
    ``publish_status``. The edition-level column is a single slot that
    survives a repoint — ``set_current_build`` never clears it — so an
    edition published for ``15.2.0`` and then advanced to ``15.2.1`` with
    a lost enqueue still reads ``published``. Reading that column would
    call the pair healthy and leave the new build unpublished forever,
    which is load-bearing for convergence repoints: a short-circuited
    build sync skips ``_enqueue_publish_for_synced_edition`` by design,
    so self-heal is the only path that can publish it.

    The history row is per pair and, on both keeper-sync paths, is
    written only by
    :func:`~docverse_server.services.publish_enqueue.enqueue_publish_for_edition`
    itself: keeper-sync skips ``EditionTrackingService`` entirely, and
    the two places it advances a pointer — ``_finalize_synced_build`` and
    ``_ensure_aggregate_edition`` — only call ``set_current_build``. So
    "no history row, or one whose ``publish_status`` is still ``NULL``"
    is exactly "a publish for this build was never enqueued". That covers
    pre-enqueue-era rows too: their builds were imported before the
    publish path existed, so no history row was ever recorded.

    The same signal supplies the dedup: a publish already in flight
    leaves the pair ``pending``, and a prior ``published`` / ``failed``
    publish is not something to re-run on every reconciliation tick —
    all three are left alone. An edition that does re-enqueue does so at
    most once, because the enqueue itself records the pair ``pending``.
    """
    if edition.current_build_id is None:
        return None
    if edition.current_build_public_id is None:
        return None
    if history is not None and history.publish_status is not None:
        return None
    return edition.current_build_id, serialize_base32_id(
        edition.current_build_public_id
    )


async def _resolve_unpublished_build_target(
    *,
    session: AsyncSession,
    history_store: EditionBuildHistoryStore,
    edition: Edition,
) -> tuple[int, str] | None:
    """Load one pair's history row and apply :func:`_unpublished_build_target`.

    The LTD leg's accessor. It reaches its editions one at a time — the
    slug-keyed lookup in :func:`_resolve_self_heal_target` — so there is
    no pair set to batch, unlike the aggregate leg.

    The read happens inside its own transaction so it does not interfere
    with ``enqueue_publish_for_edition``'s phased commits.
    """
    if edition.current_build_id is None:
        return None
    async with session.begin():
        history = await history_store.get_by_edition_and_build(
            edition_id=edition.id, build_id=edition.current_build_id
        )
    return _unpublished_build_target(edition=edition, history=history)


async def _resolve_self_heal_target(
    *,
    session: AsyncSession,
    edition_store: EditionStore,
    history_store: EditionBuildHistoryStore,
    project_id: int,
    edition_slug: str,
) -> tuple[int, str] | None:
    """Return ``(build_id, build_public_id)`` if the edition needs catch-up.

    The LTD leg reaches its edition by slug — ``edition_outcomes`` carries
    the Docverse slug, not the row — then defers to
    :func:`_resolve_unpublished_build_target` for the decision itself, so
    both legs of the self-heal pass share one rule.
    """
    async with session.begin():
        edition = await edition_store.get_by_slug(
            project_id=project_id, slug=edition_slug
        )
    if edition is None:
        return None
    return await _resolve_unpublished_build_target(
        session=session, history_store=history_store, edition=edition
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


async def _enqueue_children(
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

    The pre-check is the fast path, not the guarantee: the 5-minute
    ``keeper_sync_tier`` cron can claim the same slug between the
    ``SELECT`` and the ``INSERT``, and
    ``idx_queue_jobs_keeper_sync_project_active_uq`` — not the
    pre-check — is what actually enforces the mutex. The insert
    therefore goes through
    :meth:`~docverse_server.storage.queue_job_store.QueueJobStore.create_unless_active`,
    which turns that lost race into the same per-slug skip, exactly as
    ``_enqueue_tier_child`` does. Letting the ``IntegrityError`` escape
    instead would unwind this loop into
    ``keeper_sync_run_discovery``'s outer ``except``, failing both the
    discovery job and the whole run while silently dropping every
    remaining in-scope project.

    Run bookkeeping needs no adjustment for a skip. The run has no
    stored expected-child count: :func:`maybe_finalise_run` aggregates
    the ``queue_jobs`` rows actually attributed to the run, so a slug
    with no row simply never enters the aggregate. The two counters
    that do care are handled here — ``enqueued`` (returned, and the
    trigger for the caller's zero-child run termination) only counts
    real inserts, and the ``pending → in_progress`` transition is keyed
    off ``enqueued == 0`` rather than the loop index, so a run whose
    first slugs all lost the race still transitions on its first
    surviving child.

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
            queue_job = await queue_job_store.create_unless_active(
                kind=JobKind.keeper_sync_project,
                org_id=org_id,
                keeper_sync_run_id=run_id,
                subject_label=ltd_slug,
            )
            if queue_job is None:
                logger.info(
                    "Skipping keeper_sync_project enqueue: "
                    "lost the race for this project's active-job slot",
                    org=org_slug,
                    ltd_slug=ltd_slug,
                    source="run_discovery",
                )
                continue
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
    :func:`docverse_server.services.keeper_sync.scheduler.should_refresh_main_edition`
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
    logger = structlog.get_logger(
        "docverse_server.worker.keeper_sync_tier_main"
    )
    return await _run_tier(
        ctx=ctx, logger=logger, processor=_tier_main_for_org, tier_name="main"
    )


async def keeper_sync_tier_discovery(ctx: dict[str, Any]) -> str:
    """Cron (every 30 min): enqueue projects with unseen LTD resources.

    For each in-scope LTD project the cron checks the project-level
    ``keeper_sync_state`` row first; if missing it enqueues a
    ``keeper_sync_project`` straight away. Otherwise it lists the
    project's editions and asks
    :func:`docverse_server.services.keeper_sync.scheduler.is_unknown_resource`
    whether any edition lacks a state row. Discovery never enqueues
    twice for the same project on a single tick — one
    ``keeper_sync_project`` covers all of its editions.
    """
    logger = structlog.get_logger(
        "docverse_server.worker.keeper_sync_tier_discovery"
    )
    return await _run_tier(
        ctx=ctx,
        logger=logger,
        processor=_tier_discovery_for_org,
        tier_name="discovery",
    )


async def keeper_sync_tier_other(ctx: dict[str, Any]) -> str:
    """Cron (hourly): refresh non-``main`` editions older than the threshold.

    Walks each in-scope project's LTD editions and consults
    :func:`docverse_server.services.keeper_sync.scheduler.should_refresh_other_edition`
    against the local state row's ``date_last_synced``. The first
    stale non-``main`` edition for a project triggers one
    ``keeper_sync_project`` enqueue (which re-syncs every edition),
    so multiple stale editions do not produce duplicate children.
    Editions with no state row are left to ``tier_discovery`` so the
    two cron functions do not race for the same enqueue.
    """
    logger = structlog.get_logger(
        "docverse_server.worker.keeper_sync_tier_other"
    )
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


async def _record_main_polled(
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


async def _record_tier_polled(
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


async def _enqueue_tier_project_sync(
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
    :meth:`docverse_server.storage.queue_job_store.QueueJobStore.has_active_for_subject`
    and skips on duplicate. Tier ticks overlap (a 5-min tier_main and
    a 30-min tier_discovery both fire on :00 / :30) and a previous
    tick's job may not have started yet; skipping prevents two
    concurrent ``keeper_sync_project`` jobs from racing through
    ``_ensure_edition`` and losing the
    ``uq_editions_project_lower_slug`` race. Returns ``True`` on
    enqueue, ``False`` on skip so the caller can update its
    ``enqueued`` counter accurately.

    The pre-check is the fast path, not the guarantee: another worker
    can claim the slug between the ``SELECT`` and the ``INSERT``. The
    insert therefore goes through
    :meth:`~docverse_server.storage.queue_job_store.QueueJobStore.create_unless_active`,
    which turns that lost race into the same ``False`` skip. Letting the
    ``IntegrityError`` escape instead would unwind the caller's per-slug
    loop and — because ``_run_tier`` catches per *org* — silently drop
    every remaining project in that org for the tick.
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
        queue_job = await queue_job_store.create_unless_active(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label=ltd_slug,
        )
        if queue_job is None:
            logger.info(
                "Skipping keeper_sync_project enqueue: "
                "lost the race for this project's active-job slot",
                org=org_slug,
                ltd_slug=ltd_slug,
                tier=tier,
            )
            return False
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
