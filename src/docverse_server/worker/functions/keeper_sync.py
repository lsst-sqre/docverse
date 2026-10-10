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
  failures — fails the job. Each job is one *slice* of the project: it
  stops walking editions once ``keeper_sync_slice_budget_seconds`` runs
  out and hands the rest to a continuation job it enqueues itself, so a
  project of any size converges across a chain of sub-timeout jobs.

* ``keeper_sync_tier_main`` / ``_tier_discovery`` / ``_tier_other`` —
  cron-driven steady-state reconcilers that enqueue ``keeper_sync_
  project`` children with no run attribution. See PRD #275 §"
  Reconciliation cadence (steady state, run-independent)".
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import traceback
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

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
from docverse_server.domain.queue import QueueJob
from docverse_server.factory import Factory
from docverse_server.metrics import BuildContentCopiedEvent, DocverseEvents
from docverse_server.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_id,
)
from docverse_server.services.keeper_sync.budget import (
    SliceBudget,
    SliceProgress,
)
from docverse_server.services.keeper_sync.mappers import is_ltd_main
from docverse_server.services.keeper_sync.push_hints import (
    PushCheckOutcome,
    ltd_rebuilt_since_sync,
    prune_pushed_refs,
    read_pushed_refs,
    settle_pushed_refs,
)
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
    tier_cron_timeout,
)
from docverse_server.services.keeper_sync.service import (
    BuildCopiedCallback,
    BuildCopyReport,
    EditionSyncFailure,
    EditionSyncOutcome,
    ProjectSyncResult,
)
from docverse_server.services.keeper_sync_finalisation import (
    fail_run_for_lost_discovery,
    maybe_finalise_run,
    publish_run_completed,
)
from docverse_server.services.publish_enqueue import (
    enqueue_publish_for_edition,
)
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.keeper_sync import (
    KeeperSyncState,
    KeeperSyncStateStore,
    ResourceType,
)
from docverse_server.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse_server.storage.ltd import (
    LtdClient,
    LtdClientError,
    LtdEdition,
    LtdNotFoundError,
    LtdProductsError,
    parse_ltd_id,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.queue_backend import QueueBackend
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions._cancellation import (
    cancellation_recorded,
    discovery_run_finaliser,
    infer_cancellation_reason,
    keeper_sync_run_finaliser,
    record_cancellation,
    record_handoff_cancellation,
)
from docverse_server.worker.functions._reaper_log import (
    create_reaped_jobs_payload,
)
from docverse_server.worker.queues import KEEPER_SYNC_QUEUE_NAME

# Window before a queued child with no ``backend_job_id`` is treated as
# orphaned by ``_reconcile_run_children``. Long enough to never race
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

#: ``keeper_sync_state.annotations`` key on a project-resource state row
#: holding the resolved LTD ``main`` edition's full ``self_url``. Owned
#: by ``_tier_main_for_org`` so subsequent ticks bypass the
#: ``GET /products/<slug>/editions/`` walk and go straight to
#: ``GET /editions/<id>``.
_MAIN_EDITION_URL_KEY = "main_edition_url"
# Written by `_record_main_polled` before #799 and never read; popped on
# every write so rows stamped by earlier releases converge on the URL-only
# pointer instead of carrying an id that can contradict a re-resolved URL.
_LEGACY_MAIN_EDITION_LTD_ID_KEY = "main_edition_ltd_id"

#: Cap on the number of per-edition failure detail entries written into
#: a ``keeper_sync_project`` job's ``progress`` JSONB (and into the
#: accompanying log line). ``edition_failure_count`` is always exact;
#: only the detail list is truncated, so a project whose entire release
#: history is unreadable — LTD's oldest uploads carry no public-read
#: object ACL — cannot write an unbounded blob into the job record. The
#: tier crons cap the URL list of their unparsable-edition-URL warning
#: (:func:`_list_edition_ltd_ids`) at the same size, for the same reason.
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


async def _finalise_reaped_runs(
    *,
    run_store: KeeperSyncRunStore,
    run_ids: set[int | None],
) -> list[KeeperSyncRunWithActivity]:
    """Roll up each distinct run behind a set of reaped rows.

    Shared by both of :func:`keeper_sync_reaper`'s transactions. Reaped
    rows carry a nullable ``keeper_sync_run_id`` (tier-cron rows have
    none), so the ``None`` member is skipped rather than filtered by the
    callers. Returns the runs this call actually drove terminal, for the
    caller to publish once its transaction commits.
    """
    completions: list[KeeperSyncRunWithActivity] = []
    for run_id in run_ids:
        if run_id is None:
            continue
        completion = await maybe_finalise_run(
            run_store=run_store, run_id=run_id
        )
        if completion is not None:
            completions.append(completion)
    return completions


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
    therefore sweeps these populations:

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

    The tick therefore runs in two transactions rather than one (task
    #548). The first carries populations 1-3 and the candidate queries
    for 4-6; the queue-backend round trips then happen with no
    transaction open; the second applies whatever those verified,
    re-checking each row is still ``queued``. Keeping the backend
    outside the first transaction is what stops a stalled Redis — the
    post-outage scenario the abandoned passes exist for — from blowing
    the tick past arq's job timeout and rolling back populations 1-3
    along with it.

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
        # First transaction: the three backend-free sweeps and their run
        # finalisations, plus the abandoned passes' candidate queries.
        # Committing here is what keeps a stalled Redis from taking this
        # work down with it — arq's job timeout cancels the tick with a
        # ``CancelledError``, a ``BaseException`` no ``except Exception``
        # soft-abort can catch, so anything still uncommitted when the
        # backend hangs is lost and re-lost every following tick (task
        # #548).
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
            tier_candidates = (
                await queue_job_store.select_abandoned_tier_cron_jobs(
                    idle_after=threshold
                )
            )
            # ``run_id=None``: the reaper has no run in hand, so it
            # sweeps every run-attributed row at once and reads the runs
            # that need finalising back off the reaped rows below. The
            # discovery worker takes the scoped mode instead.
            run_candidates = (
                await queue_job_store.select_abandoned_run_children(
                    run_id=None, idle_after=threshold
                )
            )
            discovery_candidates = (
                await queue_job_store.select_abandoned_run_discovery(
                    idle_after=threshold
                )
            )
            silent_run_ids = {qj.keeper_sync_run_id for qj in reaped}
            completions += await _finalise_reaped_runs(
                run_store=run_store, run_ids=silent_run_ids
            )

        # Backend round trips with no transaction open at all: nothing
        # to roll back, no rows locked, and the sweeps above already
        # durable however long arq takes to answer.
        tier_reaps = await queue_job_store.verify_abandoned_candidates(
            tier_candidates, queue_backend=queue_backend
        )
        run_reaps = await queue_job_store.verify_abandoned_candidates(
            run_candidates, queue_backend=queue_backend
        )
        discovery_reaps = await queue_job_store.verify_abandoned_candidates(
            discovery_candidates, queue_backend=queue_backend
        )

        # Second transaction, deliberately short: apply the verified
        # reaps (each re-checking its row is still ``queued``, in case a
        # worker picked it up while the backend was being asked) and roll
        # up whatever runs they left finalisable.
        async with session.begin():
            tier_abandoned = await queue_job_store.apply_abandoned_reaps(
                tier_reaps
            )
            run_abandoned = await queue_job_store.apply_abandoned_reaps(
                run_reaps
            )
            discovery_abandoned = await queue_job_store.apply_abandoned_reaps(
                discovery_reaps
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
            abandoned_run_ids = {
                qj.keeper_sync_run_id
                for qj in (*run_abandoned, *discovery_abandoned)
            }
            # Re-entrant for a run the first transaction already tried:
            # ``maybe_finalise_run`` returns ``None`` once the run is
            # terminal, and a run whose abandoned child was still
            # ``queued`` back then could not have finalised.
            completions += await _finalise_reaped_runs(
                run_store=run_store, run_ids=abandoned_run_ids
            )
        run_ids = silent_run_ids | abandoned_run_ids

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
                reaped_jobs=create_reaped_jobs_payload(by_sweep),
            )
        else:
            logger.debug("No stuck keeper-sync queue jobs to reap")
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


@cancellation_recorded
async def keeper_sync_run_discovery(
    ctx: dict[str, Any], payload: dict[str, Any]
) -> str:
    """Fan out one ``keeper_sync_project`` job per in-scope LTD product.

    Before fanning out, :func:`_reconcile_run_children` clears the
    children a previous attempt at this same run stranded — a
    re-delivered discovery would otherwise skip each stranded child's
    slug, because a ``queued`` row holds the per-subject active-job
    mutex the fan-out's pre-check consults. That pass asks the queue
    backend about candidates it cannot judge from the row alone, so it
    is also why this function needs a
    :class:`~docverse_server.storage.queue_backend.QueueBackend`; an
    unreachable backend costs it only the arq-verified half.

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

    Raises
    ------
    asyncio.CancelledError
        When arq cancels the job — its ``keeper_sync_job_timeout_seconds``
        timeout, or a worker shutdown. Before re-raising,
        :func:`~docverse_server.worker.functions._cancellation.record_cancellation`
        fails the row and drives the run ``failed`` through
        :func:`fail_run_for_lost_discovery`, as the ``except`` branch
        does for an error.
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
            # failed this row — and rolled the run up with it — or arq
            # may have re-delivered a job another worker is still
            # running. The discovery must not fan out a second time.
            if await queue_job_store.start_if_queued(queue_job_id) is None:
                return "skipped"
        # From here on the job holds an ``in_progress`` row. arq's
        # timeout and a worker shutdown both cancel the job with a
        # ``CancelledError`` that the ``except Exception`` below never
        # sees, so the helper fails the row and the run on that path —
        # otherwise the run 409-blocks the org until
        # ``keeper_sync_reaper`` notices (#699).
        async with record_cancellation(
            ctx,
            queue_job_id=queue_job_id,
            timeout_seconds=config.keeper_sync_job_timeout_seconds,
            logger=logger,
            finalise_run=discovery_run_finaliser(run_id),
        ):
            await _reconcile_run_children(
                session=session,
                queue_job_store=queue_job_store,
                queue_backend=factory.create_queue_backend(),
                run_id=run_id,
                logger=logger,
            )

            try:
                sync_config = await _load_config_snapshot(
                    session=session,
                    factory=factory,
                    org_slug=org_slug,
                )
                if not sync_config.enabled:
                    msg = (
                        f"Keeper sync is disabled for organization "
                        f"{org_slug!r}; aborting discovery"
                    )
                    raise RuntimeError(msg)

                ltd_slugs = await _fetch_ltd_product_slugs(
                    factory=factory, config=sync_config, logger=logger
                )
                in_scope, excluded_count = _resolve_scope(
                    ltd_slugs, sync_config
                )
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
                fan_out, counts = _subtract_tombstones(
                    ltd_slugs=ltd_slugs,
                    in_scope=in_scope,
                    excluded_count=excluded_count,
                    tombstoned_slugs=tombstoned_slugs,
                )
                logger.info(
                    "Resolved keeper-sync run scope", **counts.as_log_fields()
                )

                enqueued_count = await _enqueue_children(
                    ctx=ctx,
                    session=session,
                    queue_job_store=queue_job_store,
                    run_store=run_store,
                    org_id=org_id,
                    org_slug=org_slug,
                    run_id=run_id,
                    ltd_base_url=str(sync_config.ltd_base_url),
                    ltd_slugs=fan_out,
                    logger=logger,
                )

                async with session.begin():
                    await queue_job_store.update_phase(
                        queue_job_id,
                        "complete",
                        progress={
                            "message": "Discovery complete",
                            "in_scope_count": counts.in_scope_count,
                            "fan_out_count": counts.fan_out_count,
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
                    in_scope_count=counts.in_scope_count,
                    fan_out_count=counts.fan_out_count,
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


@cancellation_recorded
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
       by the per-edition callback. Then
       :func:`_enqueue_dashboard_for_restamps` enqueues the project's
       one ``dashboard_build`` when an edition visit moved only its
       dates onto LTD's clock and published nothing (PRD #706). The
       callback collects those projects, and this step enqueues after
       the loop so the render sees every restamped edition.
    4. On success, mark the queue job ``completed`` (or
       ``completed_with_errors`` when the service isolated any
       per-edition failure); on a caught exception, mark it ``failed``
       with structured error details and re-raise so arq records the job
       as failed. Both branches call :func:`maybe_finalise_run` so a
       terminal child cannot leave the parent run stuck in
       ``in_progress``.
    5. If arq cancels the job instead — its
       ``keeper_sync_job_timeout_seconds`` timeout, or a worker shutdown
       — the ``CancelledError`` bypasses that ``except Exception``, so
       :func:`~docverse_server.worker.functions._cancellation.record_cancellation`
       fails the row from a fresh session (``errors["reason"]`` tells
       ``job_timeout`` from ``worker_shutdown``), records the slice's
       live position (see :class:`_ProjectSyncProgress`), finalises the
       parent run when there is one, and re-raises. Without it the row
       sat ``in_progress`` and the run 409-blocked the org until
       ``keeper_sync_reaper`` noticed (#699).

    Each job is one *slice* of the project's sync (PRD #765). The service
    walks editions under a :class:`SliceBudget` of
    ``keeper_sync_slice_budget_seconds``, resuming after the payload's
    ``resume_after_ltd_edition_id`` cursor, and stops between editions
    once the budget runs out. A slice that stopped there hands the rest
    of the walk to a continuation job (see :func:`_continue_in_new_job`);
    one that stopped before visiting any edition does not continue (the
    no-progress guard), so a chain can never loop on an edition too big
    for a slice. Every slice records its position on its row's
    ``progress``: ``slice_index``, ``editions_total``,
    ``editions_visited``, ``editions_remaining``,
    ``last_visited_ltd_edition_id``, and ``continued``.

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
    # Set only on a continuation job, which an earlier slice of the same
    # project enqueued; the first slice of a chain starts from the top.
    resume_after_ltd_edition_id: int | None = payload.get(
        "resume_after_ltd_edition_id"
    )
    slice_index: int = payload.get("slice_index", 0)
    started = time.monotonic()
    logger = structlog.get_logger(
        "docverse_server.worker.keeper_sync_project"
    ).bind(
        org=org_slug,
        run_id=run_id,
        ltd_slug=ltd_slug,
        slice_index=slice_index,
    )

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        run_store = factory.create_keeper_sync_run_store()
        org_store = factory.create_org_store()

        async with session.begin():
            # Late-delivery guard (PRD #538): a reaper may have already
            # failed this row and, for a run child, rolled the parent run
            # up on its behalf — or arq may have re-delivered a job
            # another worker is still running.
            queue_job = await queue_job_store.start_if_queued(queue_job_id)
            if queue_job is None:
                return "skipped"
            org = await org_store.get_by_id(org_id)

        # From here on the job holds an ``in_progress`` row. arq's
        # timeout and a worker shutdown both cancel the job with a
        # ``CancelledError`` that the ``except Exception`` below never
        # sees, so the helper fails the row and rolls the run up on that
        # path instead of leaving both to the reaper (#699).
        progress = _ProjectSyncProgress(slice_index=slice_index)
        async with record_cancellation(
            ctx,
            queue_job_id=queue_job_id,
            timeout_seconds=config.keeper_sync_job_timeout_seconds,
            logger=logger,
            progress=progress.snapshot,
            finalise_run=keeper_sync_run_finaliser(run_id),
        ):
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
                    on_build_copied=_build_on_build_copied(
                        events=ctx.get("events"),
                        org_slug=org_slug,
                        ltd_slug=ltd_slug,
                    ),
                )

                restamp_only_project_ids: set[int] = set()
                on_edition_synced = _build_on_edition_synced(
                    factory=factory,
                    session=session,
                    queue_job_store=queue_job_store,
                    org_id=org_id,
                    run_id=run_id,
                    logger=logger,
                    restamp_only_project_ids=restamp_only_project_ids,
                )

                sync_result = await service.sync_project(
                    org_id=org_id,
                    ltd_slug=ltd_slug,
                    on_edition_synced=on_edition_synced,
                    budget=_start_slice_budget(),
                    resume_after_ltd_edition_id=resume_after_ltd_edition_id,
                    progress=progress.walk,
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
                await _enqueue_dashboard_for_restamps(
                    factory=factory,
                    session=session,
                    org_id=org_id,
                    project_ids=restamp_only_project_ids,
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

            if sync_result.stopped_at_budget and sync_result.editions_visited:
                return await _continue_in_new_job(
                    ctx=ctx,
                    session=session,
                    queue_job_store=queue_job_store,
                    run_store=run_store,
                    org_store=org_store,
                    queue_job=queue_job,
                    payload=payload,
                    run_id=run_id,
                    sync_result=sync_result,
                    progress=progress,
                    started=started,
                    logger=logger,
                )
            return await _end_chain(
                ctx=ctx,
                session=session,
                queue_job_store=queue_job_store,
                run_store=run_store,
                org_store=org_store,
                queue_job_id=queue_job_id,
                run_id=run_id,
                sync_result=sync_result,
                progress=progress,
                resume_after_ltd_edition_id=resume_after_ltd_edition_id,
                logger=logger,
            )

    msg = "No database session available"
    raise RuntimeError(msg)


def _start_slice_budget() -> SliceBudget:
    """Start this job's slice budget of ``keeper_sync_slice_budget_seconds``.

    A module-level seam so tests can substitute a budget read against a
    clock they move by hand.
    """
    return SliceBudget.starting_now(config.keeper_sync_slice_budget_seconds)


async def _end_chain(
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    run_store: KeeperSyncRunStore,
    org_store: OrganizationStore,
    queue_job_id: int,
    run_id: int | None,
    sync_result: ProjectSyncResult,
    progress: _ProjectSyncProgress,
    resume_after_ltd_edition_id: int | None,
    logger: structlog.stdlib.BoundLogger,
) -> str:
    """Close a slice that ends its project's chain of jobs.

    Either the walk reached the end of LTD's edition list, or the budget
    ran out before the slice visited a single edition. The second is the
    no-progress guard: re-enqueueing would only stop at the same place,
    so the chain ends with the row ``completed_with_errors``, its
    ``progress`` saying why (``"reason": "no_progress"``), and a
    warning. Either way the row records ``"continued": false``, and the
    parent run rolls up as for any finished child.
    """
    no_progress = sync_result.stopped_at_budget
    edition_failures = sync_result.edition_failures
    slice_record = progress.snapshot() | {"continued": False}
    if no_progress:
        slice_record["reason"] = "no_progress"
    completion = await _finalise_project_job(
        session=session,
        queue_job_store=queue_job_store,
        run_store=run_store,
        queue_job_id=queue_job_id,
        run_id=run_id,
        edition_failures=edition_failures,
        slice_record=slice_record,
        has_errors=bool(edition_failures) or no_progress,
    )
    await publish_run_completed(
        events=ctx.get("events"),
        session=session,
        org_store=org_store,
        completion=completion,
        logger=logger,
    )
    if no_progress:
        logger.warning(
            "Keeper-sync slice made no progress within its budget;"
            " not continuing",
            editions_total=progress.walk.editions_total,
            editions_remaining=progress.walk.editions_remaining,
            resume_after_ltd_edition_id=resume_after_ltd_edition_id,
            slice_budget_seconds=config.keeper_sync_slice_budget_seconds,
        )
        return "completed_with_errors"
    _log_project_completion(logger=logger, sync_result=sync_result)
    return "completed_with_errors" if edition_failures else "completed"


def _job_progress(
    slice_record: Mapping[str, Any],
    edition_failures: Sequence[EditionSyncFailure],
) -> dict[str, Any]:
    """Merge a slice's position and its edition failures into one record."""
    record = dict(slice_record)
    if edition_failures:
        record |= _edition_failure_progress(edition_failures)
    return record


async def _finalise_project_job(
    *,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    run_store: KeeperSyncRunStore,
    queue_job_id: int,
    run_id: int | None,
    edition_failures: Sequence[EditionSyncFailure],
    slice_record: Mapping[str, Any],
    has_errors: bool,
) -> KeeperSyncRunWithActivity | None:
    """Close out a ``keeper_sync_project`` job whose chain ends with it.

    Records the slice's position (``slice_record``) and any per-edition
    failures the service isolated on the job's ``progress`` and marks
    the job terminal in the *same* transaction that rolls the parent
    run, so the job record and its terminal status can never disagree.
    ``has_errors`` picks ``completed_with_errors`` over ``completed``.
    Returns whatever :func:`maybe_finalise_run` returned (always
    ``None`` for a tier-cron job, which carries no ``run_id``) for the
    caller to publish after the transaction commits.

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
        await queue_job_store.update_progress(
            queue_job_id, _job_progress(slice_record, edition_failures)
        )
        await queue_job_store.complete(queue_job_id, has_errors=has_errors)
        if run_id is None:
            return None
        return await maybe_finalise_run(run_store=run_store, run_id=run_id)


async def _continue_in_new_job(
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    run_store: KeeperSyncRunStore,
    org_store: OrganizationStore,
    queue_job: QueueJob,
    payload: Mapping[str, Any],
    run_id: int | None,
    sync_result: ProjectSyncResult,
    progress: _ProjectSyncProgress,
    started: float,
    logger: structlog.stdlib.BoundLogger,
) -> str:
    """Close a slice that ran out of budget and enqueue the next one.

    One transaction completes this job's row — ``completed_with_errors``
    when the slice isolated edition failures, as for any job — with the
    slice's position and the continuation's public id on its
    ``progress``, creates the continuation's ``queue_jobs`` row, and
    rolls up the parent run. Completing *before* creating is what lets
    the new row pass ``idx_queue_jobs_keeper_sync_project_active_uq``:
    this row held the project's active-job slot until then. Doing both
    in one transaction means the project never shows a free slot (a
    tier cron or ``POST …/refresh`` mid-chain still finds it taken) and
    a run never sees its pending count touch zero between slices, so
    :func:`maybe_finalise_run` finds the pending continuation and leaves
    the run ``in_progress``. The continuation carries this row's org,
    kind, and ``subject_label``, and the payload's ``run_id`` — so a
    run's chain keeps growing the run's ``total_count``, while a tier
    cron's or a refresh's chain stays unattributed.

    The arq enqueue then follows with no transaction open, and the
    backend job id is stamped after it: the same commit-then-enqueue
    recipe as :func:`_enqueue_children` and
    :func:`_enqueue_tier_project_sync`, which leaves a crash in between
    to the existing orphan sweeps. A *cancel* in that window is
    recorded by
    :func:`~docverse_server.worker.functions._cancellation.record_handoff_cancellation`,
    which fails the stranded continuation and rolls the run up, rather
    than holding the project's slot (and the run) until a sweep.

    The continuation's payload is this job's with its own queue job ids,
    ``resume_after_ltd_edition_id`` set to the last edition this slice
    visited, and ``slice_index`` advanced by one.
    """
    edition_failures = sync_result.edition_failures
    has_errors = bool(edition_failures)
    async with session.begin():
        await queue_job_store.complete(queue_job.id, has_errors=has_errors)
        continuation = await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=queue_job.org_id,
            keeper_sync_run_id=run_id,
            subject_label=queue_job.subject_label,
        )
        continuation_job_id = serialize_base32_id(continuation.public_id)
        slice_record = progress.snapshot() | {
            "continued": True,
            "continuation_job_id": continuation_job_id,
        }
        await queue_job_store.update_progress(
            queue_job.id, _job_progress(slice_record, edition_failures)
        )
        completion = (
            await maybe_finalise_run(run_store=run_store, run_id=run_id)
            if run_id is not None
            else None
        )
    # From the commit on, the continuation's row exists and arq has no
    # job for it yet: everything up to the backend id's stamp is the
    # hand-off window.
    async with record_handoff_cancellation(
        ctx,
        queue_job_ids=lambda: (continuation.id,),
        job_function="keeper_sync_project",
        started=started,
        timeout_seconds=config.keeper_sync_job_timeout_seconds,
        logger=logger,
        finalise_run=keeper_sync_run_finaliser(run_id),
    ):
        await publish_run_completed(
            events=ctx.get("events"),
            session=session,
            org_store=org_store,
            completion=completion,
            logger=logger,
        )
        metadata = await ctx["arq_queue"].enqueue(
            "keeper_sync_project",
            _queue_name=KEEPER_SYNC_QUEUE_NAME,
            payload={
                **payload,
                "queue_job_id": continuation.id,
                "queue_job_public_id": continuation_job_id,
                "resume_after_ltd_edition_id": (
                    sync_result.last_visited_ltd_edition_id
                ),
                "slice_index": progress.slice_index + 1,
            },
        )
        async with session.begin():
            await queue_job_store.set_backend_job_id(
                continuation.id, metadata.id, queue_name=metadata.queue_name
            )
    logger.info(
        "Keeper-sync slice budget reached; continuing in a new job",
        editions_visited=sync_result.editions_visited,
        editions_remaining=sync_result.editions_remaining,
        continuation_job_id=continuation_job_id,
        restamped_edition_count=sync_result.restamped_edition_count,
        edition_failure_count=len(edition_failures),
    )
    return "completed_with_errors" if has_errors else "completed"


def _log_project_completion(
    *,
    logger: structlog.stdlib.BoundLogger,
    sync_result: ProjectSyncResult,
) -> None:
    """Emit the project sync's terminal log line, partial or clean.

    Both lines carry ``restamped_edition_count``, the number of editions
    the sync moved onto LTD's clock (see
    :attr:`ProjectSyncResult.restamped_edition_count`).
    """
    edition_failures = sync_result.edition_failures
    restamped_edition_count = sync_result.restamped_edition_count
    if not edition_failures:
        logger.info(
            "Keeper-sync project completed",
            restamped_edition_count=restamped_edition_count,
        )
        return
    logger.warning(
        "Keeper-sync project completed with edition failures",
        restamped_edition_count=restamped_edition_count,
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


@dataclass
class _ProjectSyncProgress:
    """Live position of a running ``keeper_sync_project`` slice.

    ``walk`` is handed to :meth:`KeeperSyncService.sync_project`, which
    keeps it current as it walks LTD's edition list. :meth:`snapshot`
    is what the job records on its row's ``progress`` when the slice
    ends, and what :func:`record_cancellation` records if arq cancels
    the job part way, so even a cancelled row shows how far it got.
    """

    slice_index: int
    """Position of this job in its project's chain of slices, from 0."""

    walk: SliceProgress = field(default_factory=SliceProgress)
    """The service's live walk counters."""

    def snapshot(self) -> dict[str, Any]:
        """Return the slice's position as a ``progress`` payload.

        ``editions_total`` and ``editions_remaining`` are ``None`` until
        the service has fetched LTD's edition list.
        """
        return {
            "slice_index": self.slice_index,
            "editions_total": self.walk.editions_total,
            "editions_visited": self.walk.editions_visited,
            "editions_remaining": self.walk.editions_remaining,
            "last_visited_ltd_edition_id": (
                self.walk.last_visited_ltd_edition_id
            ),
        }


def _build_on_edition_synced(
    *,
    factory: Factory,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    org_id: int,
    run_id: int | None,
    logger: structlog.stdlib.BoundLogger,
    restamp_only_project_ids: set[int],
) -> Callable[[EditionSyncOutcome], Awaitable[None]]:
    """Build the ``on_edition_synced`` callback for ``sync_project``.

    Lifting the closure out of ``keeper_sync_project``'s
    ``async for session in db_session_dependency():`` body sidesteps
    ruff B023 (the worker function does not actually iterate the
    generator more than once, but the closure-over-loop-var rule
    fires anyway).

    Each outcome goes through the publish enqueues. An outcome that
    moved a clock onto LTD's (``dates_restamped``, PRD #706) *without*
    enqueuing any publish — neither its own nor a semver aggregate's —
    also adds its project to ``restamp_only_project_ids``, the set the
    caller hands to :func:`_enqueue_dashboard_for_restamps` once the
    edition loop is over. An outcome that did enqueue a publish is left
    out because every successful publish already cascades its own
    ``dashboard_build``.
    """

    async def callback(outcome: EditionSyncOutcome) -> None:
        published_edition = await _enqueue_publish_for_synced_edition(
            factory=factory,
            session=session,
            queue_job_store=queue_job_store,
            org_id=org_id,
            run_id=run_id,
            outcome=outcome,
            logger=logger,
        )
        published_aggregates = await _enqueue_publish_for_aggregates(
            factory=factory,
            session=session,
            queue_job_store=queue_job_store,
            org_id=org_id,
            run_id=run_id,
            outcome=outcome,
            logger=logger,
        )
        if (
            outcome.dates_restamped
            and not published_edition
            and not published_aggregates
        ):
            restamp_only_project_ids.add(outcome.docverse_project_id)

    return callback


async def _enqueue_dashboard_for_restamps(
    *,
    factory: Factory,
    session: AsyncSession,
    org_id: int,
    project_ids: set[int],
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Re-render the dashboards a job's restamp-only visits left stale.

    The dashboard is rendered at publish time, so a visit that only
    moved an edition's dates onto LTD's clock (PRD #706) would otherwise
    leave ``/v/`` showing the dates it was last rendered with — the
    whole point of the full-org backfill run. ``project_ids`` is what the
    ``on_edition_synced`` callback collected (see
    :func:`_build_on_edition_synced`); each project gets exactly one
    :func:`try_enqueue_dashboard_build_by_id` per ``keeper_sync_project``
    job, however many of its editions restamped.

    This runs after the edition loop rather than from the callback on
    the first restamp-only outcome. ``dashboard_build`` runs on the
    main worker's queue, not the sync queue, so a render enqueued
    mid-loop can finish while this job is still restamping the rest of
    the project's editions. With one enqueue per job, nothing would
    re-render afterwards, and the next visit restamps nothing. Once the
    loop has returned, every edition's clock is committed. The price is
    that a job whose ``sync_project`` fails as a whole never gets here.
    Editions restamped before the failure keep their stale render until
    the project's next publish, or until a re-run restamps an edition
    the failed job never reached.

    A failure never fails the job. :func:`try_enqueue_dashboard_build_by_id`
    already logs and swallows its own. Anything that still escapes is
    captured and logged here, the same way ``sync_project`` isolates a
    raising publish-enqueue callback. That keeps the job from being
    failed after its editions were synced.
    """
    for project_id in sorted(project_ids):
        logger.info(
            "Enqueueing dashboard_build for restamped edition dates",
            project_id=project_id,
        )
        try:
            await try_enqueue_dashboard_build_by_id(
                factory=factory,
                session=session,
                logger=logger,
                org_id=org_id,
                project_id=project_id,
            )
        except Exception as exc:
            sentry_sdk.capture_exception(exc)
            logger.exception(
                "Dashboard enqueue for restamped editions raised; continuing",
                project_id=project_id,
            )


def _build_on_build_copied(
    *,
    events: DocverseEvents | None,
    org_slug: str,
    ltd_slug: str,
) -> BuildCopiedCallback | None:
    """Build the hook that publishes each build copy as a metrics event.

    :meth:`KeeperSyncService.sync_build
    <docverse_server.services.keeper_sync.service.KeeperSyncService.sync_build>`
    awaits it once per build-content copy — after a success and after a
    failure, a build-level re-run included — and each call publishes one
    ``BuildContentCopiedEvent``, so a Sasquatch dashboard can plot
    transport health over a sync campaign (PRD #685) and how far behind
    LTD's rebuild each copy finished (``ltd_lag_seconds``, PRD #713). The
    report carries the Docverse project slug; the organization and the
    LTD product slug come from the job payload.

    Returns ``None`` when the worker has no metrics events (unit tests
    that do not ask for them), so the service skips the report entirely.
    """
    if events is None:
        return None

    async def callback(report: BuildCopyReport) -> None:
        await events.build_content_copied.publish(
            BuildContentCopiedEvent(
                organization=org_slug,
                project=report.project_slug,
                ltd_slug=ltd_slug,
                object_count=report.object_count,
                total_size_bytes=report.total_size_bytes,
                duration_seconds=report.duration_seconds,
                peak_concurrent_copies=report.peak_concurrent_copies,
                retried_object_count=report.retried_object_count,
                exhausted_object_count=report.exhausted_object_count,
                build_retry_used=report.build_retry_used,
                succeeded=report.succeeded,
                ltd_lag_seconds=report.ltd_lag_seconds,
            )
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
) -> bool:
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

    The payload carries the outcome's ``ltd_date_rebuilt`` as it stands,
    so the publish's ``edition_published`` event reports how long LTD's
    rebuild took to reach the CDN (``ltd_lag``). Whether this visit
    measured a lag at all is the service's call, not this callback's:
    see :attr:`EditionSyncOutcome.ltd_date_rebuilt`.

    Returns whether a publish was enqueued, which is what tells the
    callback that this edition's dashboard refresh is already on its
    way through the publish cascade.
    """
    build_outcome = outcome.build_outcome
    if build_outcome is None:
        return False
    if build_outcome.short_circuited:
        return False
    if (
        build_outcome.docverse_build_id is None
        or build_outcome.docverse_build_public_id is None
    ):
        return False
    edition_id = outcome.docverse_edition_id
    if edition_id is None:
        return False

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
        ltd_date_rebuilt=outcome.ltd_date_rebuilt,
    )
    logger.info(
        "Enqueued publish_edition for synced build",
        edition_slug=outcome.docverse_slug,
        build_id=build_outcome.docverse_build_id,
        phase="synced",
    )
    return True


async def _enqueue_publish_for_aggregates(
    *,
    factory: Factory,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    org_id: int,
    run_id: int | None,
    outcome: EditionSyncOutcome,
    logger: structlog.stdlib.BoundLogger,
) -> bool:
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

    Each payload carries the release edition's ``ltd_date_rebuilt`` as
    the outcome reports it: the aggregate moved because that release was
    rebuilt, so its ``edition_published`` event reports the release's
    ``ltd_lag``. The service leaves it ``None`` wherever the visit
    measured no lag — a first import, or a short-circuited build whose
    rebuild an earlier visit imported — so those payloads leave it out,
    as the self-heal publishes do. See
    :attr:`EditionSyncOutcome.ltd_date_rebuilt`.

    Returns whether any aggregate publish was enqueued; like the
    edition's own, each one cascades a ``dashboard_build``.
    """
    if not outcome.aggregate_outcomes:
        return False
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
            ltd_date_rebuilt=outcome.ltd_date_rebuilt,
        )
        logger.info(
            "Enqueued publish_edition for synced build",
            edition_slug=aggregate.docverse_slug,
            build_id=aggregate.docverse_build_id,
            phase="semver_aggregate",
        )
    return True


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
    """Fetch every product slug visible on the configured LTD instance.

    The single LTD-listing fetch for every *unattended* keeper-sync
    path: ``keeper_sync_run_discovery`` and, through
    :func:`_list_in_scope_slugs`, all three tier crons. The synchronous
    scope-preview endpoint shares the fetch itself —
    :meth:`LtdProductsClient.list_product_slugs`, which normalises
    transport failures, non-2xx statuses, and a 200 whose body is not a
    usable listing into one :class:`LtdProductsError` — but applies the
    opposite error policy at its own call site, because an org admin is
    watching that response (issue #675).

    The policy here is a structured breadcrumb, then re-raise: both
    callers already wrap the whole per-run / per-org pass in an
    ``except`` that captures to Sentry and records the failure, so the
    exception must keep propagating and must *not* be captured a second
    time here.
    """
    client = factory.create_ltd_products_client(
        base_url=str(config.ltd_base_url)
    )
    try:
        return await client.list_product_slugs()
    except LtdProductsError:
        logger.exception("Failed to fetch LTD product slugs")
        raise


@dataclass(frozen=True, slots=True)
class _ScopeCounts:
    """The counts every keeper-sync scope resolution reports.

    One definition of each name, shared by the "Resolved keeper-sync run
    scope" and "Resolved keeper-sync tier scope" events, so a tier tick
    and a run's discovery job stay directly comparable — and so both
    stay comparable with the scope preview, which is the whole point of
    reusing its field names (issue #680).

    The counts compose into one identity an operator can check by eye::

        in_scope_count - tombstoned_count == fan_out_count

    ``in_scope_count`` is therefore the *config* resolution, before
    tombstones are subtracted — exactly
    :attr:`~docverse.models.KeeperSyncScopePreview.in_scope_count` —
    and ``tombstoned_count`` counts only the tombstones falling inside
    it, exactly the preview's ``tombstoned_slugs``. Reporting the whole
    org's tombstone population here instead would make the shortfall
    between a preview and the run it launches unexplainable: the
    subtrahend would be a number with no relationship to the scope.
    """

    ltd_count: int
    """Distinct product slugs the LTD instance listed."""

    in_scope_count: int
    """Slugs the config admits, *before* tombstones are subtracted."""

    excluded_count: int
    """Slugs an include rule admitted and an exclude rule removed."""

    tombstoned_count: int
    """In-scope slugs skipped because their state row is tombstoned."""

    fan_out_count: int
    """Slugs left to work on: ``in_scope_count - tombstoned_count``."""

    def as_log_fields(self) -> dict[str, int]:
        """Render the counts as structured-log keyword arguments."""
        return asdict(self)


def _subtract_tombstones(
    *,
    ltd_slugs: list[str],
    in_scope: list[str],
    excluded_count: int,
    tombstoned_slugs: set[str],
) -> tuple[list[str], _ScopeCounts]:
    """Split a config-resolved scope into its fan-out and its counts.

    The second half of every scope resolution, shared by
    ``keeper_sync_run_discovery`` and :func:`_list_in_scope_slugs`:
    drop the tombstoned slugs so a ``keeper_sync_project`` child is
    never enqueued for a Docverse-side-vetoed project (issue #396 /
    PRD #332 user story 17), and report what that cost under the names
    :class:`_ScopeCounts` defines.

    ``tombstoned_count`` is derived from the two list lengths rather
    than from ``tombstoned_slugs`` itself, so it is the size of the
    intersection with the scope — the org's other tombstones, which the
    caller's whole-table read also returns, are not this scope's
    shortfall to explain.

    Parameters
    ----------
    ltd_slugs
        Every product slug the LTD instance listed, for ``ltd_count``.
    in_scope
        The config-resolved scope, in LTD listing order.
    excluded_count
        What :func:`_resolve_scope` reported alongside ``in_scope``.
    tombstoned_slugs
        Every tombstoned project slug on the org — or an empty set when
        the caller skipped that read because the scope was already
        empty, which reaches the same answer for free.

    Returns
    -------
    tuple
        The slugs to fan out, in LTD listing order, and the counts to
        log.
    """
    fan_out = (
        [s for s in in_scope if s not in tombstoned_slugs]
        if tombstoned_slugs
        else in_scope
    )
    return fan_out, _ScopeCounts(
        ltd_count=len(ltd_slugs),
        in_scope_count=len(in_scope),
        excluded_count=excluded_count,
        tombstoned_count=len(in_scope) - len(fan_out),
        fan_out_count=len(fan_out),
    )


def _resolve_scope(
    ltd_slugs: list[str], config: KeeperSyncConfig
) -> tuple[list[str], int]:
    """Resolve an org's keeper-sync scope over an LTD product listing.

    The scope rule itself lives on the config model
    (:meth:`~docverse.models.KeeperSyncConfig.resolve_scope`, PRD
    #667): the listed slugs plus the include-pattern matches — or every
    LTD slug under the ``"*"`` wildcard — minus the excludes, which
    always win. Ordering follows the LTD listing so successive passes
    against the same LTD instance fan out deterministically.

    One classifying pass produces both values. Deriving
    ``excluded_count`` from a second, excludes-stripped pass would walk
    and re-match the whole listing again — ~1,645 slugs on lsst.io,
    every five minutes on the ``main`` tier — to populate one log field.

    Parameters
    ----------
    ltd_slugs
        Every product slug visible on the LTD instance, in LTD listing
        order.
    config
        The org's keeper-sync config snapshot.

    Returns
    -------
    tuple
        The in-scope slugs, and the number of slugs an include rule
        admitted but an exclude rule then removed — the
        ``excluded_count`` the scope log events report.
    """
    return config.resolve_scope(ltd_slugs)


async def _fetch_tombstoned_project_slugs(
    *,
    state_store: KeeperSyncStateStore,
    session: AsyncSession,
    org_id: int,
) -> set[str]:
    """Return the LTD slugs of all tombstoned project state rows.

    The four discovery paths call this once per pass and subtract the
    result from their in-scope slug list, so a ``keeper_sync_project``
    child is never enqueued for a Docverse-side-vetoed project:
    ``keeper_sync_run_discovery`` directly, and the three tier crons
    through :func:`_list_in_scope_slugs`. Without the filter,
    ``sync_project`` would short-circuit on its own tombstone check
    (PRD #332 §"Sync-side skip checks") a few milliseconds later —
    same outcome, wasted queue + DB work. Issue #396 / user story 17.
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
    attempt sweeps these rows via ``_reconcile_run_children`` once
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
            await queue_job_store.set_backend_job_id(
                queue_job.id, metadata.id, queue_name=metadata.queue_name
            )
        enqueued += 1
        logger.debug(
            "Enqueued keeper_sync_project",
            ltd_slug=ltd_slug,
            queue_job_id=queue_job.id,
        )
    return enqueued


async def _reconcile_run_children(
    *,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    queue_backend: QueueBackend,
    run_id: int,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Fail this run's dead child rows before the fan-out re-runs.

    A re-delivered (or operator-replayed) discovery re-enters the
    fan-out, but every child its earlier attempt left stranded in
    ``queued`` still holds
    ``idx_queue_jobs_keeper_sync_project_subject_active_uq``, so
    ``_enqueue_children``'s ``has_active_for_subject`` pre-check skips
    that slug and the row keeps counting toward the run's
    ``pending_count``. Both of the two ways a child strands are swept
    here, mirroring the cron reaper's populations one run at a time:

    * **Orphans.** ``_enqueue_children`` commits each child row before
      calling ``arq_queue.enqueue``, so a crash in that window leaves
      ``status='queued'``, ``backend_job_id IS NULL``, and no arq job.
      Aged out against :data:`_ORPHAN_IDLE_WINDOW`, which is short
      because "never reached arq" is decidable from the row alone.
    * **Abandoned children.** The row *did* reach arq and arq then lost
      the job (PRD #538). Age cannot distinguish that from a job merely
      backed up behind a saturated pool, so each candidate is verified
      against the queue backend and failed only when the backend has no
      record of it — and the threshold is the reaper's
      ``keeper_sync_reaper_threshold_seconds`` rather than the orphan
      window, so both sweeps of this population judge a row by the same
      clock.

    Runs in three steps rather than one transaction, for the reason
    :func:`keeper_sync_reaper` splits its tick (task #548): the backend
    round trips happen with nothing open, so a stalled Redis can neither
    roll back the orphan reaps nor hold row locks while the discovery
    burns down its arq timeout. A backend that is unreachable outright
    soft-aborts inside
    :meth:`~QueueJobStore.verify_abandoned_candidates` — it warns,
    reaps nothing, and leaves the discovery to fan out whatever slugs
    are not wedged; the cron reaper retries the population later.
    """
    abandoned_after = timedelta(
        seconds=config.keeper_sync_reaper_threshold_seconds
    )
    async with session.begin():
        orphans = await queue_job_store.fail_orphaned_run_children(
            run_id=run_id, idle_after=_ORPHAN_IDLE_WINDOW
        )
        candidates = await queue_job_store.select_abandoned_run_children(
            run_id=run_id, idle_after=abandoned_after
        )
    reaps = await queue_job_store.verify_abandoned_candidates(
        candidates, queue_backend=queue_backend
    )
    async with session.begin():
        abandoned = await queue_job_store.apply_abandoned_reaps(reaps)
    if orphans or abandoned:
        logger.warning(
            "Reconciled dead keeper-sync child queue jobs",
            orphan_count=len(orphans),
            orphan_ids=[job.id for job in orphans],
            abandoned_count=len(abandoned),
            abandoned_ids=[job.id for job in abandoned],
        )


@cancellation_recorded
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


@cancellation_recorded
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


@cancellation_recorded
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

    The tick's start is read off :func:`time.monotonic` before anything
    else and handed to every processor as ``started``: a processor that
    is cancelled while handing a child row to arq records the cancel
    against the time the cron job has run for (see
    :func:`_enqueue_tier_project_sync`).

    A pass holds no ``queue_jobs`` row of its own, so when arq cancels
    it — at the tier's
    :func:`~docverse_server.services.keeper_sync.scheduler.tier_cron_timeout`,
    or on a worker shutdown — the ``CancelledError`` (which the per-org
    ``except Exception`` never sees) is caught here only to log how far
    the pass got, from the :class:`_TierPassProgress` every processor
    advances, and is then re-raised so arq records the job as failed. A
    failure while logging is itself logged with its traceback and never
    replaces the cancel; if that fallback line fails too (a log pipeline
    that fails on every event), its failure is dropped.
    """
    started = time.monotonic()
    progress = _TierPassProgress()
    try:
        return await _run_tier_pass(
            ctx=ctx,
            logger=logger,
            processor=processor,
            tier_name=tier_name,
            started=started,
            progress=progress,
        )
    except asyncio.CancelledError:
        try:
            _log_tier_cancellation(
                logger=logger,
                tier_name=tier_name,
                started=started,
                progress=progress,
            )
        except Exception:
            # Never let the log line's own failure (a structlog processor,
            # a Sentry hook) replace the cancel: arq must still see the
            # ``CancelledError``, as ``record_cancellation`` guarantees
            # for a job that holds a row. The fallback runs through the
            # same processors, so a pipeline that fails on every event
            # fails here too; that failure is dropped so the ``raise``
            # below is always reached.
            with contextlib.suppress(Exception):
                logger.exception(
                    "Failed to log the tier pass's cancellation",
                    tier=tier_name,
                )
        raise


async def _run_tier_pass(
    *,
    ctx: dict[str, Any],
    logger: structlog.stdlib.BoundLogger,
    processor: TierOrgProcessor,
    tier_name: str,
    started: float,
    progress: _TierPassProgress,
) -> str:
    """Run one pass of a tier over every enabled org.

    The body of :func:`_run_tier`, which wraps it to log a cancellation.
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
        progress.orgs_total = len(candidates)
        for org in candidates:
            progress.begin_org(org.slug)
            try:
                enqueued_total += await processor(
                    ctx=ctx,
                    session=session,
                    factory=factory,
                    org=org,
                    logger=logger,
                    started=started,
                    progress=progress,
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                logger.exception(
                    "Keeper-sync tier processor failed for org",
                    tier=tier_name,
                    org=org.slug,
                )
            progress.end_org()
        logger.info(
            "Keeper-sync tier pass complete",
            tier=tier_name,
            candidates=len(candidates),
            enqueued=enqueued_total,
        )
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


def _log_tier_cancellation(
    *,
    logger: structlog.stdlib.BoundLogger,
    tier_name: str,
    started: float,
    progress: _TierPassProgress,
) -> None:
    """Log one warning saying how far a cancelled tier pass got.

    The same reading as
    :func:`~docverse_server.worker.functions._cancellation.record_cancellation`
    gives a job's row: the ``reason`` is inferred from how long the pass
    ran against the tier's arq timeout. ``org``, ``projects_visited``,
    ``projects_total`` and ``ltd_slug`` describe the org in flight when
    the cancel landed: ``projects_visited`` counts the slugs of its
    scope the pass had finished, and ``ltd_slug`` is the one it was on.
    """
    elapsed = time.monotonic() - started
    timeout = tier_cron_timeout(Tier(tier_name))
    logger.warning(
        "Keeper-sync tier pass cancelled",
        tier=tier_name,
        reason=infer_cancellation_reason(
            timedelta(seconds=elapsed), timeout=timeout
        ),
        elapsed_seconds=round(elapsed, 1),
        timeout_seconds=timeout.total_seconds(),
        orgs_completed=progress.orgs_completed,
        orgs_total=progress.orgs_total,
        org=progress.org,
        projects_visited=progress.projects_visited,
        projects_total=progress.projects_total,
        ltd_slug=progress.ltd_slug,
    )


@dataclass(slots=True)
class _TierPassProgress:
    """How far one tier pass has got through its orgs and their scopes.

    :func:`_run_tier` advances the org counters around each processor
    call, and each processor walks its scope through :meth:`walk`, so a
    cancelled pass can log where arq cut it off.
    """

    orgs_total: int | None = None
    """Enabled orgs the pass will visit; ``None`` until they are listed."""

    orgs_completed: int = 0
    """Orgs the pass has finished, whether or not their processor failed."""

    org: str | None = None
    """Slug of the org in flight, or ``None`` between orgs."""

    projects_total: int | None = None
    """Size of the in-flight org's scope; ``None`` until it is resolved."""

    projects_visited: int = 0
    """Slugs of the in-flight org's scope the pass has finished."""

    ltd_slug: str | None = None
    """The in-scope slug the pass is working on, if any."""

    def begin_org(self, org_slug: str) -> None:
        """Start counting a new org's scope."""
        self.org = org_slug
        self.projects_total = None
        self.projects_visited = 0
        self.ltd_slug = None

    def end_org(self) -> None:
        """Count the in-flight org as finished."""
        self.orgs_completed += 1
        self.org = None
        self.projects_total = None
        self.projects_visited = 0
        self.ltd_slug = None

    def walk(self, slugs: Sequence[str]) -> Iterator[str]:
        """Yield each in-scope slug, counting those finished before it."""
        self.projects_total = len(slugs)
        for index, slug in enumerate(slugs):
            self.projects_visited = index
            self.ltd_slug = slug
            yield slug
        self.projects_visited = len(slugs)
        self.ltd_slug = None


class TierOrgProcessor(Protocol):
    """Per-org tier processor callable shared by ``_run_tier``.

    Each tier cron (``main`` / ``discovery`` / ``other``) supplies a
    function matching this signature; it returns the number of
    ``keeper_sync_project`` children it enqueued for the org.
    ``started`` is the :func:`time.monotonic` reading the tick began at,
    passed through to :func:`_enqueue_tier_project_sync`. ``progress``
    is the pass's position, whose :meth:`_TierPassProgress.walk` the
    processor iterates its scope through.
    """

    async def __call__(
        self,
        *,
        ctx: dict[str, Any],
        session: AsyncSession,
        factory: Factory,
        org: Organization,
        logger: structlog.stdlib.BoundLogger,
        started: float,
        progress: _TierPassProgress,
    ) -> int: ...


async def _tier_main_for_org(
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    factory: Factory,
    org: Organization,
    logger: structlog.stdlib.BoundLogger,
    started: float,
    progress: _TierPassProgress,
) -> int:
    """Run one tier_main pass for a single enabled org.

    Uses :func:`should_poll_main_for_project` to skip dormant projects
    (those whose LTD ``main`` hasn't rebuilt within the hot window) on
    most ticks, capping their LTD load at one fetch per
    ``TIER_MAIN_DORMANT_INTERVAL`` instead of one per 5-minute cron
    tick. Hot projects continue to poll on the 5-min SLO.

    While ``keeper_sync_push_hot_path_enabled`` is on, a ref pushed
    inside the window makes the project hot, so its ``main`` check runs
    on every tick until the window closes, and a push to a dormant
    project's ``main`` is caught there, with the rebuild recorded on the
    project row. The project's stamps are then visited after its
    ``main`` check, whatever that check's gate decided
    (:func:`_visit_pushed_refs`): a push says LTD is about to change, so
    each live ref is checked on every tick until its sync is enqueued or
    its window passes, and an expired ref is pruned even once the
    project is dormant again. The ``main`` check and the pushed refs
    share one enqueue per project, and the stamps are settled afterwards
    (:func:`_settle_pushed_refs`). With the hot path off, the stamps are
    neither read nor written, and count for nothing in the gate.
    """
    config_snapshot = org.keeper_sync_config
    if config_snapshot is None:
        return 0
    in_scope = await _list_in_scope_slugs(
        factory=factory,
        session=session,
        org=org,
        config=config_snapshot,
        logger=logger,
    )
    if not in_scope:
        return 0
    ltd_client = factory.create_ltd_client(
        base_url=str(config_snapshot.ltd_base_url)
    )
    state_store = factory.create_keeper_sync_state_store()
    queue_job_store = factory.create_queue_job_store()
    edition_store = factory.create_edition_store()
    arq_queue = ctx["arq_queue"]
    # ``None`` while the push hot path is off: the pass then neither
    # reads nor settles the stamps, and makes exactly the LTD calls it
    # made before the hot path existed.
    push_window = config.keeper_sync_push_window
    now = datetime.now(tz=UTC)
    enqueued = 0
    for ltd_slug in progress.walk(in_scope):
        async with session.begin():
            project_state = await state_store.get(
                org_id=org.id,
                resource_type=ResourceType.project,
                ltd_slug=ltd_slug,
            )
        main_check = _MainEditionCheck()
        if should_poll_main_for_project(
            state=project_state, now=now, push_window=push_window
        ):
            main_check = await _check_main_edition(
                session=session,
                state_store=state_store,
                ltd_client=ltd_client,
                org=org,
                ltd_slug=ltd_slug,
                now=now,
                logger=logger,
            )
        push_checks: list[_PushedRefCheck] = []
        if push_window is not None and not main_check.ltd_failed:
            push_checks = await _visit_pushed_refs(
                checker=_PushedRefChecker(
                    session=session,
                    state_store=state_store,
                    edition_store=edition_store,
                    ltd_client=ltd_client,
                    org=org,
                    ltd_slug=ltd_slug,
                    project_id=(
                        project_state.docverse_id
                        if project_state is not None
                        else None
                    ),
                    fetched=main_check.fetched,
                    logger=logger,
                ),
                project_state=project_state,
                now=now,
                window=push_window,
            )
        wants_enqueue = main_check.wants_enqueue or any(
            check.outcome.enqueues for check in push_checks
        )
        enqueued_at: datetime | None = None
        if wants_enqueue and await _enqueue_tier_project_sync(
            ctx=ctx,
            started=started,
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
            enqueued_at = datetime.now(tz=UTC)
        if push_window is not None and push_checks:
            await _settle_pushed_refs(
                session=session,
                state_store=state_store,
                org=org,
                ltd_slug=ltd_slug,
                checks=push_checks,
                enqueued_at=enqueued_at,
                now=now,
                window=push_window,
                logger=logger,
            )
    return enqueued


@dataclass(frozen=True, slots=True)
class _MainEditionCheck:
    """What ``tier_main``'s check of one project's ``main`` edition found.

    The default is the check that did not run: the dormancy gate
    skipped the project this tick.
    """

    fetched: Mapping[int, LtdEdition] = field(default_factory=dict)
    """The LTD ``main`` edition the check fetched, keyed by LTD id, so
    the pushed-ref check of the ref ``main`` tracks reuses it rather
    than fetching it again."""

    wants_enqueue: bool = False
    """Whether LTD rebuilt ``main`` since keeper-sync last synced it."""

    ltd_failed: bool = False
    """Whether LTD failed to answer; the pushed refs then wait a tick."""


async def _check_main_edition(
    *,
    session: AsyncSession,
    state_store: KeeperSyncStateStore,
    ltd_client: LtdClient,
    org: Organization,
    ltd_slug: str,
    now: datetime,
    logger: structlog.stdlib.BoundLogger,
) -> _MainEditionCheck:
    """Check one polled project's LTD ``main`` edition for a rebuild.

    Fetches the edition (:func:`_find_main_edition`), records the poll
    (:func:`_record_main_polled`), and reports whether the rebuild
    calls for the project's sync (:func:`_tier_main_should_enqueue_edition`).
    An LTD failure is logged, sent to Sentry, and still recorded as a
    poll, so a flaky LTD endpoint cannot defeat the dormancy gate by
    re-polling a dormant project every five minutes.
    """
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
        await _record_main_polled(
            session=session,
            state_store=state_store,
            org_id=org.id,
            ltd_slug=ltd_slug,
            now=now,
            main_edition=None,
        )
        return _MainEditionCheck(ltd_failed=True)
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
        return _MainEditionCheck()
    return _MainEditionCheck(
        fetched={main_edition.ltd_id: main_edition},
        wants_enqueue=await _tier_main_should_enqueue_edition(
            state_store=state_store,
            session=session,
            org_id=org.id,
            main_edition=main_edition,
        ),
    )


@dataclass(frozen=True, slots=True)
class _PushedRefCheck:
    """``tier_main``'s visit to one stamped ref of a project."""

    ref: str
    """The pushed ref, normalized (``main``, not ``refs/heads/main``)."""

    pushed_at: datetime
    """The ref's push time as the visit read it from the stamp."""

    outcome: PushCheckOutcome
    """What the visit found."""


@dataclass(slots=True)
class _PushedRefChecker:
    """Checks a project's pushed refs against LTD for one ``tier_main`` tick.

    Each ref costs at most two LTD calls: the edition the ref's Docverse
    edition maps to, and, when the ref has no such edition or LTD no
    longer has it, the project's edition listing. The listing's answer
    is shared by every ref of the project, and an edition the ``main``
    check already fetched this tick is not fetched again.
    """

    session: AsyncSession
    state_store: KeeperSyncStateStore
    edition_store: EditionStore
    ltd_client: LtdClient
    org: Organization
    ltd_slug: str
    project_id: int | None
    """The Docverse project the slug syncs into, from its state row."""

    fetched: Mapping[int, LtdEdition]
    """LTD editions the ``main`` check already fetched this tick."""

    logger: structlog.stdlib.BoundLogger
    _editions: dict[int, LtdEdition] = field(default_factory=dict, init=False)
    _lists_unseen: bool | None = field(default=None, init=False)

    async def check(self, ref: str) -> PushCheckOutcome:
        """Return what LTD says about the edition a pushed ref feeds.

        ``rebuilt`` or ``unchanged`` when a synced edition tracks the
        ref and LTD still has it (:func:`ltd_rebuilt_since_sync`);
        otherwise the discovery listing check decides between
        ``new_edition`` and ``not_found``. An LTD failure is logged,
        sent to Sentry, and reported as ``error``.
        """
        state = await self._tracking_state(ref)
        try:
            if state is not None and state.ltd_id is not None:
                ltd_edition = await self._fetch(state.ltd_id)
                if ltd_edition is not None:
                    if ltd_rebuilt_since_sync(
                        state, ltd_date_rebuilt=ltd_edition.date_rebuilt
                    ):
                        return PushCheckOutcome.rebuilt
                    return PushCheckOutcome.unchanged
            if await self._lists_unseen_edition():
                return PushCheckOutcome.new_edition
        except LtdClientError as exc:
            sentry_sdk.capture_exception(exc)
            self.logger.exception(
                "Tier-main: failed to check pushed ref",
                org=self.org.slug,
                project=self.ltd_slug,
                github_ref=ref,
            )
            return PushCheckOutcome.error
        return PushCheckOutcome.not_found

    async def _tracking_state(self, ref: str) -> KeeperSyncState | None:
        """Return the state row of the synced edition tracking ``ref``.

        Every live ``git_ref``-mode Docverse edition of the project
        tracking the ref is mapped to its untombstoned edition state
        rows; when several LTD editions feed the ref, the newest (the
        highest LTD id) is the one checked.
        """
        if self.project_id is None:
            return None
        async with self.session.begin():
            editions = await self.edition_store.list_git_ref_tracking_editions(
                project_id=self.project_id, git_ref=ref
            )
            states = await self.state_store.list_for_org(
                org_id=self.org.id,
                resource_type=ResourceType.edition,
                docverse_ids=[edition.id for edition in editions],
            )
        synced = [state for state in states if state.ltd_id is not None]
        return max(synced, key=lambda state: state.ltd_id or 0, default=None)

    async def _fetch(self, ltd_id: int) -> LtdEdition | None:
        """Return LTD's edition ``ltd_id``, or ``None`` if LTD lost it."""
        edition = self.fetched.get(ltd_id) or self._editions.get(ltd_id)
        if edition is None:
            try:
                edition = await self.ltd_client.get_edition(ltd_id)
            except LtdNotFoundError:
                return None
            self._editions[ltd_id] = edition
        return edition

    async def _lists_unseen_edition(self) -> bool:
        """Return whether LTD lists an edition keeper-sync has not seen.

        The discovery tier's check (:func:`_project_needs_discovery`) on
        one project: its edition listing's LTD ids against the org's
        edition state rows, tombstoned ones counting as seen. The state
        rows are read for the listed ids only, as ``tier_main`` does not
        load the org's whole edition-state map. Answered once per
        project per tick.
        """
        if self._lists_unseen is None:
            ltd_edition_ids = await _list_edition_ltd_ids(
                ltd_client=self.ltd_client,
                org_slug=self.org.slug,
                ltd_slug=self.ltd_slug,
                tier=Tier.main,
                logger=self.logger,
            )
            async with self.session.begin():
                states = await self.state_store.list_for_org(
                    org_id=self.org.id,
                    resource_type=ResourceType.edition,
                    ltd_ids=ltd_edition_ids,
                    include_tombstoned=True,
                )
            self._lists_unseen = _has_unseen_edition(
                ltd_edition_ids=ltd_edition_ids,
                edition_state_by_ltd_id={
                    state.ltd_id: state
                    for state in states
                    if state.ltd_id is not None
                },
            )
        return self._lists_unseen


async def _visit_pushed_refs(
    *,
    checker: _PushedRefChecker,
    project_state: KeeperSyncState | None,
    now: datetime,
    window: timedelta,
) -> list[_PushedRefCheck]:
    """Visit every ref stamped on a project and say what each found.

    A ref whose window has passed is ``expired`` and costs no LTD call.
    The live refs are checked oldest push first
    (:meth:`_PushedRefChecker.check`); after an LTD failure the rest of
    the project's live refs wait for the next tick unvisited, since the
    failure has already ridden out the LTD client's retries and the
    next call would most likely do the same.
    """
    stamped = read_pushed_refs(project_state)
    live = prune_pushed_refs(stamped, now=now, window=window)
    checks: list[_PushedRefCheck] = []
    for ref, pushed_at in sorted(
        stamped.items(), key=lambda item: (item[0] in live, item[1], item[0])
    ):
        if ref in live:
            outcome = await checker.check(ref)
        else:
            outcome = PushCheckOutcome.expired
        checks.append(
            _PushedRefCheck(ref=ref, pushed_at=pushed_at, outcome=outcome)
        )
        if outcome is PushCheckOutcome.error:
            break
    return checks


async def _settle_pushed_refs(
    *,
    session: AsyncSession,
    state_store: KeeperSyncStateStore,
    org: Organization,
    ltd_slug: str,
    checks: Sequence[_PushedRefCheck],
    enqueued_at: datetime | None,
    now: datetime,
    window: timedelta,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Write a project's pushed-ref visit back to its state row and log it.

    Once the project's sync is enqueued (at ``enqueued_at``), every ref
    whose outcome called for it is cleared, and its push-to-enqueue lag
    logged; when the enqueue was skipped (``enqueued_at`` is ``None``:
    an active job for the project already holds its slot) those refs
    keep their stamps and are checked again next tick. Expired refs are
    pruned. The row is re-read ``FOR UPDATE`` and settled by
    :func:`settle_pushed_refs`, so a push stamped since the visit read
    the row keeps its stamp, and the write is skipped when nothing is
    cleared or pruned.
    """
    cleared = {
        check.ref: check.pushed_at
        for check in checks
        if enqueued_at is not None and check.outcome.enqueues
    }
    if cleared or any(
        check.outcome is PushCheckOutcome.expired for check in checks
    ):
        async with session.begin():
            state = await state_store.get(
                org_id=org.id,
                resource_type=ResourceType.project,
                ltd_slug=ltd_slug,
                for_update=True,
            )
            if state is not None:
                await state_store.upsert(
                    org_id=org.id,
                    resource_type=ResourceType.project,
                    ltd_slug=ltd_slug,
                    annotations=settle_pushed_refs(
                        state, cleared=cleared, now=now, window=window
                    ),
                )
    for check in checks:
        logger.info(
            "Tier-main: checked pushed ref",
            org=org.slug,
            project=ltd_slug,
            github_ref=check.ref,
            outcome=check.outcome.value,
            pushed_at=check.pushed_at.isoformat(),
            enqueued=check.ref in cleared,
        )
        if enqueued_at is not None and check.ref in cleared:
            logger.info(
                "Tier-main: enqueued project sync for pushed ref",
                org=org.slug,
                project=ltd_slug,
                github_ref=check.ref,
                outcome=check.outcome.value,
                push_lag_seconds=round(
                    (enqueued_at - check.pushed_at).total_seconds(), 1
                ),
            )


async def _tier_discovery_for_org(
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    factory: Factory,
    org: Organization,
    logger: structlog.stdlib.BoundLogger,
    started: float,
    progress: _TierPassProgress,
) -> int:
    """Run one tier_discovery pass for a single enabled org.

    Uses :func:`should_poll_for_tier` (with ``tier=Tier.discovery``) to
    skip dormant projects so the long tail does not pin the cron to
    ~1500 ``GET /products/<slug>/editions/`` calls every 30 min. Hot
    projects (LTD ``main`` rebuilt within ``TIER_DISCOVERY_HOT_WINDOW``)
    keep the 30-min cadence; dormant projects fall back to one pass per
    ``TIER_DISCOVERY_DORMANT_INTERVAL``.

    While ``keeper_sync_push_hot_path_enabled`` is on, a project with a
    ref pushed inside the window is hot too, so a contributor's push to
    a dormant project has discovery list its editions on every tick
    until the window closes.

    A polled project with a state row costs one LTD call, its edition
    URL listing; :func:`_project_needs_discovery` reads the edition ids
    off the URLs and fetches no edition payload. One without a state
    row costs none.
    """
    config_snapshot = org.keeper_sync_config
    if config_snapshot is None:
        return 0
    in_scope = await _list_in_scope_slugs(
        factory=factory,
        session=session,
        org=org,
        config=config_snapshot,
        logger=logger,
    )
    if not in_scope:
        return 0
    ltd_client = factory.create_ltd_client(
        base_url=str(config_snapshot.ltd_base_url)
    )
    state_store = factory.create_keeper_sync_state_store()
    queue_job_store = factory.create_queue_job_store()
    arq_queue = ctx["arq_queue"]
    push_window = config.keeper_sync_push_window
    now = datetime.now(tz=UTC)
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
    for ltd_slug in progress.walk(in_scope):
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
            push_window=push_window,
        ):
            continue
        try:
            should_enqueue = await _project_needs_discovery(
                ltd_client=ltd_client,
                org_slug=org.slug,
                ltd_slug=ltd_slug,
                project_state=project_state,
                edition_state_by_ltd_id=edition_state_by_ltd_id,
                logger=logger,
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
            ctx=ctx,
            started=started,
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
    started: float,
    progress: _TierPassProgress,
) -> int:
    """Run one tier_other pass for a single enabled org.

    Uses :func:`should_poll_for_tier` (with ``tier=Tier.other``) to
    skip dormant projects before the per-project
    ``GET /products/<slug>/editions/`` listing, so a project whose
    branches haven't been touched in months stops driving an hourly
    LTD fetch. Hot and dormant-due projects continue to list their
    edition URLs and re-enqueue when state lags past
    :data:`TIER_OTHER_REFRESH_THRESHOLD`. While
    ``keeper_sync_push_hot_path_enabled`` is on, a project with a ref
    pushed inside the window is hot too.

    The listing is the only LTD call per polled project: the check
    needs each edition's LTD id, which the URL carries, and which id is
    ``main``, which the edition's ``keeper_sync_state`` row records as
    its LTD slug, so no edition payload is fetched. A listed URL with no
    id to read is skipped with a warning (:func:`_list_edition_ltd_ids`);
    the project is still checked on the rest and still stamped polled.

    The org's edition state rows are read once per tick, before the
    per-project loop, and each polled project is checked against that
    map in memory: one SELECT per org per tick rather than one per
    polled project, most of which (single-edition technotes) list only
    ``main`` and have nothing for the check to look at.
    """
    config_snapshot = org.keeper_sync_config
    if config_snapshot is None:
        return 0
    in_scope = await _list_in_scope_slugs(
        factory=factory,
        session=session,
        org=org,
        config=config_snapshot,
        logger=logger,
    )
    if not in_scope:
        return 0
    ltd_client = factory.create_ltd_client(
        base_url=str(config_snapshot.ltd_base_url)
    )
    state_store = factory.create_keeper_sync_state_store()
    queue_job_store = factory.create_queue_job_store()
    arq_queue = ctx["arq_queue"]
    push_window = config.keeper_sync_push_window
    now = datetime.now(tz=UTC)
    # Hoist the org-wide edition-state read out of the per-slug loop,
    # as ``_tier_discovery_for_org`` does. ``_list_in_scope_slugs``
    # drops tombstoned *project* slugs; for the editions themselves the
    # default ``include_tombstoned=False`` leaves tombstoned rows out
    # of the map, so a tombstoned edition LTD still lists reads like
    # one with no state row and is never treated as stale (issue #396
    # / PRD #332 user story 17).
    async with session.begin():
        edition_states = await state_store.list_for_org(
            org_id=org.id,
            resource_type=ResourceType.edition,
        )
    edition_state_by_ltd_id = {
        s.ltd_id: s for s in edition_states if s.ltd_id is not None
    }
    enqueued = 0
    for ltd_slug in progress.walk(in_scope):
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
            push_window=push_window,
        ):
            continue
        try:
            ltd_edition_ids = await _list_edition_ltd_ids(
                ltd_client=ltd_client,
                org_slug=org.slug,
                ltd_slug=ltd_slug,
                tier=Tier.other,
                logger=logger,
            )
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
        if _has_stale_non_main_edition(
            edition_state_by_ltd_id=edition_state_by_ltd_id,
            ltd_edition_ids=ltd_edition_ids,
            now=now,
        ) and await _enqueue_tier_project_sync(
            ctx=ctx,
            started=started,
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
    *,
    factory: Factory,
    session: AsyncSession,
    org: Organization,
    config: KeeperSyncConfig,
    logger: structlog.stdlib.BoundLogger,
) -> list[str]:
    """Resolve one tier cron's candidate slugs for an org.

    Goes through :func:`_fetch_ltd_product_slugs` so the three
    tier-cron processors share the same resolution — and the same LTD
    failure policy — ``keeper_sync_run_discovery`` performs:
    list LTD's products, apply the config scope rule via
    :func:`_resolve_scope`, then hand both to
    :func:`_subtract_tombstones`, which drops the tombstoned project
    slugs (issue #396 / PRD #332 user story 17) and reports the
    :class:`_ScopeCounts` this tick logs. Lifting all of it here keeps
    the per-tier logic focused on its decision rule and gives every
    tick the same "Resolved keeper-sync tier scope" counts the
    run-scope event logs.

    The tombstone read is skipped when the config scope is already
    empty: there is nothing left for it to subtract, and the read scans
    the org's whole project-state table. The skip costs the counts
    nothing, because ``tombstoned_count`` is the size of the
    intersection with the scope — which is empty either way.
    """
    ltd_slugs = await _fetch_ltd_product_slugs(
        factory=factory, config=config, logger=logger
    )
    in_scope, excluded_count = _resolve_scope(ltd_slugs, config)
    tombstoned_slugs: set[str] = set()
    if in_scope:
        tombstoned_slugs = await _fetch_tombstoned_project_slugs(
            state_store=factory.create_keeper_sync_state_store(),
            session=session,
            org_id=org.id,
        )
    fan_out, counts = _subtract_tombstones(
        ltd_slugs=ltd_slugs,
        in_scope=in_scope,
        excluded_count=excluded_count,
        tombstoned_slugs=tombstoned_slugs,
    )
    logger.info(
        "Resolved keeper-sync tier scope",
        org=org.slug,
        **counts.as_log_fields(),
    )
    return fan_out


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
    row's ``annotations`` (``main_edition_url``) so the steady-state
    common case is one ``GET /editions/<id>`` per project per tier_main
    tick instead of the ``GET /products/<slug>/editions/`` listing plus
    an ``GET /editions/<id>`` per non-``main`` edition. With ~1500 in-
    scope LTD products each carrying many ticket-branch editions, the
    walk path was the dominant load on the LTD API; the cache reduces
    it to one HTTP call per project.

    Cache invalidation:

    * Cached fetch returns 404 (the edition was deleted on LTD) —
      discard the pointer and walk.
    * Cached fetch returns 200 but the slug no longer names ``main``
      (:func:`~docverse_server.services.keeper_sync.mappers.is_ltd_main`)
      — a maintainer renamed the edition; discard the pointer and walk.

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
            if is_ltd_main(edition.slug):
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
    """Walk LTD's edition URL list looking for the ``main`` edition.

    LTD has no slug-keyed edition lookup — every edition lives at
    ``/editions/{integer_id}``. We pull the URL list (one cheap HTTP
    call) and walk it in reverse: LTD orders the list newest-first
    and the ``main`` edition is typically the first edition created
    for a product (so it sits at the *end* of the listing), so this
    loop terminates after one fetch in the common case. An edition is
    ``main`` when its LTD slug passes
    :func:`~docverse_server.services.keeper_sync.mappers.is_ltd_main`,
    the same rule tier_other and the keeper-sync service apply. Returns
    ``None`` when no edition qualifies, which counts as "no main edition
    to refresh" rather than an error.
    """
    edition_urls = await ltd_client.list_edition_urls_for_product(product_slug)
    for url in reversed(edition_urls):
        edition = await ltd_client.get_edition_by_url(url)
        if is_ltd_main(edition.slug):
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
      is non-``None`` we additionally rewrite ``main_edition_url`` (so
      the next tick's :func:`_find_main_edition` skips the URL walk)
      and write ``date_rebuilt_seen`` on the project state row so the
      next tick's :func:`should_poll_main_for_project` can decide hot
      vs dormant from this same row.

    Existing unrelated annotation keys are preserved by merge, and the
    row is read ``FOR UPDATE`` so the merge cannot lose a concurrent
    writer's key: the keeper-sync push processor stamps
    ``github_pushed_refs`` onto this row whenever a push arrives, and
    an unlocked read taken before its stamp would write the map back
    without it. The one exception
    is the retired ``main_edition_ltd_id`` key: releases before #799
    wrote it beside ``main_edition_url`` and nothing reads it, so it is
    dropped on every write rather than carried forward where a later
    re-resolve of the URL would leave it pointing at a different edition.
    """
    async with session.begin():
        existing = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug=ltd_slug,
            for_update=True,
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
        merged.pop(_LEGACY_MAIN_EDITION_LTD_ID_KEY, None)
        date_rebuilt_for_upsert: datetime | None = None
        if main_edition is not None:
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
    ``dormant_interval``. Read-modify-write inside one transaction, the
    row read ``FOR UPDATE``, so other writers' annotation keys (the
    cached ``main_edition_url``, ``date_main_last_polled``, and the
    push processor's ``github_pushed_refs``, which a push can stamp at
    any moment) are preserved by merge.

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
            for_update=True,
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


def _split_listed_edition_urls(
    edition_urls: Sequence[str],
) -> tuple[list[int], list[str]]:
    """Split a product's edition URLs into LTD ids and unreadable URLs.

    Returns the ids :func:`parse_ltd_id` reads off the URLs, in listing
    order, and the URLs it could not read one from (no trailing integer
    path segment), also in listing order. ``parse_ltd_id`` itself keeps
    raising on such a URL; this is where the tier crons choose to skip
    it instead.
    """
    ltd_ids: list[int] = []
    unparsable: list[str] = []
    for url in edition_urls:
        try:
            ltd_ids.append(parse_ltd_id(url))
        except ValueError:
            unparsable.append(url)
    return ltd_ids, unparsable


async def _list_edition_ltd_ids(
    *,
    ltd_client: LtdClient,
    org_slug: str,
    ltd_slug: str,
    tier: Tier,
    logger: structlog.stdlib.BoundLogger,
) -> list[int]:
    """List a product's editions and return the LTD ids their URLs carry.

    One ``GET /products/<slug>/editions/``; no edition is followed. The
    discovery and other tiers call this inside their per-project
    ``except LtdClientError``, so a listing failure still costs only
    that project. A listed URL with no trailing integer id is skipped
    rather than raised: a ``ValueError`` would escape that handler, skip
    the project's polled stamp, and reach :func:`_run_tier_pass`'s
    per-org handler, dropping every remaining project of the org for
    the tick. The skip is logged as one warning per project naming the
    org, slug and the first :data:`_MAX_RECORDED_EDITION_FAILURES` URLs,
    with ``skipped_count`` the exact total, and the ids that did parse
    are returned for the tier to decide on. The URL list is capped
    because an LTD URL-shape change would make every edition of every
    in-scope product unparsable on every tick: ``pipelines`` alone would
    otherwise put 2,938 URLs into one log line.
    """
    edition_urls = await ltd_client.list_edition_urls_for_product(ltd_slug)
    ltd_ids, unparsable = _split_listed_edition_urls(edition_urls)
    if unparsable:
        logger.warning(
            "Keeper-sync tier skipped unparsable LTD edition URLs",
            tier=tier.value,
            org=org_slug,
            ltd_slug=ltd_slug,
            unparsable_urls=unparsable[:_MAX_RECORDED_EDITION_FAILURES],
            skipped_count=len(unparsable),
            parsed_count=len(ltd_ids),
        )
    return ltd_ids


async def _project_needs_discovery(
    *,
    ltd_client: LtdClient,
    org_slug: str,
    ltd_slug: str,
    project_state: Any,
    edition_state_by_ltd_id: dict[int, Any],
    logger: structlog.stdlib.BoundLogger,
) -> bool:
    """Return True when an in-scope project has any unseen LTD resource.

    The cheap check first: if the project itself has no state row,
    enqueue immediately without touching LTD. Otherwise list the
    project's edition URLs and look each one's LTD id, parsed from the
    URL, up in the pre-loaded org-wide edition-state map. The caller
    hoists the ``list_for_org(resource_type=edition)`` read out of the
    per-slug loop and passes the resulting map in: with 1500 in-scope
    projects that flips ~1500 ``list_for_org`` round-trips per
    discovery tick into one.

    The id is all this check needs, so it never follows the URLs: one
    ``GET /products/<slug>/editions/`` per project, rather than one
    more ``GET /editions/<id>`` per edition, which on ``pipelines``
    (2,938 editions) pushed the cron past arq's cron timeout. A listed
    URL with no id to read is skipped with a warning
    (:func:`_list_edition_ltd_ids`) and the check runs on the rest.

    ``project_state`` is the state row already fetched by the caller
    (so the dormancy planner and this helper share one read). Pass
    ``None`` for "no row exists yet"; the cheap-path short-circuit
    will return ``True`` without touching LTD.
    """
    if is_unknown_resource(project_state):
        return True
    ltd_edition_ids = await _list_edition_ltd_ids(
        ltd_client=ltd_client,
        org_slug=org_slug,
        ltd_slug=ltd_slug,
        tier=Tier.discovery,
        logger=logger,
    )
    return _has_unseen_edition(
        ltd_edition_ids=ltd_edition_ids,
        edition_state_by_ltd_id=edition_state_by_ltd_id,
    )


def _has_unseen_edition(
    *,
    ltd_edition_ids: Sequence[int],
    edition_state_by_ltd_id: Mapping[int, Any],
) -> bool:
    """Return whether any listed LTD edition has no state row.

    The rule of discovery's listing check, shared by
    :func:`_project_needs_discovery` and ``tier_main``'s pushed-ref
    fallback (:meth:`_PushedRefChecker._lists_unseen_edition`).
    """
    return any(
        is_unknown_resource(edition_state_by_ltd_id.get(ltd_id))
        for ltd_id in ltd_edition_ids
    )


def _has_stale_non_main_edition(
    *,
    edition_state_by_ltd_id: Mapping[int, KeeperSyncState],
    ltd_edition_ids: Sequence[int],
    now: datetime,
) -> bool:
    """Return True when any non-``main`` edition's state is past threshold.

    ``ltd_edition_ids`` are the LTD ids of the editions LTD lists for
    the project, parsed from its edition URLs; no edition payload is
    fetched, so LTD's slug for each edition is not available here.
    ``main`` is instead recognised on the state row found for each id,
    which records the LTD slug from the visit that wrote it
    (:func:`~docverse_server.services.keeper_sync.mappers.is_ltd_main`),
    and left out of the staleness check: ``tier_main`` owns that row.

    ``edition_state_by_ltd_id`` is the org's untombstoned edition
    state rows keyed by LTD id, read once per tick by
    :func:`_tier_other_for_org`, so this check makes no database call:
    a product that lists only ``main`` costs nothing beyond its LTD
    listing. A tombstoned edition has no entry, so it reads like one
    without state.

    Editions without a state row are deliberately ignored — they are
    ``tier_discovery``'s job. This decoupling keeps the two cron
    functions' decisions independent so a single missing-state row
    cannot cause two tiers to enqueue for the same project on the
    same hour.
    """
    for ltd_id in ltd_edition_ids:
        state = edition_state_by_ltd_id.get(ltd_id)
        if state is None or is_ltd_main(state.ltd_slug):
            continue
        if should_refresh_other_edition(state=state, now=now):
            return True
    return False


async def _enqueue_tier_project_sync(
    *,
    ctx: dict[str, Any],
    started: float,
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

    Between the row's commit and the backend id's stamp the cron holds a
    row arq does not know about yet. A cancel there — the cron's arq
    timeout, or a worker shutdown — would leave it an orphan holding the
    slug's active-job mutex, so every tick and run skips the project
    until ``keeper_sync_reaper``'s orphan sweep reaches it.
    :func:`~docverse_server.worker.functions._cancellation.record_handoff_cancellation`
    fails it at once instead, unless its backend id was already stamped.
    ``started`` is the tick's :func:`time.monotonic` start, against which
    that cancel is read, together with the timeout the tier's cron is
    registered with: the tier's
    :func:`~docverse_server.services.keeper_sync.scheduler.tier_cron_timeout`.
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
    async with record_handoff_cancellation(
        ctx,
        queue_job_ids=lambda: (queue_job.id,),
        job_function=f"keeper_sync_tier_{tier}",
        started=started,
        timeout_seconds=tier_cron_timeout(Tier(tier)).total_seconds(),
        logger=logger,
    ):
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
            await queue_job_store.set_backend_job_id(
                queue_job.id, metadata.id, queue_name=metadata.queue_name
            )
    return True
