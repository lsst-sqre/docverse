"""Publish an edition to the CDN.

Independently retryable arq worker function that syncs a single
edition's current build to its organization's configured CDN. The job
resolves its CDN configuration entirely from the database so retries
work without external context (per SQR-112 user story 12).
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import sentry_sdk
import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models.queue_enums import PublishStatus
from docverse_server.domain.build import Build
from docverse_server.domain.edition import Edition
from docverse_server.domain.edition_build_history import EditionBuildHistory
from docverse_server.domain.keeper_sync_run import KeeperSyncRunWithActivity
from docverse_server.exceptions import NotFoundError
from docverse_server.factory import Factory
from docverse_server.metrics import (
    EditionPublishedEvent,
    EditionPublishTrigger,
    MetricsEditionKind,
)
from docverse_server.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_id,
)
from docverse_server.services.keeper_sync_finalisation import (
    maybe_finalise_run,
    publish_run_completed,
)
from docverse_server.services.lock_service import LockKey
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.queue_job_store import QueueJobStore


@dataclass(slots=True)
class _PublishResources:
    edition: Edition
    build: Build
    history_entry: EditionBuildHistory
    history_superseded: bool
    """Whether a newer row has taken over this job's ``(edition, build)``.

    True when the row the job was enqueued for is no longer the one
    :meth:`~docverse_server.storage.edition_build_history_store.EditionBuildHistoryStore.get_by_edition_and_build`
    resolves the pair to — the shape an edition rolled off a build and
    back onto it leaves behind. Always False for a payload carrying no
    ``history_id``, which resolves the newest row by construction.
    """


@dataclass(frozen=True, slots=True)
class _PublishSkip:
    """One of :func:`_skip_reason`'s refusals, as the job records it."""

    message: str
    """Recorded as ``progress["message"]`` on the retired queue job."""

    progress_flag: str
    """The ``progress`` key set ``True`` beside the message."""

    log_event: str
    """The structlog event the retirement logs."""


async def publish_edition(ctx: dict[str, Any], payload: dict[str, Any]) -> str:
    """Sync one edition's current build to its organization's CDN.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``, ``http_client``,
        ``arq_queue``).
    payload
        Job payload with ``org_id``, ``project_slug``, ``edition_id``,
        ``edition_slug``, ``build_id``, ``build_public_id``,
        ``queue_job_id``, and ``queue_job_public_id``, plus the optional
        ``history_id`` naming the ``edition_build_history`` row the job
        was enqueued for (absent on payloads minted before that key
        existed).

    Returns
    -------
    str
        ``"completed"`` on success — and equally when one of
        :func:`_skip_reason`'s guards refused the job, a skip rather
        than a failure — ``"failed"`` if the publish attempt raised, or
        ``"skipped"`` for a row the late-delivery guard refuses.
    """
    logger = structlog.get_logger(
        "docverse_server.worker.publish_edition"
    ).bind(
        org_id=payload["org_id"],
        project=payload["project_slug"],
        edition=payload["edition_slug"],
        build=payload["build_public_id"],
        queue_job_id=payload["queue_job_public_id"],
    )

    queue_job_id: int = payload["queue_job_id"]

    started = time.monotonic()
    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        edition_store = factory.create_edition_store()
        history_store = factory.create_edition_build_history_store()
        queue_job_store = factory.create_queue_job_store()
        project_store = factory.create_project_store()
        lock_service = factory.create_lock_service()

        # Late-delivery guard first, ahead of every read (task #551).
        # Both the pre-lock project resolve below and ``_load_resources``
        # filter soft-deleted rows out, and the reap/deliver race this
        # guard absorbs arrives alongside exactly such a delete:
        # ``lifecycle_eval`` soft-deletes the edition whose publish the
        # abandoned sweep just reaped. Resolving resources first would
        # therefore raise ``NotFoundError`` into Sentry for a race the
        # reaper is meant to swallow, so the guard runs before anything
        # can fail to find a row — matching ``build_processing``'s
        # guard-first pickup ordering.
        async with session.begin():
            if await queue_job_store.start_if_queued(queue_job_id) is None:
                return "skipped"

        # Pre-lock: resolve project_id from the payload's project_slug so
        # the EDITION_UPDATE lock key can be computed. The arq payload
        # carries project_slug rather than project_id, so a small SELECT
        # is required before the lock is acquired.
        async with session.begin():
            project = await project_store.get_by_slug(
                org_id=payload["org_id"], slug=payload["project_slug"]
            )
            if project is None:
                msg = (
                    f"Project {payload['project_slug']!r} not found "
                    f"for org {payload['org_id']}"
                )
                raise NotFoundError(msg)

        lock_key = LockKey.for_edition_update(
            org_id=payload["org_id"],
            project_id=project.id,
            edition_id=payload["edition_id"],
        )
        async with lock_service.acquire(lock_key):
            async with session.begin():
                resources = await _load_resources(
                    factory=factory, payload=payload
                )
                # Both pickup guards, read under EDITION_UPDATE and
                # ahead of every write. See :func:`_skip_reason`.
                skip = _skip_reason(resources)
                if skip is None:
                    await _mark_publishing(
                        queue_job_store=queue_job_store,
                        edition_store=edition_store,
                        history_store=history_store,
                        resources=resources,
                        queue_job_id=queue_job_id,
                    )
            if skip is not None:
                await _retire_skipped_publish(
                    ctx=ctx,
                    session=session,
                    factory=factory,
                    queue_job_store=queue_job_store,
                    queue_job_id=queue_job_id,
                    resources=resources,
                    skip=skip,
                    logger=logger,
                )
                return "completed"

            publishing_service = factory.create_edition_publishing_service()
            try:
                async with session.begin():
                    pending_purge = await publishing_service.publish(
                        org_id=payload["org_id"],
                        project_slug=payload["project_slug"],
                        edition=resources.edition,
                        build=resources.build,
                        history_entry=resources.history_entry,
                    )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                logger.exception("Edition publish failed")
                completion: KeeperSyncRunWithActivity | None = None
                async with session.begin():
                    await _mark_failed(
                        edition_store=edition_store,
                        history_store=history_store,
                        queue_job_store=queue_job_store,
                        resources=resources,
                        queue_job_id=queue_job_id,
                        exc=exc,
                    )
                    completion = await _maybe_finalise_keeper_sync_run(
                        factory=factory, queue_job_id=queue_job_id
                    )
                await publish_run_completed(
                    events=ctx.get("events"),
                    session=session,
                    org_store=factory.create_org_store(),
                    completion=completion,
                    logger=logger,
                )
                return "failed"

            # Record the terminal success *before* the purge. The purge
            # is the one step long enough to be cancelled by the arq
            # per-job timeout, and ``CancelledError`` is a
            # ``BaseException`` that escapes ``purge_cdn_cache``'s
            # best-effort ``except Exception``. Completing first means a
            # cancellation there costs only the (best-effort) purge
            # instead of stranding a committed publish ``in_progress``
            # until ``publish_edition_reaper`` fails it hours later.
            completion = None
            async with session.begin():
                await queue_job_store.complete(queue_job_id)
                completion = await _maybe_finalise_keeper_sync_run(
                    factory=factory, queue_job_id=queue_job_id
                )
            logger.info("Edition publish completed")
            # Emit the post-commit metrics after the success transition
            # commits. Both emitters are fully best-effort: they swallow
            # and log any error (a metrics outage *or* a DB hiccup during
            # their own post-commit reads), so neither can fail or retry
            # an edition whose publish has already committed.
            await _publish_edition_published(
                ctx=ctx,
                session=session,
                factory=factory,
                logger=logger,
                org_id=payload["org_id"],
                project_slug=payload["project_slug"],
                edition=resources.edition,
                queue_job_id=queue_job_id,
                started=started,
                trigger_override=payload.get("trigger"),
            )
            await publish_run_completed(
                events=ctx.get("events"),
                session=session,
                org_store=factory.create_org_store(),
                completion=completion,
                logger=logger,
            )
            await try_enqueue_dashboard_build_by_id(
                factory=factory,
                session=session,
                logger=logger,
                org_id=payload["org_id"],
                project_id=resources.edition.project_id,
            )

        # EDITION_UPDATE released. Purge the CDN edge cache only now:
        # after the publish transaction committed, outside every
        # ``session.begin()`` block, *and* outside the advisory lock.
        # The purge queues on the process-wide per-hostname coalescer
        # and can then sit in the purger's rate-limit backoff for tens
        # of seconds. ``LockService.acquire`` pins a dedicated
        # ``engine.connect()`` for its whole block, so purging inside it
        # would hold a pool connection the purge does not touch — and,
        # because keeper-sync takes the same ``for_edition_update`` key
        # in ``sync_build`` and ``_ensure_aggregate_edition``, would
        # also park the sync worker's next import of this edition behind
        # a Cloudflare 429. The purge is best-effort and cannot undo the
        # committed publish.
        if pending_purge is not None:
            await publishing_service.purge_cdn_cache(pending_purge)
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


def _skip_reason(resources: _PublishResources) -> _PublishSkip | None:
    """Say why this job must publish nothing, or ``None`` to go ahead.

    Two pickup guards, both read under ``EDITION_UPDATE`` so neither can
    straddle a concurrent write, and both ahead of ``_mark_publishing``
    so a refused job leaves no trace of itself on any row.

    *Stale row.* Nothing but the payload's ``history_id`` ties a publish
    job to the ``edition_build_history`` row it was enqueued for, and
    the ``(edition, build)`` pair is not a stable address for one: an
    edition rolled off a build and back onto it has two rows for that
    pair. A job that sat on a backed-up default pool across those
    rollbacks would otherwise resolve the pair to the *newer* row — the
    one a later publish had already carried to ``published`` — set it
    back to ``publishing``, and stamp ``failed`` on it if the CDN then
    refused. The reconcile planner answers ``failed_left_alone`` for a
    failed row before it ever checks the pointer, so a genuine pointer
    loss on that edition would never be re-driven again (task #630).

    A row already at ``published`` is refused for the same reason: every
    producer sets its row ``pending`` in the transaction that creates
    the publish ``QueueJob``, so a row found terminal here was carried
    there by somebody else's job.

    *Deleted build.* Tracking committed this edition's pointer and
    enqueued the publish; a DELETE landing in the window before arq
    delivered the job leaves a build the ``purgatory_cleanup`` sweep may
    reclaim, and a KV pointer written now would outlive the objects it
    names (PRD #596).
    """
    if resources.history_superseded:
        return _PublishSkip(
            message="Edition history row superseded before publishing",
            progress_flag="superseded_skipped",
            log_event="Superseded publish skipped",
        )
    if resources.history_entry.publish_status == PublishStatus.published:
        return _PublishSkip(
            message="Edition history row already published",
            progress_flag="superseded_skipped",
            log_event="Superseded publish skipped",
        )
    if resources.build.date_deleted is not None:
        return _PublishSkip(
            message="Build was deleted before publishing",
            progress_flag="deleted_skipped",
            log_event="Deleted build skipped before publishing",
        )
    return None


async def _retire_skipped_publish(
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    factory: Factory,
    queue_job_store: QueueJobStore,
    queue_job_id: int,
    resources: _PublishResources,
    skip: _PublishSkip,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Retire a publish one of :func:`_skip_reason`'s guards refused.

    The publish-side twin of ``build_processing``'s
    ``_mark_deleted_skipped``, and it records the same thing: a flag on
    the ``progress`` of a job that completes rather than fails. Neither
    refusal is an error — an operator asked for the delete, and a
    duplicate delivery for a row another writer has already settled has
    nothing left to do — so the job must not land in Sentry or wait for
    ``publish_edition_reaper``.

    Nothing else is touched: not the history row, not the edition's
    ``publish_status``, not the edge, and no ``EditionPublishedEvent``
    is emitted for a publish that did not happen. A deleted build leaves
    the row at the ``pending`` tracking set, because it records an
    intent that was never carried out; a superseded row keeps whatever
    the writer that overtook this job put there, which is by
    construction newer than anything this job could say. The keeper-sync
    roll-up still runs: this job is terminal, so a run that was waiting
    on it must be allowed to finalise exactly as the success and failure
    paths allow it to.
    """
    logger.info(
        skip.log_event,
        history_id=resources.history_entry.id,
        history_publish_status=resources.history_entry.publish_status,
        history_superseded=resources.history_superseded,
    )
    completion: KeeperSyncRunWithActivity | None = None
    async with session.begin():
        await queue_job_store.update_phase(
            queue_job_id,
            "complete",
            progress={
                "message": skip.message,
                skip.progress_flag: True,
            },
        )
        await queue_job_store.complete(queue_job_id)
        completion = await _maybe_finalise_keeper_sync_run(
            factory=factory, queue_job_id=queue_job_id
        )
    await publish_run_completed(
        events=ctx.get("events"),
        session=session,
        org_store=factory.create_org_store(),
        completion=completion,
        logger=logger,
    )


async def _load_resources(
    *,
    factory: Factory,
    payload: dict[str, Any],
) -> _PublishResources:
    """Load project, edition, build, and history entry for the job.

    The history entry is the row named by the payload's ``history_id``
    when it carries one, and the pair's newest row otherwise. Either
    way the pair's newest row is read as well, so
    :func:`_skip_reason` can tell a job enqueued for a row the pair has
    since moved past from one whose row is still current.
    """
    project_store = factory.create_project_store()
    edition_store = factory.create_edition_store()
    build_store = factory.create_build_store()
    history_store = factory.create_edition_build_history_store()

    project = await project_store.get_by_slug(
        org_id=payload["org_id"], slug=payload["project_slug"]
    )
    if project is None:
        msg = (
            f"Project {payload['project_slug']!r} not found "
            f"for org {payload['org_id']}"
        )
        raise NotFoundError(msg)
    edition = await edition_store.get_by_slug(
        project_id=project.id, slug=payload["edition_slug"]
    )
    if edition is None or edition.id != payload["edition_id"]:
        msg = (
            f"Edition {payload['edition_slug']!r} "
            f"(id={payload['edition_id']}) not found for project "
            f"{payload['project_slug']!r}"
        )
        raise NotFoundError(msg)
    build = await build_store.get_by_id(payload["build_id"])
    if build is None:
        msg = f"Build {payload['build_id']} not found"
        raise NotFoundError(msg)
    newest_entry = await history_store.get_by_edition_and_build(
        edition_id=edition.id, build_id=build.id
    )
    if newest_entry is None:
        msg = (
            f"EditionBuildHistory not found for edition "
            f"{edition.id} and build {build.id}"
        )
        raise NotFoundError(msg)
    history_id: int | None = payload.get("history_id")
    if history_id is None:
        history_entry = newest_entry
    else:
        resolved = await history_store.get_by_id(history_id)
        if resolved is None:
            msg = f"EditionBuildHistory {history_id} not found"
            raise NotFoundError(msg)
        if resolved.edition_id != edition.id or resolved.build_id != build.id:
            msg = (
                f"EditionBuildHistory {history_id} belongs to edition "
                f"{resolved.edition_id} and build {resolved.build_id}, "
                f"not edition {edition.id} and build {build.id}"
            )
            raise NotFoundError(msg)
        history_entry = resolved
    return _PublishResources(
        edition=edition,
        build=build,
        history_entry=history_entry,
        history_superseded=history_entry.id != newest_entry.id,
    )


async def _mark_publishing(
    *,
    queue_job_store: QueueJobStore,
    edition_store: EditionStore,
    history_store: EditionBuildHistoryStore,
    resources: _PublishResources,
    queue_job_id: int,
) -> None:
    """Transition the queue job, edition, and history to publishing.

    The queued → in_progress pickup itself already happened, before the
    resource loads, so this only records the phase the picked-up job has
    reached (see the late-delivery guard at the top of
    :func:`publish_edition`).
    """
    await queue_job_store.update_phase(
        queue_job_id,
        "publishing",
        progress={"message": "Publishing edition"},
    )
    await edition_store.set_publish_status(
        edition_id=resources.edition.id, status=PublishStatus.publishing
    )
    await history_store.set_publish_status(
        history_id=resources.history_entry.id,
        status=PublishStatus.publishing,
    )


async def _mark_failed(
    *,
    edition_store: EditionStore,
    history_store: EditionBuildHistoryStore,
    queue_job_store: QueueJobStore,
    resources: _PublishResources,
    queue_job_id: int,
    exc: BaseException,
) -> None:
    """Mark the edition, history, and queue job as failed."""
    await edition_store.set_publish_status(
        edition_id=resources.edition.id, status=PublishStatus.failed
    )
    await history_store.set_publish_status(
        history_id=resources.history_entry.id,
        status=PublishStatus.failed,
    )
    await queue_job_store.fail(
        queue_job_id,
        errors={
            "message": str(exc),
            "type": type(exc).__name__,
            "traceback": traceback.format_exc(),
        },
    )


async def _maybe_finalise_keeper_sync_run(
    *,
    factory: Factory,
    queue_job_id: int,
) -> KeeperSyncRunWithActivity | None:
    """Roll up the parent keeper-sync run if this publish was attributed.

    Publish jobs enqueued by ``keeper_sync_project`` (via the shared
    ``enqueue_publish_for_edition`` helper) carry ``keeper_sync_run_id``
    on their ``queue_jobs`` row so they roll into the run's progress
    counters. Without an explicit hook here, a successfully-completed
    publish would leave the parent run perpetually ``in_progress`` —
    the keeper-sync reaper only fails *silent* ``in_progress`` rows,
    not legitimately-completed ones. Calling
    :func:`maybe_finalise_run` after each publish terminal transition
    drives the run to ``succeeded`` / ``partial_failure`` once every
    attributed child has reached terminal.

    Publishes that were *not* attributed to a keeper-sync run (the
    normal client-upload path) leave ``keeper_sync_run_id IS NULL``
    and this helper returns without touching any run row.
    """
    queue_job_store = factory.create_queue_job_store()
    run_store = factory.create_keeper_sync_run_store()
    queue_job = await queue_job_store.get(queue_job_id)
    if queue_job is None or queue_job.keeper_sync_run_id is None:
        return None
    return await maybe_finalise_run(
        run_store=run_store, run_id=queue_job.keeper_sync_run_id
    )


async def _publish_edition_published(
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    factory: Factory,
    logger: structlog.stdlib.BoundLogger,
    org_id: int,
    project_slug: str,
    edition: Edition,
    queue_job_id: int,
    started: float,
    trigger_override: str | None = None,
) -> None:
    """Emit one ``edition_published`` metric for a successful publish.

    The ``trigger`` is classified, in order:

    * ``keeper_sync`` when the publish job's ``queue_jobs`` row carries a
      ``keeper_sync_run_id`` (only the LTD-keeper backfill sets it);
    * otherwise the explicit ``trigger_override`` from the job payload
      (the rollback handler tags ``trigger=rollback``);
    * otherwise ``build`` — the ordinary client-upload fan-out.

    Fully best-effort: this runs *after* the publish has committed, so it
    swallows and logs any error — a metrics-backend outage (already
    covered by ``raise_on_error=False``) *or* a DB error during its own
    post-commit reads — rather than letting it propagate and retry an
    edition that has already published.
    """
    events = ctx.get("events")
    if events is None:
        return
    try:
        org_store = factory.create_org_store()
        queue_job_store = factory.create_queue_job_store()
        async with session.begin():
            org = await org_store.get_by_id(org_id)
            queue_job = await queue_job_store.get(queue_job_id)
        organization = org.slug if org is not None else str(org_id)
        if queue_job is not None and queue_job.keeper_sync_run_id is not None:
            trigger = EditionPublishTrigger.keeper_sync
        elif trigger_override is not None:
            trigger = EditionPublishTrigger(trigger_override)
        else:
            trigger = EditionPublishTrigger.build
        await events.edition_published.publish(
            EditionPublishedEvent(
                organization=organization,
                project=project_slug,
                edition_kind=MetricsEditionKind.from_api(edition.kind),
                trigger=trigger,
                elapsed=timedelta(seconds=time.monotonic() - started),
            )
        )
    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        logger.exception("Failed to publish edition_published metric")
