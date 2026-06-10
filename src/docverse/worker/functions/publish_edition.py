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

from docverse.client.models.queue_enums import PublishStatus
from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistory
from docverse.domain.keeper_sync_run import KeeperSyncRunWithActivity
from docverse.exceptions import NotFoundError
from docverse.factory import Factory
from docverse.metrics import (
    EditionPublishedEvent,
    EditionPublishTrigger,
    MetricsEditionKind,
)
from docverse.services.dashboard.enqueue import (
    try_enqueue_dashboard_build_by_id,
)
from docverse.services.keeper_sync_finalisation import (
    maybe_finalise_run,
    publish_run_completed,
)
from docverse.services.lock_service import LockKey
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.queue_job_store import QueueJobStore


@dataclass(slots=True)
class _PublishResources:
    edition: Edition
    build: Build
    history_entry: EditionBuildHistory


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
        ``queue_job_id``, and ``queue_job_public_id``.

    Returns
    -------
    str
        ``"completed"`` on success or ``"failed"`` if the publish
        attempt raised.
    """
    logger = structlog.get_logger("docverse.worker.publish_edition").bind(
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
                await _mark_publishing(
                    queue_job_store=queue_job_store,
                    edition_store=edition_store,
                    history_store=history_store,
                    resources=resources,
                    queue_job_id=queue_job_id,
                )

            try:
                async with session.begin():
                    await factory.create_edition_publishing_service().publish(
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
                )
                return "failed"
            completion = None
            async with session.begin():
                await queue_job_store.complete(queue_job_id)
                completion = await _maybe_finalise_keeper_sync_run(
                    factory=factory, queue_job_id=queue_job_id
                )
            logger.info("Edition publish completed")
            # Publish after the success transition commits. Best-effort:
            # production runs raise_on_error=False so a metrics outage
            # never fails the publish (no defensive try/except).
            await _publish_edition_published(
                ctx=ctx,
                session=session,
                factory=factory,
                org_id=payload["org_id"],
                project_slug=payload["project_slug"],
                edition=resources.edition,
                queue_job_id=queue_job_id,
                started=started,
            )
            await publish_run_completed(
                events=ctx.get("events"),
                session=session,
                org_store=factory.create_org_store(),
                completion=completion,
            )
            await try_enqueue_dashboard_build_by_id(
                factory=factory,
                session=session,
                logger=logger,
                org_id=payload["org_id"],
                project_id=resources.edition.project_id,
            )
            return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _load_resources(
    *,
    factory: Factory,
    payload: dict[str, Any],
) -> _PublishResources:
    """Load project, edition, build, and history entry for the job."""
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
    history_entry = await history_store.get_by_edition_and_build(
        edition_id=edition.id, build_id=build.id
    )
    if history_entry is None:
        msg = (
            f"EditionBuildHistory not found for edition "
            f"{edition.id} and build {build.id}"
        )
        raise NotFoundError(msg)
    return _PublishResources(
        edition=edition, build=build, history_entry=history_entry
    )


async def _mark_publishing(
    *,
    queue_job_store: QueueJobStore,
    edition_store: EditionStore,
    history_store: EditionBuildHistoryStore,
    resources: _PublishResources,
    queue_job_id: int,
) -> None:
    """Transition the queue job, edition, and history to publishing."""
    await queue_job_store.start(queue_job_id)
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


async def _mark_failed(  # noqa: PLR0913
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


async def _publish_edition_published(  # noqa: PLR0913
    *,
    ctx: dict[str, Any],
    session: AsyncSession,
    factory: Factory,
    org_id: int,
    project_slug: str,
    edition: Edition,
    queue_job_id: int,
    started: float,
) -> None:
    """Emit one ``edition_published`` metric for a successful publish.

    The publish job's ``queue_jobs`` row carries a ``keeper_sync_run_id``
    only when the LTD-keeper backfill drove it, so its presence
    classifies the ``trigger``.
    """
    events = ctx.get("events")
    if events is None:
        return
    org_store = factory.create_org_store()
    queue_job_store = factory.create_queue_job_store()
    async with session.begin():
        org = await org_store.get_by_id(org_id)
        queue_job = await queue_job_store.get(queue_job_id)
    organization = org.slug if org is not None else str(org_id)
    trigger = (
        EditionPublishTrigger.keeper_sync
        if queue_job is not None and queue_job.keeper_sync_run_id is not None
        else EditionPublishTrigger.build
    )
    await events.edition_published.publish(
        EditionPublishedEvent(
            organization=organization,
            project=project_slug,
            edition_kind=MetricsEditionKind.from_api(edition.kind),
            trigger=trigger,
            elapsed=timedelta(seconds=time.monotonic() - started),
        )
    )
