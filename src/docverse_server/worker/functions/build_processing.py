"""Build processing worker function.

Downloads a staged tarball, unpacks it, uploads files to the
object store under the ``__builds/{build_id}/`` prefix, and
updates editions that track the build's git ref.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import mimetypes
import tarfile
import time
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum, auto
from typing import Any

import sentry_sdk
import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildProcessingProgress,
    BuildStatus,
    EditionUpdateRef,
    PublishJobRef,
)
from docverse_server.domain.api_urls import edition_url, job_url
from docverse_server.domain.build import Build
from docverse_server.domain.content_hash import hash_manifest_pairs
from docverse_server.domain.edition_tracking import EditionTrackingResult
from docverse_server.exceptions import NotFoundError
from docverse_server.factory import Factory
from docverse_server.metrics import BuildProcessedEvent
from docverse_server.services.lock_service import LockKey
from docverse_server.services.publish_enqueue import (
    enqueue_publish_for_edition,
)
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.objectstore import ObjectStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.queue_job_store import QueueJobStore

#: Maximum number of concurrent upload tasks.
_UPLOAD_CONCURRENCY = 50


@dataclass(slots=True)
class _BuildProcessedOutcome:
    """Terminal metrics for one ``build_processing`` run.

    Carried up from the per-path helpers so the top-level function can
    emit a single ``build_processed`` event covering whichever of the
    three terminal outcomes (success, failure, stale-skip) occurred.
    """

    success: bool
    object_count: int | None
    total_size_bytes: int | None
    editions_updated: int
    editions_skipped: int
    stale_skipped: bool


class _StaleGuardOutcome(Enum):
    """How the stale-build guard resolved one ``build_processing`` job."""

    not_stale = auto()
    """This build is the newest for its ``(project, git_ref)``: run it."""

    stale_skipped = auto()
    """Superseded by a newer build, and the skip was recorded.

    Covers both rows the bookkeeping treats alike: a queue job started
    and marked ``completed`` with ``stale_skipped``, and a delivery with
    no ``queue_jobs`` row at all. The build really is superseded either
    way, so the run reports a stale-skipped success.
    """

    late_delivery = auto()
    """The pickup guard refused the row, so nothing was recorded.

    A reaper had already failed the row, or arq re-delivered a job
    another worker owns. Either way this delivery did no work and must
    not claim any (see :meth:`QueueJobStore.start_if_queued`).
    """


@dataclass(frozen=True, slots=True)
class _QueueJobPickup:
    """Outcome of resolving this arq job's ``queue_jobs`` row at pickup."""

    queue_job_id: int | None
    """Internal id of the row this worker started, if it started one."""

    skipped: bool
    """True when the row existed but the late-delivery guard refused it.

    Covers both non-``queued`` cases the guard reports — terminal because
    a reaper failed the row, or ``in_progress`` because arq re-delivered
    the job — since the body must be skipped either way.
    """


async def build_processing(
    ctx: dict[str, Any], payload: dict[str, Any]
) -> str:
    """Process a build: download tarball, unpack, upload files.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``, ``http_client``,
        ``arq_queue``).
    payload
        Job payload with ``org_id``, ``project_id``, ``build_id`` and —
        for anything enqueued since task #550 — ``queue_job_id`` /
        ``queue_job_public_id``.

    Returns
    -------
    str
        A status message.
    """
    logger = structlog.get_logger("docverse_server.worker.build_processing")
    org_id: int = payload["org_id"]
    org_slug: str = payload["org_slug"]
    project_slug: str = payload["project_slug"]
    build_id: int = payload["build_id"]
    build_public_id: str = payload["build_public_id"]
    logger = logger.bind(
        org=org_slug,
        project=project_slug,
        build=build_public_id,
    )

    started = time.monotonic()
    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        build_store = factory.create_build_store()
        org_store = factory.create_org_store()
        queue_job_store = factory.create_queue_job_store()
        lock_service = factory.create_lock_service()

        # Pre-lock: load just enough build metadata to compute the
        # BUILD_PROCESSING lock key. Acquired below before any tarball
        # work so two jobs sharing (project, git_ref) serialize.
        async with session.begin():
            build = await build_store.get_by_id(build_id)
            if build is None:
                msg = f"Build {build_id} not found"
                raise NotFoundError(msg)

        lock_key = LockKey.for_build_processing(
            org_id=org_id,
            project_id=build.project_id,
            git_ref=build.git_ref,
        )
        async with lock_service.acquire(lock_key):
            outcome: _BuildProcessedOutcome | None
            stale_guard = await _guard_stale_build(
                session=session,
                ctx=ctx,
                payload=payload,
                build_store=build_store,
                queue_job_store=queue_job_store,
                build=build,
                build_id=build_id,
                logger=logger,
            )
            if stale_guard is _StaleGuardOutcome.not_stale:
                result, outcome = await _process_build_locked(
                    session=session,
                    factory=factory,
                    org_slug=org_slug,
                    build_store=build_store,
                    org_store=org_store,
                    queue_job_store=queue_job_store,
                    ctx=ctx,
                    payload=payload,
                    build=build,
                    org_id=org_id,
                    project_slug=project_slug,
                    build_id=build_id,
                    build_public_id=build_public_id,
                    logger=logger,
                )
            else:
                result, outcome = _stale_guard_result(stale_guard)

        # A skipped queue job produces no outcome: the build body never
        # ran, so there is nothing to report as processed.
        if outcome is None:
            return result

        # Publish after releasing the lock and after the terminal DB
        # transition has committed. Best-effort: production runs
        # raise_on_error=False so a metrics outage never fails the build.
        await _publish_build_processed(
            ctx=ctx,
            org_slug=org_slug,
            project_slug=project_slug,
            outcome=outcome,
            started=started,
        )
        return result

    msg = "No database session available"
    raise RuntimeError(msg)


def _stale_guard_result(
    guard: _StaleGuardOutcome,
) -> tuple[str, _BuildProcessedOutcome | None]:
    """Map a terminal stale-guard verdict to (arq result, metrics).

    A recorded stale skip is a success the operator should see in
    ``build_processed``. A late delivery is not: the row is terminal or
    owned by another worker, so it reports ``"skipped"`` and no metric —
    the same "a skipped queue job produces no outcome" rule
    :func:`_process_build_locked` follows for the non-stale path.
    """
    if guard is _StaleGuardOutcome.late_delivery:
        return "skipped", None
    return "completed", _BuildProcessedOutcome(
        success=True,
        object_count=None,
        total_size_bytes=None,
        editions_updated=0,
        editions_skipped=0,
        stale_skipped=True,
    )


async def _publish_build_processed(
    *,
    ctx: dict[str, Any],
    org_slug: str,
    project_slug: str,
    outcome: _BuildProcessedOutcome,
    started: float,
) -> None:
    """Emit one ``build_processed`` metric for a finished build run."""
    events = ctx.get("events")
    if events is None:
        return
    await events.build_processed.publish(
        BuildProcessedEvent(
            organization=org_slug,
            project=project_slug,
            success=outcome.success,
            object_count=outcome.object_count,
            total_size_bytes=outcome.total_size_bytes,
            editions_updated=outcome.editions_updated,
            editions_skipped=outcome.editions_skipped,
            stale_skipped=outcome.stale_skipped,
            elapsed=timedelta(seconds=time.monotonic() - started),
        )
    )


async def _guard_stale_build(
    *,
    session: AsyncSession,
    ctx: dict[str, Any],
    payload: dict[str, Any],
    build_store: BuildStore,
    queue_job_store: QueueJobStore,
    build: Build,
    build_id: int,
    logger: structlog.stdlib.BoundLogger,
) -> _StaleGuardOutcome:
    """Skip and mark stale if a newer build exists for ``(project, git_ref)``.

    Runs *inside* the BUILD_PROCESSING lock so two concurrent supersession
    checks cannot race: only the newest build for ``(project, git_ref)``
    does any work; any older build observes a higher latest id and skips.

    Returns the verdict the caller turns into an arq result and metrics
    (see :func:`_stale_guard_result`); ``not_stale`` means this build
    should be processed.
    """
    async with session.begin():
        latest_build_id = await build_store.get_latest_build_id_for_ref(
            project_id=build.project_id,
            git_ref=build.git_ref,
        )
    # A newer build landing between this read and _mark_stale_skipped
    # is benign: the BUILD_PROCESSING lock serializes supersession
    # checks, so the newer build's own check will discard this
    # build's "stale" verdict and proceed correctly.
    if latest_build_id is not None and latest_build_id != build_id:
        return await _mark_stale_skipped(
            session=session,
            ctx=ctx,
            payload=payload,
            queue_job_store=queue_job_store,
            build_id=build_id,
            latest_build_id=latest_build_id,
            logger=logger,
        )
    return _StaleGuardOutcome.not_stale


async def _process_build_locked(
    *,
    session: AsyncSession,
    factory: Factory,
    build_store: BuildStore,
    org_store: OrganizationStore,
    queue_job_store: QueueJobStore,
    ctx: dict[str, Any],
    payload: dict[str, Any],
    build: Build,
    org_id: int,
    org_slug: str,
    project_slug: str,
    build_id: int,
    build_public_id: str,
    logger: structlog.stdlib.BoundLogger,
) -> tuple[str, _BuildProcessedOutcome | None]:
    """Unpack, upload, and finalize a non-stale build under the lock.

    Assumes the caller holds the BUILD_PROCESSING advisory lock and has
    already confirmed this build is the latest for ``(project, git_ref)``.

    Returns the arq status message paired with the terminal metrics for
    the run so the caller can emit one ``build_processed`` event. The
    metrics are ``None`` when the late-delivery guard (PRD #538) refused
    to pick the queue job up: nothing ran, so there is no build outcome
    to report.
    """
    # Phase 1: Mark QueueJob as in_progress and load metadata. The
    # pickup guard runs first so a skipped row costs nothing beyond the
    # lookup — no org read, no object store resolved.
    async with session.begin():
        pickup = await _start_queue_job(ctx, payload, queue_job_store)
        if pickup.skipped:
            return "skipped", None

        org = await org_store.get_by_id(org_id)
        if org is None:
            msg = f"Organization {org_id} not found"
            raise NotFoundError(msg)

        service_label = org.resolved_staging_store_label
        if service_label is None:
            msg = f"No object store service configured for org {org_id}"
            raise RuntimeError(msg)

        object_store = await factory.create_objectstore_for_org(
            org_id=org_id, service_label=service_label
        )

        queue_job_id = pickup.queue_job_id
        if queue_job_id is not None:
            await queue_job_store.update_phase(
                queue_job_id,
                "unpacking",
                progress={
                    "message": "Unpacking build into object store",
                },
            )

    # Phase 2: Upload files and mark build complete
    try:
        async with object_store, session.begin():
            object_count, total_size_bytes = await _process_build(
                object_store=object_store,
                build=build,
                build_store=build_store,
                org_slug=org_slug,
                project_slug=project_slug,
                logger=logger,
            )
    except Exception as exc:
        # Phase 3a: Mark build and queue job as failed
        sentry_sdk.capture_exception(exc)
        logger.exception("Build processing failed")
        async with session.begin():
            build_service = factory.create_build_service()
            await build_service.fail(
                build_id=build_id,
                org_slug=org_slug,
                project_slug=project_slug,
            )
            if queue_job_id is not None:
                await queue_job_store.fail(queue_job_id)
        return "failed", _BuildProcessedOutcome(
            success=False,
            object_count=None,
            total_size_bytes=None,
            editions_updated=0,
            editions_skipped=0,
            stale_skipped=False,
        )
    else:
        editions_updated, editions_skipped = await _finalize_success(
            session=session,
            factory=factory,
            build_store=build_store,
            queue_job_store=queue_job_store,
            org_id=org_id,
            org_slug=org_slug,
            project_id=build.project_id,
            project_slug=project_slug,
            build_id=build_id,
            build_public_id=build_public_id,
            queue_job_id=queue_job_id,
            object_count=object_count,
            total_size_bytes=total_size_bytes,
            logger=logger,
        )
        return "completed", _BuildProcessedOutcome(
            success=True,
            object_count=object_count,
            total_size_bytes=total_size_bytes,
            editions_updated=editions_updated,
            editions_skipped=editions_skipped,
            stale_skipped=False,
        )


async def _mark_stale_skipped(
    *,
    session: AsyncSession,
    ctx: dict[str, Any],
    payload: dict[str, Any],
    queue_job_store: QueueJobStore,
    build_id: int,
    latest_build_id: int,
    logger: structlog.stdlib.BoundLogger,
) -> _StaleGuardOutcome:
    """Mark a superseded build's QueueJob complete with a stale-skip flag.

    Operators identify these runs by ``progress["stale_skipped"]`` plus
    the dedicated log line; the QueueJob status stays ``completed``
    because nothing was wrong with the build itself — a newer build
    for the same ``(project, git_ref)`` simply took over.

    The pickup guard runs before any of that, and a row it refuses
    (task #551) short-circuits to ``late_delivery`` with nothing written
    and nothing logged as skipped: the row is terminal or in another
    worker's hands, so this delivery has no stale skip to record. A
    delivery with no ``queue_jobs`` row at all still counts as a
    recorded skip — there is simply no bookkeeping to do.
    """
    async with session.begin():
        pickup = await _start_queue_job(ctx, payload, queue_job_store)
        if pickup.skipped:
            return _StaleGuardOutcome.late_delivery
        logger.info(
            "Stale build skipped",
            build_id=build_id,
            latest_build_id=latest_build_id,
        )
        if pickup.queue_job_id is not None:
            queue_job_id = pickup.queue_job_id
            await queue_job_store.update_phase(
                queue_job_id,
                "complete",
                progress={
                    "message": (
                        "Stale build skipped; superseded by "
                        f"build id {latest_build_id}"
                    ),
                    "stale_skipped": True,
                    "latest_build_id": latest_build_id,
                },
            )
            await queue_job_store.complete(queue_job_id)
    return _StaleGuardOutcome.stale_skipped


async def _resolve_queue_job_id(
    ctx: dict[str, Any],
    payload: dict[str, Any],
    queue_job_store: QueueJobStore,
) -> int | None:
    """Resolve this delivery's ``queue_jobs`` row id, or ``None``.

    The payload's ``queue_job_id`` is authoritative and is what every
    other job kind uses. It matters here beyond consistency: the enqueue
    now happens *after* the row commits (task #550), so at delivery time
    the row's ``backend_job_id`` may not be stamped yet and the
    ``backend_job_id`` lookup below would miss a row that plainly
    exists.

    That lookup survives only as the compatibility path for jobs enqueued
    by the previous release, whose payloads predate the key and which can
    still be sitting in the queue across a rolling deploy.
    """
    payload_job_id = payload.get("queue_job_id")
    if payload_job_id is not None:
        return int(payload_job_id)
    arq_job_id: str | None = ctx.get("job_id")
    if arq_job_id is None:
        return None
    queue_job = await queue_job_store.get_by_backend_job_id(arq_job_id)
    return None if queue_job is None else queue_job.id


async def _start_queue_job(
    ctx: dict[str, Any],
    payload: dict[str, Any],
    queue_job_store: QueueJobStore,
) -> _QueueJobPickup:
    """Look up and start the QueueJob for this arq job.

    Distinguishes the two "no queue job to drive" cases the callers
    treat differently: no row at all (``build_processing`` can still be
    enqueued without one, so the build is processed anyway) versus a row
    the late-delivery guard from PRD #538 refused to pick up — terminal
    because a reaper failed it, or in progress because arq re-delivered
    the job — where the job body must be skipped entirely.
    """
    queue_job_id = await _resolve_queue_job_id(ctx, payload, queue_job_store)
    if queue_job_id is None:
        return _QueueJobPickup(queue_job_id=None, skipped=False)
    if await queue_job_store.start_if_queued(queue_job_id) is None:
        return _QueueJobPickup(queue_job_id=None, skipped=True)
    return _QueueJobPickup(queue_job_id=queue_job_id, skipped=False)


async def _resolve_api_base_url(
    factory: Factory,
    logger: structlog.stdlib.BoundLogger,
) -> str | None:
    """Resolve the Docverse API base URL via Repertoire discovery.

    Returns ``None`` — after logging a warning — when no discovery client
    is configured, the discovery lookup fails, or Docverse is not
    registered in Repertoire. Callers omit the HATEOAS URL fields in that
    case rather than failing the build, mirroring the non-fatal
    edition-tracking posture.
    """
    discovery = factory.discovery
    if discovery is None:
        logger.warning(
            "No Repertoire discovery client configured; omitting HATEOAS "
            "URLs from build_processing progress"
        )
        return None
    try:
        api_base = await discovery.url_for_internal("docverse")
    except Exception:
        logger.warning(
            "Failed to resolve Docverse API URL from Repertoire; omitting "
            "HATEOAS URLs from build_processing progress",
            exc_info=True,
        )
        return None
    if api_base is None:
        logger.warning(
            "Docverse is not registered in Repertoire; omitting HATEOAS "
            "URLs from build_processing progress"
        )
    return api_base


async def _finalize_success(
    *,
    session: AsyncSession,
    factory: Factory,
    build_store: BuildStore,
    queue_job_store: QueueJobStore,
    org_id: int,
    org_slug: str,
    project_id: int,
    project_slug: str,
    build_id: int,
    build_public_id: str,
    queue_job_id: int | None,
    object_count: int,
    total_size_bytes: int,
    logger: structlog.stdlib.BoundLogger,
) -> tuple[int, int]:
    """Run edition tracking and mark the queue job complete.

    Edition tracking failures are logged but do not fail the build.

    Returns
    -------
    tuple of int, int
        The number of editions updated and skipped (both ``0`` when
        edition tracking failed), for the ``build_processed`` metric.
    """
    # Phase 3b: Edition tracking
    tracking_result = await _track_editions(
        session=session,
        factory=factory,
        build_store=build_store,
        queue_job_store=queue_job_store,
        build_id=build_id,
        queue_job_id=queue_job_id,
        logger=logger,
    )

    # Phase 3c: Enqueue a publish_edition job for each updated edition.
    #
    # Resolve the Docverse API base URL once for every HATEOAS link in this
    # job's progress payload, but only when there are updated editions to
    # link: a build that updates nothing has no edition_url / job_url
    # to embed, so it skips the Repertoire discovery round-trip (and its
    # "unregistered" warning) entirely. ``None`` means discovery is
    # unavailable or Docverse is unregistered, in which case the URL fields
    # are omitted and the build still completes. The editions_updated
    # comprehension below only dereferences ``api_base`` while iterating
    # ``tracking_result.updated``, so leaving it ``None`` here is safe.
    api_base: str | None = None
    publish_jobs: list[dict[str, str]] = []
    if tracking_result is not None and tracking_result.updated:
        api_base = await _resolve_api_base_url(factory, logger)
        publish_jobs = await _enqueue_publish_jobs(
            session=session,
            factory=factory,
            queue_job_store=queue_job_store,
            tracking_result=tracking_result,
            org_id=org_id,
            org_slug=org_slug,
            project_id=project_id,
            project_slug=project_slug,
            build_id=build_id,
            build_public_id=build_public_id,
            api_base=api_base,
            logger=logger,
        )

    # Phase 4: Mark queue job as complete
    if queue_job_id is not None:
        has_errors = tracking_result is None
        # Build the payload through BuildProcessingProgress so it is
        # validated at write time, then dump to a plain dict for JSONB
        # storage. exclude_none keeps the stored shape minimal (and
        # matches the legacy hand-built dict): keys only appear when the
        # corresponding value is present.
        progress_model = BuildProcessingProgress(
            message="Build processing complete",
            object_count=object_count,
            total_size_bytes=total_size_bytes,
            editions_updated=(
                [
                    EditionUpdateRef(
                        slug=o.slug,
                        action=o.action,
                        edition_url=(
                            edition_url(
                                api_base,
                                org=org_slug,
                                project=project_slug,
                                edition=o.slug,
                            )
                            if api_base is not None
                            else None
                        ),
                    )
                    for o in tracking_result.updated
                ]
                if tracking_result is not None
                else None
            ),
            editions_skipped=(
                [
                    EditionUpdateRef(slug=o.slug)
                    for o in tracking_result.skipped
                ]
                if tracking_result is not None
                else None
            ),
            publish_jobs=(
                [PublishJobRef.model_validate(job) for job in publish_jobs]
                if publish_jobs
                else None
            ),
            edition_tracking_error=has_errors or None,
        )
        progress = progress_model.model_dump(exclude_none=True)
        async with session.begin():
            await queue_job_store.update_phase(
                queue_job_id, "complete", progress=progress
            )
            await queue_job_store.complete(queue_job_id, has_errors=has_errors)
    logger.info("Build processing completed")

    if tracking_result is None:
        return 0, 0
    return len(tracking_result.updated), len(tracking_result.skipped)


async def _enqueue_publish_jobs(
    *,
    session: AsyncSession,
    factory: Factory,
    queue_job_store: QueueJobStore,
    tracking_result: EditionTrackingResult,
    org_id: int,
    org_slug: str,
    project_id: int,
    project_slug: str,
    build_id: int,
    build_public_id: str,
    api_base: str | None,
    logger: structlog.stdlib.BoundLogger,
) -> list[dict[str, str]]:
    """Create a ``publish_edition`` child job for each updated edition.

    Loops over ``tracking_result.updated`` and delegates each
    ``(edition, build)`` pair to
    :func:`docverse_server.services.publish_enqueue.enqueue_publish_for_edition`,
    which owns the Phase A (DB writes) / Phase B (arq enqueue +
    backend-job-id write-back) sequencing.

    Returns a list of ``{edition_slug, publish_queue_job_public_id}``
    entries (plus a ``job_url`` HATEOAS link when ``api_base`` is
    set) suitable for embedding in the parent build job's progress.
    """
    edition_store = factory.create_edition_store()
    history_store = factory.create_edition_build_history_store()
    queue_backend = factory.create_queue_backend()

    publish_jobs: list[dict[str, str]] = []
    for outcome in tracking_result.updated:
        result = await enqueue_publish_for_edition(
            session=session,
            edition_store=edition_store,
            history_store=history_store,
            queue_job_store=queue_job_store,
            queue_backend=queue_backend,
            org_id=org_id,
            project_id=project_id,
            project_slug=project_slug,
            edition_id=outcome.edition_id,
            edition_slug=outcome.slug,
            build_id=outcome.build_id,
            build_public_id=build_public_id,
        )
        entry: dict[str, str] = {
            "edition_slug": result.edition_slug,
            "publish_queue_job_public_id": result.queue_job_public_id,
        }
        if api_base is not None:
            entry["job_url"] = job_url(
                api_base, org=org_slug, job=result.queue_job_public_id
            )
        publish_jobs.append(entry)
        logger.info(
            "Enqueued publish_edition job",
            edition_slug=result.edition_slug,
            publish_queue_job_public_id=result.queue_job_public_id,
        )
    return publish_jobs


async def _track_editions(
    *,
    session: AsyncSession,
    factory: Factory,
    build_store: BuildStore,
    queue_job_store: QueueJobStore,
    build_id: int,
    queue_job_id: int | None,
    logger: structlog.stdlib.BoundLogger,
) -> EditionTrackingResult | None:
    """Evaluate edition tracking rules for a completed build.

    Returns the tracking result, or ``None`` if tracking failed.
    """
    if queue_job_id is not None:
        async with session.begin():
            await queue_job_store.update_phase(
                queue_job_id,
                "edition_tracking",
                progress={
                    "message": "Evaluating edition tracking rules",
                },
            )

    try:
        async with session.begin():
            tracking_service = factory.create_edition_tracking_service()
            build = await build_store.get_by_id(build_id)
            if build is None:
                msg = f"Build {build_id} vanished after completion"
                raise RuntimeError(msg)
            tracking_result = await tracking_service.track_build(build)
        logger.info(
            "Edition tracking complete",
            derived_slug=tracking_result.derived_slug,
            suppressed=tracking_result.suppressed,
            editions_updated=len(tracking_result.updated),
            editions_skipped=len(tracking_result.skipped),
        )
    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        logger.exception("Edition tracking failed")
        return None
    else:
        return tracking_result


async def _process_build(
    *,
    object_store: ObjectStore,
    build: Build,
    build_store: BuildStore,
    org_slug: str,
    project_slug: str,
    logger: structlog.stdlib.BoundLogger,
) -> tuple[int, int]:
    """Download, unpack, and upload build files.

    Each extracted file is hashed as it goes by, so the build's content
    identity is derived from the same bytes that were uploaded and
    lands on the row in the same statement that marks it ``completed``.
    The algorithm is shared with keeper-sync's copier
    (:func:`~docverse_server.domain.content_hash.hash_manifest_pairs`),
    which is what lets the same documentation arriving by either route
    be recognized as the same content.

    Returns
    -------
    tuple of int, int
        The number of objects uploaded and the total size in bytes.
    """
    logger.info(
        "Downloading staging tarball",
        staging_key=build.staging_key,
    )
    # TODO(DM-54426): Full tarball loaded
    # into memory. Streaming the download would reduce peak memory
    # usage for large documentation builds.
    tarball_data = await object_store.download_object(key=build.staging_key)

    build_prefix = build.storage_prefix
    semaphore = asyncio.Semaphore(_UPLOAD_CONCURRENCY)

    async def _upload_file(name: str, data: bytes) -> int:
        async with semaphore:
            key = f"{build_prefix}{name}"
            content_type = (
                mimetypes.guess_type(name)[0] or "application/octet-stream"
            )
            await object_store.upload_object(
                key=key, data=data, content_type=content_type
            )
            return len(data)

    tasks: list[asyncio.Task[int]] = []
    manifest_entries: list[tuple[str, str]] = []
    # TODO(DM-54426): All extracted files
    # held in memory before uploads begin. Streaming extraction with
    # concurrent upload would lower peak memory for large builds.
    with tarfile.open(fileobj=io.BytesIO(tarball_data), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            file_data = f.read()
            name = member.name.removeprefix("./")
            manifest_entries.append(
                (name, hashlib.sha256(file_data).hexdigest())
            )
            task = asyncio.create_task(_upload_file(name, file_data))
            tasks.append(task)

    results = await asyncio.gather(*tasks)
    object_count = len(results)
    total_size = sum(results)
    # Empty input yields EMPTY_MANIFEST_HASH, matching what the copier
    # reports for an empty source prefix.
    content_hash = hash_manifest_pairs(manifest_entries)

    if object_count == 0:
        logger.warning(
            "Tarball contained no extractable files",
            staging_key=build.staging_key,
        )

    logger.info(
        "Upload complete",
        object_count=object_count,
        total_size_bytes=total_size,
        content_hash=content_hash,
    )

    await build_store.update_inventory(
        build_id=build.id,
        object_count=object_count,
        total_size_bytes=total_size,
        org_slug=org_slug,
        project_slug=project_slug,
    )

    await build_store.transition_status(
        build_id=build.id,
        new_status=BuildStatus.completed,
        content_hash=content_hash,
        org_slug=org_slug,
        project_slug=project_slug,
    )

    try:
        await object_store.delete_object(key=build.staging_key)
        logger.info("Deleted staging tarball", staging_key=build.staging_key)
    except Exception:
        logger.warning(
            "Failed to delete staging tarball",
            staging_key=build.staging_key,
            exc_info=True,
        )

    return object_count, total_size
