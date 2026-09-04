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
    RetiredBuildStatus,
)
from docverse_server.domain.api_urls import edition_url, job_url
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.content_hash import hash_manifest_pairs
from docverse_server.domain.edition_tracking import EditionTrackingResult
from docverse_server.domain.queue import JobStatus
from docverse_server.exceptions import InvalidBuildStateError, NotFoundError
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

#: Statuses a *retirement* can leave on a build the worker is uploading
#: for. A DELETE cancels it, the stranded-build sweep fails it, a
#: supersession retires it — every one of them a deliberate decision by
#: another actor that this build must not be published. Finding one of
#: these mid-upload is the normal, quiet close-out path
#: (:func:`_close_out_retired_build`); finding anything else that is not
#: ``processing`` is a bug, and raises.
_RETIRED_BUILD_STATUSES: frozenset[BuildStatus] = frozenset(
    {
        BuildStatus.cancelled,
        BuildStatus.failed,
        BuildStatus.superseded,
    }
)


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
    """How the pickup guards resolved one ``build_processing`` job.

    Three reasons stop a build before any work happens — it was
    superseded, it was deleted, or its row is gone — and all of them
    leave the same trace for the caller: ``"completed"`` plus a
    ``build_processed(success=True, stale_skipped=True)`` metric (see
    :func:`_stale_guard_result`). They stay separate members because the
    bookkeeping they write differs: distinct progress keys, distinct log
    lines, and a different terminal status on the build itself — or, for
    a row that has vanished, none at all.
    """

    not_stale = auto()
    """This build is live and the newest for its ``(project, git_ref)``."""

    stale_skipped = auto()
    """Superseded by a newer build, and the skip was recorded.

    Covers both rows the bookkeeping treats alike: a queue job started
    and marked ``completed`` with ``stale_skipped``, and a delivery with
    no ``queue_jobs`` row at all. The build really is superseded either
    way, so the run reports a stale-skipped success.
    """

    deleted_skipped = auto()
    """The build was soft-deleted, and was cancelled instead of published.

    The pre-work guard's verdict: the DELETE landed before the job ran,
    so :func:`_mark_deleted_skipped` retires the build itself. A DELETE
    that lands mid-upload reports :attr:`retired_mid_upload` instead —
    by then the cancel has already been written by somebody else, and it
    is only one of several ways the row can go terminal underneath a
    running upload.

    Recorded like a stale skip — the job completes, carrying
    ``deleted_skipped`` instead of ``stale_skipped`` — because the two
    are the same kind of event to an operator: a build that will never
    be published, retired deliberately rather than in error. The metric
    reuses ``stale_skipped=True`` so ``BuildProcessedEvent`` needs no
    new field for a distinction nobody queries on.
    """

    vanished_before_work = auto()
    """The build row was gone by the time the pre-work guard re-read it.

    The pre-lock read that supplies the lock key found the row, and the
    guard's re-read — which exists to see writes that landed after it —
    did not: a purge removed it, or an operator cleared it. The whole
    point of re-reading before any storage work is to stop here, so this
    verdict does no download, no unpack and no upload; it closes the
    queue job out with the same missing-row progress
    :func:`_close_out_retired_build` writes for a row that vanishes
    mid-upload.

    It needs its own path rather than folding into
    :attr:`deleted_skipped`, whose :func:`_mark_deleted_skipped` would
    raise :exc:`~docverse_server.exceptions.InvalidBuildStateError` out
    of its own cancel on a row that no longer exists.

    Reported like the other recorded skips — ``completed`` plus
    ``stale_skipped=True`` — because nothing failed: there is simply no
    build left to process.
    """

    retired_before_skip = auto()
    """The build was retired between the stale verdict and the skip.

    The stale guard's re-read is deliberately unlocked and commits
    before :func:`_mark_stale_skipped` opens the transaction that acts
    on its verdict, so a DELETE, a lifecycle reap or the stranded-build
    sweep can write a terminal status in that window. The skip stands
    down rather than asking for an edge out of ``cancelled`` or
    ``failed``, and closes the queue job out naming the status it found
    (#590).

    Reported like the other recorded skips — the job completes and the
    metric carries ``stale_skipped=True`` — because the build really was
    retired deliberately; only the *reason* differs from a supersession,
    and the queue job's progress is where that is recorded.
    """

    retired_mid_upload = auto()
    """The build stopped being ``processing`` while its files uploaded.

    The uploads run outside the transaction that would complete the
    build, so anyone holding a different opinion about the row can commit
    while they are in flight: a DELETE cancels it, the stranded-build
    sweep fails it, a supersession retires it, or a purge removes the row
    outright. Whoever wrote that verdict wins — this delivery re-reads
    the row before writing anything terminal, and closes its queue job
    out quietly (:func:`_close_out_retired_build`) rather than completing
    a build somebody already retired.

    Reported like the other recorded skips: the job completes and the
    metric carries ``stale_skipped=True``, because a deliberate
    retirement is work deliberately not done, not a crash.
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


@dataclass(frozen=True, slots=True)
class _MidUploadRetirement:
    """The build was no longer ``processing`` when the uploads finished.

    :func:`_process_build` returns this instead of an inventory when its
    pre-completion re-read finds somebody else has already retired the
    row, so the caller can close the queue job out quietly and name the
    status that was found.
    """

    status: BuildStatus | None
    """The status the re-read found, or ``None`` when the row was gone."""

    deleted: bool = False
    """True when the re-read found ``date_deleted`` stamped on the row.

    Independent of :attr:`status`, because the two are written by
    different statements: ``BuildStore.soft_delete`` stamps the timestamp
    and ``BuildService.soft_delete`` cancels the row alongside it, so a
    row can be deleted while still reading ``processing``.
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
                factory=factory,
                build_store=build_store,
                queue_job_store=queue_job_store,
                build=build,
                build_id=build_id,
                org_slug=org_slug,
                project_slug=project_slug,
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
    """Map a terminal guard verdict to (arq result, metrics).

    A recorded skip — stale, deleted, vanished, or retired before the
    skip or mid-upload — is a
    success the operator should see in ``build_processed``; they all
    report the same ``stale_skipped=True`` shape, since the event
    distinguishes "did no work on purpose" from "failed", not the
    reasons for it. A late
    delivery is neither: the row is terminal or owned by another worker,
    so it reports ``"skipped"`` and no metric — the same "a skipped
    queue job produces no outcome" rule :func:`_process_build_locked`
    follows for the non-stale path.
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
    factory: Factory,
    build_store: BuildStore,
    queue_job_store: QueueJobStore,
    build: Build,
    build_id: int,
    org_slug: str,
    project_slug: str,
    logger: structlog.stdlib.BoundLogger,
) -> _StaleGuardOutcome:
    """Skip a build that vanished, was deleted, or was superseded.

    Runs *inside* the BUILD_PROCESSING lock so two concurrent supersession
    checks cannot race: only the newest build for ``(project, git_ref)``
    does any work; any older build observes a higher latest id and skips.

    The build is re-read here rather than reused from the pre-lock read,
    because the whole point of the deletion check is to see writes that
    landed after it. ``date_deleted`` is checked *before* the latest-live
    lookup: a deleted build must be cancelled on its own account, and
    asking whether it is the newest live build for its ref is both
    pointless (it has just been excluded from that lookup) and
    misleading (it would report itself superseded by whatever is live).

    A row that has gone missing entirely is checked before both. The
    pre-lock read found it, so something removed it in between, and
    neither remaining question has an answer worth acting on: it cannot
    be deleted, and ``get_latest_build_id_for_ref`` reports ``None`` for
    an emptied ref, which reads as "this build is the newest live one"
    and would send the whole tarball down before the mid-upload guard
    finally noticed the row was gone (review of PR #583).

    The re-read is deliberately unlocked. Its transaction commits before
    either skip path runs, so a row lock taken here would be released
    before it could protect anything; the transition each skip path
    makes takes its own lock, and a DELETE that lands in between is seen
    there. ``_process_build``'s guard is the one that has to be locked,
    because its verdict and the write it gates share a transaction.

    Returns the verdict the caller turns into an arq result and metrics
    (see :func:`_stale_guard_result`); ``not_stale`` means this build
    should be processed.
    """
    async with session.begin():
        current = await build_store.get_by_id(build_id)
        deleted = current is not None and current.date_deleted is not None
        latest_build_id: int | None = None
        if current is not None and not deleted:
            latest_build_id = await build_store.get_latest_build_id_for_ref(
                project_id=build.project_id,
                git_ref=build.git_ref,
            )
    if current is None:
        return await _mark_missing_build_skipped(
            session=session,
            ctx=ctx,
            payload=payload,
            queue_job_store=queue_job_store,
            build_id=build_id,
            logger=logger,
        )
    if deleted:
        return await _mark_deleted_skipped(
            session=session,
            ctx=ctx,
            payload=payload,
            factory=factory,
            queue_job_store=queue_job_store,
            build_id=build_id,
            org_slug=org_slug,
            project_slug=project_slug,
            logger=logger,
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
            factory=factory,
            queue_job_store=queue_job_store,
            build_id=build_id,
            latest_build_id=latest_build_id,
            org_slug=org_slug,
            project_slug=project_slug,
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
            upload = await _process_build(
                object_store=object_store,
                build=build,
                build_store=build_store,
                org_slug=org_slug,
                project_slug=project_slug,
                logger=logger,
            )
    except Exception as exc:
        # Phase 3a: Mark queue job and build as failed.
        #
        # Both writes are idempotent, because either row may have gone
        # terminal underneath this worker while the files were
        # uploading: a DELETE cancels the build, the silent reaper fails
        # an idle job, and the stranded sweep fails the build behind it.
        # A strict ``fail`` on either one would raise from inside the
        # ``except`` handler, and that second exception would mask the
        # upload error, roll back the very transaction that has to close
        # the run out, and leave the run stranded — exactly the failure
        # mode this change set removes. ``exc`` stays the exception that
        # is captured and logged.
        #
        # The queue job goes first so this path takes ``queue_jobs``
        # before ``builds``, matching the reaper (which fails jobs in
        # ``fail_silent_jobs`` before locking builds in
        # ``fail_stranded_processing``) and the sibling worker paths
        # (``_mark_stale_skipped``, ``_mark_deleted_skipped``,
        # ``_close_out_retired_build``). Taking the two in the opposite
        # order is a lock-order inversion PostgreSQL resolves by
        # aborting one side.
        sentry_sdk.capture_exception(exc)
        logger.exception("Build processing failed")
        async with session.begin():
            if queue_job_id is not None:
                await queue_job_store.fail_if_active(queue_job_id)
            build_service = factory.create_build_service()
            await build_service.fail_if_unfinished(
                build_id=build_id,
                org_slug=org_slug,
                project_slug=project_slug,
            )
        return "failed", _BuildProcessedOutcome(
            success=False,
            object_count=None,
            total_size_bytes=None,
            editions_updated=0,
            editions_skipped=0,
            stale_skipped=False,
        )
    else:
        # Something retired the build while its files were uploading, so
        # it is already terminal (or gone) and must not be completed.
        if isinstance(upload, _MidUploadRetirement):
            return await _close_out_retired_build(
                session=session,
                queue_job_store=queue_job_store,
                queue_job_id=queue_job_id,
                build_id=build_id,
                status=upload.status,
                deleted=upload.deleted,
                logger=logger,
            )
        object_count, total_size_bytes = upload
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
    factory: Factory,
    queue_job_store: QueueJobStore,
    build_id: int,
    latest_build_id: int,
    org_slug: str,
    project_slug: str,
    logger: structlog.stdlib.BoundLogger,
) -> _StaleGuardOutcome:
    """Retire a superseded build and complete its QueueJob as stale.

    Operators identify these runs by ``progress["stale_skipped"]`` plus
    the dedicated log line; the QueueJob status stays ``completed``
    because nothing was wrong with the build itself — a newer build
    for the same ``(project, git_ref)`` simply took over.

    The build itself is transitioned to ``superseded`` in the *same*
    ``session.begin()`` block that completes the job, so the two commit
    or roll back together. Skipping the build without retiring it is
    what stranded rows in ``processing`` forever (#575): with no worker
    on the build and no job left to run, ``processing`` was a lie no
    later path would correct.

    The transition goes through
    :meth:`BuildService.supersede_if_unfinished` rather than the strict
    :meth:`~BuildService.supersede`, because the verdict this acts on
    was reached in a *different*, deliberately unlocked transaction
    (:func:`_guard_stale_build`) that has already committed. A DELETE, a
    lifecycle reap or the stranded-build sweep committing in that window
    leaves no edge out of the status it wrote, and the strict call would
    raise :exc:`InvalidBuildStateError` out of the transaction that has
    to complete the queue job: a Sentry event, a rolled-back job left
    ``queued``, and — for a lifecycle cancel, which stamps no
    ``date_deleted`` for the deleted-skip guard to catch — an arq retry
    that re-enters here and raises all over again (#590). When it stands
    down, the job is closed out with the retired-build progress instead,
    naming the status that was actually found; the follow-up read is
    safe because ``supersede_if_unfinished`` took the row lock and this
    transaction still holds it.

    The pickup guard runs before any of that, and a row it refuses
    (task #551) short-circuits to ``late_delivery`` with nothing written
    and nothing logged as skipped: the row is terminal or in another
    worker's hands, so this delivery has neither a stale skip to record
    nor any business retiring the build. A delivery with no
    ``queue_jobs`` row at all still counts as a recorded skip — the
    build is still superseded, there is simply no job bookkeeping to do.
    """
    async with session.begin():
        pickup = await _start_queue_job(ctx, payload, queue_job_store)
        if pickup.skipped:
            return _StaleGuardOutcome.late_delivery
        build_service = factory.create_build_service()
        superseded = await build_service.supersede_if_unfinished(
            build_id=build_id,
            org_slug=org_slug,
            project_slug=project_slug,
        )
        if superseded is None:
            build_store = factory.create_build_store()
            retired = await build_store.get_by_id(build_id)
            status = retired.status if retired is not None else None
            logger.info(
                "Build retired before its stale skip; closing its job out",
                build_id=build_id,
                latest_build_id=latest_build_id,
                build_status=status.value if status is not None else None,
            )
            outcome = _StaleGuardOutcome.retired_before_skip
            progress = _retired_build_progress(status)
        else:
            logger.info(
                "Stale build skipped",
                build_id=build_id,
                latest_build_id=latest_build_id,
            )
            outcome = _StaleGuardOutcome.stale_skipped
            progress = {
                "message": (
                    "Stale build skipped; superseded by "
                    f"build id {latest_build_id}"
                ),
                "stale_skipped": True,
                "latest_build_id": latest_build_id,
            }
        if pickup.queue_job_id is not None:
            queue_job_id = pickup.queue_job_id
            await queue_job_store.update_phase(
                queue_job_id,
                "complete",
                progress=progress,
            )
            await queue_job_store.complete(queue_job_id)
    return outcome


async def _mark_deleted_skipped(
    *,
    session: AsyncSession,
    ctx: dict[str, Any],
    payload: dict[str, Any],
    factory: Factory,
    queue_job_store: QueueJobStore,
    build_id: int,
    org_slug: str,
    project_slug: str,
    logger: structlog.stdlib.BoundLogger,
) -> _StaleGuardOutcome:
    """Retire a build deleted before processing and complete its QueueJob.

    Mirrors :func:`_mark_stale_skipped` — same pickup guard, same
    completed job, same single ``session.begin()`` block so the job
    completion and the build transition commit or roll back together —
    and differs only in what it records: ``progress["deleted_skipped"]``
    and a transition to ``cancelled`` rather than ``superseded``.

    The cancel is idempotent. ``BuildService.soft_delete`` cancels a
    non-terminal build as it deletes it, so the common ordering leaves
    this guard re-asserting a status the row already has; a row deleted
    before that behaviour existed (or by a DELETE that raced this
    worker's pre-lock read) is still ``processing`` and is the case that
    needs the transition. Either way the build must not be published,
    and nothing is downloaded, unpacked, uploaded or tracked.
    """
    async with session.begin():
        pickup = await _start_queue_job(ctx, payload, queue_job_store)
        if pickup.skipped:
            return _StaleGuardOutcome.late_delivery
        logger.info("Deleted build skipped", build_id=build_id)
        build_service = factory.create_build_service()
        await build_service.cancel(
            build_id=build_id,
            org_slug=org_slug,
            project_slug=project_slug,
        )
        if pickup.queue_job_id is not None:
            queue_job_id = pickup.queue_job_id
            await queue_job_store.update_phase(
                queue_job_id,
                "complete",
                progress={
                    "message": "Build was deleted before processing",
                    "deleted_skipped": True,
                },
            )
            await queue_job_store.complete(queue_job_id)
    return _StaleGuardOutcome.deleted_skipped


async def _mark_missing_build_skipped(
    *,
    session: AsyncSession,
    ctx: dict[str, Any],
    payload: dict[str, Any],
    queue_job_store: QueueJobStore,
    build_id: int,
    logger: structlog.stdlib.BoundLogger,
) -> _StaleGuardOutcome:
    """Close a job out whose build vanished before any work started.

    The third sibling of :func:`_mark_stale_skipped` and
    :func:`_mark_deleted_skipped`: same pickup guard, same completed
    job. It differs in having no build to transition — the row is gone,
    so there is no status to write and nothing to lock. Both siblings
    would raise on that: ``cancel`` and ``supersede`` need a row.

    The job carries the same missing-row progress
    :func:`_close_out_retired_build` writes when a row disappears
    mid-upload, because an operator reading the two is looking at the
    same event; only the amount of wasted work differs, and the point of
    guarding here is that this one wastes none.
    """
    async with session.begin():
        pickup = await _start_queue_job(ctx, payload, queue_job_store)
        if pickup.skipped:
            return _StaleGuardOutcome.late_delivery
        logger.info(
            "Build row disappeared before processing; closing its job out",
            build_id=build_id,
        )
        if pickup.queue_job_id is not None:
            queue_job_id = pickup.queue_job_id
            await queue_job_store.update_phase(
                queue_job_id,
                "complete",
                progress=_retired_build_progress(None),
            )
            await queue_job_store.complete(queue_job_id)
    return _StaleGuardOutcome.vanished_before_work


def _retired_build_progress(
    status: BuildStatus | None, *, deleted: bool = False
) -> dict[str, Any]:
    """Describe a build retired underneath the worker, for its job.

    Shared by the two paths that find somebody else has already written
    a terminal status: :func:`_close_out_retired_build`, once the files
    are uploaded, and :func:`_mark_stale_skipped`, when the retirement
    lands between the stale verdict and the skip that acts on it.

    Every case names the status that was actually found, both in the
    human-readable message and in ``retired_status``, so an operator
    reading the job does not have to guess which of the several possible
    retirements happened.

    A vanished row is named too, as
    :attr:`~docverse.models.RetiredBuildStatus.missing` rather than
    ``None``. ``None`` made that outcome unrepresentable: it is a
    *declared* field on
    :class:`~docverse.models.BuildProcessingProgress`, whose
    drop-``None`` serializer strips every key it left unset, so the
    vanished case served the same payload as a job that was never
    retired at all. ``missing`` is not a ``BuildStatus`` — there is no
    row to write it to, and that enum is persisted behind the
    ``builds_status_check`` constraint — which is why
    ``RetiredBuildStatus`` exists alongside it.

    The ``cancelled`` case keeps its original message *and* the
    ``deleted_skipped`` flag the DELETE path established, because that
    flag is an existing operator contract; the statuses that only became
    reachable here (review of PR #583, finding f2) get no flag of their
    own, since ``retired_status`` already tells them apart.
    """
    was_deleted = deleted or status is BuildStatus.cancelled
    if status is None:
        message = "Build row disappeared while it was processing"
    elif was_deleted:
        message = "Build was deleted while it was processing"
    else:
        message = f"Build was {status.value} while it was processing"
    retired_status = (
        RetiredBuildStatus.missing
        if status is None
        else RetiredBuildStatus(status.value)
    )
    progress: dict[str, Any] = {
        "message": message,
        "retired_status": retired_status.value,
    }
    if was_deleted:
        progress["deleted_skipped"] = True
    return progress


async def _close_out_retired_build(
    *,
    session: AsyncSession,
    queue_job_store: QueueJobStore,
    queue_job_id: int | None,
    build_id: int,
    status: BuildStatus | None,
    deleted: bool = False,
    logger: structlog.stdlib.BoundLogger,
) -> tuple[str, _BuildProcessedOutcome | None]:
    """Close out a build retired while its files were uploading.

    The late sibling of :func:`_mark_deleted_skipped`. That guard runs
    before any work and can still retire the build itself; by the time
    this runs somebody else has already written the terminal status — a
    DELETE cancelling the row on its way out
    (:meth:`BuildService.soft_delete`), the stranded-build sweep failing
    it, a supersession, or a purge that removed the row entirely — so
    all that is left is bookkeeping: record the skip on the queue job
    and complete it. The build keeps the status it was given, edition
    tracking never runs, and nothing is published.

    *Every* non-``processing`` outcome comes here rather than down the
    error path, because none of them is a crash. Letting a retired build
    reach the completion would raise ``InvalidBuildStateError`` from
    inside the worker's transaction, and the error path would then
    report a deliberate retirement as a failure: a Sentry event and a
    ``build_processed(success=False)`` metric for a build nobody wanted
    published (review of PR #583, finding f2). That path closes its
    rows out idempotently now, so it would no longer *break* on a
    retired build — it would simply be lying about one.

    The commonest way to arrive here with ``failed`` is the reaper, and
    it is worth naming because it discards real work. The stranded-build
    sweep has no heartbeat from this worker: an upload still running
    after ``build_processing_reaper_threshold_seconds`` (8 hours by
    default) has its queue job reaped as silent and its build failed
    behind it, and when the upload finally lands the row is already
    terminal, so everything just uploaded is dropped on the floor here.
    That is deliberate. A build that takes hours to unpack and upload is
    a wedged worker far more often than an honest one, and the threshold
    is where the operator says so; publishing past it — hours after the
    job was declared dead, onto editions that have moved on since —
    would be worse than discarding it. An upload that legitimately needs
    longer wants the threshold raised, not this guard softened.

    The uploaded objects are left where they landed: everything already
    written under the build's ``storage_prefix``, plus the
    ``staging_key`` tarball — a whole unpacked tree, where the pre-work
    guard (:func:`_mark_deleted_skipped`) strands only the tarball.
    Nothing in the tree reclaims them yet. They are orphaned until the
    ``purgatory_cleanup`` job tracked in DM-54691 (SQR-112, "Soft delete
    and purgatory") hard-deletes a soft-deleted build's objects once the
    organization's ``purgatory_retention`` has elapsed; do not go
    looking for that purge here. Deleting them from the worker instead
    would be worse: a cancelled build is soft-deleted, not gone, and
    stays restorable right up until that purge runs.

    Reports the pickup guards' recorded-skip metric shape: to an
    operator this is the same event, a build deliberately retired rather
    than one that failed.
    """
    logger.info(
        "Build retired during processing; closing its job out",
        build_id=build_id,
        build_status=status.value if status is not None else None,
        deleted=deleted,
    )
    if queue_job_id is not None:
        async with session.begin():
            job = await queue_job_store.get_for_update(queue_job_id)
            if job is None or job.status is not JobStatus.in_progress:
                # Whatever retired the build usually retired this row
                # first — the stranded sweep only fails builds whose job
                # is no longer live, so the silent reaper got here
                # before it. Completing a row a reaper already failed
                # raises InvalidJobStateError out of the very
                # transaction that exists to close the job out, so the
                # reaper's verdict and its postmortem trail stand
                # untouched.
                logger.info(
                    "Queue job already terminal; leaving it as it stands",
                    queue_job_id=queue_job_id,
                    job_status=job.status.value if job is not None else None,
                )
            else:
                await queue_job_store.update_phase(
                    queue_job_id,
                    "complete",
                    progress=_retired_build_progress(status, deleted=deleted),
                )
                await queue_job_store.complete(queue_job_id)
    return _stale_guard_result(_StaleGuardOutcome.retired_mid_upload)


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

    Re-reads the build before tracking it, because the row can go
    terminal between the worker's completion write and this call: a
    DELETE takes no BUILD_PROCESSING lock, and
    :meth:`BuildStore.transition_status` is a plain read-then-write, so
    either UPDATE can be the one that survives. A row that came out of
    that race soft-deleted, or carrying any status but ``completed``, is
    one nobody should publish — and ``get_by_id`` does not filter
    ``date_deleted``, so tracking would otherwise move the edition
    pointer onto it and enqueue a ``publish_edition`` job for a build
    that will never be served.

    Such a build is skipped rather than treated as an error: an empty
    result (not ``None``) so the caller closes the queue job out
    normally, since nothing about this run failed.

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
            build = await build_store.get_by_id(build_id)
            if build is None:
                msg = f"Build {build_id} vanished after completion"
                raise RuntimeError(msg)
            deleted = build.date_deleted is not None
            if deleted or build.status is not BuildStatus.completed:
                logger.info(
                    "Skipping edition tracking for a retired build",
                    build_id=build_id,
                    build_status=build.status.value,
                    deleted=deleted,
                )
                return EditionTrackingResult(
                    derived_slug=None, suppressed=False
                )
            tracking_service = factory.create_edition_tracking_service()
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
) -> tuple[int, int] | _MidUploadRetirement:
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
    tuple of int, int or _MidUploadRetirement
        The number of objects uploaded and the total size in bytes, or
        a :class:`_MidUploadRetirement` naming the status found when
        somebody retired the build while those uploads were in flight,
        so it must not be completed (see
        :func:`_close_out_retired_build`).
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

    # Nothing that retires a ``processing`` build takes the
    # BUILD_PROCESSING lock, so the row can go terminal at any point
    # during the download and uploads above: a DELETE cancels it, the
    # stranded-build sweep fails it, a supersession retires it, or a
    # purge removes it outright. Re-read it before writing anything
    # terminal of our own — completing a retired build would publish
    # work somebody decided to drop, and the transition would raise
    # InvalidBuildStateError inside the transaction that still has to
    # close out the queue job (#575).
    #
    # The test is "was this retired out from under us", not "was it
    # cancelled": ``cancelled``, ``failed`` and ``superseded`` each mean
    # a different actor already had the last word, and treating those as
    # crashes reports a deliberate retirement as a bug (review of PR
    # #583, finding f2). ``date_deleted`` counts too, whatever the
    # status column says: ``BuildStore.soft_delete`` stamps it without
    # the cancel ``BuildService.soft_delete`` pairs with it, and
    # ``get_for_update`` does not filter deleted rows, so a row that is
    # deleted but still reads ``processing`` would otherwise be
    # published and have its staging tarball — the deleted build's only
    # route back — deleted along the way.
    #
    # ``failed`` here is usually the reaper's: a build whose upload runs
    # past ``build_processing_reaper_threshold_seconds`` (8 h by
    # default) has its queue job reaped and its build failed while this
    # worker is still uploading, and the finished upload is then
    # discarded here. That is the accepted trade — see
    # :func:`_close_out_retired_build` — not a bug to work around by
    # publishing late.
    #
    # Anything else — ``pending``, or a ``completed`` this worker did
    # not write — is nobody's retirement, so it raises rather than being
    # filed as one. A ``pending`` row closed out quietly would be worse
    # than a Sentry event: ``fail_stranded_processing`` sweeps only
    # ``processing``, so nothing would ever move it again.
    #
    # Locked, and in the same transaction as the completion below, so
    # the guard and the write cannot straddle somebody else's commit:
    # this blocks behind a retirement that is mid-flight and then sees
    # it, and one arriving after this point blocks behind the completion
    # and stands down. Either way exactly one terminal status survives
    # (review of PR #583, finding f1).
    current = await build_store.get_for_update(build_id=build.id)
    deleted = current is not None and current.date_deleted is not None
    if current is None or deleted or current.status in _RETIRED_BUILD_STATUSES:
        logger.info(
            "Build retired during processing; skipping completion",
            build_status=current.status.value if current is not None else None,
            deleted=deleted,
            object_count=object_count,
            total_size_bytes=total_size,
        )
        return _MidUploadRetirement(
            status=current.status if current is not None else None,
            deleted=deleted,
        )
    if current.status is not BuildStatus.processing:
        # Not a retirement: nothing in the tree writes ``pending`` or
        # ``completed`` onto a build a worker is uploading for, so this
        # is a bug or an out-of-band enqueue. Closing it out quietly
        # would file it as a deliberate retirement — no Sentry event,
        # and a ``pending`` row left outside every sweep's reach, since
        # ``fail_stranded_processing`` matches only ``processing``.
        raise InvalidBuildStateError(
            current_state=current.status.value,
            target_state=BuildStatus.completed.value,
            build_public_id=serialize_base32_id(current.public_id),
            org_slug=org_slug,
            project_slug=project_slug,
            message=(
                f"Build {serialize_base32_id(current.public_id)} was "
                f"{current.status.value!r}, not 'processing', when its "
                "upload finished"
            ),
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
