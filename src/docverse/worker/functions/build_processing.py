"""Build processing worker function.

Downloads a staged tarball, unpacks it, uploads files to the
object store under the ``__builds/{build_id}/`` prefix, and
updates editions that track the build's git ref.
"""

from __future__ import annotations

import asyncio
import io
import mimetypes
import tarfile
from typing import Any

import httpx
import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import BuildStatus
from docverse.domain.build import Build
from docverse.domain.edition_tracking import EditionTrackingResult
from docverse.exceptions import NotFoundError
from docverse.factory import WorkerFactory
from docverse.services.credential_encryptor import CredentialEncryptor
from docverse.storage.build_store import BuildStore
from docverse.storage.objectstore import ObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_job_store import QueueJobStore

#: Maximum number of concurrent upload tasks.
_UPLOAD_CONCURRENCY = 50


async def build_processing(
    ctx: dict[str, Any], payload: dict[str, Any]
) -> str:
    """Process a build: download tarball, unpack, upload files.

    Parameters
    ----------
    ctx
        arq worker context (contains encryptor).
    payload
        Job payload with ``org_id``, ``project_id``, ``build_id``.

    Returns
    -------
    str
        A status message.
    """
    logger = structlog.get_logger("docverse.worker.build_processing")
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

    encryptor: CredentialEncryptor = ctx["encryptor"]
    http_client: httpx.AsyncClient = ctx["http_client"]

    async for session in db_session_dependency():
        factory = WorkerFactory(
            session=session,
            logger=logger,
            credential_encryptor=encryptor,
            http_client=http_client,
        )
        build_store = BuildStore(session=session, logger=logger)
        org_store = OrganizationStore(session=session, logger=logger)
        queue_job_store = QueueJobStore(session=session, logger=logger)

        # Phase 1: Load metadata and mark QueueJob as in_progress
        async with session.begin():
            build = await build_store.get_by_id(build_id)
            if build is None:
                msg = f"Build {build_id} not found"
                raise NotFoundError(msg)

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

            queue_job_id = await _start_queue_job(ctx, queue_job_store)
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
                    logger=logger,
                )
        except Exception:
            # Phase 3a: Mark build and queue job as failed
            logger.exception("Build processing failed")
            async with session.begin():
                build_service = factory.create_build_service()
                await build_service.fail(build_id=build_id)
                if queue_job_id is not None:
                    await queue_job_store.fail(queue_job_id)
            return "failed"
        else:
            await _finalize_success(
                session=session,
                factory=factory,
                build_store=build_store,
                queue_job_store=queue_job_store,
                build_id=build_id,
                queue_job_id=queue_job_id,
                object_count=object_count,
                total_size_bytes=total_size_bytes,
                logger=logger,
            )
            return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _start_queue_job(
    ctx: dict[str, Any],
    queue_job_store: QueueJobStore,
) -> int | None:
    """Look up and start the QueueJob for this arq job.

    Returns the queue job's internal ID, or ``None`` if no matching
    QueueJob exists.
    """
    arq_job_id: str | None = ctx.get("job_id")
    if arq_job_id is None:
        return None
    queue_job = await queue_job_store.get_by_backend_job_id(arq_job_id)
    if queue_job is None:
        return None
    await queue_job_store.start(queue_job.id)
    return queue_job.id


async def _finalize_success(  # noqa: PLR0913
    *,
    session: async_scoped_session[AsyncSession],
    factory: WorkerFactory,
    build_store: BuildStore,
    queue_job_store: QueueJobStore,
    build_id: int,
    queue_job_id: int | None,
    object_count: int,
    total_size_bytes: int,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Run edition tracking and mark the queue job complete.

    Edition tracking failures are logged but do not fail the build.
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

    # Phase 4: Mark queue job as complete
    if queue_job_id is not None:
        progress: dict[str, object] = {
            "message": "Build processing complete",
            "object_count": object_count,
            "total_size_bytes": total_size_bytes,
        }
        if tracking_result is not None:
            progress["editions_updated"] = [
                {"slug": o.slug, "action": o.action}
                for o in tracking_result.updated
            ]
            progress["editions_skipped"] = [
                {"slug": o.slug} for o in tracking_result.skipped
            ]
        has_errors = tracking_result is None
        if has_errors:
            progress["edition_tracking_error"] = True
        async with session.begin():
            await queue_job_store.update_phase(
                queue_job_id, "complete", progress=progress
            )
            await queue_job_store.complete(queue_job_id, has_errors=has_errors)
    logger.info("Build processing completed")


async def _track_editions(  # noqa: PLR0913
    *,
    session: async_scoped_session[AsyncSession],
    factory: WorkerFactory,
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
                raise RuntimeError(msg)  # noqa: TRY301
            tracking_result = await tracking_service.track_build(build)
        logger.info(
            "Edition tracking complete",
            derived_slug=tracking_result.derived_slug,
            suppressed=tracking_result.suppressed,
            editions_updated=len(tracking_result.updated),
            editions_skipped=len(tracking_result.skipped),
        )
    except Exception:
        logger.exception("Edition tracking failed")
        return None
    else:
        return tracking_result


async def _process_build(
    *,
    object_store: ObjectStore,
    build: Build,
    build_store: BuildStore,
    logger: structlog.stdlib.BoundLogger,
) -> tuple[int, int]:
    """Download, unpack, and upload build files.

    Returns
    -------
    tuple of int, int
        The number of objects uploaded and the total size in bytes.
    """
    logger.info(
        "Downloading staging tarball",
        staging_key=build.staging_key,
    )
    # TODO(DM-54426): Full tarball loaded  # noqa: TD003, FIX002
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
    # TODO(DM-54426): All extracted files  # noqa: TD003, FIX002
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
            task = asyncio.create_task(_upload_file(name, file_data))
            tasks.append(task)

    results = await asyncio.gather(*tasks)
    object_count = len(results)
    total_size = sum(results)

    if object_count == 0:
        logger.warning(
            "Tarball contained no extractable files",
            staging_key=build.staging_key,
        )

    logger.info(
        "Upload complete",
        object_count=object_count,
        total_size_bytes=total_size,
    )

    await build_store.update_inventory(
        build_id=build.id,
        object_count=object_count,
        total_size_bytes=total_size,
    )

    await build_store.transition_status(
        build_id=build.id, new_status=BuildStatus.completed
    )

    try:
        await object_store.delete_object(key=build.staging_key)
        logger.info("Deleted staging tarball", staging_key=build.staging_key)
    except Exception:  # noqa: BLE001
        logger.warning(
            "Failed to delete staging tarball",
            staging_key=build.staging_key,
            exc_info=True,
        )

    return object_count, total_size
