"""Build processing worker function.

Downloads a staged tarball, unpacks it, and uploads files to the
object store under the ``__builds/{build_id}/`` prefix.
"""

from __future__ import annotations

import asyncio
import io
import mimetypes
import tarfile
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse.client.models import BuildStatus
from docverse.domain.build import Build
from docverse.exceptions import NotFoundError
from docverse.factory import WorkerFactory
from docverse.services.credential_encryptor import CredentialEncryptor
from docverse.storage.build_store import BuildStore
from docverse.storage.organization_store import OrganizationStore

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
    project_id: int = payload["project_id"]
    build_id: int = payload["build_id"]
    logger = logger.bind(
        org_id=org_id,
        project_id=project_id,
        build_id=build_id,
    )

    encryptor: CredentialEncryptor = ctx["encryptor"]

    async for session in db_session_dependency():
        async with session.begin():
            factory = WorkerFactory(
                session=session,
                logger=logger,
                credential_encryptor=encryptor,
            )
            build_store = BuildStore(session=session, logger=logger)
            org_store = OrganizationStore(session=session, logger=logger)

            build = await build_store.get_by_id(build_id)
            if build is None:
                msg = f"Build {build_id} not found"
                raise NotFoundError(msg)

            org = await org_store.get_by_id(org_id)
            if org is None:
                msg = f"Organization {org_id} not found"
                raise NotFoundError(msg)

            credential_label = (
                org.staging_credential_label or org.publishing_credential_label
            )
            if credential_label is None:
                msg = f"No object store credential configured for org {org_id}"
                raise RuntimeError(msg)

            try:
                object_store = await factory.create_objectstore_for_org(
                    org_id=org_id, credential_label=credential_label
                )
                async with object_store:
                    await _process_build(
                        object_store=object_store,
                        build=build,
                        build_store=build_store,
                        logger=logger,
                    )
                await session.commit()
                logger.info("Build processing completed")
            except Exception:
                logger.exception("Build processing failed")
                await session.rollback()
                async with session.begin():
                    build_service = factory.create_build_service()
                    await build_service.fail(build_id=build_id)
                    await session.commit()
                return "failed"
            else:
                return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _process_build(
    *,
    object_store: Any,
    build: Build,
    build_store: BuildStore,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Download, unpack, and upload build files."""
    logger.info(
        "Downloading staging tarball",
        staging_key=build.staging_key,
    )
    tarball_data = await object_store.download_object(key=build.staging_key)

    build_prefix = f"__builds/{build.id}/"
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
    with tarfile.open(fileobj=io.BytesIO(tarball_data), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            file_data = f.read()
            task = asyncio.create_task(_upload_file(member.name, file_data))
            tasks.append(task)

    results = await asyncio.gather(*tasks)
    object_count = len(results)
    total_size = sum(results)

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

    await object_store.delete_object(key=build.staging_key)
    logger.info(
        "Deleted staging tarball",
        staging_key=build.staging_key,
    )
