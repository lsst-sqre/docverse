"""Publish an edition to the CDN.

Independently retryable arq worker function that syncs a single
edition's current build to its organization's configured CDN. The job
resolves its CDN configuration entirely from the database so retries
work without external context (per SQR-112 user story 12).
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Any

import httpx
import structlog
from safir.arq import ArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models.queue_enums import PublishStatus
from docverse.config import Configuration
from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistory
from docverse.exceptions import NotFoundError
from docverse.factory import Factory
from docverse.services.credential_encryptor import CredentialEncryptor
from docverse.services.dashboard_trigger import (
    try_enqueue_dashboard_build_by_id,
)
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore

config = Configuration()


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
        arq worker context.
    payload
        Job payload with ``org_id``, ``project_slug``, ``edition_id``,
        ``edition_slug``, ``build_id``, ``build_public_id``, and
        ``queue_job_id``.

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
        queue_job_id=payload["queue_job_id"],
    )

    encryptor: CredentialEncryptor = ctx["encryptor"]
    http_client: httpx.AsyncClient = ctx["http_client"]
    arq_queue: ArqQueue | None = ctx.get("arq_queue")
    queue_job_id: int = payload["queue_job_id"]

    async for session in db_session_dependency():
        factory = Factory(
            session=session,
            logger=logger,
            credential_encryptor=encryptor,
            http_client=http_client,
            arq_queue=arq_queue,
            default_queue_name=config.arq_queue_name,
        )
        edition_store = EditionStore(session=session, logger=logger)
        history_store = EditionBuildHistoryStore(
            session=session, logger=logger
        )
        queue_job_store = QueueJobStore(session=session, logger=logger)

        async with session.begin():
            resources = await _load_resources(
                session=session, logger=logger, payload=payload
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
            logger.exception("Edition publish failed")
            async with session.begin():
                await _mark_failed(
                    edition_store=edition_store,
                    history_store=history_store,
                    queue_job_store=queue_job_store,
                    resources=resources,
                    queue_job_id=queue_job_id,
                    exc=exc,
                )
            return "failed"
        async with session.begin():
            await queue_job_store.complete(queue_job_id)
        logger.info("Edition publish completed")
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
    session: AsyncSession,
    logger: structlog.stdlib.BoundLogger,
    payload: dict[str, Any],
) -> _PublishResources:
    """Load project, edition, build, and history entry for the job."""
    project_store = ProjectStore(session=session, logger=logger)
    edition_store = EditionStore(session=session, logger=logger)
    build_store = BuildStore(session=session, logger=logger)
    history_store = EditionBuildHistoryStore(session=session, logger=logger)

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
