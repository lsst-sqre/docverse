"""Dashboard build worker function.

Renders one project's dashboard artifacts (HTML + switcher JSON in the
MVP slice) and uploads them to the project's object store.
"""

from __future__ import annotations

import traceback
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog
from safir.arq import ArqQueue
from safir.dependencies.db_session import db_session_dependency

from docverse.config import Configuration
from docverse.exceptions import NotFoundError
from docverse.factory import Factory
from docverse.services.credential_encryptor import CredentialEncryptor
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_job_store import QueueJobStore

config = Configuration()


async def dashboard_build(ctx: dict[str, Any], payload: dict[str, Any]) -> str:
    """Render and publish one project's dashboard.

    Parameters
    ----------
    ctx
        arq worker context (encryptor, http_client, queue).
    payload
        Job payload with ``org_id``, ``org_slug``, ``project_id``,
        ``project_slug``, ``queue_job_id``, ``queue_job_public_id``.

    Returns
    -------
    str
        ``"completed"`` on success or ``"failed"`` if rendering raised.
    """
    logger = structlog.get_logger("docverse.worker.dashboard_build").bind(
        org=payload["org_slug"],
        project=payload["project_slug"],
        queue_job_id=payload["queue_job_public_id"],
    )
    org_id: int = payload["org_id"]
    project_id: int = payload["project_id"]
    queue_job_id: int = payload["queue_job_id"]

    encryptor: CredentialEncryptor = ctx["encryptor"]
    http_client: httpx.AsyncClient = ctx["http_client"]
    arq_queue: ArqQueue | None = ctx.get("arq_queue")

    async for session in db_session_dependency():
        factory = Factory(
            session=session,
            logger=logger,
            credential_encryptor=encryptor,
            http_client=http_client,
            arq_queue=arq_queue,
            default_queue_name=config.arq_queue_name,
        )
        queue_job_store = QueueJobStore(session=session, logger=logger)
        org_store = OrganizationStore(session=session, logger=logger)

        async with session.begin():
            await queue_job_store.start(queue_job_id)
            await queue_job_store.update_phase(
                queue_job_id,
                "rendering",
                progress={"message": "Rendering dashboard artifacts"},
            )

        try:
            async with session.begin():
                org = await org_store.get_by_id(org_id)
                if org is None:
                    msg = f"Organization {org_id} not found"
                    raise NotFoundError(msg)  # noqa: TRY301
                service_label = org.resolved_staging_store_label
                if service_label is None:
                    msg = (
                        f"No object store service configured for org {org_id}"
                    )
                    raise RuntimeError(msg)  # noqa: TRY301

                publisher = factory.create_dashboard_publisher(config=config)
                rendered_at = datetime.now(tz=UTC)
                context = await publisher.build_context(
                    org_id=org_id,
                    project_id=project_id,
                    rendered_at=rendered_at,
                )
                object_store = await factory.create_objectstore_for_org(
                    org_id=org_id, service_label=service_label
                )

            async with session.begin():
                await queue_job_store.update_phase(
                    queue_job_id,
                    "uploading",
                    progress={
                        "message": "Uploading dashboard artifacts",
                        "object_count": 0,
                    },
                )
            async with object_store:
                progress = await publisher.render_and_upload(
                    context=context, object_store=object_store
                )
        except Exception as exc:
            logger.exception("Dashboard build failed")
            async with session.begin():
                await queue_job_store.fail(
                    queue_job_id,
                    errors={
                        "message": str(exc),
                        "type": type(exc).__name__,
                        "traceback": traceback.format_exc(),
                    },
                )
            return "failed"

        async with session.begin():
            await queue_job_store.update_phase(
                queue_job_id,
                "complete",
                progress={
                    "message": "Dashboard build complete",
                    "object_count": progress.object_count,
                    "total_size_bytes": progress.total_size_bytes,
                    "rendered_at": context.rendered_at.isoformat(),
                },
            )
            await queue_job_store.complete(queue_job_id)
        logger.info("Dashboard build completed")
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)
