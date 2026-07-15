"""Dashboard build worker function.

Renders one project's dashboard artifacts (HTML + switcher JSON in the
MVP slice) and uploads them to the project's object store.
"""

from __future__ import annotations

import time
import traceback
from datetime import UTC, datetime, timedelta
from typing import Any

import sentry_sdk
import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse.exceptions import NotFoundError
from docverse.metrics import DashboardBuiltEvent
from docverse.services.lock_service import LockKey


async def dashboard_build(ctx: dict[str, Any], payload: dict[str, Any]) -> str:
    """Render and publish one project's dashboard.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``, ``http_client``,
        ``arq_queue``).
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

    started = time.monotonic()
    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        org_store = factory.create_org_store()
        lock_service = factory.create_lock_service()

        lock_key = LockKey.for_project(org_id=org_id, project_id=project_id)
        async with lock_service.acquire(lock_key):
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
                        raise NotFoundError(msg)
                    service_label = org.resolved_staging_store_label
                    if service_label is None:
                        msg = (
                            f"No object store service configured for "
                            f"org {org_id}"
                        )
                        raise RuntimeError(msg)

                    publisher = factory.create_dashboard_publisher()
                    rendered_at = datetime.now(tz=UTC)
                    context = await publisher.build_context(
                        org_id=org_id,
                        project_id=project_id,
                        rendered_at=rendered_at,
                    )
                    object_store = await factory.create_objectstore_for_org(
                        org_id=org_id, service_label=service_label
                    )
                    # Preload the template source in the same short
                    # transaction so the upload loop below runs with no
                    # open DB transaction — GitHub-backed sources cache
                    # their bytes at resolve time.
                    resolved = await publisher.resolve_template(
                        org_id=org_id, project_id=project_id
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
                        context=context,
                        object_store=object_store,
                        resolved=resolved,
                    )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
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
                # Publish after the failed transition commits. Best-effort:
                # production runs raise_on_error=False so a metrics outage
                # never fails the build (no defensive try/except).
                await _publish_dashboard_built(
                    ctx=ctx,
                    org_slug=payload["org_slug"],
                    project_slug=payload["project_slug"],
                    success=False,
                    object_count=None,
                    total_size_bytes=None,
                    started=started,
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
            # Publish after the terminal transition commits (same
            # best-effort rationale as the failure branch above).
            await _publish_dashboard_built(
                ctx=ctx,
                org_slug=payload["org_slug"],
                project_slug=payload["project_slug"],
                success=True,
                object_count=progress.object_count,
                total_size_bytes=progress.total_size_bytes,
                started=started,
            )
            return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _publish_dashboard_built(
    *,
    ctx: dict[str, Any],
    org_slug: str,
    project_slug: str,
    success: bool,
    object_count: int | None,
    total_size_bytes: int | None,
    started: float,
) -> None:
    """Emit one ``dashboard_built`` metric for a finished dashboard build."""
    events = ctx.get("events")
    if events is None:
        return
    await events.dashboard_built.publish(
        DashboardBuiltEvent(
            organization=org_slug,
            project=project_slug,
            success=success,
            object_count=object_count,
            total_size_bytes=total_size_bytes,
            elapsed=timedelta(seconds=time.monotonic() - started),
        )
    )
