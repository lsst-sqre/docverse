"""Best-effort helpers that enqueue ``dashboard_build`` jobs.

Dashboard re-renders fire as side effects of lifecycle events (edition
create/update/delete/rollback, project update, publish_edition success).
The triggering flow must complete successfully even if the enqueue
fails — e.g. Redis is momentarily unavailable or the org/project has
been concurrently deleted. This module wraps
:class:`DashboardPublishingService` with try/except so callers can fire
and forget.
"""

from __future__ import annotations

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.factory import Factory

__all__ = [
    "try_enqueue_dashboard_build_by_id",
    "try_enqueue_dashboard_build_by_slug",
]


async def try_enqueue_dashboard_build_by_slug(
    *,
    factory: Factory,
    session: AsyncSession,
    logger: structlog.stdlib.BoundLogger,
    org_slug: str,
    project_slug: str,
) -> None:
    """Enqueue one ``dashboard_build`` job in its own transaction.

    Exceptions are logged but never re-raised, so the caller's flow is
    not broken by an enqueue failure. The enqueue runs in a freshly
    started transaction on ``session`` — the caller must have already
    committed any work it wants persisted.
    """
    try:
        async with session.begin():
            service = factory.create_dashboard_publishing_service()
            await service.enqueue_for_project_slug(
                org_slug=org_slug, project_slug=project_slug
            )
            await session.commit()
    except Exception:
        logger.exception(
            "Failed to enqueue dashboard_build",
            org_slug=org_slug,
            project_slug=project_slug,
        )


async def try_enqueue_dashboard_build_by_id(
    *,
    factory: Factory,
    session: AsyncSession,
    logger: structlog.stdlib.BoundLogger,
    org_id: int,
    project_id: int,
) -> None:
    """ID-based variant of :func:`try_enqueue_dashboard_build_by_slug`."""
    try:
        async with session.begin():
            service = factory.create_dashboard_publishing_service()
            await service.enqueue_for_project(
                org_id=org_id, project_id=project_id
            )
            await session.commit()
    except Exception:
        logger.exception(
            "Failed to enqueue dashboard_build",
            org_id=org_id,
            project_id=project_id,
        )
