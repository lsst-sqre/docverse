"""Enqueue helper for the ``project_github_resolve`` worker.

Project create / update handlers call
:func:`try_enqueue_project_github_resolve_by_id` outside the request
transaction to fire off the opportunistic
``installation_id`` / ``owner_id`` / ``repo_id`` resolution PRD #346
introduces. The helper is a thin parallel to ``dashboard.enqueue
.try_enqueue_dashboard_build_by_slug``: it owns its own transaction so
the caller's flow is never broken by an enqueue failure, and it skips
the enqueue cleanly when the project has no GitHub binding so the
worker queue stays signal-only.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sentry_sdk
import structlog

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from docverse.factory import Factory

__all__ = ["try_enqueue_project_github_resolve_by_id"]


async def try_enqueue_project_github_resolve_by_id(
    *,
    factory: Factory,
    session: AsyncSession,
    logger: structlog.stdlib.BoundLogger,
    project_id: int,
) -> None:
    """Enqueue one ``project_github_resolve`` job in its own transaction.

    Exceptions are logged but never re-raised, so the caller's flow is
    not broken by an enqueue failure. The enqueue runs in a freshly
    started transaction on ``session`` — the caller must have already
    committed any work it wants persisted.

    Skipped when the project has no GitHub binding (no
    ``github_owner`` / ``github_repo``): the worker would just return
    ``"skipped"`` after a single DB read, and avoiding the enqueue
    keeps the queue signal-only.
    """
    try:
        async with session.begin():
            project_store = factory.create_project_store()
            project = await project_store.get_by_id(project_id)
            if (
                project is None
                or project.github_owner is None
                or project.github_repo is None
            ):
                return
            queue_backend = factory.create_queue_backend()
            await queue_backend.enqueue(
                "project_github_resolve",
                {"project_id": project_id},
            )
            await session.commit()
    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        logger.exception(
            "Failed to enqueue project_github_resolve",
            project_id=project_id,
        )
