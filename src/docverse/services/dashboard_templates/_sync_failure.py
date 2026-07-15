"""Shared DB-write helper for dashboard-template sync failures."""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING

from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.queue_job_store import QueueJobStore

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

__all__ = ["mark_dashboard_sync_failed"]


async def mark_dashboard_sync_failed(
    *,
    session: AsyncSession,
    binding_store: DashboardGitHubTemplateBindingStore,
    binding_id: int,
    exc: BaseException,
    error_message: str,
    queue_job_store: QueueJobStore | None = None,
    queue_job_id: int | None = None,
) -> None:
    """Mark a dashboard sync as failed in the database.

    Always flips the binding to ``last_sync_status="failed"`` with
    ``error_message`` as the ``last_sync_error``. When both
    ``queue_job_store`` and ``queue_job_id`` are supplied, additionally
    fails the queue-job row with the exception type and traceback. Each
    DB write opens its own ``session.begin()`` block, matching the
    existing call-site style at the worker and enqueue paths.

    Lives one layer up from ``worker/functions/dashboard_sync.py`` so
    ``services/dashboard_templates/enqueue.py`` can import it without
    pulling the worker-functions package (and its factory-loop import
    chain) into the service layer.
    """
    async with session.begin():
        await binding_store.update_sync_state(
            binding_id=binding_id,
            last_sync_status="failed",
            last_sync_error=error_message,
        )
    if queue_job_store is not None and queue_job_id is not None:
        async with session.begin():
            await queue_job_store.fail(
                queue_job_id,
                errors={
                    "message": str(exc),
                    "type": type(exc).__name__,
                    "traceback": traceback.format_exc(),
                },
            )
