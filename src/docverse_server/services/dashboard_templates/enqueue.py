"""Service that enqueues ``dashboard_sync`` jobs."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sentry_sdk
import structlog

from docverse.models.queue_enums import JobKind
from docverse_server.domain.queue import QueueJob
from docverse_server.exceptions import NotFoundError
from docverse_server.services.queue_dispatch import QueueDispatcher
from docverse_server.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
)
from docverse_server.storage.queue_job_store import QueueJobStore

from ._sync_failure import mark_dashboard_sync_failed

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from docverse_server.factory import Factory

__all__ = [
    "DashboardSyncEnqueuer",
    "try_enqueue_dashboard_sync",
]


class DashboardSyncEnqueuer:
    """Create the ``QueueJob`` row and enqueue a ``dashboard_sync`` arq job.

    Mirrors
    :class:`docverse_server.services.dashboard.enqueue.DashboardBuildEnqueuer`
    at the enqueue layer — the heavy work (GitHub fetch, upsert,
    fan-out) lives in the worker function.
    """

    def __init__(
        self,
        *,
        binding_store: DashboardGitHubTemplateBindingStore,
        dispatcher: QueueDispatcher,
        queue_job_store: QueueJobStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._binding_store = binding_store
        self._dispatcher = dispatcher
        self._queue_job_store = queue_job_store
        self._logger = logger

    async def enqueue(self, binding_id: int) -> QueueJob:
        """Enqueue one ``dashboard_sync`` job for a binding.

        Raises
        ------
        NotFoundError
            If the binding cannot be loaded.
        """
        binding = await self._binding_store.get_by_id(binding_id)
        if binding is None:
            msg = f"Dashboard template binding {binding_id} not found"
            raise NotFoundError(msg)

        queue_job = await self._queue_job_store.create(
            kind=JobKind.dashboard_sync,
            org_id=binding.org_id,
            project_id=binding.project_id,
        )
        # Back-point the binding at the freshly-created queue job so an
        # operator who reads ``last_sync_status="failed"`` can click
        # straight through to the traceback. Runs in the same
        # transaction as the queue-job insert, so the two rows land
        # together or not at all.
        await self._binding_store.set_last_sync_queue_job(
            binding_id=binding.id, queue_job_id=queue_job.id
        )
        self._dispatcher.defer(
            queue_job=queue_job,
            job_type="dashboard_sync",
            payload={"binding_id": binding.id},
        )
        return queue_job


async def try_enqueue_dashboard_sync(
    *,
    factory: Factory,
    session: AsyncSession,
    logger: structlog.stdlib.BoundLogger,
    binding_id: int,
) -> QueueJob | None:
    """Enqueue one ``dashboard_sync`` job in its own transaction.

    Returns the freshly-created :class:`QueueJob` on success so the
    caller can surface ``last_sync_job_url`` in its response.
    Returns ``None`` on any failure — exceptions are logged but never
    re-raised, so the caller's flow (typically a binding PUT handler)
    is not broken by an enqueue failure.

    The ``queue_jobs`` row is written in a freshly started transaction
    on ``session`` — the caller must have already committed the binding
    write it wants persisted — and the arq enqueue follows that
    transaction's commit, never precedes it (task #550).

    If the enqueue fails, a second transaction flips the binding's
    ``last_sync_status`` to ``"failed"`` with a descriptive
    ``last_sync_error``. That way the row does not sit in ``"pending"``
    forever after a silent enqueue drop — operators see the failure by
    reading the binding, and the existing force-sync endpoint is the
    recovery path.
    """
    try:
        async with session.begin():
            service = factory.create_dashboard_sync_enqueuer()
            queue_job = await service.enqueue(binding_id)
            await session.commit()
        await factory.queue_dispatcher.dispatch()
    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        logger.exception(
            "Failed to enqueue dashboard_sync", binding_id=binding_id
        )
        try:
            binding_store = (
                factory.create_dashboard_github_template_binding_store()
            )
            await mark_dashboard_sync_failed(
                session=session,
                binding_store=binding_store,
                binding_id=binding_id,
                exc=exc,
                error_message=f"Enqueue failed: {exc}",
            )
        except Exception as exc:
            sentry_sdk.capture_exception(exc)
            logger.exception(
                "Failed to mark binding as enqueue-failed",
                binding_id=binding_id,
            )
        return None
    else:
        return queue_job
