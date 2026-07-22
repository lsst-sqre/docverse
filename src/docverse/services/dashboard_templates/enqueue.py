"""Service that enqueues ``dashboard_sync`` jobs."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sentry_sdk
import structlog

from docverse.client.models.queue_enums import JobKind
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import QueueJob
from docverse.exceptions import NotFoundError
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.queue_backend import QueueBackend
from docverse.storage.queue_job_store import QueueJobStore

from ._sync_failure import mark_dashboard_sync_failed

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from docverse.factory import Factory

__all__ = [
    "DashboardSyncEnqueuer",
    "try_enqueue_dashboard_sync",
]


class DashboardSyncEnqueuer:
    """Create the ``QueueJob`` row and enqueue a ``dashboard_sync`` arq job.

    Mirrors :class:`docverse.services.dashboard.enqueue.DashboardBuildEnqueuer`
    at the enqueue layer — the heavy work (GitHub fetch, upsert,
    fan-out) lives in the worker function.
    """

    def __init__(
        self,
        *,
        binding_store: DashboardGitHubTemplateBindingStore,
        queue_backend: QueueBackend,
        queue_job_store: QueueJobStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._binding_store = binding_store
        self._queue_backend = queue_backend
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
        # transaction as the queue-job insert; if the backend enqueue
        # below fails, both writes roll back.
        await self._binding_store.set_last_sync_queue_job(
            binding_id=binding.id, queue_job_id=queue_job.id
        )
        backend_job_id = await self._queue_backend.enqueue(
            "dashboard_sync",
            {
                "binding_id": binding.id,
                "queue_job_id": queue_job.id,
                "queue_job_public_id": serialize_base32_id(
                    queue_job.public_id
                ),
            },
        )
        return await self._queue_job_store.set_backend_job_id(
            queue_job.id, backend_job_id
        )


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

    The enqueue runs in a freshly started transaction on ``session`` —
    the caller must have already committed the binding write it wants
    persisted.

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
