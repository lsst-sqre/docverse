"""Service that enqueues ``dashboard_build`` jobs."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from docverse.client.models.queue_enums import JobKind
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.project import Project
from docverse.domain.queue import QueueJob
from docverse.exceptions import NotFoundError
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_backend import QueueBackend
from docverse.storage.queue_job_store import QueueJobStore

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from docverse.factory import Factory

__all__ = [
    "DashboardBuildEnqueuer",
    "try_enqueue_dashboard_build_by_id",
    "try_enqueue_dashboard_build_by_slug",
]


class DashboardBuildEnqueuer:
    """Enqueue dashboard render jobs.

    This is the thin slice in the MVP — it creates the ``QueueJob`` row
    and enqueues the matching ``dashboard_build`` arq job. Renderer
    orchestration lives in :class:`DashboardPublisher`, invoked by the
    worker.
    """

    def __init__(
        self,
        *,
        org_store: OrganizationStore,
        project_store: ProjectStore,
        queue_backend: QueueBackend,
        queue_job_store: QueueJobStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._org_store = org_store
        self._project_store = project_store
        self._queue_backend = queue_backend
        self._queue_job_store = queue_job_store
        self._logger = logger

    async def enqueue_for_project(
        self,
        *,
        org_id: int,
        project_id: int,
    ) -> QueueJob | None:
        """Enqueue one ``dashboard_build`` job for a single project.

        Returns the new :class:`QueueJob` on success, or ``None`` when a
        ``dashboard_build`` row keyed on ``(org_id, project_id)`` is
        already ``queued`` or ``in_progress`` — the cascade in
        :func:`docverse.worker.functions.publish_edition` fires once per
        successful publish, so a 1000-edition keeper-sync project would
        otherwise produce 1000 redundant ``dashboard_build`` rows for
        the same project. The first publish in the burst wins; later
        cascades that race past it (e.g. across worker restarts) get a
        fresh row once the active one terminates, matching the
        "at most one *queued* dashboard_build" semantics, not "at most
        one ever".

        Raises
        ------
        NotFoundError
            If the org or project cannot be loaded.
        """
        org = await self._org_store.get_by_id(org_id)
        if org is None:
            msg = f"Organization {org_id} not found"
            raise NotFoundError(msg)
        project = await self._project_store.get_by_id(project_id)
        if project is None:
            msg = f"Project {project_id} not found"
            raise NotFoundError(msg)

        if await self._queue_job_store.has_active_dashboard_build(
            org_id=org_id, project_id=project_id
        ):
            self._logger.info(
                "Skipping dashboard_build enqueue: active job exists",
                org_id=org_id,
                project_id=project_id,
            )
            return None

        queue_job = await self._queue_job_store.create(
            kind=JobKind.dashboard_build,
            org_id=org_id,
            project_id=project_id,
        )
        backend_job_id = await self._queue_backend.enqueue(
            "dashboard_build",
            {
                "org_id": org_id,
                "org_slug": org.slug,
                "project_id": project_id,
                "project_slug": project.slug,
                "queue_job_id": queue_job.id,
                "queue_job_public_id": serialize_base32_id(
                    queue_job.public_id
                ),
            },
        )
        return await self._queue_job_store.set_backend_job_id(
            queue_job.id, backend_job_id
        )

    async def enqueue_for_project_slug(
        self,
        *,
        org_slug: str,
        project_slug: str,
    ) -> QueueJob | None:
        """Resolve org+project slugs and enqueue a single job.

        Returns ``None`` on the same dedup-skip condition as
        :meth:`enqueue_for_project`.

        Raises
        ------
        NotFoundError
            If the org or project cannot be resolved.
        """
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        project = await self._project_store.get_by_slug(
            org_id=org.id, slug=project_slug
        )
        if project is None:
            msg = f"Project {project_slug!r} not found"
            raise NotFoundError(msg)
        return await self.enqueue_for_project(
            org_id=org.id, project_id=project.id
        )

    async def enqueue_for_org(
        self,
        *,
        org_id: int,
    ) -> list[tuple[Project, QueueJob]]:
        """Enqueue one ``dashboard_build`` per non-deleted project in an org.

        Returns a list of ``(project, queue_job)`` pairs in the order
        projects were enumerated (slug ascending). Projects that already
        have an active ``dashboard_build`` row are filtered out, so the
        returned list may be shorter than the project count for the org
        — the operator sees "rebuilt N of M projects; the rest already
        have a dashboard_build queued or in flight" rather than getting
        a duplicate row per project. An empty list is returned for orgs
        with no non-deleted projects (or when every project is already
        deduplicated).

        Raises
        ------
        NotFoundError
            If the org cannot be loaded.
        """
        org = await self._org_store.get_by_id(org_id)
        if org is None:
            msg = f"Organization {org_id} not found"
            raise NotFoundError(msg)
        projects = await self._project_store.list_all_by_org(org_id)
        results: list[tuple[Project, QueueJob]] = []
        for project in projects:
            queue_job = await self.enqueue_for_project(
                org_id=org_id, project_id=project.id
            )
            if queue_job is None:
                continue
            results.append((project, queue_job))
        return results


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
            service = factory.create_dashboard_build_enqueuer()
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
            service = factory.create_dashboard_build_enqueuer()
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
