"""Service that enqueues ``dashboard_build`` jobs."""

from __future__ import annotations

import structlog

from docverse.client.models.queue_enums import JobKind
from docverse.domain.project import Project
from docverse.domain.queue import QueueJob
from docverse.exceptions import NotFoundError
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_backend import QueueBackend
from docverse.storage.queue_job_store import QueueJobStore

__all__ = ["DashboardPublishingService"]


class DashboardPublishingService:
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
    ) -> QueueJob:
        """Enqueue one ``dashboard_build`` job for a single project.

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
    ) -> QueueJob:
        """Resolve org+project slugs and enqueue a single job.

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
        projects were enumerated (slug ascending). An empty list is
        returned for orgs with no non-deleted projects.

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
            results.append((project, queue_job))
        return results
