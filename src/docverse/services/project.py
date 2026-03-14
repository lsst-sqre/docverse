"""Service for managing projects."""

from __future__ import annotations

import structlog

from docverse.client.models import ProjectCreate, ProjectUpdate
from docverse.domain.project import Project
from docverse.storage.project_store import ProjectStore


class ProjectService:
    """Business logic for project management."""

    def __init__(
        self,
        store: ProjectStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._logger = logger

    async def create(self, *, org_id: int, data: ProjectCreate) -> Project:
        """Create a new project."""
        project = await self._store.create(org_id=org_id, data=data)
        self._logger.info("Created project", slug=data.slug, org_id=org_id)
        return project

    async def get_by_slug(self, *, org_id: int, slug: str) -> Project | None:
        """Get a project by slug within an organization."""
        return await self._store.get_by_slug(org_id=org_id, slug=slug)

    async def list_by_org(self, org_id: int) -> list[Project]:
        """List all projects for an organization."""
        return await self._store.list_by_org(org_id)

    async def update(
        self, *, org_id: int, slug: str, data: ProjectUpdate
    ) -> Project | None:
        """Update a project."""
        project = await self._store.update(org_id=org_id, slug=slug, data=data)
        if project is not None:
            self._logger.info("Updated project", slug=slug, org_id=org_id)
        return project

    async def soft_delete(self, *, org_id: int, slug: str) -> bool:
        """Soft-delete a project."""
        deleted = await self._store.soft_delete(org_id=org_id, slug=slug)
        if deleted:
            self._logger.info("Soft-deleted project", slug=slug, org_id=org_id)
        return deleted
