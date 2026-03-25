"""Service for managing projects."""

from __future__ import annotations

import structlog
from safir.database import CountedPaginatedList, PaginationCursor

from docverse.client.models import ProjectCreate, ProjectUpdate
from docverse.domain.project import Project
from docverse.exceptions import ConflictError, NotFoundError
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.pagination import ProjectSearchCursor
from docverse.storage.project_store import ProjectStore


class ProjectService:
    """Business logic for project management."""

    def __init__(
        self,
        store: ProjectStore,
        org_store: OrganizationStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._org_store = org_store
        self._logger = logger

    async def _resolve_org_id(self, org_slug: str) -> int:
        """Resolve an organization slug to its internal ID."""
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        return org.id

    async def create(self, *, org_slug: str, data: ProjectCreate) -> Project:
        """Create a new project.

        Raises
        ------
        ConflictError
            If a project with the same slug already exists.
        """
        org_id = await self._resolve_org_id(org_slug)
        existing = await self._store.get_by_slug(org_id=org_id, slug=data.slug)
        if existing is not None:
            msg = f"Project with slug {data.slug!r} already exists"
            raise ConflictError(msg)
        project = await self._store.create(org_id=org_id, data=data)
        self._logger.info("Created project", slug=data.slug, org=org_slug)
        return project

    async def get_by_slug(self, *, org_slug: str, slug: str) -> Project:
        """Get a project by slug within an organization.

        Raises
        ------
        NotFoundError
            If the project is not found.
        """
        org_id = await self._resolve_org_id(org_slug)
        project = await self._store.get_by_slug(org_id=org_id, slug=slug)
        if project is None:
            msg = f"Project {slug!r} not found"
            raise NotFoundError(msg)
        return project

    async def list_by_org(
        self,
        org_slug: str,
        *,
        query: str | None = None,
        cursor_type: type[PaginationCursor[Project]] | None = None,
        cursor: PaginationCursor[Project] | None = None,
        limit: int,
    ) -> CountedPaginatedList[Project, PaginationCursor[Project]]:
        """List all projects for an organization."""
        org_id = await self._resolve_org_id(org_slug)
        if query is not None:
            search_cursor = (
                cursor if isinstance(cursor, ProjectSearchCursor) else None
            )
            return await self._store.search_by_org(
                org_id, query=query, limit=limit, cursor=search_cursor
            )
        if cursor_type is None:
            msg = "cursor_type is required when query is not set"
            raise RuntimeError(msg)
        return await self._store.list_by_org(
            org_id, cursor_type=cursor_type, cursor=cursor, limit=limit
        )

    async def update(
        self, *, org_slug: str, slug: str, data: ProjectUpdate
    ) -> Project:
        """Update a project.

        Raises
        ------
        NotFoundError
            If the project is not found.
        """
        org_id = await self._resolve_org_id(org_slug)
        project = await self._store.update(org_id=org_id, slug=slug, data=data)
        if project is None:
            msg = f"Project {slug!r} not found"
            raise NotFoundError(msg)
        self._logger.info("Updated project", slug=slug, org=org_slug)
        return project

    async def soft_delete(self, *, org_slug: str, slug: str) -> None:
        """Soft-delete a project.

        Raises
        ------
        NotFoundError
            If the project is not found.
        """
        org_id = await self._resolve_org_id(org_slug)
        deleted = await self._store.soft_delete(org_id=org_id, slug=slug)
        if not deleted:
            msg = f"Project {slug!r} not found"
            raise NotFoundError(msg)
        self._logger.info("Soft-deleted project", slug=slug, org=org_slug)
