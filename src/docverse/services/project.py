"""Service for managing projects."""

from __future__ import annotations

import structlog
from safir.database import CountedPaginatedList, PaginationCursor

from docverse.client.models import (
    DefaultEditionConfig,
    EditionKind,
    ProjectCreate,
    ProjectUpdate,
)
from docverse.domain.edition import Edition
from docverse.domain.organization import Organization
from docverse.domain.project import Project
from docverse.exceptions import ConflictError, NotFoundError
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.pagination import ProjectSearchCursor
from docverse.storage.project_store import ProjectStore

DEFAULT_EDITION_SLUG = "__main"
"""Slug for the default edition auto-created with every project."""


class ProjectService:
    """Business logic for project management."""

    def __init__(
        self,
        store: ProjectStore,
        org_store: OrganizationStore,
        edition_store: EditionStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._org_store = org_store
        self._edition_store = edition_store
        self._logger = logger

    async def _resolve_org(self, org_slug: str) -> Organization:
        """Resolve an organization slug to its domain object."""
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        return org

    @staticmethod
    def _resolve_default_edition_config(
        request_config: DefaultEditionConfig | None,
        org: Organization,
    ) -> DefaultEditionConfig:
        """Resolve default edition config through the precedence chain.

        Order: explicit request > organization default > hardcoded fallback.
        """
        if request_config is not None:
            return request_config
        if org.default_edition_config is not None:
            return DefaultEditionConfig.model_validate(
                org.default_edition_config
            )
        return DefaultEditionConfig()

    async def create(
        self, *, org_slug: str, data: ProjectCreate
    ) -> tuple[Project, Edition]:
        """Create a new project with its default ``__main`` edition.

        Raises
        ------
        ConflictError
            If a project with the same slug already exists.
        """
        org = await self._resolve_org(org_slug)
        existing = await self._store.get_by_slug(org_id=org.id, slug=data.slug)
        if existing is not None:
            msg = f"Project with slug {data.slug!r} already exists"
            raise ConflictError(msg)
        project = await self._store.create(org_id=org.id, data=data)

        config = self._resolve_default_edition_config(
            data.default_edition, org
        )
        default_edition = await self._edition_store.create_internal(
            project_id=project.id,
            slug=DEFAULT_EDITION_SLUG,
            title=config.title,
            kind=EditionKind.main,
            tracking_mode=config.tracking_mode,
            tracking_params=config.tracking_params or {"git_ref": "main"},
            lifecycle_exempt=config.lifecycle_exempt,
        )

        self._logger.info("Created project", slug=data.slug, org=org_slug)
        return project, default_edition

    async def get_by_slug(self, *, org_slug: str, slug: str) -> Project:
        """Get a project by slug within an organization.

        Raises
        ------
        NotFoundError
            If the project is not found.
        """
        org = await self._resolve_org(org_slug)
        project = await self._store.get_by_slug(org_id=org.id, slug=slug)
        if project is None:
            msg = f"Project {slug!r} not found"
            raise NotFoundError(msg)
        return project

    async def get_default_edition(self, project_id: int) -> Edition | None:
        """Fetch the ``__main`` edition for a project."""
        return await self._edition_store.get_by_slug(
            project_id=project_id, slug=DEFAULT_EDITION_SLUG
        )

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
        org = await self._resolve_org(org_slug)
        if query is not None:
            search_cursor = (
                cursor if isinstance(cursor, ProjectSearchCursor) else None
            )
            return await self._store.search_by_org(
                org.id, query=query, limit=limit, cursor=search_cursor
            )
        if cursor_type is None:
            msg = "cursor_type is required when query is not set"
            raise RuntimeError(msg)
        return await self._store.list_by_org(
            org.id, cursor_type=cursor_type, cursor=cursor, limit=limit
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
        org = await self._resolve_org(org_slug)
        project = await self._store.update(org_id=org.id, slug=slug, data=data)
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
        org = await self._resolve_org(org_slug)
        deleted = await self._store.soft_delete(org_id=org.id, slug=slug)
        if not deleted:
            msg = f"Project {slug!r} not found"
            raise NotFoundError(msg)
        self._logger.info("Soft-deleted project", slug=slug, org=org_slug)
