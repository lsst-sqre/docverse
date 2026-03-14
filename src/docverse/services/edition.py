"""Service for managing editions."""

from __future__ import annotations

import structlog

from docverse.client.models import EditionCreate, EditionUpdate
from docverse.domain.edition import Edition
from docverse.storage.edition_store import EditionStore


class EditionService:
    """Business logic for edition management."""

    def __init__(
        self,
        store: EditionStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._logger = logger

    async def create(self, *, project_id: int, data: EditionCreate) -> Edition:
        """Create a new edition."""
        edition = await self._store.create(project_id=project_id, data=data)
        self._logger.info(
            "Created edition",
            slug=data.slug,
            project_id=project_id,
        )
        return edition

    async def get_by_slug(
        self, *, project_id: int, slug: str
    ) -> Edition | None:
        """Get an edition by slug within a project."""
        return await self._store.get_by_slug(project_id=project_id, slug=slug)

    async def list_by_project(self, project_id: int) -> list[Edition]:
        """List all editions for a project."""
        return await self._store.list_by_project(project_id)

    async def update(
        self, *, project_id: int, slug: str, data: EditionUpdate
    ) -> Edition | None:
        """Update an edition."""
        edition = await self._store.update(
            project_id=project_id, slug=slug, data=data
        )
        if edition is not None:
            self._logger.info(
                "Updated edition", slug=slug, project_id=project_id
            )
        return edition

    async def set_current_build(
        self, *, edition_id: int, build_id: int
    ) -> Edition:
        """Set the current build for an edition."""
        edition = await self._store.set_current_build(
            edition_id=edition_id, build_id=build_id
        )
        self._logger.info(
            "Set current build for edition",
            edition_id=edition_id,
            build_id=build_id,
        )
        return edition

    async def soft_delete(self, *, project_id: int, slug: str) -> bool:
        """Soft-delete an edition."""
        deleted = await self._store.soft_delete(
            project_id=project_id, slug=slug
        )
        if deleted:
            self._logger.info(
                "Soft-deleted edition", slug=slug, project_id=project_id
            )
        return deleted
