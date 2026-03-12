"""Service for managing organizations."""

from __future__ import annotations

import structlog

from docverse.client.models import OrganizationCreate, OrganizationUpdate
from docverse.domain.organization import Organization
from docverse.storage.organization_store import OrganizationStore


class OrganizationService:
    """Business logic for organization management."""

    def __init__(
        self,
        store: OrganizationStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._logger = logger

    async def create(self, data: OrganizationCreate) -> Organization:
        """Create a new organization."""
        org = await self._store.create(data)
        self._logger.info("Created organization", slug=data.slug)
        return org

    async def get_by_slug(self, slug: str) -> Organization | None:
        """Get an organization by slug."""
        return await self._store.get_by_slug(slug)

    async def list_all(self) -> list[Organization]:
        """List all organizations."""
        return await self._store.list_all()

    async def update(
        self, slug: str, data: OrganizationUpdate
    ) -> Organization | None:
        """Update an organization by slug."""
        org = await self._store.update(slug, data)
        if org is not None:
            self._logger.info("Updated organization", slug=slug)
        return org

    async def delete(self, slug: str) -> bool:
        """Delete an organization by slug.

        Returns
        -------
        bool
            True if the organization was deleted, False if
            not found.
        """
        deleted = await self._store.delete(slug)
        if deleted:
            self._logger.info("Deleted organization", slug=slug)
        return deleted
