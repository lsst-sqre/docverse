"""Service for managing organizations."""

from __future__ import annotations

import structlog

from docverse.client.models import OrganizationCreate, OrganizationUpdate
from docverse.client.models.infrastructure import (
    SERVICE_PROVIDER_CATEGORY,
    ServiceCategory,
)
from docverse.domain.organization import Organization
from docverse.exceptions import ConflictError
from docverse.storage.organization_service_store import (
    OrganizationServiceStore,
)
from docverse.storage.organization_store import OrganizationStore

# Mapping from slot field name to required service category.
_SLOT_CATEGORY: dict[str, ServiceCategory] = {
    "publishing_store_label": ServiceCategory.object_storage,
    "staging_store_label": ServiceCategory.object_storage,
    "cdn_service_label": ServiceCategory.cdn,
    "dns_service_label": ServiceCategory.dns,
}


class OrganizationService:
    """Business logic for organization management."""

    def __init__(
        self,
        store: OrganizationStore,
        service_store: OrganizationServiceStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._service_store = service_store
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
        # Validate slot labels if any are being set
        updates = data.model_dump(exclude_unset=True)
        slot_updates = {
            k: v for k, v in updates.items() if k in _SLOT_CATEGORY and v
        }
        if slot_updates:
            org = await self._store.get_by_slug(slug)
            if org is None:
                return None
            await self._validate_slot_labels(org.id, slot_updates)

        org = await self._store.update(slug, data)
        if org is not None:
            self._logger.info("Updated organization", slug=slug)
        return org

    async def _validate_slot_labels(
        self, org_id: int, slot_updates: dict[str, str]
    ) -> None:
        """Validate that slot labels reference existing, compatible services.

        Parameters
        ----------
        org_id
            Organization ID.
        slot_updates
            Mapping from slot field name to service label.

        Raises
        ------
        ConflictError
            If a service label doesn't exist or has an incompatible category.
        """
        for slot_name, label in slot_updates.items():
            required_category = _SLOT_CATEGORY[slot_name]
            svc = await self._service_store.get_by_label(
                organization_id=org_id, label=label
            )
            if svc is None:
                msg = f"Service {label!r} not found for slot {slot_name!r}"
                raise ConflictError(msg)
            svc_category = SERVICE_PROVIDER_CATEGORY[svc.provider]
            if svc_category != required_category:
                msg = (
                    f"Slot {slot_name!r} requires a"
                    f" {required_category} service, but service"
                    f" {label!r} has category {svc_category!r}"
                )
                raise ConflictError(msg)

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
