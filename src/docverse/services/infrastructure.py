"""Service for managing organization infrastructure services."""

from __future__ import annotations

from typing import Any

import structlog

from docverse.client.models.infrastructure import (
    SERVICE_PROVIDER_CATEGORY,
    SERVICE_PROVIDER_CREDENTIAL,
    ServiceProvider,
)
from docverse.domain.organization_service import OrganizationService
from docverse.exceptions import ConflictError, NotFoundError
from docverse.storage.organization_credential_store import (
    OrganizationCredentialStore,
)
from docverse.storage.organization_service_store import (
    OrganizationServiceStore,
)
from docverse.storage.organization_store import OrganizationStore


class InfrastructureService:
    """Business logic for organization infrastructure services.

    Manages the service layer of the three-layer infrastructure model:
    credentials -> services -> slot assignments.
    """

    def __init__(
        self,
        store: OrganizationServiceStore,
        credential_store: OrganizationCredentialStore,
        org_store: OrganizationStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._credential_store = credential_store
        self._org_store = org_store
        self._logger = logger

    async def _resolve_org_id(self, org_slug: str) -> int:
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        return org.id

    async def create(
        self,
        *,
        org_slug: str,
        label: str,
        provider: ServiceProvider,
        config: dict[str, Any],
        credential_label: str,
    ) -> OrganizationService:
        """Create a new infrastructure service.

        Validates that the credential exists and is compatible with the
        service provider before creating the service.
        """
        org_id = await self._resolve_org_id(org_slug)

        # Check for duplicate label
        existing = await self._store.get_by_label(
            organization_id=org_id, label=label
        )
        if existing is not None:
            msg = (
                f"Service with label {label!r} already exists"
                f" for organization {org_slug!r}"
            )
            raise ConflictError(msg)

        # Validate credential exists and is compatible
        cred_result = await self._credential_store.get_by_label(
            organization_id=org_id, label=credential_label
        )
        if cred_result is None:
            msg = (
                f"Credential {credential_label!r} not found"
                f" for organization {org_slug!r}"
            )
            raise NotFoundError(msg)

        cred, _encrypted = cred_result
        compatible_providers = SERVICE_PROVIDER_CREDENTIAL[provider]
        if cred.provider not in compatible_providers:
            expected = ", ".join(str(p) for p in compatible_providers)
            msg = (
                f"Service provider {provider!r} requires a credential"
                f" with provider {expected}, but credential"
                f" {credential_label!r} has provider {cred.provider!r}"
            )
            raise ConflictError(msg)

        # Derive category from provider
        category = SERVICE_PROVIDER_CATEGORY[provider]

        svc = await self._store.create(
            organization_id=org_id,
            label=label,
            category=category,
            provider=provider,
            config=config,
            credential_label=credential_label,
        )
        self._logger.info(
            "Created organization service",
            org_slug=org_slug,
            label=label,
            category=category,
            provider=provider,
        )
        return svc

    async def get_by_label(
        self, *, org_slug: str, label: str
    ) -> OrganizationService:
        """Fetch a service by label.

        Raises
        ------
        NotFoundError
            If the service does not exist.
        """
        org_id = await self._resolve_org_id(org_slug)
        svc = await self._store.get_by_label(
            organization_id=org_id, label=label
        )
        if svc is None:
            msg = f"Service {label!r} not found for organization {org_slug!r}"
            raise NotFoundError(msg)
        return svc

    async def list_by_org(self, *, org_slug: str) -> list[OrganizationService]:
        """List all services for an organization."""
        org_id = await self._resolve_org_id(org_slug)
        return await self._store.list_by_org(org_id)

    async def list_by_org_id(
        self, *, org_id: int
    ) -> list[OrganizationService]:
        """List all services for an organization by ID."""
        return await self._store.list_by_org(org_id)

    async def delete(self, *, org_slug: str, label: str) -> None:
        """Delete a service.

        Raises
        ------
        NotFoundError
            If the service does not exist.
        ConflictError
            If the service is referenced by an organization slot.
        """
        org_id = await self._resolve_org_id(org_slug)

        # Check if any org slot references this service
        org = await self._org_store.get_by_id(org_id)
        if org is not None:
            slot_refs = {
                "publishing_store_label": org.publishing_store_label,
                "staging_store_label": org.staging_store_label,
                "cdn_service_label": org.cdn_service_label,
                "dns_service_label": org.dns_service_label,
            }
            for slot_name, slot_label in slot_refs.items():
                if slot_label == label:
                    msg = (
                        f"Cannot delete service {label!r}: it is referenced"
                        f" by organization slot {slot_name!r}"
                    )
                    raise ConflictError(msg)

        deleted = await self._store.delete(organization_id=org_id, label=label)
        if not deleted:
            msg = f"Service {label!r} not found for organization {org_slug!r}"
            raise NotFoundError(msg)
        self._logger.info(
            "Deleted organization service",
            org_slug=org_slug,
            label=label,
        )
