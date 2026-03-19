"""Service for managing organization credentials."""

from __future__ import annotations

import json
from typing import Any

import structlog

from docverse.domain.organization_credential import OrganizationCredential
from docverse.exceptions import ConflictError, NotFoundError
from docverse.services.credential_encryptor import CredentialEncryptor
from docverse.storage.organization_credential_store import (
    OrganizationCredentialStore,
)
from docverse.storage.organization_store import OrganizationStore


class CredentialService:
    """Business logic for organization credential management.

    Encrypts credential payloads on creation and decrypts them only
    for internal use (never exposed via API responses).
    """

    def __init__(
        self,
        store: OrganizationCredentialStore,
        org_store: OrganizationStore,
        encryptor: CredentialEncryptor,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._org_store = org_store
        self._encryptor = encryptor
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
        service_type: str,
        credential: dict[str, Any],
    ) -> OrganizationCredential:
        """Create a new credential, encrypting the payload."""
        org_id = await self._resolve_org_id(org_slug)

        # Check for duplicate label
        existing = await self._store.get_by_label(
            organization_id=org_id, label=label
        )
        if existing is not None:
            msg = (
                f"Credential with label {label!r} already exists"
                f" for organization {org_slug!r}"
            )
            raise ConflictError(msg)

        plaintext = json.dumps(credential).encode()
        encrypted = self._encryptor.encrypt(plaintext)

        cred = await self._store.create(
            organization_id=org_id,
            label=label,
            service_type=service_type,
            encrypted_credential=encrypted,
        )
        self._logger.info(
            "Created organization credential",
            org_slug=org_slug,
            label=label,
            service_type=service_type,
        )
        return cred

    async def list_by_org(
        self, *, org_slug: str
    ) -> list[OrganizationCredential]:
        """List all credentials for an organization (without payloads)."""
        org_id = await self._resolve_org_id(org_slug)
        return await self._store.list_by_org(org_id)

    async def delete(self, *, org_slug: str, label: str) -> None:
        """Delete a credential."""
        org_id = await self._resolve_org_id(org_slug)
        deleted = await self._store.delete(organization_id=org_id, label=label)
        if not deleted:
            msg = (
                f"Credential {label!r} not found for organization {org_slug!r}"
            )
            raise NotFoundError(msg)
        self._logger.info(
            "Deleted organization credential",
            org_slug=org_slug,
            label=label,
        )

    async def get_by_label(
        self, *, org_slug: str, label: str
    ) -> OrganizationCredential:
        """Fetch a credential by label (without decrypting).

        Parameters
        ----------
        org_slug
            Organization slug.
        label
            Credential label.

        Returns
        -------
        OrganizationCredential
            The credential domain model.

        Raises
        ------
        NotFoundError
            If the credential does not exist.
        """
        org_id = await self._resolve_org_id(org_slug)
        result = await self._store.get_by_label(
            organization_id=org_id, label=label
        )
        if result is None:
            msg = (
                f"Credential {label!r} not found for organization {org_slug!r}"
            )
            raise NotFoundError(msg)
        cred, _encrypted = result
        return cred

    async def get_decrypted(
        self, *, org_id: int, label: str
    ) -> tuple[OrganizationCredential, dict[str, Any]]:
        """Fetch and decrypt a credential for internal use.

        Parameters
        ----------
        org_id
            Organization ID.
        label
            Credential label.

        Returns
        -------
        tuple
            The credential domain model and the decrypted payload.

        Raises
        ------
        NotFoundError
            If the credential does not exist.
        """
        result = await self._store.get_by_label(
            organization_id=org_id, label=label
        )
        if result is None:
            msg = f"Credential {label!r} not found"
            raise NotFoundError(msg)
        cred, encrypted = result
        plaintext = self._encryptor.decrypt(encrypted)
        payload: dict[str, Any] = json.loads(plaintext)
        return cred, payload
