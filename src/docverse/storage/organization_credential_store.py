"""Database operations for the organization_credentials table."""

from __future__ import annotations

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.dbschema.organization_credential import SqlOrganizationCredential
from docverse.domain.organization_credential import OrganizationCredential


class OrganizationCredentialStore:
    """Direct database operations for organization credentials."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(
        self,
        *,
        organization_id: int,
        label: str,
        provider: str,
        encrypted_credentials: bytes,
    ) -> OrganizationCredential:
        """Insert a new credential row."""
        row = SqlOrganizationCredential(
            organization_id=organization_id,
            label=label,
            provider=provider,
            encrypted_credentials=encrypted_credentials,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return OrganizationCredential.model_validate(row)

    async def get_by_label(
        self, *, organization_id: int, label: str
    ) -> tuple[OrganizationCredential, bytes] | None:
        """Fetch a credential by org ID and label.

        Returns the domain model and the encrypted credential bytes,
        or None if not found.
        """
        result = await self._session.execute(
            select(SqlOrganizationCredential).where(
                SqlOrganizationCredential.organization_id == organization_id,
                SqlOrganizationCredential.label == label,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return (
            OrganizationCredential.model_validate(row),
            row.encrypted_credentials,
        )

    async def list_by_org(
        self, organization_id: int
    ) -> list[OrganizationCredential]:
        """List all credentials for an organization."""
        result = await self._session.execute(
            select(SqlOrganizationCredential)
            .where(
                SqlOrganizationCredential.organization_id == organization_id,
            )
            .order_by(SqlOrganizationCredential.label)
        )
        rows = result.scalars().all()
        return [OrganizationCredential.model_validate(r) for r in rows]

    async def delete(self, *, organization_id: int, label: str) -> bool:
        """Delete a credential by org ID and label.

        Returns
        -------
        bool
            True if deleted, False if not found.
        """
        result = await self._session.execute(
            select(SqlOrganizationCredential).where(
                SqlOrganizationCredential.organization_id == organization_id,
                SqlOrganizationCredential.label == label,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return False
        await self._session.delete(row)
        return True
