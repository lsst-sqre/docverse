"""Database operations for the organization_services table."""

from __future__ import annotations

from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import OrganizationServiceUpdate
from docverse.dbschema.organization_service import SqlOrganizationService
from docverse.domain.organization_service import OrganizationService


class OrganizationServiceStore:
    """Direct database operations for organization services."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(  # noqa: PLR0913
        self,
        *,
        organization_id: int,
        label: str,
        category: str,
        provider: str,
        config: dict[str, Any],
        credential_label: str,
    ) -> OrganizationService:
        """Insert a new service row."""
        row = SqlOrganizationService(
            organization_id=organization_id,
            label=label,
            category=category,
            provider=provider,
            config=config,
            credential_label=credential_label,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return OrganizationService.model_validate(row)

    async def get_by_label(
        self, *, organization_id: int, label: str
    ) -> OrganizationService | None:
        """Fetch a service by org ID and label."""
        result = await self._session.execute(
            select(SqlOrganizationService).where(
                SqlOrganizationService.organization_id == organization_id,
                SqlOrganizationService.label == label,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return OrganizationService.model_validate(row)

    async def list_by_org(
        self, organization_id: int
    ) -> list[OrganizationService]:
        """List all services for an organization."""
        result = await self._session.execute(
            select(SqlOrganizationService)
            .where(
                SqlOrganizationService.organization_id == organization_id,
            )
            .order_by(SqlOrganizationService.label)
        )
        rows = result.scalars().all()
        return [OrganizationService.model_validate(r) for r in rows]

    async def list_by_category(
        self, *, organization_id: int, category: str
    ) -> list[OrganizationService]:
        """List services for an organization filtered by category."""
        result = await self._session.execute(
            select(SqlOrganizationService)
            .where(
                SqlOrganizationService.organization_id == organization_id,
                SqlOrganizationService.category == category,
            )
            .order_by(SqlOrganizationService.label)
        )
        rows = result.scalars().all()
        return [OrganizationService.model_validate(r) for r in rows]

    async def update(
        self,
        *,
        organization_id: int,
        label: str,
        data: OrganizationServiceUpdate,
    ) -> OrganizationService | None:
        """Update a service by org ID and label."""
        result = await self._session.execute(
            select(SqlOrganizationService).where(
                SqlOrganizationService.organization_id == organization_id,
                SqlOrganizationService.label == label,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        updates = data.model_dump(exclude_unset=True)
        for key, value in updates.items():
            setattr(row, key, value)
        await self._session.flush()
        await self._session.refresh(row)
        return OrganizationService.model_validate(row)

    async def delete(self, *, organization_id: int, label: str) -> bool:
        """Delete a service by org ID and label.

        Returns
        -------
        bool
            True if deleted, False if not found.
        """
        result = await self._session.execute(
            select(SqlOrganizationService).where(
                SqlOrganizationService.organization_id == organization_id,
                SqlOrganizationService.label == label,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return False
        await self._session.delete(row)
        return True
