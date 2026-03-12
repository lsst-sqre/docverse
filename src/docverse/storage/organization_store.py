"""Database operations for the organizations table."""

from __future__ import annotations

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import OrganizationCreate, OrganizationUpdate
from docverse.dbschema.organization import SqlOrganization
from docverse.domain.organization import Organization


class OrganizationStore:
    """Direct database operations for organizations."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(self, data: OrganizationCreate) -> Organization:
        """Insert a new organization row."""
        row = SqlOrganization(
            slug=data.slug,
            title=data.title,
            base_domain=data.base_domain,
            url_scheme=data.url_scheme,
            root_path_prefix=data.root_path_prefix,
            slug_rewrite_rules=data.slug_rewrite_rules,
            lifecycle_rules=data.lifecycle_rules,
            purgatory_retention=data.purgatory_retention,
        )
        self._session.add(row)
        await self._session.flush()
        return Organization.model_validate(row)

    async def get_by_slug(self, slug: str) -> Organization | None:
        """Fetch an organization by slug."""
        result = await self._session.execute(
            select(SqlOrganization).where(SqlOrganization.slug == slug)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return Organization.model_validate(row)

    async def list_all(self) -> list[Organization]:
        """List all organizations ordered by slug."""
        result = await self._session.execute(
            select(SqlOrganization).order_by(SqlOrganization.slug)
        )
        rows = result.scalars().all()
        return [Organization.model_validate(r) for r in rows]

    async def update(
        self, slug: str, data: OrganizationUpdate
    ) -> Organization | None:
        """Update an organization by slug."""
        result = await self._session.execute(
            select(SqlOrganization).where(SqlOrganization.slug == slug)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        updates = data.model_dump(exclude_unset=True)
        for key, value in updates.items():
            setattr(row, key, value)
        await self._session.flush()
        await self._session.refresh(row)
        return Organization.model_validate(row)

    async def delete(self, slug: str) -> bool:
        """Delete an organization by slug.

        Returns
        -------
        bool
            True if the organization was deleted, False if not found.
        """
        result = await self._session.execute(
            select(SqlOrganization).where(SqlOrganization.slug == slug)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return False
        await self._session.delete(row)
        return True
