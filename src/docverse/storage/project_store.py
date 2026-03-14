"""Database operations for the projects table."""

from __future__ import annotations

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session
from sqlalchemy.sql import func

from docverse.client.models import ProjectCreate, ProjectUpdate
from docverse.dbschema.project import SqlProject
from docverse.domain.project import Project


class ProjectStore:
    """Direct database operations for projects."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(self, *, org_id: int, data: ProjectCreate) -> Project:
        """Insert a new project row."""
        row = SqlProject(
            slug=data.slug,
            title=data.title,
            org_id=org_id,
            doc_repo=data.doc_repo,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return Project.model_validate(row)

    async def get_by_slug(self, *, org_id: int, slug: str) -> Project | None:
        """Fetch a project by org_id and slug."""
        result = await self._session.execute(
            select(SqlProject).where(
                SqlProject.org_id == org_id,
                SqlProject.slug == slug,
                SqlProject.date_deleted.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return Project.model_validate(row)

    async def list_by_org(self, org_id: int) -> list[Project]:
        """List all non-deleted projects for an organization."""
        result = await self._session.execute(
            select(SqlProject)
            .where(
                SqlProject.org_id == org_id,
                SqlProject.date_deleted.is_(None),
            )
            .order_by(SqlProject.slug)
        )
        rows = result.scalars().all()
        return [Project.model_validate(r) for r in rows]

    async def update(
        self, *, org_id: int, slug: str, data: ProjectUpdate
    ) -> Project | None:
        """Update a project by org_id and slug."""
        result = await self._session.execute(
            select(SqlProject).where(
                SqlProject.org_id == org_id,
                SqlProject.slug == slug,
                SqlProject.date_deleted.is_(None),
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
        return Project.model_validate(row)

    async def soft_delete(self, *, org_id: int, slug: str) -> bool:
        """Soft-delete a project by setting date_deleted.

        Returns
        -------
        bool
            True if the project was soft-deleted, False if not found.
        """
        result = await self._session.execute(
            select(SqlProject).where(
                SqlProject.org_id == org_id,
                SqlProject.slug == slug,
                SqlProject.date_deleted.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return False
        row.date_deleted = func.now()
        await self._session.flush()
        return True
