"""Database operations for the ``dashboard_template_bindings`` table."""

from __future__ import annotations

from dataclasses import dataclass

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.dashboard_template_binding import (
    SqlDashboardTemplateBinding,
)
from docverse.domain.dashboard_template_binding import DashboardTemplateBinding


@dataclass(frozen=True)
class DashboardTemplateBindingCreate:
    """Inputs for creating a binding row."""

    org_id: int
    project_id: int | None
    github_owner: str
    github_repo: str
    github_ref: str
    root_path: str = "/"


class DashboardTemplateBindingStore:
    """Direct database operations for dashboard template bindings."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(
        self, data: DashboardTemplateBindingCreate
    ) -> DashboardTemplateBinding:
        """Insert a new binding row."""
        row = SqlDashboardTemplateBinding(
            org_id=data.org_id,
            project_id=data.project_id,
            github_owner=data.github_owner,
            github_repo=data.github_repo,
            github_ref=data.github_ref,
            root_path=data.root_path,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return DashboardTemplateBinding.model_validate(row)

    async def get_by_id(
        self, binding_id: int
    ) -> DashboardTemplateBinding | None:
        """Fetch a binding by internal ID."""
        result = await self._session.execute(
            select(SqlDashboardTemplateBinding).where(
                SqlDashboardTemplateBinding.id == binding_id,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return DashboardTemplateBinding.model_validate(row)

    async def get_org_default(
        self, org_id: int
    ) -> DashboardTemplateBinding | None:
        """Fetch the org-default binding (``project_id IS NULL``)."""
        result = await self._session.execute(
            select(SqlDashboardTemplateBinding).where(
                SqlDashboardTemplateBinding.org_id == org_id,
                SqlDashboardTemplateBinding.project_id.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return DashboardTemplateBinding.model_validate(row)

    async def get_project_override(
        self, *, org_id: int, project_id: int
    ) -> DashboardTemplateBinding | None:
        """Fetch the project-specific binding for ``project_id``."""
        result = await self._session.execute(
            select(SqlDashboardTemplateBinding).where(
                SqlDashboardTemplateBinding.org_id == org_id,
                SqlDashboardTemplateBinding.project_id == project_id,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return DashboardTemplateBinding.model_validate(row)

    async def update_source(
        self,
        *,
        binding_id: int,
        github_owner: str,
        github_repo: str,
        github_ref: str,
        root_path: str,
    ) -> DashboardTemplateBinding | None:
        """Update the GitHub source coordinates of an existing binding."""
        row = await self._get_row(binding_id)
        if row is None:
            return None
        row.github_owner = github_owner
        row.github_repo = github_repo
        row.github_ref = github_ref
        row.root_path = root_path
        await self._session.flush()
        await self._session.refresh(row)
        return DashboardTemplateBinding.model_validate(row)

    async def update_sync_state(
        self,
        *,
        binding_id: int,
        last_sync_status: str,
        last_sync_error: str | None = None,
        content_id: int | None = None,
    ) -> DashboardTemplateBinding | None:
        """Update sync-state fields after a sync attempt.

        ``content_id`` is only assigned when provided; passing ``None``
        leaves the existing pointer in place so a failed sync keeps the
        last-good content reference.
        """
        row = await self._get_row(binding_id)
        if row is None:
            return None
        row.last_sync_status = last_sync_status
        row.last_sync_error = last_sync_error
        if content_id is not None:
            row.content_id = content_id
        await self._session.flush()
        await self._session.refresh(row)
        return DashboardTemplateBinding.model_validate(row)

    async def delete(self, binding_id: int) -> bool:
        """Delete a binding row.

        Returns ``True`` if a row was deleted, ``False`` if not found.
        """
        row = await self._get_row(binding_id)
        if row is None:
            return False
        await self._session.delete(row)
        await self._session.flush()
        return True

    async def _get_row(
        self, binding_id: int
    ) -> SqlDashboardTemplateBinding | None:
        result = await self._session.execute(
            select(SqlDashboardTemplateBinding).where(
                SqlDashboardTemplateBinding.id == binding_id,
            )
        )
        return result.scalar_one_or_none()
