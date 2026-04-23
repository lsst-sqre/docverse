"""Database operations for the ``dashboard_github_template_bindings`` table."""

from __future__ import annotations

from dataclasses import dataclass

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.dashboard_github_template_binding import (
    SqlDashboardGitHubTemplateBinding,
)
from docverse.domain.dashboard_github_template import (
    DashboardGitHubTemplateBinding,
)

__all__ = [
    "DashboardGitHubTemplateBindingCreate",
    "DashboardGitHubTemplateBindingStore",
]


@dataclass(frozen=True)
class DashboardGitHubTemplateBindingCreate:
    """Inputs for creating a binding row."""

    org_id: int
    project_id: int | None
    github_owner: str
    github_repo: str
    github_ref: str
    root_path: str = "/"
    github_owner_id: int | None = None
    github_repo_id: int | None = None
    github_installation_id: int | None = None


class DashboardGitHubTemplateBindingStore:
    """Direct database operations for dashboard GitHub template bindings."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def create(
        self, data: DashboardGitHubTemplateBindingCreate
    ) -> DashboardGitHubTemplateBinding:
        """Insert a new binding row."""
        row = SqlDashboardGitHubTemplateBinding(
            org_id=data.org_id,
            project_id=data.project_id,
            github_owner=data.github_owner,
            github_repo=data.github_repo,
            github_ref=data.github_ref,
            root_path=data.root_path,
            github_owner_id=data.github_owner_id,
            github_repo_id=data.github_repo_id,
            github_installation_id=data.github_installation_id,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return DashboardGitHubTemplateBinding.model_validate(row)

    async def get_by_id(
        self, binding_id: int
    ) -> DashboardGitHubTemplateBinding | None:
        """Fetch a binding by internal ID."""
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplateBinding).where(
                SqlDashboardGitHubTemplateBinding.id == binding_id,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return DashboardGitHubTemplateBinding.model_validate(row)

    async def get_org_default(
        self, org_id: int
    ) -> DashboardGitHubTemplateBinding | None:
        """Fetch the org-default binding (``project_id IS NULL``)."""
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplateBinding).where(
                SqlDashboardGitHubTemplateBinding.org_id == org_id,
                SqlDashboardGitHubTemplateBinding.project_id.is_(None),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return DashboardGitHubTemplateBinding.model_validate(row)

    async def get_project_override(
        self, *, org_id: int, project_id: int
    ) -> DashboardGitHubTemplateBinding | None:
        """Fetch the project-specific binding for ``project_id``."""
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplateBinding).where(
                SqlDashboardGitHubTemplateBinding.org_id == org_id,
                SqlDashboardGitHubTemplateBinding.project_id == project_id,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return DashboardGitHubTemplateBinding.model_validate(row)

    async def update_source(
        self,
        *,
        binding_id: int,
        github_owner: str,
        github_repo: str,
        github_ref: str,
        root_path: str,
    ) -> DashboardGitHubTemplateBinding | None:
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
        return DashboardGitHubTemplateBinding.model_validate(row)

    async def update_sync_state(  # noqa: PLR0913
        self,
        *,
        binding_id: int,
        last_sync_status: str,
        last_sync_error: str | None = None,
        github_template_id: int | None = None,
        github_owner_id: int | None = None,
        github_repo_id: int | None = None,
        github_installation_id: int | None = None,
    ) -> DashboardGitHubTemplateBinding | None:
        """Update sync-state fields after a sync attempt.

        ``github_template_id`` and the three ``github_*_id`` fields are
        only assigned when provided; passing ``None`` leaves the
        existing values in place so a failed sync keeps the last-good
        template reference and previously-captured GitHub identities.
        """
        row = await self._get_row(binding_id)
        if row is None:
            return None
        row.last_sync_status = last_sync_status
        row.last_sync_error = last_sync_error
        if github_template_id is not None:
            row.github_template_id = github_template_id
        if github_owner_id is not None:
            row.github_owner_id = github_owner_id
        if github_repo_id is not None:
            row.github_repo_id = github_repo_id
        if github_installation_id is not None:
            row.github_installation_id = github_installation_id
        await self._session.flush()
        await self._session.refresh(row)
        return DashboardGitHubTemplateBinding.model_validate(row)

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
    ) -> SqlDashboardGitHubTemplateBinding | None:
        result = await self._session.execute(
            select(SqlDashboardGitHubTemplateBinding).where(
                SqlDashboardGitHubTemplateBinding.id == binding_id,
            )
        )
        return result.scalar_one_or_none()
