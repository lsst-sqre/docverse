"""Business logic for dashboard-template binding CRUD."""

from __future__ import annotations

from dataclasses import dataclass

import structlog

from docverse.client.models import DashboardTemplateBindingCreate
from docverse.domain.dashboard_github_template import (
    DashboardGitHubTemplateBinding,
)
from docverse.exceptions import NotFoundError
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.dashboard_templates.github.binding_store import (
    DashboardGitHubTemplateBindingCreate as _BindingCreate,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

__all__ = [
    "DashboardTemplateBindingResult",
    "DashboardTemplateBindingService",
]


@dataclass(frozen=True)
class DashboardTemplateBindingResult:
    """Outcome of a PUT binding call.

    ``created`` distinguishes a brand-new row from an in-place update so
    the handler can return 201 vs. 200. ``changed`` is ``False`` when
    the PUT was a no-op because every source field already matched the
    stored row — in that case, no write happened and ``date_updated``
    is unchanged.
    """

    binding: DashboardGitHubTemplateBinding
    created: bool
    changed: bool


class DashboardTemplateBindingService:
    """CRUD for dashboard-template bindings scoped to an organization.

    The service owns slug → ID resolution and the create-vs-update
    branching so handlers stay a thin HTTP layer. It deliberately does
    *not* enqueue a sync on PUT — that wiring lands with the worker
    slice (see PRD #232).
    """

    def __init__(
        self,
        binding_store: DashboardGitHubTemplateBindingStore,
        org_store: OrganizationStore,
        project_store: ProjectStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._binding_store = binding_store
        self._org_store = org_store
        self._project_store = project_store
        self._logger = logger

    async def _resolve_org_id(self, org_slug: str) -> int:
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        return org.id

    async def _resolve_project_id(
        self, *, org_id: int, project_slug: str
    ) -> int:
        project = await self._project_store.get_by_slug(
            org_id=org_id, slug=project_slug
        )
        if project is None:
            msg = f"Project {project_slug!r} not found"
            raise NotFoundError(msg)
        return project.id

    async def get_org_default(
        self, *, org_slug: str
    ) -> DashboardGitHubTemplateBinding:
        """Fetch the org's default binding or raise :class:`NotFoundError`."""
        org_id = await self._resolve_org_id(org_slug)
        binding = await self._binding_store.get_org_default(org_id)
        if binding is None:
            msg = (
                f"No dashboard-template binding for organization {org_slug!r}"
            )
            raise NotFoundError(msg)
        return binding

    async def put_org_default(
        self,
        *,
        org_slug: str,
        data: DashboardTemplateBindingCreate,
    ) -> DashboardTemplateBindingResult:
        """Create or update the org-default binding."""
        org_id = await self._resolve_org_id(org_slug)
        existing = await self._binding_store.get_org_default(org_id)
        return await self._upsert(
            org_id=org_id,
            project_id=None,
            existing=existing,
            data=data,
        )

    async def delete_org_default(self, *, org_slug: str) -> None:
        """Delete the org-default binding or raise :class:`NotFoundError`."""
        org_id = await self._resolve_org_id(org_slug)
        existing = await self._binding_store.get_org_default(org_id)
        if existing is None:
            msg = (
                f"No dashboard-template binding for organization {org_slug!r}"
            )
            raise NotFoundError(msg)
        await self._binding_store.delete(existing.id)
        self._logger.info(
            "Deleted org-default dashboard-template binding",
            org_slug=org_slug,
            binding_id=existing.id,
        )

    async def get_project_override(
        self, *, org_slug: str, project_slug: str
    ) -> DashboardGitHubTemplateBinding:
        """Fetch a project override or raise :class:`NotFoundError`."""
        org_id = await self._resolve_org_id(org_slug)
        project_id = await self._resolve_project_id(
            org_id=org_id, project_slug=project_slug
        )
        binding = await self._binding_store.get_project_override(
            org_id=org_id, project_id=project_id
        )
        if binding is None:
            msg = (
                f"No dashboard-template binding for project {project_slug!r}"
                f" in organization {org_slug!r}"
            )
            raise NotFoundError(msg)
        return binding

    async def put_project_override(
        self,
        *,
        org_slug: str,
        project_slug: str,
        data: DashboardTemplateBindingCreate,
    ) -> DashboardTemplateBindingResult:
        """Create or update a project-override binding."""
        org_id = await self._resolve_org_id(org_slug)
        project_id = await self._resolve_project_id(
            org_id=org_id, project_slug=project_slug
        )
        existing = await self._binding_store.get_project_override(
            org_id=org_id, project_id=project_id
        )
        return await self._upsert(
            org_id=org_id,
            project_id=project_id,
            existing=existing,
            data=data,
        )

    async def delete_project_override(
        self, *, org_slug: str, project_slug: str
    ) -> None:
        """Delete a project override or raise :class:`NotFoundError`."""
        org_id = await self._resolve_org_id(org_slug)
        project_id = await self._resolve_project_id(
            org_id=org_id, project_slug=project_slug
        )
        existing = await self._binding_store.get_project_override(
            org_id=org_id, project_id=project_id
        )
        if existing is None:
            msg = (
                f"No dashboard-template binding for project {project_slug!r}"
                f" in organization {org_slug!r}"
            )
            raise NotFoundError(msg)
        await self._binding_store.delete(existing.id)
        self._logger.info(
            "Deleted project dashboard-template binding",
            org_slug=org_slug,
            project_slug=project_slug,
            binding_id=existing.id,
        )

    async def _upsert(
        self,
        *,
        org_id: int,
        project_id: int | None,
        existing: DashboardGitHubTemplateBinding | None,
        data: DashboardTemplateBindingCreate,
    ) -> DashboardTemplateBindingResult:
        if existing is None:
            binding = await self._binding_store.create(
                _BindingCreate(
                    org_id=org_id,
                    project_id=project_id,
                    github_owner=data.github_owner,
                    github_repo=data.github_repo,
                    github_ref=data.github_ref,
                    root_path=data.root_path,
                )
            )
            self._logger.info(
                "Created dashboard-template binding",
                binding_id=binding.id,
                org_id=org_id,
                project_id=project_id,
            )
            return DashboardTemplateBindingResult(
                binding=binding, created=True, changed=True
            )

        # Idempotent no-op when every source field already matches.
        if (
            existing.github_owner == data.github_owner
            and existing.github_repo == data.github_repo
            and existing.github_ref == data.github_ref
            and existing.root_path == data.root_path
        ):
            return DashboardTemplateBindingResult(
                binding=existing, created=False, changed=False
            )

        updated = await self._binding_store.update_source(
            binding_id=existing.id,
            github_owner=data.github_owner,
            github_repo=data.github_repo,
            github_ref=data.github_ref,
            root_path=data.root_path,
        )
        if updated is None:
            msg = (
                f"Binding {existing.id} disappeared mid-transaction during "
                "dashboard-template binding update"
            )
            raise RuntimeError(msg)
        self._logger.info(
            "Updated dashboard-template binding",
            binding_id=updated.id,
            org_id=org_id,
            project_id=project_id,
        )
        return DashboardTemplateBindingResult(
            binding=updated, created=False, changed=True
        )
