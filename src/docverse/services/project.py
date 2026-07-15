"""Service for managing projects."""

from __future__ import annotations

from typing import Any

import structlog
from docverse.client.models import (
    DefaultEditionConfig,
    EditionKind,
    ProjectCreate,
    ProjectUpdate,
)
from safir.database import CountedPaginatedList, PaginationCursor

from docverse.domain.edition import Edition
from docverse.domain.organization import Organization
from docverse.domain.project import Project
from docverse.exceptions import ConflictError, NotFoundError
from docverse.storage.edition_store import EditionStore
from docverse.storage.keeper_sync import TombstoneReason
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.pagination import ProjectSearchCursor
from docverse.storage.project_store import ProjectStore

DEFAULT_EDITION_SLUG = "__main"
"""Slug for the default edition auto-created with every project."""


class ProjectService:
    """Business logic for project management."""

    def __init__(
        self,
        store: ProjectStore,
        org_store: OrganizationStore,
        edition_store: EditionStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._org_store = org_store
        self._edition_store = edition_store
        self._logger = logger

    async def _resolve_org(self, org_slug: str) -> Organization:
        """Resolve an organization slug to its domain object."""
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        return org

    @staticmethod
    def _resolve_default_edition_config(
        request_config: DefaultEditionConfig | None,
        org: Organization,
    ) -> DefaultEditionConfig:
        """Resolve default edition config through the precedence chain.

        Order: explicit request > organization default > hardcoded fallback.
        """
        if request_config is not None:
            return request_config
        if org.default_edition_config is not None:
            return DefaultEditionConfig.model_validate(
                org.default_edition_config
            )
        return DefaultEditionConfig()

    @staticmethod
    def _resolve_github_for_create(
        data: ProjectCreate,
    ) -> tuple[str | None, str | None]:
        """Resolve ``(github_owner, github_repo)`` for ``ProjectCreate``.

        The binding comes solely from the structured ``github`` sub-
        object; both columns stay NULL when it is absent. The validator
        on ``ProjectCreate`` guarantees ``source_url`` is never a
        ``github.com`` URL and never co-exists with ``github``, so there
        is nothing to parse out of ``source_url`` here.
        """
        if data.github is not None:
            return data.github.owner, data.github.repo
        return None, None

    @staticmethod
    def _resolve_github_for_update(
        data: ProjectUpdate,
    ) -> dict[str, Any]:
        """Resolve column updates for the github_* / source_url fields.

        The structured ``github`` binding is canonical, so a PATCH that
        touches it also reconciles the cosmetic ``source_url`` column:

        * ``github`` set → write ``github_owner``/``github_repo``, clear
          the three opportunistically-captured numeric columns so they
          re-resolve against the new repo, **and null ``source_url``** so
          a project that was previously non-GitHub does not keep a stale
          free-form URL alongside its new binding.
        * ``github: null`` → clear all five github_* columns; the binding
          is gone and the derived URL falls back to ``source_url``.
        * neither, but a non-null ``source_url`` (guaranteed non-GitHub
          by the validator) → clear all five github_* columns so the
          project flips to the non-GitHub URL.
        * otherwise → no github_* / source_url overrides; the
          ``exclude_unset`` model dump in the store handles a plain
          ``source_url: null`` clear on its own.
        """
        cleared = {
            "github_owner": None,
            "github_repo": None,
            "github_owner_id": None,
            "github_repo_id": None,
            "github_installation_id": None,
        }
        if "github" in data.model_fields_set:
            if data.github is None:
                return cleared
            return {
                "github_owner": data.github.owner,
                "github_repo": data.github.repo,
                "github_owner_id": None,
                "github_repo_id": None,
                "github_installation_id": None,
                "source_url": None,
            }
        if data.source_url is not None:
            return cleared
        return {}

    async def create(
        self, *, org_slug: str, data: ProjectCreate
    ) -> tuple[Organization, Project, Edition]:
        """Create a new project with its default ``__main`` edition.

        Raises
        ------
        ConflictError
            If a project with the same slug already exists.
        """
        org = await self._resolve_org(org_slug)
        existing = await self._store.get_by_slug(org_id=org.id, slug=data.slug)
        if existing is not None:
            msg = f"Project with slug {data.slug!r} already exists"
            raise ConflictError(msg)
        github_owner, github_repo = self._resolve_github_for_create(data)
        project = await self._store.create(
            org_id=org.id,
            data=data,
            github_owner=github_owner,
            github_repo=github_repo,
        )

        config = self._resolve_default_edition_config(
            data.default_edition, org
        )
        default_edition = await self._edition_store.create_internal(
            project_id=project.id,
            slug=DEFAULT_EDITION_SLUG,
            title=config.title,
            kind=EditionKind.main,
            tracking_mode=config.tracking_mode,
            tracking_params=config.tracking_params or {"git_ref": "main"},
            lifecycle_exempt=config.lifecycle_exempt,
        )

        self._logger.info("Created project", slug=data.slug, org=org_slug)
        return org, project, default_edition

    async def get_by_slug(
        self, *, org_slug: str, slug: str
    ) -> tuple[Organization, Project]:
        """Get a project by slug within an organization.

        Raises
        ------
        NotFoundError
            If the project is not found.
        """
        org = await self._resolve_org(org_slug)
        project = await self._store.get_by_slug(org_id=org.id, slug=slug)
        if project is None:
            msg = f"Project {slug!r} not found"
            raise NotFoundError(msg)
        return org, project

    async def get_default_edition(self, project_id: int) -> Edition | None:
        """Fetch the ``__main`` edition for a project."""
        return await self._edition_store.get_by_slug(
            project_id=project_id, slug=DEFAULT_EDITION_SLUG
        )

    async def list_by_org(
        self,
        org_slug: str,
        *,
        query: str | None = None,
        cursor_type: type[PaginationCursor[Project]] | None = None,
        cursor: PaginationCursor[Project] | None = None,
        limit: int,
    ) -> tuple[
        Organization,
        CountedPaginatedList[Project, PaginationCursor[Project]],
    ]:
        """List all projects for an organization."""
        org = await self._resolve_org(org_slug)
        if query is not None:
            search_cursor = (
                cursor if isinstance(cursor, ProjectSearchCursor) else None
            )
            result = await self._store.search_by_org(
                org.id, query=query, limit=limit, cursor=search_cursor
            )
            return org, result
        if cursor_type is None:
            msg = "cursor_type is required when query is not set"
            raise RuntimeError(msg)
        result = await self._store.list_by_org(
            org.id, cursor_type=cursor_type, cursor=cursor, limit=limit
        )
        return org, result

    async def update(
        self, *, org_slug: str, slug: str, data: ProjectUpdate
    ) -> tuple[Organization, Project]:
        """Update a project.

        Raises
        ------
        NotFoundError
            If the project is not found.
        """
        org = await self._resolve_org(org_slug)
        extra_updates = self._resolve_github_for_update(data)
        project = await self._store.update(
            org_id=org.id,
            slug=slug,
            data=data,
            extra_updates=extra_updates,
        )
        if project is None:
            msg = f"Project {slug!r} not found"
            raise NotFoundError(msg)
        self._logger.info("Updated project", slug=slug, org=org_slug)
        return org, project

    async def soft_delete(
        self, *, org_slug: str, slug: str
    ) -> tuple[Organization, list[str]]:
        """Soft-delete a project.

        Returns the resolved :class:`Organization` and the slug list of
        the project's non-deleted editions captured before the soft-
        delete. The handler keys the post-commit CDN unpublish on
        ``org.id`` (no slug re-resolution) and iterates the slug list
        without needing an additional ``EditionStore`` read pass.

        Raises
        ------
        NotFoundError
            If the project is not found.
        """
        org = await self._resolve_org(org_slug)
        project = await self._store.get_by_slug(org_id=org.id, slug=slug)
        if project is None:
            msg = f"Project {slug!r} not found"
            raise NotFoundError(msg)
        editions = await self._edition_store.list_all_by_project(project.id)
        edition_slugs = [e.slug for e in editions]
        deleted = await self._store.soft_delete(
            org_id=org.id,
            slug=slug,
            reason=TombstoneReason.manual_delete,
        )
        if not deleted:
            msg = f"Project {slug!r} not found"
            raise NotFoundError(msg)
        self._logger.info(
            "Soft-deleted project",
            slug=slug,
            org=org_slug,
            edition_count=len(edition_slugs),
        )
        return org, edition_slugs
