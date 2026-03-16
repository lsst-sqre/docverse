"""Service for managing builds."""

from __future__ import annotations

import structlog
from safir.database import CountedPaginatedList

from docverse.client.models import BuildCreate, BuildStatus, JobKind
from docverse.domain.base32id import validate_base32_id
from docverse.domain.build import Build
from docverse.domain.project import Project
from docverse.domain.queue import QueueJob
from docverse.exceptions import NotFoundError
from docverse.storage.build_store import BuildStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.pagination import BuildDateCreatedCursor
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_backend import QueueBackend
from docverse.storage.queue_job_store import QueueJobStore


class BuildService:
    """Business logic for build management."""

    def __init__(  # noqa: PLR0913
        self,
        store: BuildStore,
        org_store: OrganizationStore,
        project_store: ProjectStore,
        queue_backend: QueueBackend,
        queue_job_store: QueueJobStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._org_store = org_store
        self._project_store = project_store
        self._queue_backend = queue_backend
        self._queue_job_store = queue_job_store
        self._logger = logger

    async def _resolve_project(
        self, org_slug: str, project_slug: str
    ) -> Project:
        """Resolve org slug + project slug to the Project domain object."""
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        project = await self._project_store.get_by_slug(
            org_id=org.id, slug=project_slug
        )
        if project is None:
            msg = f"Project {project_slug!r} not found"
            raise NotFoundError(msg)
        return project

    def _validate_build_id(self, build_id: str) -> int:
        """Validate a base32 build ID string and return the int form."""
        try:
            return validate_base32_id(build_id)
        except ValueError as exc:
            msg = f"Invalid build ID {build_id!r}"
            raise NotFoundError(msg) from exc

    async def _resolve_build(self, project_id: int, build_id: str) -> Build:
        """Validate base32 ID and fetch the build, raising if not found."""
        public_id = self._validate_build_id(build_id)
        build = await self._store.get_by_public_id(
            project_id=project_id, public_id=public_id
        )
        if build is None:
            msg = f"Build {build_id!r} not found"
            raise NotFoundError(msg)
        return build

    async def create(
        self,
        *,
        org_slug: str,
        project_slug: str,
        data: BuildCreate,
        uploader: str,
    ) -> Build:
        """Create a new build with status=pending."""
        project = await self._resolve_project(org_slug, project_slug)
        build = await self._store.create(
            project_id=project.id, data=data, uploader=uploader
        )
        self._logger.info(
            "Created build",
            build_id=build.id,
            project_id=project.id,
            git_ref=data.git_ref,
        )
        return build

    async def signal_upload_complete(
        self,
        *,
        org_slug: str,
        project_slug: str,
        build_id: str,
    ) -> tuple[Build, QueueJob]:
        """Signal upload complete, transition to processing, enqueue job.

        Parameters
        ----------
        build_id
            Base32-encoded public build ID.

        Returns
        -------
        tuple
            The updated Build and the created QueueJob.
        """
        project = await self._resolve_project(org_slug, project_slug)
        build = await self._resolve_build(project.id, build_id)

        build = await self._store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        self._logger.info(
            "Build upload complete, transitioning to processing",
            build_id=build.id,
        )

        backend_job_id = await self._queue_backend.enqueue(
            "build_processing",
            {
                "org_id": project.org_id,
                "project_id": project.id,
                "build_id": build.id,
            },
        )
        queue_job = await self._queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=project.org_id,
            backend_job_id=backend_job_id,
            project_id=project.id,
            build_id=build.id,
        )
        return build, queue_job

    async def get_by_public_id(
        self,
        *,
        org_slug: str,
        project_slug: str,
        build_id: str,
    ) -> Build:
        """Get a build by its base32 public ID.

        Raises
        ------
        NotFoundError
            If the build is not found.
        """
        project = await self._resolve_project(org_slug, project_slug)
        return await self._resolve_build(project.id, build_id)

    async def list_by_project(
        self,
        *,
        org_slug: str,
        project_slug: str,
        cursor: BuildDateCreatedCursor | None = None,
        limit: int,
        status: BuildStatus | None = None,
    ) -> CountedPaginatedList[Build, BuildDateCreatedCursor]:
        """List all builds for a project."""
        project = await self._resolve_project(org_slug, project_slug)
        return await self._store.list_by_project(
            project.id, cursor=cursor, limit=limit, status=status
        )

    async def complete(self, *, build_id: int) -> Build:
        """Mark a build as completed."""
        build = await self._store.transition_status(
            build_id=build_id, new_status=BuildStatus.completed
        )
        self._logger.info("Build completed", build_id=build_id)
        return build

    async def fail(self, *, build_id: int) -> Build:
        """Mark a build as failed."""
        build = await self._store.transition_status(
            build_id=build_id, new_status=BuildStatus.failed
        )
        self._logger.info("Build failed", build_id=build_id)
        return build

    async def soft_delete(
        self,
        *,
        org_slug: str,
        project_slug: str,
        build_id: str,
    ) -> None:
        """Soft-delete a build.

        Parameters
        ----------
        build_id
            Base32-encoded public build ID.

        Raises
        ------
        NotFoundError
            If the build is not found.
        """
        project = await self._resolve_project(org_slug, project_slug)
        build = await self._resolve_build(project.id, build_id)
        deleted = await self._store.soft_delete(build_id=build.id)
        if not deleted:
            msg = f"Build {build_id!r} not found"
            raise NotFoundError(msg)
        self._logger.info("Soft-deleted build", build_id=build.id)
