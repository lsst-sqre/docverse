"""Service for managing builds."""

from __future__ import annotations

import structlog

from docverse.client.models import BuildCreate, BuildStatus
from docverse.domain.build import Build
from docverse.storage.build_store import BuildStore


class BuildService:
    """Business logic for build management."""

    def __init__(
        self,
        store: BuildStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._logger = logger

    async def create(
        self, *, project_id: int, data: BuildCreate, uploader: str
    ) -> Build:
        """Create a new build with status=uploading."""
        build = await self._store.create(
            project_id=project_id, data=data, uploader=uploader
        )
        self._logger.info(
            "Created build",
            build_id=build.id,
            project_id=project_id,
            git_ref=data.git_ref,
        )
        return build

    async def signal_upload_complete(self, *, build_id: int) -> Build:
        """Signal that upload is complete, transitioning to processing."""
        build = await self._store.transition_status(
            build_id=build_id, new_status=BuildStatus.processing
        )
        self._logger.info(
            "Build upload complete, transitioning to processing",
            build_id=build_id,
        )
        return build

    async def get_by_public_id(
        self, *, project_id: int, public_id: int
    ) -> Build | None:
        """Get a build by public_id within a project."""
        return await self._store.get_by_public_id(
            project_id=project_id, public_id=public_id
        )

    async def list_by_project(self, project_id: int) -> list[Build]:
        """List all builds for a project."""
        return await self._store.list_by_project(project_id)

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

    async def soft_delete(self, *, build_id: int) -> bool:
        """Soft-delete a build."""
        deleted = await self._store.soft_delete(build_id=build_id)
        if deleted:
            self._logger.info("Soft-deleted build", build_id=build_id)
        return deleted
