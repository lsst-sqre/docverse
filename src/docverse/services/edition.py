"""Service for managing editions."""

from __future__ import annotations

import structlog
from safir.database import CountedPaginatedList, PaginationCursor

from docverse.client.models import EditionCreate, EditionKind, EditionUpdate
from docverse.client.models.queue_enums import JobKind, PublishStatus
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.edition import Edition
from docverse.domain.edition_build_history import EditionBuildHistoryWithBuild
from docverse.exceptions import ConflictError, NotFoundError
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.pagination import EditionBuildHistoryPositionCursor
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_backend import QueueBackend
from docverse.storage.queue_job_store import QueueJobStore
from docverse.validation import parse_base32_id


class EditionService:
    """Business logic for edition management."""

    def __init__(  # noqa: PLR0913
        self,
        store: EditionStore,
        org_store: OrganizationStore,
        project_store: ProjectStore,
        logger: structlog.stdlib.BoundLogger,
        history_store: EditionBuildHistoryStore,
        build_store: BuildStore,
        queue_backend: QueueBackend,
        queue_job_store: QueueJobStore,
    ) -> None:
        self._store = store
        self._org_store = org_store
        self._project_store = project_store
        self._logger = logger
        self._history_store = history_store
        self._build_store = build_store
        self._queue_backend = queue_backend
        self._queue_job_store = queue_job_store

    async def _resolve_project_id(
        self, org_slug: str, project_slug: str
    ) -> int:
        """Resolve org slug + project slug to a project internal ID."""
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
        return project.id

    async def create(
        self,
        *,
        org_slug: str,
        project_slug: str,
        data: EditionCreate,
    ) -> Edition:
        """Create a new edition.

        Raises
        ------
        ConflictError
            If an edition with the same slug already exists.
        """
        project_id = await self._resolve_project_id(org_slug, project_slug)
        existing = await self._store.get_by_slug(
            project_id=project_id, slug=data.slug
        )
        if existing is not None:
            msg = f"Edition with slug {data.slug!r} already exists"
            raise ConflictError(msg)
        edition = await self._store.create(project_id=project_id, data=data)
        self._logger.info(
            "Created edition",
            slug=data.slug,
            org=org_slug,
            project=project_slug,
        )
        return edition

    async def get_by_slug(
        self,
        *,
        org_slug: str,
        project_slug: str,
        slug: str,
    ) -> Edition:
        """Get an edition by slug within a project.

        Raises
        ------
        NotFoundError
            If the edition is not found.
        """
        project_id = await self._resolve_project_id(org_slug, project_slug)
        edition = await self._store.get_by_slug(
            project_id=project_id, slug=slug
        )
        if edition is None:
            msg = f"Edition {slug!r} not found"
            raise NotFoundError(msg)
        return edition

    async def list_by_project(  # noqa: PLR0913
        self,
        *,
        org_slug: str,
        project_slug: str,
        cursor_type: type[PaginationCursor[Edition]],
        cursor: PaginationCursor[Edition] | None = None,
        limit: int,
        kind: EditionKind | None = None,
    ) -> CountedPaginatedList[Edition, PaginationCursor[Edition]]:
        """List all editions for a project."""
        project_id = await self._resolve_project_id(org_slug, project_slug)
        return await self._store.list_by_project(
            project_id,
            cursor_type=cursor_type,
            cursor=cursor,
            limit=limit,
            kind=kind,
        )

    async def update(
        self,
        *,
        org_slug: str,
        project_slug: str,
        slug: str,
        data: EditionUpdate,
    ) -> Edition:
        """Update an edition.

        If ``data.build`` is set, apply an emergency build override: point
        the edition at the target build (even one not in history), record
        a new history entry, mark the edition ``publish_status=pending``,
        and enqueue a ``publish_edition`` job. Unlike rollback, this path
        bypasses the history-membership guard.

        Raises
        ------
        NotFoundError
            If the edition or target build is not found.
        """
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
        project_id = project.id

        build_public_id = data.build
        other_updates = EditionUpdate.model_validate(
            data.model_dump(exclude={"build"}, exclude_unset=True)
        )

        edition = await self._store.update(
            project_id=project_id, slug=slug, data=other_updates
        )
        if edition is None:
            msg = f"Edition {slug!r} not found"
            raise NotFoundError(msg)

        if build_public_id is not None:
            edition = await self._apply_build_override(
                org_id=org.id,
                project_id=project_id,
                project_slug=project_slug,
                edition=edition,
                build_public_id=build_public_id,
            )

        self._logger.info(
            "Updated edition", slug=slug, org=org_slug, project=project_slug
        )
        return edition

    async def _apply_build_override(
        self,
        *,
        org_id: int,
        project_id: int,
        project_slug: str,
        edition: Edition,
        build_public_id: str,
    ) -> Edition:
        """Point ``edition`` at an arbitrary build (emergency override)."""
        public_id = parse_base32_id(build_public_id, resource="build")

        build = await self._build_store.get_by_public_id(
            project_id=project_id, public_id=public_id
        )
        if build is None:
            msg = f"Build {build_public_id!r} not found"
            raise NotFoundError(msg)

        updated_edition = await self._store.set_current_build(
            edition_id=edition.id,
            build_id=build.id,
            skip_date_guard=True,
        )
        if updated_edition is None:
            msg = "set_current_build returned None with skip_date_guard=True"
            raise RuntimeError(msg)

        new_history_entry = await self._history_store.record(
            edition_id=edition.id, build_id=build.id
        )

        await self._store.set_publish_status(
            edition_id=edition.id, status=PublishStatus.pending
        )
        await self._history_store.set_publish_status(
            history_id=new_history_entry.id, status=PublishStatus.pending
        )
        updated_edition.publish_status = PublishStatus.pending

        child_job = await self._queue_job_store.create(
            kind=JobKind.publish_edition,
            org_id=org_id,
            project_id=project_id,
            build_id=build.id,
            edition_id=edition.id,
        )
        backend_job_id = await self._queue_backend.enqueue(
            "publish_edition",
            {
                "org_id": org_id,
                "project_slug": project_slug,
                "edition_id": edition.id,
                "edition_slug": edition.slug,
                "build_id": build.id,
                "build_public_id": serialize_base32_id(build.public_id),
                "queue_job_id": child_job.id,
            },
        )

        self._logger.info(
            "Applied edition build override",
            edition_id=edition.id,
            build=build_public_id,
            publish_queue_job_public_id=serialize_base32_id(
                child_job.public_id
            ),
            publish_backend_job_id=backend_job_id,
        )
        return updated_edition

    async def set_current_build(
        self, *, edition_id: int, build_id: int
    ) -> Edition | None:
        """Set the current build for an edition.

        Returns
        -------
        Edition or None
            The updated edition, or ``None`` if the update was skipped
            because the edition already points to a newer build.
        """
        edition = await self._store.set_current_build(
            edition_id=edition_id, build_id=build_id
        )
        if edition is None:
            self._logger.info(
                "Skipped stale build for edition",
                edition_id=edition_id,
                build_id=build_id,
            )
        else:
            self._logger.info(
                "Set current build for edition",
                edition_id=edition_id,
                build_id=build_id,
            )
        return edition

    async def list_history(  # noqa: PLR0913
        self,
        *,
        org_slug: str,
        project_slug: str,
        edition_slug: str,
        cursor: EditionBuildHistoryPositionCursor | None = None,
        limit: int,
        include_deleted: bool = False,
    ) -> CountedPaginatedList[
        EditionBuildHistoryWithBuild,
        EditionBuildHistoryPositionCursor,
    ]:
        """List build history for an edition."""
        project_id = await self._resolve_project_id(org_slug, project_slug)
        edition = await self._store.get_by_slug(
            project_id=project_id, slug=edition_slug
        )
        if edition is None:
            msg = f"Edition {edition_slug!r} not found"
            raise NotFoundError(msg)
        return await self._history_store.list_by_edition_with_build_info(
            edition.id,
            cursor=cursor,
            limit=limit,
            include_deleted=include_deleted,
        )

    async def rollback(
        self,
        *,
        org_slug: str,
        project_slug: str,
        edition_slug: str,
        build_public_id: str,
    ) -> Edition:
        """Roll back an edition to a previously-recorded build.

        Parameters
        ----------
        org_slug
            Organization slug.
        project_slug
            Project slug.
        edition_slug
            Edition slug.
        build_public_id
            Base32 public ID of the target build.

        Raises
        ------
        NotFoundError
            If the edition, build, or history entry is not found.
        """
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
        project_id = project.id

        edition = await self._store.get_by_slug(
            project_id=project_id, slug=edition_slug
        )
        if edition is None:
            msg = f"Edition {edition_slug!r} not found"
            raise NotFoundError(msg)

        public_id = parse_base32_id(build_public_id, resource="build")

        build = await self._build_store.get_by_public_id(
            project_id=project_id, public_id=public_id
        )
        if build is None:
            msg = f"Build {build_public_id!r} not found"
            raise NotFoundError(msg)

        history_entry = await self._history_store.get_by_edition_and_build(
            edition_id=edition.id, build_id=build.id
        )
        if history_entry is None:
            msg = "Build is not in this edition's history"
            raise NotFoundError(msg)

        updated_edition = await self._store.set_current_build(
            edition_id=edition.id,
            build_id=build.id,
            skip_date_guard=True,
        )
        if updated_edition is None:
            msg = "set_current_build returned None with skip_date_guard=True"
            raise RuntimeError(msg)

        new_history_entry = await self._history_store.record(
            edition_id=edition.id, build_id=build.id
        )

        await self._store.set_publish_status(
            edition_id=edition.id, status=PublishStatus.pending
        )
        await self._history_store.set_publish_status(
            history_id=new_history_entry.id, status=PublishStatus.pending
        )
        updated_edition.publish_status = PublishStatus.pending

        child_job = await self._queue_job_store.create(
            kind=JobKind.publish_edition,
            org_id=org.id,
            project_id=project_id,
            build_id=build.id,
            edition_id=edition.id,
        )
        backend_job_id = await self._queue_backend.enqueue(
            "publish_edition",
            {
                "org_id": org.id,
                "project_slug": project_slug,
                "edition_id": edition.id,
                "edition_slug": edition.slug,
                "build_id": build.id,
                "build_public_id": serialize_base32_id(build.public_id),
                "queue_job_id": child_job.id,
            },
        )

        self._logger.info(
            "Rolled back edition",
            slug=edition_slug,
            org=org_slug,
            project=project_slug,
            build=build_public_id,
            publish_queue_job_public_id=serialize_base32_id(
                child_job.public_id
            ),
            publish_backend_job_id=backend_job_id,
        )
        return updated_edition

    async def soft_delete(
        self, *, org_slug: str, project_slug: str, slug: str
    ) -> None:
        """Soft-delete an edition.

        Raises
        ------
        NotFoundError
            If the edition is not found.
        """
        project_id = await self._resolve_project_id(org_slug, project_slug)
        deleted = await self._store.soft_delete(
            project_id=project_id, slug=slug
        )
        if not deleted:
            msg = f"Edition {slug!r} not found"
            raise NotFoundError(msg)
        self._logger.info(
            "Soft-deleted edition",
            slug=slug,
            org=org_slug,
            project=project_slug,
        )
