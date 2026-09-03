"""Service for managing builds."""

from __future__ import annotations

import structlog
from safir.database import CountedPaginatedList

from docverse.models import BuildCreate, BuildStatus, JobKind
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.project import Project
from docverse_server.domain.queue import QueueJob
from docverse_server.exceptions import NotFoundError
from docverse_server.services.queue_dispatch import QueueDispatcher
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.pagination import BuildDateCreatedCursor
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.validation import parse_base32_id

# The two statuses a build can still leave under its own power: it is
# waiting for a worker, or a worker has it. Everything else is terminal
# and keeps the status it earned, which is why the retirement helpers
# below (:meth:`BuildService.cancel_if_unfinished` and
# :meth:`BuildService.fail_if_unfinished`) test membership here before
# writing. Only these two would otherwise leave a retired build claiming
# to be waiting for, or held by, a worker.
_UNFINISHED_STATUSES: frozenset[BuildStatus] = frozenset(
    {BuildStatus.pending, BuildStatus.processing}
)


class BuildService:
    """Business logic for build management."""

    def __init__(
        self,
        *,
        store: BuildStore,
        org_store: OrganizationStore,
        project_store: ProjectStore,
        dispatcher: QueueDispatcher,
        queue_job_store: QueueJobStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._store = store
        self._org_store = org_store
        self._project_store = project_store
        self._dispatcher = dispatcher
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
        return parse_base32_id(build_id, resource="build")

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
            project_id=project.id,
            project_slug=project_slug,
            data=data,
            uploader=uploader,
        )
        self._logger.info(
            "Created build",
            build=serialize_base32_id(build.public_id),
            org=org_slug,
            project=project_slug,
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
        """Signal upload complete, transition to processing, queue the job.

        The arq enqueue is *deferred*, not skipped: this runs inside the
        handler's transaction, so anything handed to arq here could be
        delivered to a worker that cannot yet see the ``queue_jobs`` row
        this method inserts. The enqueue is registered on the factory's
        :class:`~docverse_server.services.queue_dispatch.QueueDispatcher`
        instead, and the handler issues it after committing.

        Parameters
        ----------
        build_id
            Base32-encoded public build ID.

        Returns
        -------
        tuple
            The updated Build and the created QueueJob. The job's
            ``backend_job_id`` is still ``None`` — the dispatcher stamps
            it once arq has accepted the job.
        """
        project = await self._resolve_project(org_slug, project_slug)
        build = await self._resolve_build(project.id, build_id)

        build = await self._store.transition_status(
            build_id=build.id,
            new_status=BuildStatus.processing,
            org_slug=org_slug,
            project_slug=project_slug,
        )
        self._logger.info(
            "Build upload complete, transitioning to processing",
            build=build_id,
            org=org_slug,
            project=project_slug,
        )

        queue_job = await self._queue_job_store.create(
            kind=JobKind.build_processing,
            org_id=project.org_id,
            project_id=project.id,
            build_id=build.id,
        )
        self._dispatcher.defer(
            queue_job=queue_job,
            job_type="build_processing",
            payload={
                "org_id": project.org_id,
                "org_slug": org_slug,
                "project_id": project.id,
                "project_slug": project_slug,
                "build_id": build.id,
                "build_public_id": serialize_base32_id(build.public_id),
            },
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

    async def complete(
        self,
        *,
        build_id: int,
        org_slug: str | None = None,
        project_slug: str | None = None,
    ) -> Build:
        """Mark a build as completed."""
        build = await self._store.transition_status(
            build_id=build_id,
            new_status=BuildStatus.completed,
            org_slug=org_slug,
            project_slug=project_slug,
        )
        self._logger.info(
            "Build completed",
            build=serialize_base32_id(build.public_id),
        )
        return build

    async def fail(
        self,
        *,
        build_id: int,
        org_slug: str | None = None,
        project_slug: str | None = None,
    ) -> Build:
        """Mark a build as failed."""
        build = await self._store.transition_status(
            build_id=build_id,
            new_status=BuildStatus.failed,
            org_slug=org_slug,
            project_slug=project_slug,
        )
        self._logger.info(
            "Build failed", build=serialize_base32_id(build.public_id)
        )
        return build

    async def supersede(
        self,
        *,
        build_id: int,
        org_slug: str | None = None,
        project_slug: str | None = None,
    ) -> Build:
        """Mark a build superseded by a newer build for the same ref.

        Terminal and blameless: nothing was wrong with the build, a
        newer live build for the same ``(project, git_ref)`` simply took
        over before this one was processed. The ``build_processing``
        stale-skip path calls this in the same transaction that
        completes the build's queue job, so a skipped build never stays
        stranded in ``processing`` with no worker on it.
        """
        build = await self._store.transition_status(
            build_id=build_id,
            new_status=BuildStatus.superseded,
            org_slug=org_slug,
            project_slug=project_slug,
        )
        self._logger.info(
            "Build superseded",
            build=serialize_base32_id(build.public_id),
        )
        return build

    async def cancel(
        self,
        *,
        build_id: int,
        org_slug: str | None = None,
        project_slug: str | None = None,
    ) -> Build:
        """Mark a build cancelled because it was deleted before publishing.

        Two paths cancel a build — the DELETE handler, and the worker's
        guard against processing a build that was deleted out from under
        it — and either can run second. A build that is *already*
        ``cancelled`` is therefore returned unchanged rather than raising
        :exc:`~docverse_server.exceptions.InvalidBuildStateError`: the
        caller asked for a state the row is already in, so there is
        nothing to report. The no-op is scoped to ``cancelled`` alone;
        deleting a ``completed`` or ``failed`` build still raises,
        because those rows keep the status they earned.

        The idempotency read takes the row lock the transition itself
        needs, so "is it already cancelled?" is answered from the row as
        it stands rather than from a snapshot a concurrent worker may
        already have moved past.

        Raises
        ------
        InvalidBuildStateError
            If the build is not found, or is in a terminal status other
            than ``cancelled``.
        """
        existing = await self._store.get_for_update(build_id=build_id)
        if existing is not None and existing.status == BuildStatus.cancelled:
            return existing
        build = await self._store.transition_status(
            build_id=build_id,
            new_status=BuildStatus.cancelled,
            org_slug=org_slug,
            project_slug=project_slug,
        )
        self._logger.info(
            "Build cancelled",
            build=serialize_base32_id(build.public_id),
        )
        return build

    async def cancel_if_unfinished(
        self,
        *,
        build_id: int,
        org_slug: str | None = None,
        project_slug: str | None = None,
    ) -> Build | None:
        """Cancel a build unless it already finished.

        The retirement half of a soft-delete, for the callers that hold
        an internal build id and must not disturb a build that already
        reached ``completed``, ``failed`` or ``superseded``: the DELETE
        handler by way of :meth:`soft_delete`, and the lifecycle
        reaper's build-history-orphan rule, which matches never-finished
        builds by design (it falls back to ``date_created`` when there
        is no ``date_completed``).

        The status is re-read here rather than trusted from the caller's
        snapshot, because a lifecycle tick evaluates rules against rows
        it loaded earlier in the run. It is re-read *under the row lock*
        because the answer decides what gets written next: a worker
        committing a completion between an unlocked read and the cancel
        would leave this asking for a transition the row can no longer
        make, and :exc:`InvalidBuildStateError` would come out of a
        DELETE that has a perfectly good answer — leave the earned
        status alone and soft-delete. Holding the lock to the end of the
        caller's transaction also means a worker racing the other way
        blocks and sees ``cancelled``.

        Returns
        -------
        Build or None
            The cancelled build, or ``None`` when the row had already
            finished (or vanished) and was left as it stands.
        """
        existing = await self._store.get_for_update(build_id=build_id)
        if existing is None or existing.status not in _UNFINISHED_STATUSES:
            return None
        return await self.cancel(
            build_id=build_id,
            org_slug=org_slug,
            project_slug=project_slug,
        )

    async def fail_if_unfinished(
        self,
        *,
        build_id: int,
        org_slug: str | None = None,
        project_slug: str | None = None,
    ) -> Build | None:
        """Fail a build unless it already finished.

        The worker's error path calls this rather than :meth:`fail`,
        because the row it is failing may have gone terminal underneath
        it: a DELETE cancels a ``processing`` build without taking the
        BUILD_PROCESSING lock, and the stranded-build sweep fails one.
        Letting :exc:`InvalidBuildStateError` escape there would abort
        the very transaction that has to mark the queue job failed,
        leaving the job stranded ``in_progress`` — the state this whole
        change set exists to remove.

        The read is locked for the same reason
        :meth:`cancel_if_unfinished`'s is: the status it returns decides
        whether a write happens at all, and an unlocked one could be
        stale by the time :meth:`fail` acted on it — which would raise
        the very error this method exists to avoid.

        Returns
        -------
        Build or None
            The failed build, or ``None`` when the row had already
            reached a terminal status (or vanished) and keeps the status
            it earned.
        """
        existing = await self._store.get_for_update(build_id=build_id)
        if existing is None or existing.status not in _UNFINISHED_STATUSES:
            self._logger.info(
                "Build already terminal; leaving its status as it stands",
                build_id=build_id,
                status=existing.status.value if existing is not None else None,
            )
            return None
        return await self.fail(
            build_id=build_id,
            org_slug=org_slug,
            project_slug=project_slug,
        )

    async def soft_delete(
        self,
        *,
        org_slug: str,
        project_slug: str,
        build_id: str,
    ) -> None:
        """Soft-delete a build, cancelling it if it has not finished.

        A ``pending`` or ``processing`` build is transitioned to
        ``cancelled`` in the same transaction that stamps
        ``date_deleted``, so no reader ever sees a deleted build still
        claiming a worker is on it. A build that already reached
        ``completed``, ``failed`` or ``superseded`` keeps the status it
        earned and only gains ``date_deleted``.

        Every path that soft-deletes a build goes through the same
        retirement step — this method for a DELETE, and
        :meth:`cancel_if_unfinished` directly for the lifecycle
        reaper — so the "deleted implies finished" reading holds however
        the row was deleted.

        The worker's deleted-self guard cancels too, and either side may
        run second; :meth:`cancel` is idempotent for exactly that
        reason.

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
        cancelled = await self.cancel_if_unfinished(
            build_id=build.id,
            org_slug=org_slug,
            project_slug=project_slug,
        )
        status = cancelled.status if cancelled is not None else build.status
        deleted = await self._store.soft_delete(build_id=build.id)
        if not deleted:
            msg = f"Build {build_id!r} not found"
            raise NotFoundError(msg)
        self._logger.info(
            "Soft-deleted build",
            build=build_id,
            org=org_slug,
            project=project_slug,
            status=status.value,
        )
