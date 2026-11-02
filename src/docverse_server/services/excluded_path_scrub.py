"""Enqueueing and per-build scanning for the excluded-path scrub.

Every ingest point now leaves the cache paths named in
`docverse.excluded_paths` out of a build, but builds stored before that
still carry them: keeper-sync alone copied on the order of 100 GB of
Sphinx ``.doctrees/`` pickles into R2. The scrub is the admin-triggered,
per-project job that finds those objects in existing builds and reports
them (and, outside a dry run, removes them).
"""

from __future__ import annotations

import structlog

from docverse.models import ExcludedPathScrubProgress
from docverse.models.queue_enums import JobKind
from docverse_server.domain.build import Build
from docverse_server.domain.excluded_path_scrub import (
    BuildScrubMatch,
    match_excluded_objects,
)
from docverse_server.domain.organization import Organization
from docverse_server.domain.queue import QueueJob
from docverse_server.exceptions import NotFoundError
from docverse_server.services.queue_dispatch import QueueDispatcher
from docverse_server.storage.objectstore import ObjectStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.queues import MAINTENANCE_QUEUE_NAME

__all__ = ["EXCLUDED_PATH_SCRUB_JOB_TYPE", "ExcludedPathScrubService"]

EXCLUDED_PATH_SCRUB_JOB_TYPE = "excluded_path_scrub"
"""arq function name the scrub's queue job is enqueued under."""


class ExcludedPathScrubService:
    """Queue a project's excluded-path scrub, and scan one build for it.

    Neither entry point commits. :meth:`enqueue` runs inside the request
    handler's transaction and defers its arq enqueue to the dispatcher,
    which the handler drives after committing; :meth:`scan_build` is
    session-free, so the worker can list a build's objects with no
    transaction open.
    """

    def __init__(
        self,
        *,
        project_store: ProjectStore,
        queue_job_store: QueueJobStore,
        dispatcher: QueueDispatcher,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._project_store = project_store
        self._queue_job_store = queue_job_store
        self._dispatcher = dispatcher
        self._logger = logger

    async def enqueue(
        self, *, org: Organization, project_slug: str, dry_run: bool
    ) -> QueueJob:
        """Queue a scrub of one project, or return the one already active.

        A project runs at most one scrub at a time, keyed on
        ``(org_id, subject_label)`` with the project's slug as the
        subject, as keeper-sync keys its per-project jobs. When a scrub
        is already queued or in progress for the project it is returned
        as is, whatever ``dry_run`` this request asked for: the caller
        reads the running job's report to see which kind it is.

        A new job's ``progress`` starts as an empty
        `~docverse.models.ExcludedPathScrubProgress` carrying
        ``dry_run``, so the request's mode is recorded on the job before
        any worker picks it up.

        Parameters
        ----------
        org
            Organization the project belongs to.
        project_slug
            Slug of the project to scrub.
        dry_run
            Whether the scrub only reports what it would remove.

        Returns
        -------
        QueueJob
            The newly queued job, or the project's active scrub.

        Raises
        ------
        NotFoundError
            If the organization has no live project with that slug.
        """
        project = await self._project_store.get_by_slug(
            org_id=org.id, slug=project_slug
        )
        if project is None:
            msg = f"Project {project_slug!r} not found"
            raise NotFoundError(msg)

        active = await self._queue_job_store.get_active_for_subject(
            org_id=org.id,
            kind=JobKind.excluded_path_scrub,
            subject_label=project.slug,
        )
        if active is not None:
            self._logger.info(
                "Excluded-path scrub already active for project",
                org=org.slug,
                project=project.slug,
                queue_job_id=active.id,
            )
            return active

        queue_job = await self._queue_job_store.create(
            kind=JobKind.excluded_path_scrub,
            org_id=org.id,
            project_id=project.id,
            subject_label=project.slug,
        )
        queue_job = await self._queue_job_store.update_progress(
            queue_job.id,
            ExcludedPathScrubProgress(dry_run=dry_run).model_dump(mode="json"),
        )
        self._dispatcher.defer(
            queue_job=queue_job,
            job_type=EXCLUDED_PATH_SCRUB_JOB_TYPE,
            payload={
                "org_id": org.id,
                "org_slug": org.slug,
                "project_id": project.id,
                "project_slug": project.slug,
                "dry_run": dry_run,
            },
            queue_name=MAINTENANCE_QUEUE_NAME,
        )
        self._logger.info(
            "Enqueued excluded-path scrub",
            org=org.slug,
            project=project.slug,
            dry_run=dry_run,
            queue_job_id=queue_job.id,
        )
        return queue_job

    async def scan_build(
        self, *, build: Build, object_store: ObjectStore
    ) -> BuildScrubMatch:
        """List one build's objects and select its excluded ones.

        Parameters
        ----------
        build
            The build to scan. Only its ``storage_prefix`` is read.
        object_store
            The open store holding the build's tree.

        Returns
        -------
        BuildScrubMatch
            The build's excluded objects, grouped by entry. Empty when
            the build holds none.

        Raises
        ------
        Exception
            Whatever the store raises for a listing it refuses; nothing
            is caught, so the worker can record the failure against the
            build.
        """
        objects = await object_store.list_object_entries(
            prefix=build.storage_prefix
        )
        return match_excluded_objects(
            objects, storage_prefix=build.storage_prefix
        )
