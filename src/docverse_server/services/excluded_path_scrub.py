"""Enqueueing and per-build work for the excluded-path scrub.

Every ingest point now leaves the cache paths named in
`docverse.excluded_paths` out of a build, but builds stored before that
still carry them: keeper-sync alone copied on the order of 100 GB of
Sphinx ``.doctrees/`` pickles into R2. The scrub is the admin-triggered,
per-project job that finds those objects in existing builds and reports
them (and, outside a dry run, removes them).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog

from docverse.models import ExcludedPathScrubProgress
from docverse.models.queue_enums import JobKind
from docverse_server.domain.build import Build
from docverse_server.domain.excluded_path_scrub import (
    SCRUBBED_ANNOTATION,
    BuildScrubMatch,
    SizedObject,
    match_excluded_objects,
    scrub_skip_reason,
)
from docverse_server.domain.organization import Organization
from docverse_server.domain.queue import QueueJob
from docverse_server.exceptions import NotFoundError
from docverse_server.services.queue_dispatch import QueueDispatcher
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.objectstore import ObjectStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.queues import MAINTENANCE_QUEUE_NAME

__all__ = [
    "DELETE_CONCURRENCY",
    "EXCLUDED_PATH_SCRUB_JOB_TYPE",
    "ExcludedPathScrubService",
    "ScrubDeletion",
]

EXCLUDED_PATH_SCRUB_JOB_TYPE = "excluded_path_scrub"
"""arq function name the scrub's queue job is enqueued under."""

DELETE_CONCURRENCY = 16
"""Object deletes one build's scrub keeps in flight at once.

A Sphinx build's cache is on the order of a hundred objects, so this
clears one in a handful of round trips without opening a connection per
object; builds are scrubbed one after another, so it is also the bound
for the whole job.
"""


@dataclass(frozen=True, slots=True)
class ScrubDeletion:
    """What deleting one build's excluded objects actually removed."""

    deleted: tuple[SizedObject, ...]
    """The objects the store accepted the delete of."""

    failures: tuple[tuple[str, Exception], ...]
    """Each object whose delete failed, by key, with the error."""

    @property
    def objects_deleted(self) -> int:
        """Number of objects removed."""
        return len(self.deleted)

    @property
    def bytes_deleted(self) -> int:
        """Combined size of the objects removed."""
        return sum(obj.size for obj in self.deleted)

    @property
    def complete(self) -> bool:
        """Whether every excluded object was removed."""
        return not self.failures

    @property
    def error(self) -> str | None:
        """A one-line account of the failed deletes, or `None`.

        Names the first failure only, so a store refusing every delete
        of a large cache does not write one line per object into the
        report.
        """
        if not self.failures:
            return None
        key, exc = self.failures[0]
        total = len(self.deleted) + len(self.failures)
        return (
            f"{len(self.failures)} of {total} object deletes failed;"
            f" first {key}: {type(exc).__name__}: {exc}"
        )


class ExcludedPathScrubService:
    """Queue a project's excluded-path scrub, and do its per-build work.

    No entry point commits. :meth:`enqueue` runs inside the request
    handler's transaction and defers its arq enqueue to the dispatcher,
    which the handler drives after committing. :meth:`scan_build` and
    :meth:`delete_excluded` are session-free, so the worker can list and
    delete a build's objects with no transaction open; the worker runs
    :meth:`skip_reason` and :meth:`record_scrub` inside short
    transactions of its own.
    """

    def __init__(
        self,
        *,
        project_store: ProjectStore,
        build_store: BuildStore,
        queue_job_store: QueueJobStore,
        dispatcher: QueueDispatcher,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._project_store = project_store
        self._build_store = build_store
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

    async def skip_reason(self, *, build_id: int) -> str | None:
        """Say whether a build is still the scrub's to delete from.

        An unlocked read, run just before the scrub deletes anything: a
        build deleted or finished by another scrub since the job listed
        it keeps its objects. The decision that guards the inventory
        write is :meth:`record_scrub`'s, made again under the row lock.

        Parameters
        ----------
        build_id
            The build about to be scrubbed.

        Returns
        -------
        str or None
            Why the build is skipped, or `None` to go ahead; see
            `~docverse_server.domain.excluded_path_scrub.scrub_skip_reason`.
        """
        return scrub_skip_reason(await self._build_store.get_by_id(build_id))

    async def delete_excluded(
        self, *, match: BuildScrubMatch, object_store: ObjectStore
    ) -> ScrubDeletion:
        """Delete one build's excluded objects, at most a few at a time.

        Every object is attempted, at most :data:`DELETE_CONCURRENCY` in
        flight, and a failed delete does not stop the rest: the result
        says exactly which objects went, so the build's inventory can be
        corrected by what was removed rather than what was meant to be.

        Parameters
        ----------
        match
            The build's excluded objects, from :meth:`scan_build`.
        object_store
            The open store holding the build's tree.

        Returns
        -------
        ScrubDeletion
            The objects removed and the deletes that failed.
        """
        semaphore = asyncio.Semaphore(DELETE_CONCURRENCY)

        async def _delete(obj: SizedObject) -> Exception | None:
            async with semaphore:
                try:
                    await object_store.delete_object(key=obj.key)
                except Exception as exc:
                    return exc
            return None

        objects = match.objects
        outcomes = await asyncio.gather(*(_delete(obj) for obj in objects))
        return ScrubDeletion(
            deleted=tuple(
                obj
                for obj, exc in zip(objects, outcomes, strict=True)
                if exc is None
            ),
            failures=tuple(
                (obj.key, exc)
                for obj, exc in zip(objects, outcomes, strict=True)
                if exc is not None
            ),
        )

    async def record_scrub(
        self, *, build_id: int, deletion: ScrubDeletion
    ) -> str | None:
        """Correct a scrubbed build's inventory and stamp it as scrubbed.

        Run in one transaction after :meth:`delete_excluded`. The build
        is re-read under ``SELECT ... FOR UPDATE`` first, and left alone
        when it has been deleted since, is no longer ``completed``, or
        already carries the
        `~docverse_server.domain.excluded_path_scrub.SCRUBBED_ANNOTATION`
        stamp. That last check is what stops two scrubs of one project
        that raced over the same build from both subtracting its cache:
        their deletes are idempotent, and only the first to take the lock
        writes.

        Otherwise the removed objects are subtracted from the build's
        ``object_count`` and ``total_size_bytes``, and, only when every
        delete succeeded, the stamp records when and how much was
        removed. A build with failed deletes stays unstamped, so a re-run
        lists it again and subtracts just what this run left behind.
        ``content_hash`` is not touched.

        Parameters
        ----------
        build_id
            The scrubbed build.
        deletion
            What :meth:`delete_excluded` removed from it.

        Returns
        -------
        str or None
            Why the build was left alone, or `None` once it is written.

        Raises
        ------
        InvalidBuildStateError
            If the store refuses the inventory write.
        """
        current = await self._build_store.get_for_update(build_id=build_id)
        reason = scrub_skip_reason(current)
        if reason is not None:
            return reason
        if deletion.objects_deleted:
            await self._build_store.decrement_inventory(
                build_id=build_id,
                object_count=deletion.objects_deleted,
                total_size_bytes=deletion.bytes_deleted,
            )
        if deletion.complete:
            await self._build_store.set_annotation(
                build_id=build_id,
                key=SCRUBBED_ANNOTATION,
                value={
                    "date_scrubbed": datetime.now(tz=UTC).isoformat(),
                    "objects_removed": deletion.objects_deleted,
                    "bytes_removed": deletion.bytes_deleted,
                },
            )
        return None
