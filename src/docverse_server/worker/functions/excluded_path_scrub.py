"""arq worker function for the per-project ``excluded_path_scrub`` job.

``POST /orgs/{org}/projects/{project}/builds/scrub-excluded-paths``
writes one ``queue_jobs`` row with ``kind='excluded_path_scrub'`` and
``subject_label`` set to the project slug. This worker is its body: it
walks the project's completed, non-deleted builds that no real scrub has
stamped yet, one build at a time, lists each build's prefix with object
sizes, and selects the keys `docverse.excluded_paths` excludes, grouped
by excluded entry (``.doctrees/``, ``.buildinfo``, ``sub/.doctrees/``).

A dry run stops there. A real run then, for each affected build:

1. re-reads the build and skips it if it has been deleted, retired or
   stamped by another scrub since the job listed it, leaving its
   objects alone;
2. deletes the excluded objects, a bounded number at a time;
3. in one transaction, re-reads the build under its row lock, skips it
   (objects already gone) if it was retired or stamped in the meantime,
   and otherwise subtracts what was removed from ``object_count`` and
   ``total_size_bytes`` and stamps it ``excluded_paths_scrubbed`` —
   only when every delete succeeded, so a later run retries the rest.

``content_hash`` is never rewritten. A stamped build is left out of
later scrubs, so a re-run only revisits the builds this one did not
finish.

The job's ``progress`` is its report, an
`~docverse.models.ExcludedPathScrubProgress`: summary counters plus one
row per build that held excluded paths or could not be examined. The
report is rewritten every :data:`PROGRESS_FLUSH_BUILDS` builds and at
the end, so a scrub of a project with thousands of builds can be read
while it runs without rewriting a growing JSONB document once per build.

Listing and deleting run with no transaction open; each database read
or write takes its own short transaction, so a long scrub never pins a
connection idle-in-transaction or holds a build's row lock across
object-store calls.

A per-build failure (a listing or delete the store refuses, an inventory
write the database rejects) becomes an error row and the loop moves on,
so the job ends ``completed`` or ``completed_with_errors``. Only a
failure that makes the whole scrub impossible — the organization is
gone, or it has no object store — fails the queue-job row and re-raises.
"""

from __future__ import annotations

import traceback
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    ExcludedPathScrubBuild,
    ExcludedPathScrubEntry,
    ExcludedPathScrubProgress,
)
from docverse_server.config import config
from docverse_server.domain.api_urls import build_url
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.excluded_path_scrub import BuildScrubMatch
from docverse_server.exceptions import NotFoundError
from docverse_server.factory import Factory
from docverse_server.services.excluded_path_scrub import (
    ExcludedPathScrubService,
)
from docverse_server.storage.objectstore import ObjectStore
from docverse_server.worker.functions._cancellation import (
    cancellation_recorded,
    record_cancellation,
)

__all__ = ["PROGRESS_FLUSH_BUILDS", "ScrubReport", "excluded_path_scrub"]

PROGRESS_FLUSH_BUILDS = 20
"""Builds examined between two writes of the report to ``progress``.

Bounds both how stale a mid-flight read of the report can be and how
often the growing report is rewritten. The report is also written once
the last build is done, whatever the count.
"""

type _BuildUrlMaker = Callable[[str], str | None]
"""Maps a build's public id to its API URL, or `None` if unresolvable."""


@dataclass(slots=True)
class ScrubReport:
    """The scrub's running tally, rendered into the job's ``progress``."""

    dry_run: bool
    """Whether the request asked only for a report."""

    builds_examined: int = 0
    """Builds whose prefix was listed, affected or not."""

    builds_affected: int = 0
    """Builds found holding at least one excluded path."""

    objects_removed: int = 0
    """Excluded objects found across the affected builds."""

    bytes_removed: int = 0
    """Combined size of :attr:`objects_removed`."""

    builds_failed: int = 0
    """Builds that could not be examined or finished."""

    builds_skipped: int = 0
    """Affected builds a real run left alone because they were retired
    or scrubbed by another run after this one listed them."""

    rows: list[ExcludedPathScrubBuild] = field(default_factory=list)
    """One row per affected or failed build, in scrub order."""

    @property
    def has_errors(self) -> bool:
        """Whether the job should complete as ``completed_with_errors``."""
        return self.builds_failed > 0

    def add_affected(
        self,
        *,
        build_id: str,
        build_url: str | None,
        edition_slugs: list[str],
        match: BuildScrubMatch,
        objects_removed: int,
        bytes_removed: int,
        skip_reason: str | None = None,
        error: str | None = None,
    ) -> None:
        """Count an examined build with excluded paths and add its row.

        ``objects_removed`` and ``bytes_removed`` are what was deleted
        from the build (in a dry run, what would be), which a skip or a
        failed delete makes less than ``match`` holds. A ``skip_reason``
        counts the build as skipped and an ``error`` as failed.
        """
        self.builds_examined += 1
        self.builds_affected += 1
        self.objects_removed += objects_removed
        self.bytes_removed += bytes_removed
        if skip_reason is not None:
            self.builds_skipped += 1
        if error is not None:
            self.builds_failed += 1
        self.rows.append(
            ExcludedPathScrubBuild(
                build_id=build_id,
                build_url=build_url,
                edition_slugs=edition_slugs,
                entries=[
                    ExcludedPathScrubEntry(
                        path=entry.path,
                        object_count=entry.object_count,
                        total_size_bytes=entry.total_size_bytes,
                    )
                    for entry in match.entries
                ],
                skip_reason=skip_reason,
                error=error,
            )
        )

    def add_clean(self) -> None:
        """Count an examined build that holds no excluded paths."""
        self.builds_examined += 1

    def add_failed(
        self, *, build_id: str, build_url: str | None, error: str
    ) -> None:
        """Count a build that could not be examined and add its row."""
        self.builds_failed += 1
        self.rows.append(
            ExcludedPathScrubBuild(
                build_id=build_id, build_url=build_url, error=error
            )
        )

    def as_progress(self) -> dict[str, Any]:
        """Render the tally as the ``queue_jobs.progress`` JSONB body."""
        return ExcludedPathScrubProgress(
            dry_run=self.dry_run,
            builds_examined=self.builds_examined,
            builds_affected=self.builds_affected,
            objects_removed=self.objects_removed,
            bytes_removed=self.bytes_removed,
            builds_failed=self.builds_failed,
            builds_skipped=self.builds_skipped,
            builds=list(self.rows),
        ).model_dump(mode="json")


@cancellation_recorded
async def excluded_path_scrub(
    ctx: dict[str, Any], payload: dict[str, Any]
) -> str:
    """Find, and unless a dry run remove, one project's excluded paths.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``).
    payload
        Job payload with ``org_id``, ``org_slug``, ``project_id``,
        ``project_slug``, ``dry_run`` and ``queue_job_id`` (the row the
        scrub endpoint created).

    Returns
    -------
    str
        ``"skipped"`` when the late-delivery guard refused the row,
        ``"completed"`` when every build was examined (and, in a real
        run, scrubbed or skipped), and ``"completed_with_errors"`` when
        at least one build could not be. A failure that makes the whole
        scrub impossible marks the queue job ``failed`` and re-raises so
        arq logs the job as failed.

    Raises
    ------
    asyncio.CancelledError
        When arq cancels the job — the maintenance pool's per-job
        timeout, or a worker shutdown — after
        :func:`~docverse_server.worker.functions._cancellation.record_cancellation`
        has failed the row with the report so far.
    """
    org_id: int = payload["org_id"]
    org_slug: str = payload["org_slug"]
    project_id: int = payload["project_id"]
    project_slug: str = payload["project_slug"]
    dry_run: bool = payload["dry_run"]
    queue_job_id: int = payload["queue_job_id"]
    logger = structlog.get_logger(
        "docverse_server.worker.excluded_path_scrub"
    ).bind(org=org_slug, project=project_slug, dry_run=dry_run)
    report = ScrubReport(dry_run=dry_run)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()

        async with session.begin():
            # Late-delivery guard (PRD #538): a reaper may already have
            # failed this row, or arq may be re-delivering a job another
            # worker is still running.
            if await queue_job_store.start_if_queued(queue_job_id) is None:
                return "skipped"

        async with record_cancellation(
            ctx,
            queue_job_id=queue_job_id,
            timeout_seconds=config.maintenance_job_timeout_seconds,
            logger=logger,
            progress=report.as_progress,
        ):
            try:
                builds, object_store = await _prepare(
                    session=session,
                    factory=factory,
                    org_id=org_id,
                    project_id=project_id,
                )
                api_base = await _resolve_api_base_url(factory, logger)
                async with object_store:
                    await _scrub_builds(
                        session=session,
                        factory=factory,
                        builds=builds,
                        object_store=object_store,
                        report=report,
                        queue_job_id=queue_job_id,
                        build_url_for=_build_url_maker(
                            api_base,
                            org_slug=org_slug,
                            project_slug=project_slug,
                        ),
                        logger=logger,
                    )
            except Exception as exc:
                logger.exception("Excluded-path scrub failed for project")
                async with session.begin():
                    await queue_job_store.update_progress(
                        queue_job_id, report.as_progress()
                    )
                    await queue_job_store.fail(
                        queue_job_id,
                        errors={
                            "message": str(exc),
                            "type": type(exc).__name__,
                            "traceback": traceback.format_exc(),
                        },
                    )
                raise

            async with session.begin():
                await queue_job_store.update_progress(
                    queue_job_id, report.as_progress()
                )
                await queue_job_store.complete(
                    queue_job_id, has_errors=report.has_errors
                )
            logger.info(
                "Excluded-path scrub completed for project",
                builds_examined=report.builds_examined,
                builds_affected=report.builds_affected,
                objects_removed=report.objects_removed,
                bytes_removed=report.bytes_removed,
                builds_failed=report.builds_failed,
                builds_skipped=report.builds_skipped,
            )
            return (
                "completed_with_errors" if report.has_errors else "completed"
            )

    msg = "No database session available"
    raise RuntimeError(msg)


async def _prepare(
    *,
    session: AsyncSession,
    factory: Factory,
    org_id: int,
    project_id: int,
) -> tuple[list[Build], ObjectStore]:
    """Read the project's candidate builds and resolve the org's store.

    One short read transaction, closed before any object is listed. The
    store is the org's ``resolved_staging_store_label``, where
    ``build_processing`` writes a build's tree and the store the
    ``purgatory_cleanup`` sweep reclaims builds from. An org with no
    such label has nowhere the scrub could look, so the job fails
    rather than reporting a clean project.
    """
    org_store = factory.create_org_store()
    build_store = factory.create_build_store()
    async with session.begin():
        org = await org_store.get_by_id(org_id)
        if org is None:
            msg = f"Organization {org_id} not found"
            raise NotFoundError(msg)
        service_label = org.resolved_staging_store_label
        if service_label is None:
            msg = f"No object store service configured for org {org_id}"
            raise RuntimeError(msg)
        builds = await build_store.list_scrub_candidates(project_id=project_id)
        object_store = await factory.create_objectstore_for_org(
            org_id=org_id, service_label=service_label
        )
    return builds, object_store


async def _scrub_builds(
    *,
    session: AsyncSession,
    factory: Factory,
    builds: list[Build],
    object_store: ObjectStore,
    report: ScrubReport,
    queue_job_id: int,
    build_url_for: _BuildUrlMaker,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Scrub each build in turn, isolating per-build failures.

    A build whose listing fails, or that fails before anything is
    deleted, gets an error row from here; once its objects are being
    deleted, :func:`_remove_excluded` records the outcome itself, so the
    objects already gone are still counted.
    """
    service = factory.create_excluded_path_scrub_service()
    edition_store = factory.create_edition_store()
    queue_job_store = factory.create_queue_job_store()
    for index, build in enumerate(builds, start=1):
        public_id = serialize_base32_id(build.public_id)
        url = build_url_for(public_id)
        log = logger.bind(build=public_id)
        try:
            match = await service.scan_build(
                build=build, object_store=object_store
            )
            if not match.entries:
                report.add_clean()
            else:
                # Edition slugs are read only for an affected build,
                # since an unaffected one gets no row to put them in.
                async with session.begin():
                    slugs = (
                        await edition_store.list_live_slugs_by_current_build(
                            build_id=build.id
                        )
                    )
                log.info(
                    "Found excluded paths in a build",
                    entries=[entry.path for entry in match.entries],
                    object_count=match.object_count,
                    total_size_bytes=match.total_size_bytes,
                )
                await _scrub_affected(
                    session=session,
                    service=service,
                    build=build,
                    match=match,
                    object_store=object_store,
                    report=report,
                    row=_RowIdentity(
                        build_id=public_id, build_url=url, edition_slugs=slugs
                    ),
                    logger=log,
                )
        except Exception as exc:
            report.add_failed(
                build_id=public_id,
                build_url=url,
                error=f"{type(exc).__name__}: {exc}",
            )
            log.exception("Failed to scrub a build of excluded paths")
        if index % PROGRESS_FLUSH_BUILDS == 0:
            async with session.begin():
                await queue_job_store.update_progress(
                    queue_job_id, report.as_progress()
                )


@dataclass(frozen=True, slots=True)
class _RowIdentity:
    """The parts of an affected build's report row that name it."""

    build_id: str
    build_url: str | None
    edition_slugs: list[str]


async def _scrub_affected(
    *,
    session: AsyncSession,
    service: ExcludedPathScrubService,
    build: Build,
    match: BuildScrubMatch,
    object_store: ObjectStore,
    report: ScrubReport,
    row: _RowIdentity,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Report one affected build and, unless a dry run, scrub it."""
    if report.dry_run:
        report.add_affected(
            build_id=row.build_id,
            build_url=row.build_url,
            edition_slugs=row.edition_slugs,
            match=match,
            objects_removed=match.object_count,
            bytes_removed=match.total_size_bytes,
        )
        return

    # The listing may be seconds old and the candidate list much older:
    # a build retired since keeps its objects.
    async with session.begin():
        skip_reason = await service.skip_reason(build_id=build.id)
    if skip_reason is not None:
        logger.info(
            "Skipping a build retired since the scrub listed it",
            skip_reason=skip_reason,
        )
        report.add_affected(
            build_id=row.build_id,
            build_url=row.build_url,
            edition_slugs=row.edition_slugs,
            match=match,
            objects_removed=0,
            bytes_removed=0,
            skip_reason=skip_reason,
        )
        return

    await _remove_excluded(
        session=session,
        service=service,
        build=build,
        match=match,
        object_store=object_store,
        report=report,
        row=row,
        logger=logger,
    )


async def _remove_excluded(
    *,
    session: AsyncSession,
    service: ExcludedPathScrubService,
    build: Build,
    match: BuildScrubMatch,
    object_store: ObjectStore,
    report: ScrubReport,
    row: _RowIdentity,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Delete one build's excluded objects and record what went.

    Whatever happens after the deletes, the row counts the objects that
    are gone, since they are gone from the store either way: a failed
    delete or a rejected inventory write becomes the row's ``error``,
    and a build retired while its objects were being deleted its
    ``skip_reason``.
    """
    deletion = await service.delete_excluded(
        match=match, object_store=object_store
    )
    errors: list[str] = []
    if deletion.error is not None:
        errors.append(deletion.error)
        logger.error(
            "Failed to delete some excluded objects from a build",
            objects_deleted=deletion.objects_deleted,
            objects_failed=len(deletion.failures),
            error=deletion.error,
        )
    skip_reason: str | None = None
    try:
        async with session.begin():
            skip_reason = await service.record_scrub(
                build_id=build.id, deletion=deletion
            )
    except Exception as exc:
        errors.append(f"{type(exc).__name__}: {exc}")
        logger.exception(
            "Failed to record the excluded objects removed from a build",
            objects_deleted=deletion.objects_deleted,
            bytes_deleted=deletion.bytes_deleted,
        )
    else:
        if skip_reason is not None:
            logger.warning(
                "Left a build's row alone after deleting its excluded"
                " objects: it was retired or scrubbed meanwhile",
                skip_reason=skip_reason,
                objects_deleted=deletion.objects_deleted,
            )
        else:
            logger.info(
                "Removed excluded paths from a build",
                objects_deleted=deletion.objects_deleted,
                bytes_deleted=deletion.bytes_deleted,
                stamped=deletion.complete,
            )
    report.add_affected(
        build_id=row.build_id,
        build_url=row.build_url,
        edition_slugs=row.edition_slugs,
        match=match,
        objects_removed=deletion.objects_deleted,
        bytes_removed=deletion.bytes_deleted,
        skip_reason=skip_reason,
        error="; ".join(errors) if errors else None,
    )


def _build_url_maker(
    api_base: str | None, *, org_slug: str, project_slug: str
) -> _BuildUrlMaker:
    """Bind the API base and slugs for minting report rows' build URLs."""

    def make(public_id: str) -> str | None:
        if api_base is None:
            return None
        return build_url(
            api_base, org=org_slug, project=project_slug, build=public_id
        )

    return make


async def _resolve_api_base_url(
    factory: Factory, logger: structlog.stdlib.BoundLogger
) -> str | None:
    """Resolve the Docverse API base URL via Repertoire discovery.

    Returns `None` — after logging a warning — when no discovery client
    is configured, the lookup fails, or Docverse is not registered, in
    which case the report's rows carry no ``build_url`` rather than the
    scrub failing over a link.
    """
    discovery = factory.discovery
    if discovery is None:
        logger.warning(
            "No Repertoire discovery client configured; omitting build "
            "URLs from the excluded-path scrub report"
        )
        return None
    try:
        api_base = await discovery.url_for_internal("docverse")
    except Exception:
        logger.warning(
            "Failed to resolve Docverse API URL from Repertoire; omitting "
            "build URLs from the excluded-path scrub report",
            exc_info=True,
        )
        return None
    if api_base is None:
        logger.warning(
            "Docverse is not registered in Repertoire; omitting build "
            "URLs from the excluded-path scrub report"
        )
    return api_base
