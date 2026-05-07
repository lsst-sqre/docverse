"""Service that orchestrates the lifecycle of keeper-sync runs.

This service is the operator-facing seam for the ``/orgs/{org}/
keeper-sync/runs`` endpoints. It owns the ``POST`` handler's
"create the run row, enqueue discovery, return the queue-job link"
flow plus the read-side ``GET`` operations. The deeper per-project
/ per-edition / per-build sync logic lives elsewhere (the
``KeeperSyncService`` from #287, called by the
``keeper_sync_project`` worker function).
"""

from __future__ import annotations

import structlog
from safir.database import CountedPaginatedList
from sqlalchemy.exc import IntegrityError

from docverse.client.models import (
    JobKind,
    KeeperSyncRunKind,
    KeeperSyncRunStatus,
)
from docverse.domain.keeper_sync_run import (
    KeeperSyncRun,
    KeeperSyncRunWithCounters,
)
from docverse.domain.organization import Organization
from docverse.domain.queue import JobStatus, QueueJob
from docverse.exceptions import ConflictError, NotFoundError
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.pagination import (
    KeeperSyncRunDateStartedCursor,
    QueueJobDateCreatedCursor,
)
from docverse.storage.queue_backend import QueueBackend
from docverse.storage.queue_job_store import QueueJobStore

__all__ = ["KEEPER_SYNC_QUEUE_NAME", "KeeperSyncRunService"]


KEEPER_SYNC_QUEUE_NAME = "docverse:sync-queue"
"""arq queue name dedicated to LTD-sync work.

The queue is isolated from the regular ``docverse:queue`` so a noisy
backfill cannot starve ``build_processing`` and ``publish_edition``
jobs.
"""


class KeeperSyncRunService:
    """Operator-facing service for keeper-sync run lifecycle."""

    def __init__(
        self,
        *,
        org_store: OrganizationStore,
        run_store: KeeperSyncRunStore,
        queue_backend: QueueBackend,
        queue_job_store: QueueJobStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._org_store = org_store
        self._run_store = run_store
        self._queue_backend = queue_backend
        self._queue_job_store = queue_job_store
        self._logger = logger

    async def start_run(
        self,
        *,
        org_slug: str,
        kind: KeeperSyncRunKind = KeeperSyncRunKind.backfill,
    ) -> tuple[KeeperSyncRun, QueueJob]:
        """Create a run row and enqueue ``keeper_sync_run_discovery``.

        Returns the freshly created run plus the ``queue_jobs`` row
        that backs the discovery enqueue. Raises 409 ``ConflictError``
        when sync is disabled on the org or when a non-terminal run
        already exists — both surface to the operator with
        actionable messages.
        """
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        config = org.keeper_sync_config
        if config is None or not config.enabled:
            msg = (
                f"LTD Keeper sync is not enabled for organization {org_slug!r}"
            )
            raise ConflictError(msg)

        # Pre-check that no non-terminal run exists. The DB partial
        # unique index is the authoritative backstop, but checking
        # first lets us return a clean 409 without burning an
        # auto-incremented ID inside a savepoint.
        if await self._run_store.has_non_terminal_run(org_id=org.id):
            msg = (
                f"A keeper-sync run is already in progress for "
                f"organization {org_slug!r}"
            )
            raise ConflictError(msg)

        try:
            run = await self._run_store.create(org_id=org.id, kind=kind)
        except IntegrityError as exc:
            # Lost the race against another concurrent ``POST /runs``
            # — the partial unique index fired. Translate into the
            # same 409 the pre-check would have surfaced.
            msg = (
                f"A keeper-sync run is already in progress for "
                f"organization {org_slug!r}"
            )
            raise ConflictError(msg) from exc

        queue_job = await self._queue_job_store.create(
            kind=JobKind.keeper_sync_run_discovery,
            org_id=org.id,
            keeper_sync_run_id=run.id,
            subject_label=f"discovery for {org_slug}",
        )
        backend_job_id = await self._queue_backend.enqueue(
            "keeper_sync_run_discovery",
            {
                "org_id": org.id,
                "org_slug": org.slug,
                "run_id": run.id,
                "queue_job_id": queue_job.id,
            },
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        queue_job = await self._queue_job_store.set_backend_job_id(
            queue_job.id, backend_job_id
        )
        self._logger.info(
            "Started keeper-sync run",
            org=org_slug,
            run_id=run.id,
            kind=kind.value,
        )
        return run, queue_job

    async def get_run(
        self,
        *,
        org_slug: str,
        run_id: int,
    ) -> KeeperSyncRunWithCounters:
        """Fetch a run by id, with derived ``queue_jobs`` counters."""
        org = await self._require_org(org_slug)
        run = await self._run_store.get(run_id)
        if run is None or run.org_id != org.id:
            msg = (
                f"Keeper sync run {run_id} not found for organization "
                f"{org_slug!r}"
            )
            raise NotFoundError(msg)
        counters = await self._run_store.aggregate_counters(run_id=run.id)
        return KeeperSyncRunWithCounters(run=run, counters=counters)

    async def list_runs(
        self,
        *,
        org_slug: str,
        status: KeeperSyncRunStatus | None = None,
        cursor: KeeperSyncRunDateStartedCursor | None = None,
        limit: int,
    ) -> CountedPaginatedList[KeeperSyncRun, KeeperSyncRunDateStartedCursor]:
        """List runs for an org, newest-first, with optional status filter."""
        org = await self._require_org(org_slug)
        return await self._run_store.list_by_org(
            org_id=org.id,
            status=status,
            cursor=cursor,
            limit=limit,
        )

    async def list_run_jobs(
        self,
        *,
        org_slug: str,
        run_id: int,
        status: JobStatus | None = None,
        cursor: QueueJobDateCreatedCursor | None = None,
        limit: int,
    ) -> CountedPaginatedList[QueueJob, QueueJobDateCreatedCursor]:
        """List child queue jobs attributed to a run, newest-first.

        Validates that the run exists and belongs to the requested org
        before paging — cross-org access surfaces as 404 (the same
        shape as a missing run id) rather than 403, mirroring the
        single-run ``GET`` handler.
        """
        org = await self._require_org(org_slug)
        run = await self._run_store.get(run_id)
        if run is None or run.org_id != org.id:
            msg = (
                f"Keeper sync run {run_id} not found for organization "
                f"{org_slug!r}"
            )
            raise NotFoundError(msg)
        return await self._queue_job_store.list_by_keeper_sync_run(
            run_id=run.id,
            status=status,
            cursor=cursor,
            limit=limit,
        )

    async def _require_org(self, org_slug: str) -> Organization:
        org = await self._org_store.get_by_slug(org_slug)
        if org is None:
            msg = f"Organization {org_slug!r} not found"
            raise NotFoundError(msg)
        return org
