"""Handler-level response models for keeper-sync run endpoints."""

from __future__ import annotations

from typing import Self

from starlette.requests import Request

from docverse.client.models import KeeperSyncRun as _KeeperSyncRunBase
from docverse.client.models import (
    KeeperSyncRunCreated as _KeeperSyncRunCreatedBase,
)
from docverse.client.models import KeeperSyncRunKind, KeeperSyncRunStatus
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.keeper_sync_run import (
    KeeperSyncRun as KeeperSyncRunDomain,
)
from docverse.domain.keeper_sync_run import (
    KeeperSyncRunCounters as KeeperSyncRunCountersDomain,
)
from docverse.domain.queue import QueueJob as QueueJobDomain

__all__ = ["KeeperSyncRun", "KeeperSyncRunCreated"]


class KeeperSyncRun(_KeeperSyncRunBase):
    """Keeper sync run response model with HATEOAS ``self_url``."""

    @classmethod
    def from_domain(
        cls,
        run: KeeperSyncRunDomain,
        counters: KeeperSyncRunCountersDomain,
        request: Request,
        org_slug: str,
    ) -> Self:
        """Compose the response from a run plus its derived counters."""
        return cls(
            self_url=str(
                request.url_for(
                    "get_org_keeper_sync_run",
                    org=org_slug,
                    run_id=run.id,
                )
            ),
            id=run.id,
            kind=KeeperSyncRunKind(run.kind),
            status=KeeperSyncRunStatus(run.status),
            pending_count=counters.pending_count,
            succeeded_count=counters.succeeded_count,
            failed_count=counters.failed_count,
            total_count=counters.total_count,
            date_started=run.date_started,
            date_finished=run.date_finished,
        )


class KeeperSyncRunCreated(_KeeperSyncRunCreatedBase):
    """``POST /runs`` response — new run plus discovery queue-job link."""

    @classmethod
    def from_domain(
        cls,
        run: KeeperSyncRunDomain,
        counters: KeeperSyncRunCountersDomain,
        queue_job: QueueJobDomain,
        request: Request,
        org_slug: str,
    ) -> Self:
        """Build the 202 envelope from the run + enqueued queue-job."""
        queue_job_id = serialize_base32_id(queue_job.public_id)
        return cls(
            run=KeeperSyncRun.from_domain(run, counters, request, org_slug),
            queue_job_id=queue_job_id,
            queue_job_url=str(
                request.url_for("get_queue_job", job=queue_job_id)
            ),
        )
