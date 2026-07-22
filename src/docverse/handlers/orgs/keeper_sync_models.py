"""Handler-level response models for keeper-sync run endpoints."""

from __future__ import annotations

from typing import Self

from pydantic import HttpUrl
from starlette.requests import Request

from docverse.client.models import (
    KeeperSyncEditionStatus as _KeeperSyncEditionStatusBase,
)
from docverse.client.models import (
    KeeperSyncProjectRefreshAccepted as _KeeperSyncProjectRefreshAcceptedBase,
)
from docverse.client.models import (
    KeeperSyncProjectStatus as _KeeperSyncProjectStatusBase,
)
from docverse.client.models import (
    KeeperSyncResourceType,
    KeeperSyncRunKind,
    KeeperSyncRunStatus,
    KeeperSyncTombstoneReason,
)
from docverse.client.models import KeeperSyncRun as _KeeperSyncRunBase
from docverse.client.models import (
    KeeperSyncRunCreated as _KeeperSyncRunCreatedBase,
)
from docverse.client.models import (
    KeeperSyncTombstone as _KeeperSyncTombstoneBase,
)
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.edition import Edition as EditionDomain
from docverse.domain.keeper_sync_run import (
    KeeperSyncRun as KeeperSyncRunDomain,
)
from docverse.domain.keeper_sync_run import (
    KeeperSyncRunActivity as KeeperSyncRunActivityDomain,
)
from docverse.domain.queue import QueueJob as QueueJobDomain
from docverse.exceptions import KeeperSyncInvariantError
from docverse.services.keeper_sync_project import KeeperSyncProjectStatusResult
from docverse.storage.keeper_sync import KeeperSyncState

__all__ = [
    "KeeperSyncEditionStatus",
    "KeeperSyncProjectRefreshAccepted",
    "KeeperSyncProjectStatus",
    "KeeperSyncRun",
    "KeeperSyncRunCreated",
    "KeeperSyncTombstone",
]


class KeeperSyncRun(_KeeperSyncRunBase):
    """Keeper sync run response model with HATEOAS ``self_url``."""

    @classmethod
    def from_domain(
        cls,
        run: KeeperSyncRunDomain,
        activity: KeeperSyncRunActivityDomain,
        request: Request,
        org_slug: str,
    ) -> Self:
        """Compose the response from a run plus its derived activity."""
        run_public_id = serialize_base32_id(run.public_id)
        return cls(
            self_url=HttpUrl(
                str(
                    request.url_for(
                        "get_org_keeper_sync_run",
                        org=org_slug,
                        run=run_public_id,
                    )
                )
            ),
            jobs_url=HttpUrl(
                str(
                    request.url_for(
                        "get_org_keeper_sync_run_jobs",
                        org=org_slug,
                        run=run_public_id,
                    )
                )
            ),
            id=run_public_id,
            kind=KeeperSyncRunKind(run.kind),
            status=KeeperSyncRunStatus(run.status),
            pending_count=activity.pending_count,
            succeeded_count=activity.succeeded_count,
            failed_count=activity.failed_count,
            total_count=activity.total_count,
            date_started=run.date_started,
            date_finished=run.date_finished,
            date_last_activity=activity.date_last_activity,
        )


class KeeperSyncRunCreated(_KeeperSyncRunCreatedBase):
    """``POST /runs`` response — new run plus discovery queue-job link."""

    @classmethod
    def from_domain(
        cls,
        run: KeeperSyncRunDomain,
        activity: KeeperSyncRunActivityDomain,
        queue_job: QueueJobDomain,
        request: Request,
        org_slug: str,
    ) -> Self:
        """Build the 202 envelope from the run + enqueued queue-job."""
        job_id = serialize_base32_id(queue_job.public_id)
        return cls(
            run=KeeperSyncRun.from_domain(run, activity, request, org_slug),
            job_id=job_id,
            job_url=HttpUrl(
                str(request.url_for("get_org_job", org=org_slug, job=job_id))
            ),
        )


class KeeperSyncEditionStatus(_KeeperSyncEditionStatusBase):
    """Edition-status entry with HATEOAS ``edition_url``."""

    @classmethod
    def from_domain(
        cls,
        edition: EditionDomain,
        state: KeeperSyncState | None,
        request: Request,
        org_slug: str,
        project_slug: str,
    ) -> Self:
        """Compose the entry from a Docverse edition + optional state row."""
        return cls(
            edition_url=HttpUrl(
                str(
                    request.url_for(
                        "get_edition",
                        org=org_slug,
                        project=project_slug,
                        edition=edition.slug,
                    )
                )
            ),
            slug=edition.slug,
            kind=edition.kind,
            ltd_id=state.ltd_id if state is not None else None,
            ltd_slug=state.ltd_slug if state is not None else None,
            date_last_synced=(
                state.date_last_synced if state is not None else None
            ),
        )


class KeeperSyncProjectRefreshAccepted(_KeeperSyncProjectRefreshAcceptedBase):
    """``POST /projects/{ltd_slug}/refresh`` response envelope."""

    @classmethod
    def from_domain(
        cls,
        queue_job: QueueJobDomain,
        request: Request,
        org_slug: str,
    ) -> Self:
        """Build the 202 envelope from the enqueued queue-job."""
        job_id = serialize_base32_id(queue_job.public_id)
        return cls(
            job_id=job_id,
            job_url=HttpUrl(
                str(request.url_for("get_org_job", org=org_slug, job=job_id))
            ),
        )


class KeeperSyncProjectStatus(_KeeperSyncProjectStatusBase):
    """Project-status response wrapper that mints HATEOAS URLs."""

    @classmethod
    def from_domain(
        cls,
        result: KeeperSyncProjectStatusResult,
        request: Request,
    ) -> Self:
        """Compose the response from the service result + request URLs.

        ``project_url`` is ``None`` when no Docverse project has been
        imported yet for this LTD slug — i.e. when the project-resource
        ``keeper_sync_state`` row is missing or its ``docverse_id`` is
        ``None``. ``main_edition`` is ``None`` under the same condition
        and also when the project has no ``__main`` edition yet.
        ``org_url``, ``sync_refresh_url``, and ``editions_sync_url``
        are always populated.
        """
        main_edition: _KeeperSyncEditionStatusBase | None = None
        project_url: HttpUrl | None = None
        if result.docverse_project_slug is not None:
            project_url = HttpUrl(
                str(
                    request.url_for(
                        "get_project",
                        org=result.org_slug,
                        project=result.docverse_project_slug,
                    )
                )
            )
            if result.main_edition_row is not None:
                main_edition = KeeperSyncEditionStatus.from_domain(
                    result.main_edition_row.edition,
                    result.main_edition_row.state,
                    request,
                    result.org_slug,
                    result.docverse_project_slug,
                )
        return cls(
            self_url=HttpUrl(
                str(
                    request.url_for(
                        "get_org_keeper_sync_project_status",
                        org=result.org_slug,
                        ltd_slug=result.ltd_slug,
                    )
                )
            ),
            org_url=HttpUrl(
                str(request.url_for("get_organization", org=result.org_slug))
            ),
            project_url=project_url,
            sync_refresh_url=HttpUrl(
                str(
                    request.url_for(
                        "post_org_keeper_sync_project_refresh",
                        org=result.org_slug,
                        ltd_slug=result.ltd_slug,
                    )
                )
            ),
            editions_sync_url=HttpUrl(
                str(
                    request.url_for(
                        "get_org_keeper_sync_project_editions",
                        org=result.org_slug,
                        ltd_slug=result.ltd_slug,
                    )
                )
            ),
            ltd_slug=result.ltd_slug,
            project_state=result.project_state,
            tier_status=result.tier_status,
            main_edition=main_edition,
            edition_diff=result.edition_diff,
        )


class KeeperSyncTombstone(_KeeperSyncTombstoneBase):
    """Tombstone entry wrapper minting the HATEOAS DELETE URL."""

    @classmethod
    def from_domain(
        cls,
        state: KeeperSyncState,
        display_path: str,
        request: Request,
        org_slug: str,
    ) -> Self:
        """Compose the entry from a state row + derived display path.

        ``state.date_tombstoned`` and ``state.tombstone_reason`` must
        be non-null — the caller (admin list endpoint) only invokes
        this for tombstoned rows. The guard below enforces that
        invariant at runtime and narrows both fields for static
        analysis.
        """
        if state.date_tombstoned is None or state.tombstone_reason is None:
            msg = (
                "KeeperSyncTombstone.from_domain requires a tombstoned "
                f"state row; public_id={state.public_id} has no tombstone"
            )
            raise KeeperSyncInvariantError(msg)
        tombstone_public_id = serialize_base32_id(state.public_id)
        return cls(
            self_url=HttpUrl(
                str(
                    request.url_for(
                        "delete_org_keeper_sync_tombstone",
                        org=org_slug,
                        tombstone=tombstone_public_id,
                    )
                )
            ),
            id=tombstone_public_id,
            resource_type=KeeperSyncResourceType(state.resource_type),
            ltd_slug=state.ltd_slug,
            ltd_id=state.ltd_id,
            docverse_id=state.docverse_id,
            date_tombstoned=state.date_tombstoned,
            tombstone_reason=KeeperSyncTombstoneReason(state.tombstone_reason),
            tombstone_note=state.tombstone_note,
            display_path=display_path,
        )
