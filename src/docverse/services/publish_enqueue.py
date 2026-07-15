"""Shared publish-edition enqueue helper.

Both the ``build_processing`` worker (after edition tracking) and the
``keeper_sync_project`` worker (after a synced build is finalized) need
to drive an edition through the publish path: mark the edition + its
``EditionBuildHistory`` row ``pending``, create a child ``QueueJob``,
and enqueue a ``publish_edition`` arq job. Centralizing that pattern
here keeps both call sites aligned and lets the keeper-sync worker tag
its publish jobs with ``keeper_sync_run_id`` for run-attributed
progress aggregation.

The two-phase commit-then-enqueue split lives inside
:func:`enqueue_publish_for_edition`: Phase A (the DB writes) commits
in its own ``session.begin()`` block before Phase B (the arq enqueue
plus ``backend_job_id`` write-back) runs, so a Phase B failure leaves
recoverable rows behind rather than silently dropping the publish.
"""

from __future__ import annotations

from docverse.client.models.queue_enums import JobKind, PublishStatus
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.domain.base32id import serialize_base32_id
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.queue_backend import QueueBackend
from docverse.storage.queue_job_store import QueueJobStore

__all__ = ["PublishEnqueueResult", "enqueue_publish_for_edition"]


class PublishEnqueueResult:
    """The publish ``QueueJob`` created and arq job enqueued for one pair."""

    __slots__ = (
        "backend_job_id",
        "edition_slug",
        "queue_job_id",
        "queue_job_public_id",
    )

    def __init__(
        self,
        *,
        edition_slug: str,
        queue_job_id: int,
        queue_job_public_id: str,
        backend_job_id: str,
    ) -> None:
        self.edition_slug = edition_slug
        self.queue_job_id = queue_job_id
        self.queue_job_public_id = queue_job_public_id
        self.backend_job_id = backend_job_id


async def enqueue_publish_for_edition(
    *,
    session: AsyncSession,
    edition_store: EditionStore,
    history_store: EditionBuildHistoryStore,
    queue_job_store: QueueJobStore,
    queue_backend: QueueBackend,
    org_id: int,
    project_id: int,
    project_slug: str,
    edition_id: int,
    edition_slug: str,
    build_id: int,
    build_public_id: str,
    keeper_sync_run_id: int | None = None,
) -> PublishEnqueueResult:
    """Drive one ``(edition, build)`` pair through the publish path.

    Phase A (single ``session.begin()`` transaction):

    * Set the edition's ``publish_status`` to ``pending``.
    * Look up the matching ``EditionBuildHistory`` row; if none exists
      (the keeper-sync path skips ``EditionTrackingService``, so the
      history row hasn't been recorded yet) record one. The normal
      ``build_processing`` flow always pre-records the history row, so
      this lookup hits an existing row there.
    * Set the history row's ``publish_status`` to ``pending``.
    * Insert the child ``publish_edition`` ``QueueJob`` row carrying
      ``keeper_sync_run_id`` when supplied so run-attributed progress
      aggregation can roll it up.

    Phase B (after Phase A commits):

    * Enqueue the ``publish_edition`` arq job on
      :class:`QueueBackend`'s default queue (the regular Docverse queue,
      not the dedicated keeper-sync queue).
    * Write the arq backend job ID back onto the ``QueueJob`` row in a
      short follow-up transaction.

    Phase A's commit-before-enqueue ordering is the same shape as the
    pre-existing ``build_processing._enqueue_publish_jobs`` two-phase
    split: a Phase B failure leaves the DB rows in a single recoverable
    shape (edition + history pending, child ``QueueJob`` queued without
    a ``backend_job_id``) that a future reconciliation pass can observe
    and resolve, instead of silently dropping the publish.
    """
    async with session.begin():
        await edition_store.set_publish_status(
            edition_id=edition_id, status=PublishStatus.pending
        )
        history = await history_store.get_by_edition_and_build(
            edition_id=edition_id, build_id=build_id
        )
        if history is None:
            history = await history_store.record(
                edition_id=edition_id, build_id=build_id
            )
        await history_store.set_publish_status(
            history_id=history.id, status=PublishStatus.pending
        )
        child_job = await queue_job_store.create(
            kind=JobKind.publish_edition,
            org_id=org_id,
            project_id=project_id,
            build_id=build_id,
            edition_id=edition_id,
            keeper_sync_run_id=keeper_sync_run_id,
        )
        child_job_id = child_job.id
        child_public_id = serialize_base32_id(child_job.public_id)

    backend_job_id = await queue_backend.enqueue(
        "publish_edition",
        {
            "org_id": org_id,
            "project_slug": project_slug,
            "edition_id": edition_id,
            "edition_slug": edition_slug,
            "build_id": build_id,
            "build_public_id": build_public_id,
            "queue_job_id": child_job_id,
            "queue_job_public_id": child_public_id,
        },
    )
    async with session.begin():
        await queue_job_store.set_backend_job_id(child_job_id, backend_job_id)

    return PublishEnqueueResult(
        edition_slug=edition_slug,
        queue_job_id=child_job_id,
        queue_job_public_id=child_public_id,
        backend_job_id=backend_job_id,
    )
