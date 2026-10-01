"""Unit tests for the deferred queue dispatcher."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
import structlog
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import OrganizationCreate
from docverse_server.config import Configuration
from docverse_server.domain.queue import JobKind, JobStatus, QueueJob
from docverse_server.services.queue_dispatch import QueueDispatcher
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.queue_backend import EnqueuedJob
from docverse_server.storage.queue_job_store import QueueJobStore
from tests.support.arq_cancel import HangUntilCancelled
from tests.support.arq_testing import get_jobs_by_name
from tests.support.queue_dispatch import make_dispatcher

_config = Configuration()


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


class _RaisingQueueBackend:
    """Queue backend whose ``enqueue`` always fails."""

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> EnqueuedJob:
        msg = "queue backend unreachable"
        raise RuntimeError(msg)

    async def get_job_metadata(
        self, backend_job_id: str, *, queue_name: str | None = None
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    async def get_job_result(self, backend_job_id: str) -> object | None:
        raise NotImplementedError


async def _seed_job(
    session: AsyncSession, *, slug: str
) -> tuple[QueueJobStore, QueueJob]:
    """Create an org plus one committed, queued ``dashboard_build`` row.

    Leaves no transaction open: ``dispatch`` opens its own, so a stray
    autobegun read between the commit and the dispatch would collide
    with it — the same contract the ``try_enqueue_*`` helpers carry.
    """
    logger = _logger()
    org_store = OrganizationStore(session=session, logger=logger)
    store = QueueJobStore(session=session, logger=logger)
    async with session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug=slug,
                title=f"Dispatch Org {slug}",
                base_domain=f"{slug}.example.com",
            )
        )
        job = await store.create(kind=JobKind.dashboard_build, org_id=org.id)
        await session.commit()
    return store, job


@pytest.mark.asyncio
async def test_defer_does_not_touch_the_backend(
    app: None, db_session: AsyncSession
) -> None:
    """Nothing reaches arq until ``dispatch`` is called.

    This is the whole point of the indirection: the row's transaction
    has to commit before a worker can be handed a job pointing at it.
    """
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    _, job = await _seed_job(db_session, slug="qd-defer")
    dispatcher = make_dispatcher(db_session, arq_queue=arq_queue)

    dispatcher.defer(
        queue_job=job, job_type="dashboard_build", payload={"org_id": 1}
    )

    assert get_jobs_by_name(arq_queue, "dashboard_build") == []
    assert len(dispatcher.pending) == 1


@pytest.mark.asyncio
async def test_dispatch_enqueues_and_stamps_the_row(
    app: None, db_session: AsyncSession
) -> None:
    """``dispatch`` enqueues each deferral and records its arq identity."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    _, job = await _seed_job(db_session, slug="qd-dispatch")
    dispatcher = make_dispatcher(db_session, arq_queue=arq_queue)

    dispatcher.defer(
        queue_job=job, job_type="dashboard_build", payload={"org_id": 1}
    )
    (stamped,) = await dispatcher.dispatch()

    enqueued = get_jobs_by_name(arq_queue, "dashboard_build")
    assert len(enqueued) == 1
    assert stamped.backend_job_id == enqueued[0].id
    assert stamped.backend_queue_name == _config.arq_queue_name
    # The row's own identity travels in the payload so the receiving
    # worker never has to resolve itself through backend_job_id.
    assert enqueued[0].kwargs["payload"]["queue_job_id"] == job.id
    assert dispatcher.pending == ()


@pytest.mark.asyncio
async def test_dispatch_failure_leaves_an_orphan_shaped_row(
    app: None, db_session: AsyncSession
) -> None:
    """A backend outage leaves the row for the reapers' orphan sweep.

    The failure mode has to stay recoverable: ``queued`` with
    ``backend_job_id IS NULL`` is the shape ``fail_orphaned_jobs``
    reclaims, unlike a job arq knows about with no row to drive it.
    """
    store, job = await _seed_job(db_session, slug="qd-failure")
    dispatcher = QueueDispatcher(
        session=db_session,
        queue_backend=_RaisingQueueBackend(),
        queue_job_store=store,
        logger=_logger(),
    )

    dispatcher.defer(
        queue_job=job, job_type="dashboard_build", payload={"org_id": 1}
    )
    with pytest.raises(RuntimeError, match="unreachable"):
        await dispatcher.dispatch()

    row = await store.get(job.id)
    assert row is not None
    assert row.status == JobStatus.queued
    assert row.backend_job_id is None


@pytest.mark.asyncio
async def test_discard_drops_pending_without_enqueueing(
    app: None, db_session: AsyncSession
) -> None:
    """``discard`` forgets deferrals, so a later ``dispatch`` skips them.

    The caller's transaction rolled back and took the deferred rows with
    it; handing arq those jobs on the next ``dispatch`` would deliver a
    job whose row a worker can never find.
    """
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    _, job = await _seed_job(db_session, slug="qd-discard")
    dispatcher = make_dispatcher(db_session, arq_queue=arq_queue)

    dispatcher.defer(
        queue_job=job, job_type="dashboard_build", payload={"org_id": 1}
    )
    (discarded,) = dispatcher.discard()

    assert discarded.queue_job_id == job.id
    assert dispatcher.pending == ()
    assert await dispatcher.dispatch() == []
    assert get_jobs_by_name(arq_queue, "dashboard_build") == []


@pytest.mark.asyncio
async def test_undispatched_tracks_rows_until_they_are_stamped(
    app: None, db_session: AsyncSession
) -> None:
    """A deferred row is undispatched until ``dispatch`` stamps it."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    _, job = await _seed_job(db_session, slug="qd-undispatched")
    dispatcher = make_dispatcher(db_session, arq_queue=arq_queue)

    dispatcher.defer(
        queue_job=job, job_type="dashboard_build", payload={"org_id": 1}
    )
    assert dispatcher.undispatched() == (job.id,)

    await dispatcher.dispatch()
    assert dispatcher.undispatched() == ()


@pytest.mark.asyncio
async def test_undispatched_covers_a_row_mid_dispatch(
    app: None, db_session: AsyncSession
) -> None:
    """A row ``dispatch`` has taken but not yet stamped is still listed.

    ``dispatch`` empties :attr:`QueueDispatcher.pending` before it starts
    enqueueing, so a job cancelled mid-dispatch — the window the arq
    cancellation helper's hand-off variant covers — would otherwise have
    no record of the row it left an orphan. A row whose enqueue failed
    stays listed for the same reason.
    """
    store, job = await _seed_job(db_session, slug="qd-in-flight")
    hang = HangUntilCancelled()
    dispatcher = QueueDispatcher(
        session=db_session,
        queue_backend=_HangingQueueBackend(hang),
        queue_job_store=store,
        logger=_logger(),
    )
    dispatcher.defer(
        queue_job=job, job_type="dashboard_build", payload={"org_id": 1}
    )

    task = asyncio.create_task(dispatcher.dispatch())
    await hang.reached.wait()
    assert dispatcher.pending == ()
    assert dispatcher.undispatched() == (job.id,)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert dispatcher.undispatched() == (job.id,)


class _HangingQueueBackend(_RaisingQueueBackend):
    """Queue backend whose ``enqueue`` hangs until it is cancelled."""

    def __init__(self, hang: HangUntilCancelled) -> None:
        self._hang = hang

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> EnqueuedJob:
        return await self._hang(job_type, payload, queue_name)
