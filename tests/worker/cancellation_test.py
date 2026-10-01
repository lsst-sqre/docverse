"""Tests for the arq cancellation helper.

``docverse_server.worker.functions._cancellation.record_cancellation``
is the context manager a ``queue_jobs``-backed arq function wraps its
body in so that arq's cancel — its per-job timeout, or a worker shutdown
— fails the job's row instead of stranding it ``in_progress`` for the
reaper (PRD #765, #699). These tests drive the helper directly around a
job body that never finishes and cancel the task the way arq does; the
worker-level wiring is covered next to each worker function's own tests
(e.g. ``keeper_sync_project_test.py``).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import timedelta
from typing import Any

import httpx
import pytest
import structlog
from safir.dependencies.db_session import db_session_dependency
from safir.testing.sentry import capture_events_fixture, sentry_init_fixture
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import JobKind, OrganizationCreate
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.queue import JobStatus, QueueJob
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions import (
    _cancellation as cancellation_module,
)
from docverse_server.worker.functions._cancellation import (
    JOB_TIMEOUT_MESSAGE,
    record_cancellation,
)
from tests.worker.conftest import make_worker_ctx

TIMEOUT_SECONDS = 3600
"""The pool timeout the tests hand the helper."""


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_started_job(
    db_session: AsyncSession,
    *,
    started_ago: timedelta | None = None,
) -> QueueJob:
    """Seed an org and one ``in_progress`` queue job, as a worker leaves it.

    ``started_ago`` backdates the row's ``date_started`` so a test can
    put the cancel past (or short of) the pool timeout without waiting.
    """
    async with db_session.begin():
        org = await OrganizationStore(
            session=db_session, logger=_logger()
        ).create(
            OrganizationCreate(
                slug="cancel-org",
                title="Cancel Org",
                base_domain="cancel-org.example.com",
            )
        )
        store = QueueJobStore(session=db_session, logger=_logger())
        job = await store.create(kind=JobKind.dashboard_build, org_id=org.id)
        started = await store.start_if_queued(job.id)
        assert started is not None
        if started_ago is not None:
            await db_session.execute(
                update(SqlQueueJob)
                .where(SqlQueueJob.id == job.id)
                .values(date_started=func.now() - started_ago)
            )
    return started


async def _cancel_running_job(
    ctx: dict[str, Any],
    *,
    queue_job_id: int,
    timeout_seconds: float = TIMEOUT_SECONDS,
    progress: Callable[[], Mapping[str, Any]] | None = None,
) -> None:
    """Cancel a never-ending job body run under the helper.

    Mirrors arq: the body runs in its own task, the task is cancelled
    while the body is awaiting, and the ``CancelledError`` must come
    back out of the task once the helper has recorded it.
    """
    running = asyncio.Event()

    async def _job() -> None:
        async with record_cancellation(
            ctx,
            queue_job_id=queue_job_id,
            timeout_seconds=timeout_seconds,
            logger=_logger(),
            progress=progress,
        ):
            running.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(_job())
    waiter = asyncio.create_task(running.wait())
    await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
    waiter.cancel()
    if task.done():
        # The body stopped before it ever ran: surface why rather than
        # waiting forever for a signal that will not come.
        await task
        pytest.fail("job body returned before it was cancelled")
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def _get_job(queue_job_id: int) -> QueueJob:
    async for session in db_session_dependency():
        async with session.begin():
            job = await QueueJobStore(session=session, logger=_logger()).get(
                queue_job_id
            )
        assert job is not None
        return job
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_cancel_fails_the_row_and_reraises(
    app: None, db_session: AsyncSession
) -> None:
    """A cancelled body fails its row with a ``CancelledError`` payload.

    The cancel lands well short of the pool timeout, so it reads as a
    worker shutdown (a rolling deploy's SIGTERM), and the cancel still
    propagates so arq records the job as failed.
    """
    job = await _seed_started_job(db_session)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    await _cancel_running_job(ctx, queue_job_id=job.id)
    await ctx["http_client"].aclose()

    failed = await _get_job(job.id)
    assert failed.status == JobStatus.failed
    assert failed.date_completed is not None
    assert failed.errors is not None
    assert failed.errors["type"] == "CancelledError"
    assert failed.errors["reason"] == "worker_shutdown"
    assert failed.errors["timeout_seconds"] == TIMEOUT_SECONDS
    assert 0 <= failed.errors["elapsed_seconds"] < TIMEOUT_SECONDS
    assert "worker shut down" in failed.errors["message"]


@pytest.mark.asyncio
async def test_cancel_at_the_pool_timeout_reads_as_job_timeout(
    app: None, db_session: AsyncSession
) -> None:
    """A cancel once the row has run for the pool timeout is the timeout.

    The row is backdated past ``timeout_seconds``, as arq's own
    ``wait_for`` would find it when it cancels the job.
    """
    job = await _seed_started_job(
        db_session, started_ago=timedelta(seconds=TIMEOUT_SECONDS + 60)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    await _cancel_running_job(ctx, queue_job_id=job.id)
    await ctx["http_client"].aclose()

    failed = await _get_job(job.id)
    assert failed.status == JobStatus.failed
    assert failed.errors is not None
    assert failed.errors["reason"] == "job_timeout"
    assert failed.errors["elapsed_seconds"] >= TIMEOUT_SECONDS + 60
    assert "timeout" in failed.errors["message"]


@pytest.mark.asyncio
async def test_cancel_just_short_of_the_timeout_reads_as_job_timeout(
    app: None, db_session: AsyncSession
) -> None:
    """The row's pickup latency does not turn a timeout into a shutdown.

    arq starts the timeout clock before the job has stamped
    ``date_started``, so a real timeout measures slightly under
    ``timeout_seconds`` from the row; the helper's slack absorbs it.
    """
    job = await _seed_started_job(
        db_session, started_ago=timedelta(seconds=TIMEOUT_SECONDS - 1)
    )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    await _cancel_running_job(ctx, queue_job_id=job.id)
    await ctx["http_client"].aclose()

    failed = await _get_job(job.id)
    assert failed.errors is not None
    assert failed.errors["reason"] == "job_timeout"


@pytest.mark.parametrize(
    ("started_ago", "expected_event_count"),
    [
        pytest.param(
            timedelta(seconds=TIMEOUT_SECONDS + 60), 1, id="job_timeout"
        ),
        pytest.param(timedelta(seconds=0), 0, id="worker_shutdown"),
    ],
)
@pytest.mark.asyncio
async def test_cancel_captures_to_sentry_only_for_a_timeout(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    started_ago: timedelta,
    expected_event_count: int,
) -> None:
    """A timeout pages; a shutdown cancel is routine and does not.

    Every rolling deploy cancels whatever the old pod was running, so a
    Sentry event per ``worker_shutdown`` would be noise. A job that ran
    out its pool timeout is the signal an operator needs.
    """
    job = await _seed_started_job(db_session, started_ago=started_ago)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    with sentry_init_fixture() as init:
        init(environment="test")
        captured = capture_events_fixture(monkeypatch)()
        await _cancel_running_job(ctx, queue_job_id=job.id)
    await ctx["http_client"].aclose()

    assert len(captured.errors) == expected_event_count
    if expected_event_count == 0:
        return
    event = captured.errors[0]
    assert event["message"] == JOB_TIMEOUT_MESSAGE
    assert event["level"] == "warning"
    assert event["tags"]["job_function"] == JobKind.dashboard_build.value
    # The public id is high-cardinality, so it rides in the context.
    assert "job_public_id" not in event["tags"]
    context = event["contexts"]["queue_job_cancellation"]
    assert context["job_public_id"] == serialize_base32_id(job.public_id)
    assert context["reason"] == "job_timeout"
    assert context["timeout_seconds"] == TIMEOUT_SECONDS
    assert context["elapsed_seconds"] >= TIMEOUT_SECONDS + 60


@pytest.mark.asyncio
async def test_cleanup_failure_is_logged_and_the_cancel_still_propagates(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The helper never raises out of its own cleanup.

    A worker shutdown is exactly when the database may be unreachable.
    If the fresh session cannot even be opened, the failure is logged
    with its traceback, the row is left for the reaper, and the original
    ``CancelledError`` — not the cleanup's exception — still reaches arq.
    """
    job = await _seed_started_job(db_session)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    async def _unreachable_database() -> AsyncIterator[AsyncSession]:
        msg = "database unreachable"
        raise RuntimeError(msg)
        yield  # pragma: no cover - makes this an async generator

    monkeypatch.setattr(
        cancellation_module, "db_session_dependency", _unreachable_database
    )
    with capture_logs() as logs:
        await _cancel_running_job(ctx, queue_job_id=job.id)
    await ctx["http_client"].aclose()

    failures = [
        log
        for log in logs
        if log["event"] == "Failed to record the queue job's cancellation"
    ]
    assert len(failures) == 1
    assert failures[0]["log_level"] == "error"
    assert failures[0]["exc_info"] is True
    assert not any(log["event"] == "Queue job cancelled" for log in logs)
    monkeypatch.undo()
    untouched = await _get_job(job.id)
    assert untouched.status == JobStatus.in_progress


@pytest.mark.asyncio
async def test_cancel_leaves_a_terminal_row_as_it_stands(
    app: None, db_session: AsyncSession
) -> None:
    """A row the job already closed out keeps the status it earned.

    ``publish_edition`` completes its row before its best-effort CDN
    purge precisely so a cancel there cannot strand it; recording that
    cancel must not then turn the completed publish into a failure.
    """
    job = await _seed_started_job(db_session)
    async with db_session.begin():
        await QueueJobStore(session=db_session, logger=_logger()).complete(
            job.id
        )
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())

    with capture_logs() as logs:
        await _cancel_running_job(ctx, queue_job_id=job.id)
    await ctx["http_client"].aclose()

    completed = await _get_job(job.id)
    assert completed.status == JobStatus.completed
    assert completed.errors is None
    assert not any(log["event"] == "Queue job cancelled" for log in logs)


@pytest.mark.asyncio
async def test_cancel_merges_the_callers_progress_into_the_row(
    app: None, db_session: AsyncSession
) -> None:
    """Whatever progress the job exposes is merged onto the failed row.

    The callable is read at cancel time, so it reports how far the job
    actually got; keys the job recorded earlier survive the merge.
    """
    job = await _seed_started_job(db_session)
    async with db_session.begin():
        await QueueJobStore(
            session=db_session, logger=_logger()
        ).update_progress(job.id, {"slice_index": 0})
    ctx = make_worker_ctx(http_client=httpx.AsyncClient())
    visited = {"count": 0}

    def _progress() -> dict[str, Any]:
        return {"editions_visited": visited["count"]}

    visited["count"] = 7
    await _cancel_running_job(ctx, queue_job_id=job.id, progress=_progress)
    await ctx["http_client"].aclose()

    failed = await _get_job(job.id)
    assert failed.status == JobStatus.failed
    assert failed.progress == {"slice_index": 0, "editions_visited": 7}
