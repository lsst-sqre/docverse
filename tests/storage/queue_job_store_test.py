"""Tests for QueueJobStore."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.domain.queue import JobKind, JobStatus
from docverse.exceptions import InvalidJobStateError
from docverse.storage.queue_job_store import QueueJobStore


@pytest.fixture
def store(
    db_session: async_scoped_session[AsyncSession],
) -> QueueJobStore:
    logger = structlog.get_logger("docverse")
    return QueueJobStore(session=db_session, logger=logger)


@pytest.mark.asyncio
async def test_create_job(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await db_session.commit()
    assert job.status == JobStatus.queued
    assert job.public_id > 0
    assert job.kind == JobKind.build_processing
    assert job.org_id == 1
    assert job.date_created is not None
    assert job.date_started is None
    assert job.date_completed is None


@pytest.mark.asyncio
async def test_start_job(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        started = await store.start(job.id)
        await db_session.commit()
    assert started.status == JobStatus.in_progress
    assert started.date_started is not None


@pytest.mark.asyncio
async def test_start_job_wrong_status(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await store.start(job.id)
        with pytest.raises(InvalidJobStateError):
            await store.start(job.id)
        await db_session.commit()


@pytest.mark.asyncio
async def test_update_phase(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await store.start(job.id)
        updated = await store.update_phase(
            job.id, "uploading", progress={"step": 1}
        )
        await db_session.commit()
    assert updated.phase == "uploading"
    assert updated.progress == {"step": 1}


@pytest.mark.asyncio
async def test_update_progress_merge(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await store.start(job.id)
        await store.update_progress(job.id, {"a": 1, "b": 2})
        merged = await store.update_progress(job.id, {"b": 99, "c": 3})
        await db_session.commit()
    assert merged.progress is not None
    assert merged.progress["a"] == 1
    assert merged.progress["b"] == 99
    assert merged.progress["c"] == 3


@pytest.mark.asyncio
async def test_update_progress_from_null(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        updated = await store.update_progress(job.id, {"key": "value"})
        await db_session.commit()
    assert updated.progress == {"key": "value"}


@pytest.mark.asyncio
async def test_complete_job(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await store.start(job.id)
        completed = await store.complete(job.id)
        await db_session.commit()
    assert completed.status == JobStatus.completed
    assert completed.date_completed is not None


@pytest.mark.asyncio
async def test_complete_with_errors(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await store.start(job.id)
        completed = await store.complete(job.id, has_errors=True)
        await db_session.commit()
    assert completed.status == JobStatus.completed_with_errors
    assert completed.date_completed is not None


@pytest.mark.asyncio
async def test_fail_job(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await store.start(job.id)
        failed = await store.fail(
            job.id, errors={"message": "something went wrong"}
        )
        await db_session.commit()
    assert failed.status == JobStatus.failed
    assert failed.date_completed is not None
    assert failed.errors == {"message": "something went wrong"}


@pytest.mark.asyncio
async def test_cancel_queued_job(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        cancelled = await store.cancel(job.id)
        await db_session.commit()
    assert cancelled.status == JobStatus.cancelled
    assert cancelled.date_completed is not None


@pytest.mark.asyncio
async def test_cancel_in_progress_job(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await store.start(job.id)
        cancelled = await store.cancel(job.id)
        await db_session.commit()
    assert cancelled.status == JobStatus.cancelled
    assert cancelled.date_completed is not None


@pytest.mark.asyncio
async def test_cancel_completed_job_raises(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await store.start(job.id)
        await store.complete(job.id)
        with pytest.raises(InvalidJobStateError):
            await store.cancel(job.id)
        await db_session.commit()


@pytest.mark.asyncio
async def test_get_by_public_id(
    db_session: async_scoped_session[AsyncSession],
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        fetched = await store.get_by_public_id(job.public_id)
        await db_session.commit()
    assert fetched is not None
    assert fetched.id == job.id
    assert fetched.public_id == job.public_id
