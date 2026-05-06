"""Tests for QueueJobStore."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.queue import JobKind, JobStatus
from docverse.exceptions import InvalidJobStateError
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore


@pytest.fixture
def store(
    db_session: AsyncSession,
) -> QueueJobStore:
    logger = structlog.get_logger("docverse")
    return QueueJobStore(session=db_session, logger=logger)


@pytest.mark.asyncio
async def test_create_job(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await db_session.commit()
    assert job.status == JobStatus.queued
    assert job.public_id > 0
    assert job.kind == JobKind.build_processing
    assert job.org_id == 1
    assert job.edition_id is None
    assert job.date_created is not None
    assert job.date_started is None
    assert job.date_completed is None


@pytest.mark.asyncio
async def test_publish_edition_job_with_edition_id(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """``edition_id`` can be set on a publish_edition QueueJob row."""
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        org_store = OrganizationStore(session=db_session, logger=logger)
        proj_store = ProjectStore(session=db_session, logger=logger)
        edition_store = EditionStore(session=db_session, logger=logger)
        org = await org_store.create(
            OrganizationCreate(
                slug="qj-org",
                title="QJ",
                base_domain="qj.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="qj-proj",
                title="QJ Project",
                doc_repo="https://github.com/example/repo",
            ),
        )
        edition = await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug="qj-ed",
                title="QJ Ed",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        job = await store.create(
            kind=JobKind.publish_edition,
            org_id=org.id,
            edition_id=edition.id,
        )
        await db_session.commit()
    assert job.kind == JobKind.publish_edition
    assert job.edition_id == edition.id

    async with db_session.begin():
        row = await db_session.get(SqlQueueJob, job.id)
        assert row is not None
        assert row.edition_id == edition.id


@pytest.mark.asyncio
async def test_start_job(
    db_session: AsyncSession,
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
    db_session: AsyncSession,
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
    db_session: AsyncSession,
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
    db_session: AsyncSession,
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
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        updated = await store.update_progress(job.id, {"key": "value"})
        await db_session.commit()
    assert updated.progress == {"key": "value"}


@pytest.mark.asyncio
async def test_complete_job(
    db_session: AsyncSession,
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
    db_session: AsyncSession,
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
    db_session: AsyncSession,
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
    db_session: AsyncSession,
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
    db_session: AsyncSession,
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
    db_session: AsyncSession,
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
async def test_set_backend_job_id(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """``set_backend_job_id`` records an arq job ID on an existing row."""
    async with db_session.begin():
        job = await store.create(kind=JobKind.publish_edition, org_id=1)
        assert job.backend_job_id is None
        updated = await store.set_backend_job_id(job.id, "arq-job-42")
        await db_session.commit()
    assert updated.backend_job_id == "arq-job-42"

    async with db_session.begin():
        refetched = await store.get(job.id)
    assert refetched is not None
    assert refetched.backend_job_id == "arq-job-42"


@pytest.mark.asyncio
async def test_get_by_public_id(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        fetched = await store.get_by_public_id(job.public_id)
        await db_session.commit()
    assert fetched is not None
    assert fetched.id == job.id
    assert fetched.public_id == job.public_id


async def _seed_org_and_run(
    db_session: AsyncSession, *, slug: str = "ks-org"
) -> tuple[int, int]:
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title="KS Org",
            base_domain=f"{slug}.example.com",
        )
    )
    run = SqlKeeperSyncRun(org_id=org.id, kind="backfill", status="pending")
    db_session.add(run)
    await db_session.flush()
    await db_session.refresh(run)
    return org.id, run.id


@pytest.mark.asyncio
async def test_fail_orphaned_run_children_fails_old_orphan(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Old queued child with no backend_job_id is reconciled to failed."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(db_session)
        orphan = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
        )
        # Backdate so the orphan is older than the idle window.
        row = await db_session.get(SqlQueueJob, orphan.id)
        assert row is not None
        row.date_created = datetime.now(tz=UTC) - timedelta(minutes=10)
        await db_session.flush()

        failed = await store.fail_orphaned_run_children(
            run_id=run_id, idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert len(failed) == 1
    assert failed[0].id == orphan.id
    assert failed[0].status == JobStatus.failed
    assert failed[0].date_completed is not None
    assert failed[0].errors is not None
    assert "orphan" in failed[0].errors["message"].lower()


@pytest.mark.asyncio
async def test_fail_orphaned_run_children_skips_recent_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Rows newer than the idle window are left alone (in-flight discovery)."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(db_session)
        # Created "now" — younger than the 5-minute window.
        await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
        )

        failed = await store.fail_orphaned_run_children(
            run_id=run_id, idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


@pytest.mark.asyncio
async def test_fail_orphaned_run_children_skips_rows_with_backend_id(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Rows that already have a backend_job_id are not orphans."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(db_session)
        job = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
            backend_job_id="arq-job-real",
        )
        # Backdate so age alone wouldn't protect it.
        row = await db_session.get(SqlQueueJob, job.id)
        assert row is not None
        row.date_created = datetime.now(tz=UTC) - timedelta(minutes=30)
        await db_session.flush()

        failed = await store.fail_orphaned_run_children(
            run_id=run_id, idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


@pytest.mark.asyncio
async def test_fail_orphaned_run_children_skips_started_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An in_progress row without a backend_job_id is not reaped.

    The row reached the worker, so the orphan-tail diagnosis no longer
    applies. Reaping it would race the running child.
    """
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(db_session)
        job = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
        )
        await store.start(job.id)
        row = await db_session.get(SqlQueueJob, job.id)
        assert row is not None
        row.date_created = datetime.now(tz=UTC) - timedelta(minutes=30)
        await db_session.flush()

        failed = await store.fail_orphaned_run_children(
            run_id=run_id, idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


@pytest.mark.asyncio
async def test_fail_orphaned_run_children_scoped_to_run(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An orphan attached to a different run is left untouched."""
    async with db_session.begin():
        org_a_id, run_a_id = await _seed_org_and_run(db_session, slug="ks-a")
        _, run_b_id = await _seed_org_and_run(db_session, slug="ks-b")
        # Orphan on run B (older than window) — should NOT be touched
        # by a reconciliation scoped to run A.
        orphan_b = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_a_id,
            keeper_sync_run_id=run_b_id,
        )
        row = await db_session.get(SqlQueueJob, orphan_b.id)
        assert row is not None
        row.date_created = datetime.now(tz=UTC) - timedelta(minutes=30)
        await db_session.flush()

        failed = await store.fail_orphaned_run_children(
            run_id=run_a_id, idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []
    async with db_session.begin():
        refetched = await store.get(orphan_b.id)
    assert refetched is not None
    assert refetched.status == JobStatus.queued
