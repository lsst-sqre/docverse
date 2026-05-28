"""Tests for QueueJobStore."""

from __future__ import annotations

from dataclasses import dataclass
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
from docverse.domain.base32id import generate_base32_id, validate_base32_id
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
                source_url="https://example.com/example/repo",
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
async def test_fail_silent_run_children_fails_old_in_progress(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An in_progress child past the idle threshold is reaped to failed."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(db_session)
        stuck = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
            backend_job_id="arq-job-stuck",
        )
        await store.start(stuck.id)
        # Backdate date_started past the idle threshold.
        row = await db_session.get(SqlQueueJob, stuck.id)
        assert row is not None
        row.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        await db_session.flush()

        reaped = await store.fail_silent_run_children(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert len(reaped) == 1
    assert reaped[0].id == stuck.id
    assert reaped[0].status == JobStatus.failed
    assert reaped[0].date_completed is not None
    assert reaped[0].errors is not None
    msg = reaped[0].errors["message"].lower()
    assert "stuck" in msg or "reaper" in msg or "silent" in msg


@pytest.mark.asyncio
async def test_fail_silent_run_children_skips_recent_in_progress(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An in_progress child within the idle window is left alone."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(db_session)
        recent = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
            backend_job_id="arq-job-recent",
        )
        await store.start(recent.id)

        reaped = await store.fail_silent_run_children(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
async def test_fail_silent_run_children_skips_completed_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A completed child is not reaped even when ``date_started`` is old."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(db_session)
        done = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
            backend_job_id="arq-job-done",
        )
        await store.start(done.id)
        await store.complete(done.id)
        row = await db_session.get(SqlQueueJob, done.id)
        assert row is not None
        row.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        await db_session.flush()

        reaped = await store.fail_silent_run_children(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
async def test_fail_silent_run_children_skips_queued_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A queued (not-yet-started) child is left to ``fail_orphaned_*``.

    The silent-run reaper only targets jobs that the worker actually
    picked up and then went silent on. Orphans without ``date_started``
    are reconciled by the discovery-time orphan sweep instead.
    """
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(db_session)
        await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
            backend_job_id="arq-job-queued",
        )

        reaped = await store.fail_silent_run_children(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
async def test_fail_silent_run_children_skips_non_keeper_sync_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Reaper only touches rows attached to a keeper-sync run."""
    async with db_session.begin():
        unrelated = await store.create(
            kind=JobKind.build_processing,
            org_id=1,
        )
        await store.start(unrelated.id)
        row = await db_session.get(SqlQueueJob, unrelated.id)
        assert row is not None
        row.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        await db_session.flush()

        reaped = await store.fail_silent_run_children(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
async def test_fail_silent_run_children_returns_distinct_run_ids(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Reaper returns rows from each run so callers can finalise both."""
    async with db_session.begin():
        org_a_id, run_a_id = await _seed_org_and_run(db_session, slug="ks-aa")
        _, run_b_id = await _seed_org_and_run(db_session, slug="ks-bb")
        # Two stuck children on run A, one on run B.
        for backend_id in ("arq-a1", "arq-a2"):
            j = await store.create(
                kind=JobKind.keeper_sync_project,
                org_id=org_a_id,
                keeper_sync_run_id=run_a_id,
                backend_job_id=backend_id,
            )
            await store.start(j.id)
            r = await db_session.get(SqlQueueJob, j.id)
            assert r is not None
            r.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        b_job = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_a_id,
            keeper_sync_run_id=run_b_id,
            backend_job_id="arq-b1",
        )
        await store.start(b_job.id)
        r = await db_session.get(SqlQueueJob, b_job.id)
        assert r is not None
        r.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        await db_session.flush()

        reaped = await store.fail_silent_run_children(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert len(reaped) == 3
    reaped_run_ids = {qj.keeper_sync_run_id for qj in reaped}
    assert reaped_run_ids == {run_a_id, run_b_id}


async def _seed_org_only(
    db_session: AsyncSession, *, slug: str = "ks-tc-org"
) -> int:
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title="KS TC Org",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


@pytest.mark.asyncio
async def test_fail_silent_tier_cron_jobs_fails_old_in_progress(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A run-less keeper_sync_project past the idle threshold is reaped."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session)
        stuck = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="phalanx",
            backend_job_id="arq-tc-stuck",
        )
        await store.start(stuck.id)
        row = await db_session.get(SqlQueueJob, stuck.id)
        assert row is not None
        row.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        await db_session.flush()

        reaped = await store.fail_silent_tier_cron_jobs(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert len(reaped) == 1
    assert reaped[0].id == stuck.id
    assert reaped[0].status == JobStatus.failed
    assert reaped[0].date_completed is not None
    assert reaped[0].errors is not None
    assert reaped[0].errors["type"] == "SilentTierCronJob"


@pytest.mark.asyncio
async def test_fail_silent_tier_cron_jobs_skips_recent_in_progress(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A run-less in_progress row within the idle window is left alone."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-recent")
        recent = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="phalanx",
            backend_job_id="arq-tc-recent",
        )
        await store.start(recent.id)

        reaped = await store.fail_silent_tier_cron_jobs(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
async def test_fail_silent_tier_cron_jobs_skips_run_attributed_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Run-attributed silent rows are reaped by fail_silent_run_children."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="ks-tc-with-run"
        )
        run_attrib = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
            backend_job_id="arq-with-run",
        )
        await store.start(run_attrib.id)
        row = await db_session.get(SqlQueueJob, run_attrib.id)
        assert row is not None
        row.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        await db_session.flush()

        reaped = await store.fail_silent_tier_cron_jobs(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
async def test_fail_silent_tier_cron_jobs_skips_non_keeper_sync_kinds(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Only ``keeper_sync_project`` rows are in scope."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-kind")
        unrelated = await store.create(
            kind=JobKind.build_processing,
            org_id=org_id,
            backend_job_id="arq-build",
        )
        await store.start(unrelated.id)
        row = await db_session.get(SqlQueueJob, unrelated.id)
        assert row is not None
        row.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        await db_session.flush()

        reaped = await store.fail_silent_tier_cron_jobs(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
async def test_fail_silent_tier_cron_jobs_skips_completed_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A terminal row is not reaped even when ``date_started`` is old."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-done")
        done = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="phalanx",
            backend_job_id="arq-tc-done",
        )
        await store.start(done.id)
        await store.complete(done.id)
        row = await db_session.get(SqlQueueJob, done.id)
        assert row is not None
        row.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        await db_session.flush()

        reaped = await store.fail_silent_tier_cron_jobs(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
async def test_fail_silent_tier_cron_jobs_unblocks_has_active_for_subject(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """After a reap, the subject mutex frees up for the next tier tick."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-unblock")
        stuck = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="phalanx",
            backend_job_id="arq-tc-unblock",
        )
        await store.start(stuck.id)
        row = await db_session.get(SqlQueueJob, stuck.id)
        assert row is not None
        row.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        await db_session.flush()

        # Before the reap, the subject mutex is engaged.
        before = await store.has_active_for_subject(
            org_id=org_id,
            kind=JobKind.keeper_sync_project,
            subject_label="phalanx",
        )
        assert before is True

        reaped = await store.fail_silent_tier_cron_jobs(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert len(reaped) == 1
    async with db_session.begin():
        after = await store.has_active_for_subject(
            org_id=org_id,
            kind=JobKind.keeper_sync_project,
            subject_label="phalanx",
        )
    assert after is False


@pytest.mark.asyncio
async def test_fail_orphaned_tier_cron_jobs_fails_old_orphan(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An old run-less queued row with no ``backend_job_id`` is reaped."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-orphan")
        orphan = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="phalanx",
        )
        row = await db_session.get(SqlQueueJob, orphan.id)
        assert row is not None
        row.date_created = datetime.now(tz=UTC) - timedelta(minutes=10)
        await db_session.flush()

        failed = await store.fail_orphaned_tier_cron_jobs(
            idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert len(failed) == 1
    assert failed[0].id == orphan.id
    assert failed[0].status == JobStatus.failed
    assert failed[0].date_completed is not None
    assert failed[0].errors is not None
    assert failed[0].errors["type"] == "OrphanedTierCronJob"


@pytest.mark.asyncio
async def test_fail_orphaned_tier_cron_jobs_skips_recent_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Run-less queued rows newer than the idle window are left alone."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-orphan-recent")
        # Created "now" — younger than the 5-minute window.
        await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="phalanx",
        )

        failed = await store.fail_orphaned_tier_cron_jobs(
            idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


@pytest.mark.asyncio
async def test_fail_orphaned_tier_cron_jobs_skips_rows_with_backend_id(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Rows that already have a backend_job_id are not orphans."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-orphan-backend")
        job = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="phalanx",
            backend_job_id="arq-real",
        )
        row = await db_session.get(SqlQueueJob, job.id)
        assert row is not None
        row.date_created = datetime.now(tz=UTC) - timedelta(minutes=30)
        await db_session.flush()

        failed = await store.fail_orphaned_tier_cron_jobs(
            idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


@pytest.mark.asyncio
async def test_fail_orphaned_tier_cron_jobs_skips_run_attributed_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Run-attributed orphans are reaped by ``fail_orphaned_run_children``."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="ks-tc-orphan-run"
        )
        run_attrib = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
            subject_label="phalanx",
        )
        row = await db_session.get(SqlQueueJob, run_attrib.id)
        assert row is not None
        row.date_created = datetime.now(tz=UTC) - timedelta(minutes=30)
        await db_session.flush()

        failed = await store.fail_orphaned_tier_cron_jobs(
            idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


@pytest.mark.asyncio
async def test_fail_orphaned_tier_cron_jobs_skips_non_keeper_sync_kinds(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Only ``keeper_sync_project`` rows are in scope for orphan reaps."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-orphan-kind")
        unrelated = await store.create(
            kind=JobKind.build_processing,
            org_id=org_id,
            keeper_sync_run_id=None,
        )
        row = await db_session.get(SqlQueueJob, unrelated.id)
        assert row is not None
        row.date_created = datetime.now(tz=UTC) - timedelta(minutes=30)
        await db_session.flush()

        failed = await store.fail_orphaned_tier_cron_jobs(
            idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


@pytest.mark.asyncio
async def test_fail_orphaned_tier_cron_jobs_skips_started_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A run-less in_progress row is not an orphan: silent path owns it."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-orphan-started")
        job = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="phalanx",
        )
        await store.start(job.id)
        row = await db_session.get(SqlQueueJob, job.id)
        assert row is not None
        row.date_created = datetime.now(tz=UTC) - timedelta(minutes=30)
        await db_session.flush()

        failed = await store.fail_orphaned_tier_cron_jobs(
            idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


@pytest.mark.asyncio
async def test_fail_orphaned_tier_cron_jobs_unblocks_has_active_for_subject(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """After a reap, the subject mutex frees up for the next tier tick."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-orphan-unblock")
        orphan = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="phalanx",
        )
        row = await db_session.get(SqlQueueJob, orphan.id)
        assert row is not None
        row.date_created = datetime.now(tz=UTC) - timedelta(minutes=10)
        await db_session.flush()

        before = await store.has_active_for_subject(
            org_id=org_id,
            kind=JobKind.keeper_sync_project,
            subject_label="phalanx",
        )
        assert before is True

        failed = await store.fail_orphaned_tier_cron_jobs(
            idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert len(failed) == 1
    async with db_session.begin():
        after = await store.has_active_for_subject(
            org_id=org_id,
            kind=JobKind.keeper_sync_project,
            subject_label="phalanx",
        )
    assert after is False


@pytest.mark.asyncio
async def test_has_active_for_subject_returns_true_for_queued_row(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A queued row matching ``(org_id, kind, subject_label)`` is active."""
    async with db_session.begin():
        await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=42,
            subject_label="pipelines",
        )
        active = await store.has_active_for_subject(
            org_id=42,
            kind=JobKind.keeper_sync_project,
            subject_label="pipelines",
        )
        await db_session.commit()
    assert active is True


@pytest.mark.asyncio
async def test_has_active_for_subject_returns_true_for_in_progress_row(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An ``in_progress`` row also counts as active."""
    async with db_session.begin():
        job = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=42,
            subject_label="pipelines",
        )
        await store.start(job.id)
        active = await store.has_active_for_subject(
            org_id=42,
            kind=JobKind.keeper_sync_project,
            subject_label="pipelines",
        )
        await db_session.commit()
    assert active is True


@pytest.mark.asyncio
async def test_has_active_for_subject_returns_false_for_terminal_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Completed / failed / cancelled rows are not active."""
    async with db_session.begin():
        completed = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=42,
            subject_label="pipelines",
        )
        await store.start(completed.id)
        await store.complete(completed.id)

        failed = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=42,
            subject_label="pipelines",
        )
        await store.start(failed.id)
        await store.fail(failed.id)

        cancelled = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=42,
            subject_label="pipelines",
        )
        await store.cancel(cancelled.id)

        active = await store.has_active_for_subject(
            org_id=42,
            kind=JobKind.keeper_sync_project,
            subject_label="pipelines",
        )
        await db_session.commit()
    assert active is False


@pytest.mark.asyncio
async def test_has_active_for_subject_returns_false_when_no_rows_match(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """No matching rows for ``(org, kind, subject)`` → not active."""
    async with db_session.begin():
        active = await store.has_active_for_subject(
            org_id=42,
            kind=JobKind.keeper_sync_project,
            subject_label="pipelines",
        )
        await db_session.commit()
    assert active is False


@pytest.mark.asyncio
async def test_has_active_for_subject_filters_by_kind(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A queued row of a different ``kind`` does not count."""
    async with db_session.begin():
        await store.create(
            kind=JobKind.publish_edition,
            org_id=42,
            subject_label="pipelines",
        )
        active = await store.has_active_for_subject(
            org_id=42,
            kind=JobKind.keeper_sync_project,
            subject_label="pipelines",
        )
        await db_session.commit()
    assert active is False


@pytest.mark.asyncio
async def test_has_active_for_subject_filters_by_org_and_subject(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Differing ``org_id`` or ``subject_label`` excludes the row."""
    async with db_session.begin():
        await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=42,
            subject_label="pipelines",
        )
        # Same kind+subject but different org → not active for org=99.
        cross_org = await store.has_active_for_subject(
            org_id=99,
            kind=JobKind.keeper_sync_project,
            subject_label="pipelines",
        )
        # Same kind+org but different subject → not active for "other".
        cross_subject = await store.has_active_for_subject(
            org_id=42,
            kind=JobKind.keeper_sync_project,
            subject_label="other",
        )
        await db_session.commit()
    assert cross_org is False
    assert cross_subject is False


@pytest.mark.asyncio
async def test_has_active_dashboard_build_returns_true_for_queued_row(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A queued ``dashboard_build`` matching ``(org, project)`` is active."""
    async with db_session.begin():
        await store.create(
            kind=JobKind.dashboard_build,
            org_id=42,
            project_id=7,
        )
        active = await store.has_active_dashboard_build(
            org_id=42, project_id=7
        )
        await db_session.commit()
    assert active is True


@pytest.mark.asyncio
async def test_has_active_dashboard_build_returns_true_for_in_progress_row(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An ``in_progress`` ``dashboard_build`` row also counts as active."""
    async with db_session.begin():
        job = await store.create(
            kind=JobKind.dashboard_build,
            org_id=42,
            project_id=7,
        )
        await store.start(job.id)
        active = await store.has_active_dashboard_build(
            org_id=42, project_id=7
        )
        await db_session.commit()
    assert active is True


@pytest.mark.asyncio
async def test_has_active_dashboard_build_returns_false_for_terminal_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Completed / failed / cancelled rows are not active."""
    async with db_session.begin():
        completed = await store.create(
            kind=JobKind.dashboard_build,
            org_id=42,
            project_id=7,
        )
        await store.start(completed.id)
        await store.complete(completed.id)

        failed = await store.create(
            kind=JobKind.dashboard_build,
            org_id=42,
            project_id=7,
        )
        await store.start(failed.id)
        await store.fail(failed.id)

        cancelled = await store.create(
            kind=JobKind.dashboard_build,
            org_id=42,
            project_id=7,
        )
        await store.cancel(cancelled.id)

        active = await store.has_active_dashboard_build(
            org_id=42, project_id=7
        )
        await db_session.commit()
    assert active is False


@pytest.mark.asyncio
async def test_has_active_dashboard_build_returns_false_with_no_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """No matching rows for ``(org, project)`` → not active."""
    async with db_session.begin():
        active = await store.has_active_dashboard_build(
            org_id=42, project_id=7
        )
        await db_session.commit()
    assert active is False


@pytest.mark.asyncio
async def test_has_active_dashboard_build_filters_by_kind(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A queued row of a different ``kind`` does not count."""
    async with db_session.begin():
        await store.create(
            kind=JobKind.publish_edition,
            org_id=42,
            project_id=7,
        )
        active = await store.has_active_dashboard_build(
            org_id=42, project_id=7
        )
        await db_session.commit()
    assert active is False


@pytest.mark.asyncio
async def test_has_active_dashboard_build_filters_by_org_and_project(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Differing ``org_id`` or ``project_id`` excludes the row."""
    async with db_session.begin():
        await store.create(
            kind=JobKind.dashboard_build,
            org_id=42,
            project_id=7,
        )
        cross_org = await store.has_active_dashboard_build(
            org_id=99, project_id=7
        )
        cross_project = await store.has_active_dashboard_build(
            org_id=42, project_id=8
        )
        await db_session.commit()
    assert cross_org is False
    assert cross_project is False


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


# ---------------------------------------------------------------------
# lifecycle_eval reaper helpers
# ---------------------------------------------------------------------


async def _seed_lifecycle_eval_row(
    db_session: AsyncSession,
    *,
    org_id: int,
    status: JobStatus,
    backend_job_id: str | None,
    date_started: datetime | None = None,
    date_created_offset: timedelta | None = None,
) -> int:
    """Insert one ``kind='lifecycle_eval'`` row with explicit timestamps.

    The store's ``create`` does not expose ``lifecycle_eval_run_id`` (the
    dispatcher sibling task adds that), and ``status`` defaults to
    ``queued``. The reaper-helper tests drive every field that the
    sweep predicates consult, so direct ``SqlQueueJob`` construction is
    cleaner than threading two-step setup through ``create`` +
    ``start``.
    """
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=backend_job_id,
        kind=JobKind.lifecycle_eval.value,
        status=status.value,
        org_id=org_id,
        date_started=date_started,
    )
    db_session.add(row)
    await db_session.flush()
    if date_created_offset is not None:
        row.date_created = datetime.now(tz=UTC) - date_created_offset
        await db_session.flush()
    return row.id


@pytest.mark.asyncio
async def test_fail_silent_lifecycle_eval_jobs_reaps_old_in_progress(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An ``in_progress`` lifecycle_eval row past the threshold is failed."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="lce-reap-1")
        stuck_id = await _seed_lifecycle_eval_row(
            db_session,
            org_id=org_id,
            status=JobStatus.in_progress,
            backend_job_id="arq-lce-stuck",
            date_started=datetime.now(tz=UTC) - timedelta(hours=10),
        )

        reaped = await store.fail_silent_lifecycle_eval_jobs(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert len(reaped) == 1
    assert reaped[0].id == stuck_id
    assert reaped[0].status == JobStatus.failed
    assert reaped[0].errors is not None
    assert reaped[0].errors["type"] == "SilentWorker"


@pytest.mark.asyncio
async def test_fail_silent_lifecycle_eval_jobs_skips_recent(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An ``in_progress`` row within the idle window is left alone."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="lce-reap-2")
        await _seed_lifecycle_eval_row(
            db_session,
            org_id=org_id,
            status=JobStatus.in_progress,
            backend_job_id="arq-lce-fresh",
            date_started=datetime.now(tz=UTC),
        )

        reaped = await store.fail_silent_lifecycle_eval_jobs(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
async def test_fail_silent_lifecycle_eval_jobs_skips_other_kinds(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Other ``kind`` values stay out of scope, mirroring keeper-sync split."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="lce-reap-3")
        unrelated = await store.create(
            kind=JobKind.build_processing,
            org_id=org_id,
            backend_job_id="arq-build",
        )
        await store.start(unrelated.id)
        row = await db_session.get(SqlQueueJob, unrelated.id)
        assert row is not None
        row.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        await db_session.flush()

        reaped = await store.fail_silent_lifecycle_eval_jobs(
            idle_after=timedelta(hours=6)
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
async def test_fail_orphaned_lifecycle_eval_jobs_reaps_old_orphan(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A ``queued`` lifecycle_eval row with no ``backend_job_id`` is failed."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="lce-orphan-1")
        orphan_id = await _seed_lifecycle_eval_row(
            db_session,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=None,
            date_created_offset=timedelta(minutes=10),
        )

        failed = await store.fail_orphaned_lifecycle_eval_jobs(
            idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert len(failed) == 1
    assert failed[0].id == orphan_id
    assert failed[0].status == JobStatus.failed
    assert failed[0].errors is not None
    assert failed[0].errors["type"] == "OrphanedQueueJob"


@pytest.mark.asyncio
async def test_fail_orphaned_lifecycle_eval_jobs_skips_rows_with_backend_id(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A queued row that already has a backend_job_id is not an orphan."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="lce-orphan-2")
        await _seed_lifecycle_eval_row(
            db_session,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id="arq-lce-enqueued",
            date_created_offset=timedelta(minutes=30),
        )

        failed = await store.fail_orphaned_lifecycle_eval_jobs(
            idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


@pytest.mark.asyncio
async def test_fail_orphaned_lifecycle_eval_jobs_skips_started_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An ``in_progress`` row is not an orphan; silent sweep owns it."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="lce-orphan-3")
        await _seed_lifecycle_eval_row(
            db_session,
            org_id=org_id,
            status=JobStatus.in_progress,
            backend_job_id=None,
            date_started=datetime.now(tz=UTC) - timedelta(hours=1),
            date_created_offset=timedelta(minutes=30),
        )

        failed = await store.fail_orphaned_lifecycle_eval_jobs(
            idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


# ---------------------------------------------------------------------
# Run-less reaper storage tests (PRD #367)
# ---------------------------------------------------------------------
#
# The four run-less reapers (dashboard_build, publish_edition,
# build_processing, dashboard_sync) share the same two-method storage
# API — fail_silent_jobs(kind, ...) and fail_orphaned_jobs(kind, ...) —
# so their unit tests are parametrized over the four kinds. Each spec
# carries the kind, the kind's production idle_after default, and a
# "well past threshold" offset for stuck-row seeds. The cross-kind
# isolation tests derive their "other kinds" list from the spec table
# (every other run-less kind plus ``lifecycle_eval``).


@dataclass(frozen=True)
class RunlessReaperSpec:
    """One row per run-less kind for the parametrized storage tests."""

    kind: JobKind
    silent_idle_after: timedelta
    silent_past_offset: timedelta
    slug_prefix: str

    @property
    def label(self) -> str:
        return self.kind.value


RUNLESS_REAPER_SPECS = [
    RunlessReaperSpec(
        kind=JobKind.dashboard_build,
        silent_idle_after=timedelta(minutes=30),
        silent_past_offset=timedelta(hours=1),
        slug_prefix="dbr",
    ),
    RunlessReaperSpec(
        kind=JobKind.publish_edition,
        silent_idle_after=timedelta(hours=4),
        silent_past_offset=timedelta(hours=5),
        slug_prefix="per",
    ),
    RunlessReaperSpec(
        kind=JobKind.build_processing,
        silent_idle_after=timedelta(hours=8),
        silent_past_offset=timedelta(hours=9),
        slug_prefix="bpr",
    ),
    RunlessReaperSpec(
        kind=JobKind.dashboard_sync,
        silent_idle_after=timedelta(hours=6),
        silent_past_offset=timedelta(hours=7),
        slug_prefix="dsr",
    ),
]


_runless_param = pytest.mark.parametrize(
    "spec",
    RUNLESS_REAPER_SPECS,
    ids=lambda s: s.label,
)


async def _seed_runless_row(
    db_session: AsyncSession,
    *,
    kind: JobKind,
    org_id: int,
    status: JobStatus,
    backend_job_id: str | None,
    date_started: datetime | None = None,
    date_created_offset: timedelta | None = None,
) -> int:
    """Insert one row of ``kind`` with explicit timestamps for sweep tests."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=backend_job_id,
        kind=kind.value,
        status=status.value,
        org_id=org_id,
        date_started=date_started,
    )
    db_session.add(row)
    await db_session.flush()
    if date_created_offset is not None:
        row.date_created = datetime.now(tz=UTC) - date_created_offset
        await db_session.flush()
    return row.id


def _other_runless_kinds(spec: RunlessReaperSpec) -> list[JobKind]:
    """All run-less kinds plus ``lifecycle_eval``, excluding ``spec.kind``."""
    return [s.kind for s in RUNLESS_REAPER_SPECS if s.kind != spec.kind] + [
        JobKind.lifecycle_eval
    ]


@pytest.mark.asyncio
@_runless_param
async def test_fail_silent_jobs_reaps_old_in_progress(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """An ``in_progress`` row of ``kind`` past the threshold is failed."""
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-reap-1"
        )
        stuck_id = await _seed_runless_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.in_progress,
            backend_job_id=f"arq-{spec.slug_prefix}-stuck",
            date_started=datetime.now(tz=UTC) - spec.silent_past_offset,
        )

        reaped = await store.fail_silent_jobs(
            spec.kind, idle_after=spec.silent_idle_after
        )
        await db_session.commit()

    assert len(reaped) == 1
    assert reaped[0].id == stuck_id
    assert reaped[0].status == JobStatus.failed
    assert reaped[0].errors is not None
    assert reaped[0].errors["type"] == "SilentWorker"
    assert reaped[0].date_completed is not None


@pytest.mark.asyncio
@_runless_param
async def test_fail_silent_jobs_skips_recent(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """An ``in_progress`` row within the idle window is left alone."""
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-reap-2"
        )
        await _seed_runless_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.in_progress,
            backend_job_id=f"arq-{spec.slug_prefix}-fresh",
            date_started=datetime.now(tz=UTC),
        )

        reaped = await store.fail_silent_jobs(
            spec.kind, idle_after=spec.silent_idle_after
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
@_runless_param
async def test_fail_silent_jobs_skips_other_kinds(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """Cross-kind scoping for the silent sweep.

    An ``in_progress`` row past the threshold of every other run-less
    kind (plus ``lifecycle_eval``) must be left alone by the target
    kind's silent sweep. Matches PRD #367 "Testing Decisions" —
    cross-kind scoping.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-reap-3"
        )
        for idx, kind in enumerate(_other_runless_kinds(spec)):
            unrelated = await store.create(
                kind=kind,
                org_id=org_id,
                backend_job_id=f"arq-other-{idx}",
            )
            await store.start(unrelated.id)
            row = await db_session.get(SqlQueueJob, unrelated.id)
            assert row is not None
            row.date_started = datetime.now(tz=UTC) - spec.silent_past_offset
            await db_session.flush()

        reaped = await store.fail_silent_jobs(
            spec.kind, idle_after=spec.silent_idle_after
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
@_runless_param
async def test_fail_silent_jobs_skips_queued_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """Status respect: the silent sweep ignores ``queued`` rows.

    The orphan sweep owns ``queued`` rows; the silent sweep is
    confined to ``in_progress``.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-reap-4"
        )
        await _seed_runless_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=None,
            date_created_offset=spec.silent_past_offset,
        )

        reaped = await store.fail_silent_jobs(
            spec.kind, idle_after=spec.silent_idle_after
        )
        await db_session.commit()

    assert reaped == []


@pytest.mark.asyncio
@_runless_param
async def test_fail_orphaned_jobs_reaps_old_orphan(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """A ``queued`` row of ``kind`` with no ``backend_job_id`` fails."""
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-orphan-1"
        )
        orphan_id = await _seed_runless_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=None,
            date_created_offset=timedelta(minutes=10),
        )

        failed = await store.fail_orphaned_jobs(
            spec.kind, idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert len(failed) == 1
    assert failed[0].id == orphan_id
    assert failed[0].status == JobStatus.failed
    assert failed[0].errors is not None
    assert failed[0].errors["type"] == "OrphanedQueueJob"


@pytest.mark.asyncio
@_runless_param
async def test_fail_orphaned_jobs_skips_rows_with_backend_id(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """A queued row that already has a backend_job_id is not an orphan."""
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-orphan-2"
        )
        await _seed_runless_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=f"arq-{spec.slug_prefix}-enqueued",
            date_created_offset=timedelta(minutes=30),
        )

        failed = await store.fail_orphaned_jobs(
            spec.kind, idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


@pytest.mark.asyncio
@_runless_param
async def test_fail_orphaned_jobs_skips_in_progress(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """An ``in_progress`` row is not an orphan; the silent sweep owns it."""
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-orphan-3"
        )
        await _seed_runless_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.in_progress,
            backend_job_id=None,
            date_started=datetime.now(tz=UTC) - spec.silent_past_offset,
            date_created_offset=timedelta(minutes=30),
        )

        failed = await store.fail_orphaned_jobs(
            spec.kind, idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []


@pytest.mark.asyncio
@_runless_param
async def test_fail_orphaned_jobs_skips_other_kinds(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """Cross-kind scoping for the orphan sweep.

    A ``queued`` row of every other run-less kind plus
    ``lifecycle_eval`` with no ``backend_job_id`` past the idle
    window must be left alone by the target kind's orphan sweep.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-orphan-4"
        )
        for idx, kind in enumerate(_other_runless_kinds(spec)):
            row = SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                backend_job_id=None,
                kind=kind.value,
                status=JobStatus.queued.value,
                org_id=org_id,
                subject_label=f"orphan-{idx}",
            )
            db_session.add(row)
            await db_session.flush()
            row.date_created = datetime.now(tz=UTC) - timedelta(minutes=30)
            await db_session.flush()

        failed = await store.fail_orphaned_jobs(
            spec.kind, idle_after=timedelta(minutes=5)
        )
        await db_session.commit()

    assert failed == []
