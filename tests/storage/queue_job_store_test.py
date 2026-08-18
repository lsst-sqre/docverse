"""Tests for QueueJobStore."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import structlog
from safir.dependencies.db_session import db_session_dependency
from safir.testing.sentry import capture_events_fixture, sentry_init_fixture
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse_server.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import (
    generate_base32_id,
    serialize_base32_id,
    validate_base32_id,
)
from docverse_server.domain.queue import JobKind, JobStatus, QueueJob
from docverse_server.exceptions import InvalidJobStateError, JobNotFoundError
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_backend import QueueBackend
from docverse_server.storage.queue_job_store import (
    _ACTIVE_JOB_UNIQUE_INDEXES,
    LATE_DELIVERY_IN_PROGRESS_MESSAGE,
    QueueJobStore,
)


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


def test_store_has_no_raising_start_method() -> None:
    """``start`` is gone so no pickup path can get raising semantics.

    Task #547: the raising ``start`` had zero production callers once
    every pickup site moved to :meth:`QueueJobStore.start_if_queued`, and
    leaving a natural-named sibling around invited the next pickup point
    to reach for it and reintroduce ``InvalidJobStateError`` at pickup.
    """
    assert not hasattr(QueueJobStore, "start")


@pytest.mark.asyncio
async def test_start_if_queued_starts_queued_row(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A ``queued`` row is picked up: in_progress with a start timestamp."""
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        started = await store.start_if_queued(job.id)
        await db_session.commit()
    assert started is not None
    assert started.id == job.id
    assert started.status == JobStatus.in_progress
    assert started.date_started is not None


@pytest.mark.parametrize(
    "status",
    [
        JobStatus.failed,
        JobStatus.cancelled,
        JobStatus.completed,
        JobStatus.completed_with_errors,
        JobStatus.in_progress,
    ],
)
@pytest.mark.asyncio
async def test_start_if_queued_returns_none_for_non_queued_row(
    db_session: AsyncSession,
    store: QueueJobStore,
    status: JobStatus,
) -> None:
    """A row a reaper already moved off ``queued`` yields ``None``.

    This is the late-delivery guard: arq handing the worker a job whose
    row is no longer ``queued`` must not raise ``InvalidJobStateError``.
    """
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        row = await db_session.get(SqlQueueJob, job.id)
        assert row is not None
        row.status = status.value
        await db_session.flush()

        result = await store.start_if_queued(job.id)
        await db_session.commit()

    assert result is None
    async with db_session.begin():
        unchanged = await store.get(job.id)
        assert unchanged is not None
        assert unchanged.status == status
        assert unchanged.date_started is None


@pytest.mark.parametrize(
    "status",
    [
        JobStatus.failed,
        JobStatus.cancelled,
        JobStatus.completed,
        JobStatus.completed_with_errors,
    ],
)
@pytest.mark.asyncio
async def test_start_if_queued_terminal_row_captures_no_sentry_event(
    db_session: AsyncSession,
    store: QueueJobStore,
    monkeypatch: pytest.MonkeyPatch,
    status: JobStatus,
) -> None:
    """A terminal row is the race the reaper triad is meant to absorb.

    Task #547 splits the two non-``queued`` cases apart: a row a reaper
    (or a cancel, or a prior successful run) already moved to a terminal
    status is expected fallout of the reap/deliver race, so it stays a
    warning log with **no** Sentry event.
    """
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        row = await db_session.get(SqlQueueJob, job.id)
        assert row is not None
        row.status = status.value
        await db_session.flush()

        with sentry_init_fixture() as init:
            init(environment="test")
            captured = capture_events_fixture(monkeypatch)()
            result = await store.start_if_queued(job.id)
        await db_session.commit()

    assert result is None
    assert captured.errors == []


@pytest.mark.asyncio
async def test_start_if_queued_in_progress_row_captures_warning_event(
    db_session: AsyncSession,
    store: QueueJobStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ``in_progress`` row means arq re-delivered a job already running.

    Task #547: on the default pool (``retry_jobs=True``, ``max_tries``
    unset) arq re-delivers after worker death or a routine SIGTERM pod
    rotation. Absorbing that silently would hide a crash-looping kind
    until the reaper noticed hours later, so the guard captures a
    warning-level Sentry event naming the job's public id, kind, and
    current status.
    """
    async with db_session.begin():
        job = await store.create(kind=JobKind.dashboard_build, org_id=1)
        picked_up = await store.start_if_queued(job.id)
        assert picked_up is not None

        with sentry_init_fixture() as init:
            init(environment="test")
            captured = capture_events_fixture(monkeypatch)()
            result = await store.start_if_queued(job.id)
        await db_session.commit()

    assert result is None
    assert len(captured.errors) == 1
    event = captured.errors[0]
    assert event["level"] == "warning"
    assert event["message"] == LATE_DELIVERY_IN_PROGRESS_MESSAGE
    assert event["tags"]["job_function"] == JobKind.dashboard_build.value
    assert event["tags"]["job_current_state"] == JobStatus.in_progress.value
    # The public id is high-cardinality, so it rides in the context
    # rather than a tag (see the docverse-exceptions recipe).
    assert "job_public_id" not in event["tags"]
    context = event["contexts"]["queue_job_late_delivery"]
    assert context["job_public_id"] == serialize_base32_id(job.public_id)
    assert context["job_function"] == JobKind.dashboard_build.value
    assert context["current_state"] == JobStatus.in_progress.value


@pytest.mark.asyncio
async def test_start_if_queued_raises_for_missing_row(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A missing row is a programming error, not a race — it still raises."""
    async with db_session.begin():
        with pytest.raises(JobNotFoundError):
            await store.start_if_queued(-1)


@pytest.mark.asyncio
async def test_update_phase(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    async with db_session.begin():
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await store.start_if_queued(job.id)
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
        await store.start_if_queued(job.id)
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
        await store.start_if_queued(job.id)
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
        await store.start_if_queued(job.id)
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
        await store.start_if_queued(job.id)
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
        await store.start_if_queued(job.id)
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
        await store.start_if_queued(job.id)
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
    run = SqlKeeperSyncRun(
        public_id=validate_base32_id(generate_base32_id()),
        org_id=org.id,
        kind="backfill",
        status="pending",
    )
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
        await store.start_if_queued(job.id)
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
        await store.start_if_queued(stuck.id)
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
        await store.start_if_queued(recent.id)

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
        await store.start_if_queued(done.id)
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
        await store.start_if_queued(unrelated.id)
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
            await store.start_if_queued(j.id)
            r = await db_session.get(SqlQueueJob, j.id)
            assert r is not None
            r.date_started = datetime.now(tz=UTC) - timedelta(hours=10)
        b_job = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_a_id,
            keeper_sync_run_id=run_b_id,
            backend_job_id="arq-b1",
        )
        await store.start_if_queued(b_job.id)
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
        await store.start_if_queued(stuck.id)
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
        await store.start_if_queued(recent.id)

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
        await store.start_if_queued(run_attrib.id)
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
        await store.start_if_queued(unrelated.id)
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
        await store.start_if_queued(done.id)
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
        await store.start_if_queued(stuck.id)
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
        await store.start_if_queued(job.id)
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
        await store.start_if_queued(job.id)
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
        await store.start_if_queued(completed.id)
        await store.complete(completed.id)

        failed = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=42,
            subject_label="pipelines",
        )
        await store.start_if_queued(failed.id)
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
        await store.start_if_queued(job.id)
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
        await store.start_if_queued(completed.id)
        await store.complete(completed.id)

        failed = await store.create(
            kind=JobKind.dashboard_build,
            org_id=42,
            project_id=7,
        )
        await store.start_if_queued(failed.id)
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
# lifecycle reaper helpers
# ---------------------------------------------------------------------


async def _seed_lifecycle_family_row(
    db_session: AsyncSession,
    *,
    kind: JobKind,
    org_id: int,
    status: JobStatus,
    backend_job_id: str | None,
    date_started: datetime | None = None,
    date_created_offset: timedelta | None = None,
) -> int:
    """Insert one ``lifecycle_reaper``-owned row with explicit timestamps.

    ``kind`` selects the family — ``lifecycle_eval`` or
    ``git_ref_audit``, the two the reaper sweeps. The store's ``create``
    does not expose ``lifecycle_eval_run_id`` (the dispatcher sibling
    task adds that), and ``status`` defaults to ``queued``. The
    reaper-helper tests drive every field that the sweep predicates
    consult, so direct ``SqlQueueJob`` construction is cleaner than
    threading two-step setup through ``create`` + ``start``.
    """
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


@pytest.mark.asyncio
async def test_fail_silent_lifecycle_eval_jobs_reaps_old_in_progress(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An ``in_progress`` lifecycle_eval row past the threshold is failed."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="lce-reap-1")
        stuck_id = await _seed_lifecycle_family_row(
            db_session,
            kind=JobKind.lifecycle_eval,
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
        await _seed_lifecycle_family_row(
            db_session,
            kind=JobKind.lifecycle_eval,
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
        await store.start_if_queued(unrelated.id)
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
        orphan_id = await _seed_lifecycle_family_row(
            db_session,
            kind=JobKind.lifecycle_eval,
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
        await _seed_lifecycle_family_row(
            db_session,
            kind=JobKind.lifecycle_eval,
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
        await _seed_lifecycle_family_row(
            db_session,
            kind=JobKind.lifecycle_eval,
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
    project_id: int | None = None,
    keeper_sync_run_id: int | None = None,
) -> int:
    """Insert one row of ``kind`` with explicit timestamps for sweep tests."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=backend_job_id,
        kind=kind.value,
        status=status.value,
        org_id=org_id,
        project_id=project_id,
        keeper_sync_run_id=keeper_sync_run_id,
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
            await store.start_if_queued(unrelated.id)
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


# ---------------------------------------------------------------------
# Abandoned sweep storage tests (PRD #538)
# ---------------------------------------------------------------------
#
# The abandoned sweep closes the gap between the silent sweep (which
# requires ``in_progress``) and the orphan sweep (which requires
# ``backend_job_id IS NULL``): a ``queued`` row that *did* reach arq
# and was then lost by it. Because age alone cannot distinguish "lost"
# from "backed up behind a long queue", every candidate is verified
# against :meth:`QueueBackend.get_job_metadata` before being failed.


class _StubQueueBackend:
    """Minimal :class:`QueueBackend` double for the abandoned sweeps.

    ``known_ids`` are the ``backend_job_id`` values arq still has a
    record of; any other ID reads as lost. ``error``, when set, makes
    :meth:`get_job_metadata` raise instead — modelling an unreachable
    Redis so the sweep's abort path can be exercised.
    """

    def __init__(
        self,
        *,
        known_ids: set[str] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.known_ids = known_ids if known_ids is not None else set()
        self.error = error
        self.queried: list[str] = []

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> str:
        raise NotImplementedError

    async def get_job_metadata(
        self, backend_job_id: str
    ) -> dict[str, Any] | None:
        self.queried.append(backend_job_id)
        if self.error is not None:
            raise self.error
        if backend_job_id in self.known_ids:
            return {"id": backend_job_id, "status": "queued"}
        return None

    async def get_job_result(self, backend_job_id: str) -> object | None:
        return None


@pytest.mark.asyncio
@_runless_param
async def test_fail_abandoned_jobs_reaps_arq_lost_row(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """A ``queued`` row past the threshold that arq lost is failed."""
    backend_job_id = f"arq-{spec.slug_prefix}-lost"
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-abandoned-1"
        )
        abandoned_id = await _seed_runless_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=backend_job_id,
            date_created_offset=spec.silent_past_offset,
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_jobs(
            spec.kind,
            idle_after=spec.silent_idle_after,
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == [backend_job_id]
    assert len(failed) == 1
    assert failed[0].id == abandoned_id
    assert failed[0].status == JobStatus.failed
    assert failed[0].date_completed is not None
    assert failed[0].errors is not None
    assert failed[0].errors["type"] == "AbandonedQueueJob"


@pytest.mark.asyncio
@_runless_param
async def test_fail_abandoned_jobs_spares_row_arq_still_knows(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """The healthy-job race: a backed-up but live job is never reaped.

    A ``queued`` row can sit past the threshold simply because the
    worker pool is saturated. Verifying against the backend before
    failing is what separates that case from a genuinely lost job, so
    an arq-known row survives *regardless* of age.
    """
    backend_job_id = f"arq-{spec.slug_prefix}-alive"
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-abandoned-2"
        )
        healthy_id = await _seed_runless_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=backend_job_id,
            date_created_offset=timedelta(days=30),
        )

        backend = _StubQueueBackend(known_ids={backend_job_id})
        failed = await store.fail_abandoned_jobs(
            spec.kind,
            idle_after=spec.silent_idle_after,
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == [backend_job_id]
    assert failed == []

    async with db_session.begin():
        survivor = await store.get(healthy_id)
        assert survivor is not None
        assert survivor.status == JobStatus.queued


@pytest.mark.asyncio
@_runless_param
async def test_fail_abandoned_jobs_skips_row_inside_threshold(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """Threshold boundary: a fresh queued row is not even a candidate.

    A row younger than ``idle_after`` must not be queried against the
    backend at all — the dispatcher may still be mid-enqueue, and a
    just-created job legitimately has no arq record yet.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-abandoned-3"
        )
        await _seed_runless_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=f"arq-{spec.slug_prefix}-fresh",
            date_created_offset=spec.silent_idle_after - timedelta(minutes=1),
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_jobs(
            spec.kind,
            idle_after=spec.silent_idle_after,
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == []
    assert failed == []


@pytest.mark.asyncio
@_runless_param
async def test_fail_abandoned_jobs_leaves_silent_and_orphan_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """Sweep non-interference: the other two sweeps keep their rows.

    An ``in_progress`` row belongs to :meth:`fail_silent_jobs` and a
    ``queued`` row with no ``backend_job_id`` to
    :meth:`fail_orphaned_jobs`; the abandoned sweep must claim neither,
    so the three sweeps stay attributable in postmortems.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-abandoned-4"
        )
        silent_id = await _seed_runless_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.in_progress,
            backend_job_id=f"arq-{spec.slug_prefix}-silent",
            date_started=datetime.now(tz=UTC) - spec.silent_past_offset,
            date_created_offset=spec.silent_past_offset,
        )
        orphan_id = await _seed_runless_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=None,
            date_created_offset=spec.silent_past_offset,
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_jobs(
            spec.kind,
            idle_after=spec.silent_idle_after,
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == []
    assert failed == []

    async with db_session.begin():
        silent = await store.get(silent_id)
        orphan = await store.get(orphan_id)
        assert silent is not None
        assert silent.status == JobStatus.in_progress
        assert orphan is not None
        assert orphan.status == JobStatus.queued


@pytest.mark.asyncio
@_runless_param
async def test_fail_abandoned_jobs_skips_other_kinds(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: RunlessReaperSpec,
) -> None:
    """Cross-kind scoping for the abandoned sweep.

    An arq-lost ``queued`` row of every other run-less kind plus
    ``lifecycle_eval`` must be left to that kind's own reaper.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-abandoned-5"
        )
        for idx, kind in enumerate(_other_runless_kinds(spec)):
            await _seed_runless_row(
                db_session,
                kind=kind,
                org_id=org_id,
                status=JobStatus.queued,
                backend_job_id=f"arq-other-lost-{idx}",
                date_created_offset=spec.silent_past_offset,
            )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_jobs(
            spec.kind,
            idle_after=spec.silent_idle_after,
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == []
    assert failed == []


@pytest.mark.asyncio
async def test_fail_abandoned_jobs_skips_run_attributed_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Run-attributed rows have exactly one owner: the run-children sweep.

    ``publish_edition`` jobs cascaded out of a keeper-sync project sync
    carry a ``keeper_sync_run_id``, so without this scoping both the
    run-less ``publish_edition`` sweep and
    :meth:`QueueJobStore.fail_abandoned_run_children` claim them. The
    run-less sweep does not participate in run finalisation, so when it
    won the row the parent ``keeper_sync_runs`` row stayed
    ``in_progress`` forever and 409-blocked every later run for the org.
    """
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="per-abandoned-run-attributed"
        )
        attributed_id = await _seed_runless_row(
            db_session,
            kind=JobKind.publish_edition,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id="arq-per-run-attributed",
            date_created_offset=timedelta(hours=5),
            keeper_sync_run_id=run_id,
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_jobs(
            JobKind.publish_edition,
            idle_after=timedelta(hours=4),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == []
    assert failed == []

    async with db_session.begin():
        survivor = await store.get(attributed_id)
        assert survivor is not None
        assert survivor.status == JobStatus.queued
        # The single owner does claim it, so the row is not stranded.
        claimed = await store.fail_abandoned_run_children(
            idle_after=timedelta(minutes=90),
            queue_backend=_StubQueueBackend(),
        )
        await db_session.commit()

    assert [job.id for job in claimed] == [attributed_id]


@pytest.mark.asyncio
async def test_fail_abandoned_jobs_aborts_when_backend_unreachable(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """An unreachable backend aborts the sweep instead of reaping blind.

    ``get_job_metadata`` raising means the sweep cannot tell a lost job
    from a live one, so it must fail open: no row is mutated and the
    next tick retries once Redis is back.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="abandoned-unreachable")
        candidate_id = await _seed_runless_row(
            db_session,
            kind=JobKind.dashboard_build,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id="arq-unreachable",
            date_created_offset=timedelta(hours=1),
        )

        backend = _StubQueueBackend(error=ConnectionError("redis is down"))
        failed = await store.fail_abandoned_jobs(
            JobKind.dashboard_build,
            idle_after=timedelta(minutes=30),
            queue_backend=backend,
        )
        await db_session.commit()

    assert failed == []

    async with db_session.begin():
        candidate = await store.get(candidate_id)
        assert candidate is not None
        assert candidate.status == JobStatus.queued
        assert candidate.errors is None


@pytest.mark.asyncio
async def test_fail_abandoned_jobs_frees_dashboard_build_mutex(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """The wedge this PRD exists to clear: reaping frees the mutex.

    ``idx_queue_jobs_dashboard_build_active_uq`` counts a ``queued`` row
    as an active job, so an abandoned row blocks every later dashboard
    build for that project. After the sweep,
    :meth:`create_unless_active` must hand back a fresh row rather than
    ``None``.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="abandoned-mutex")
        wedged_id = await _seed_runless_row(
            db_session,
            kind=JobKind.dashboard_build,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id="arq-wedged",
            date_created_offset=timedelta(hours=1),
            project_id=4242,
        )

        blocked = await store.create_unless_active(
            kind=JobKind.dashboard_build,
            org_id=org_id,
            project_id=4242,
        )
        assert blocked is None

        failed = await store.fail_abandoned_jobs(
            JobKind.dashboard_build,
            idle_after=timedelta(minutes=30),
            queue_backend=_StubQueueBackend(),
        )
        assert len(failed) == 1

        fresh = await store.create_unless_active(
            kind=JobKind.dashboard_build,
            org_id=org_id,
            project_id=4242,
        )
        await db_session.commit()

    assert fresh is not None
    assert fresh.id != wedged_id
    assert fresh.status == JobStatus.queued


# ---------------------------------------------------------------------
# Keeper-sync abandoned sweeps (PRD #538, task #540)
# ---------------------------------------------------------------------
#
# The two keeper-sync families each guard something a wedged ``queued``
# row freezes: the tier-cron rows guard
# :meth:`QueueJobStore.has_active_for_subject` (so a wedge stops the
# project's cron sync forever), and the run children guard run
# finalisation (so a wedge parks the parent run in ``in_progress``).
# Both mirror their orphan sibling's scoping, swapping
# ``backend_job_id IS NULL`` for the arq-verified
# ``backend_job_id IS NOT NULL`` predicate.


async def _seed_abandoned_keeper_sync_row(
    db_session: AsyncSession,
    *,
    org_id: int,
    backend_job_id: str,
    created_offset: timedelta,
    run_id: int | None = None,
    subject_label: str | None = None,
) -> int:
    """Insert one ``keeper_sync_project`` row: queued, with an arq ID."""
    store = QueueJobStore(
        session=db_session, logger=structlog.get_logger("docverse")
    )
    job = await store.create(
        kind=JobKind.keeper_sync_project,
        org_id=org_id,
        keeper_sync_run_id=run_id,
        subject_label=subject_label,
        backend_job_id=backend_job_id,
    )
    row = await db_session.get(SqlQueueJob, job.id)
    assert row is not None
    row.date_created = datetime.now(tz=UTC) - created_offset
    await db_session.flush()
    return job.id


@pytest.mark.asyncio
async def test_fail_abandoned_tier_cron_jobs_reaps_arq_lost_row(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A run-less queued row whose arq job vanished is failed."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-abandoned-1")
        abandoned_id = await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-tc-lost",
            created_offset=timedelta(hours=3),
            subject_label="phalanx",
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_tier_cron_jobs(
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == ["arq-tc-lost"]
    assert len(failed) == 1
    assert failed[0].id == abandoned_id
    assert failed[0].status == JobStatus.failed
    assert failed[0].date_completed is not None
    assert failed[0].errors is not None
    assert failed[0].errors["type"] == "AbandonedQueueJob"


@pytest.mark.asyncio
async def test_fail_abandoned_tier_cron_jobs_spares_row_arq_still_knows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A tier-cron row arq still knows about survives at any age."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-abandoned-2")
        healthy_id = await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-tc-alive",
            created_offset=timedelta(days=30),
            subject_label="phalanx",
        )

        backend = _StubQueueBackend(known_ids={"arq-tc-alive"})
        failed = await store.fail_abandoned_tier_cron_jobs(
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == ["arq-tc-alive"]
    assert failed == []

    async with db_session.begin():
        survivor = await store.get(healthy_id)
        assert survivor is not None
        assert survivor.status == JobStatus.queued


@pytest.mark.asyncio
async def test_fail_abandoned_tier_cron_jobs_skips_row_inside_threshold(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Threshold boundary: a fresh tier-cron row is not even a candidate."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-abandoned-3")
        await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-tc-fresh",
            created_offset=timedelta(minutes=89),
            subject_label="phalanx",
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_tier_cron_jobs(
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == []
    assert failed == []


@pytest.mark.asyncio
async def test_fail_abandoned_tier_cron_jobs_skips_run_attributed_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Run-attributed rows belong to ``fail_abandoned_run_children``."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="ks-tc-abandoned-4"
        )
        await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-tc-run-attributed",
            created_offset=timedelta(hours=3),
            run_id=run_id,
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_tier_cron_jobs(
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == []
    assert failed == []


@pytest.mark.asyncio
async def test_fail_abandoned_tier_cron_jobs_skips_non_keeper_sync_kinds(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Only ``keeper_sync_project`` rows are in scope for this sweep."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-abandoned-5")
        unrelated = await store.create(
            kind=JobKind.build_processing,
            org_id=org_id,
            keeper_sync_run_id=None,
            backend_job_id="arq-tc-other-kind",
        )
        row = await db_session.get(SqlQueueJob, unrelated.id)
        assert row is not None
        row.date_created = datetime.now(tz=UTC) - timedelta(hours=3)
        await db_session.flush()

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_tier_cron_jobs(
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == []
    assert failed == []


@pytest.mark.asyncio
async def test_fail_abandoned_tier_cron_jobs_unblocks_subject_mutex(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """The wedge this sweep clears: the subject mutex frees up.

    ``has_active_for_subject`` counts a ``queued`` row as an in-flight
    sync, so one abandoned tier-cron row parks its project behind the
    mutex on every later tier tick.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug="ks-tc-abandoned-6")
        await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-tc-wedged",
            created_offset=timedelta(hours=3),
            subject_label="phalanx",
        )

        before = await store.has_active_for_subject(
            org_id=org_id,
            kind=JobKind.keeper_sync_project,
            subject_label="phalanx",
        )
        assert before is True

        failed = await store.fail_abandoned_tier_cron_jobs(
            idle_after=timedelta(minutes=90),
            queue_backend=_StubQueueBackend(),
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
async def test_fail_abandoned_run_children_reaps_arq_lost_row(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A run child whose arq job vanished is failed for its own run."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="ks-rc-abandoned-1"
        )
        abandoned_id = await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-rc-lost",
            created_offset=timedelta(hours=3),
            run_id=run_id,
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_run_children(
            run_id=run_id,
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == ["arq-rc-lost"]
    assert len(failed) == 1
    assert failed[0].id == abandoned_id
    assert failed[0].status == JobStatus.failed
    assert failed[0].date_completed is not None
    assert failed[0].errors is not None
    assert failed[0].errors["type"] == "AbandonedQueueJob"


@pytest.mark.asyncio
async def test_fail_abandoned_run_children_spares_row_arq_still_knows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A run child arq still knows about survives at any age."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="ks-rc-abandoned-2"
        )
        healthy_id = await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-rc-alive",
            created_offset=timedelta(days=30),
            run_id=run_id,
        )

        backend = _StubQueueBackend(known_ids={"arq-rc-alive"})
        failed = await store.fail_abandoned_run_children(
            run_id=run_id,
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == ["arq-rc-alive"]
    assert failed == []

    async with db_session.begin():
        survivor = await store.get(healthy_id)
        assert survivor is not None
        assert survivor.status == JobStatus.queued


@pytest.mark.asyncio
async def test_fail_abandoned_run_children_skips_row_inside_threshold(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Threshold boundary: a fresh run child is not even a candidate."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="ks-rc-abandoned-3"
        )
        await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-rc-fresh",
            created_offset=timedelta(minutes=89),
            run_id=run_id,
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_run_children(
            run_id=run_id,
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == []
    assert failed == []


@pytest.mark.asyncio
async def test_fail_abandoned_run_children_skips_tier_cron_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Tier-cron rows never cross-match the run-scoped sweep.

    Neither the per-run form nor the reaper's all-runs form may claim a
    row with no ``keeper_sync_run_id`` — those belong to
    :meth:`QueueJobStore.fail_abandoned_tier_cron_jobs`.
    """
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="ks-rc-abandoned-4"
        )
        await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-rc-tier-cron",
            created_offset=timedelta(hours=3),
            subject_label="phalanx",
        )

        backend = _StubQueueBackend()
        scoped = await store.fail_abandoned_run_children(
            run_id=run_id,
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        all_runs = await store.fail_abandoned_run_children(
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == []
    assert scoped == []
    assert all_runs == []


async def _seed_abandoned_discovery_row(
    db_session: AsyncSession,
    *,
    org_id: int,
    run_id: int,
    backend_job_id: str,
    created_offset: timedelta,
) -> int:
    """Insert the run's own ``keeper_sync_run_discovery`` row, queued.

    ``KeeperSyncRunService.start_run`` writes exactly this shape —
    ``keeper_sync_run_id`` set to the run it fans out for — so the
    discovery row is run-attributed just like the children it enqueues.
    """
    store = QueueJobStore(
        session=db_session, logger=structlog.get_logger("docverse")
    )
    job = await store.create(
        kind=JobKind.keeper_sync_run_discovery,
        org_id=org_id,
        keeper_sync_run_id=run_id,
        subject_label="discovery for ks-org",
        backend_job_id=backend_job_id,
    )
    row = await db_session.get(SqlQueueJob, job.id)
    assert row is not None
    row.date_created = datetime.now(tz=UTC) - created_offset
    await db_session.flush()
    return job.id


@pytest.mark.asyncio
async def test_fail_abandoned_run_children_skips_discovery_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """The run's own discovery row is not one of its children.

    ``fail_abandoned_run_children`` claims run-attributed rows so the
    caller can roll the parent up via ``maybe_finalise_run``, which
    computes ``partial_failure`` from the child counters. A failed
    *discovery* means the fan-out never happened at all, and the worker
    path fails the whole run — so the discovery row needs its own sweep
    rather than being mislabelled and mis-finalised as a child.
    """
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="ks-rc-abandoned-7"
        )
        discovery_id = await _seed_abandoned_discovery_row(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="arq-discovery-lost",
            created_offset=timedelta(hours=3),
        )

        backend = _StubQueueBackend()
        scoped = await store.fail_abandoned_run_children(
            run_id=run_id,
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        all_runs = await store.fail_abandoned_run_children(
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == []
    assert scoped == []
    assert all_runs == []

    async with db_session.begin():
        survivor = await store.get(discovery_id)
        assert survivor is not None
        assert survivor.status == JobStatus.queued


@pytest.mark.asyncio
async def test_fail_abandoned_run_children_scoped_to_run(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """With a ``run_id``, another run's abandoned child is left alone."""
    async with db_session.begin():
        org_a_id, run_a_id = await _seed_org_and_run(
            db_session, slug="ks-rc-abandoned-5a"
        )
        org_b_id, run_b_id = await _seed_org_and_run(
            db_session, slug="ks-rc-abandoned-5b"
        )
        mine_id = await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_a_id,
            backend_job_id="arq-rc-mine",
            created_offset=timedelta(hours=3),
            run_id=run_a_id,
        )
        other_id = await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_b_id,
            backend_job_id="arq-rc-other",
            created_offset=timedelta(hours=3),
            run_id=run_b_id,
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_run_children(
            run_id=run_a_id,
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == ["arq-rc-mine"]
    assert [job.id for job in failed] == [mine_id]

    async with db_session.begin():
        other = await store.get(other_id)
        assert other is not None
        assert other.status == JobStatus.queued


@pytest.mark.asyncio
async def test_fail_abandoned_run_children_without_run_id_sweeps_all_runs(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """The cron reaper's mode reaps every run's abandoned children.

    The reaper has no run in hand, so it sweeps run-attributed rows in
    one query and reads the affected run IDs back off the reaped rows.
    """
    async with db_session.begin():
        org_a_id, run_a_id = await _seed_org_and_run(
            db_session, slug="ks-rc-abandoned-6a"
        )
        org_b_id, run_b_id = await _seed_org_and_run(
            db_session, slug="ks-rc-abandoned-6b"
        )
        await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_a_id,
            backend_job_id="arq-rc-all-a",
            created_offset=timedelta(hours=3),
            run_id=run_a_id,
        )
        await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_b_id,
            backend_job_id="arq-rc-all-b",
            created_offset=timedelta(hours=3),
            run_id=run_b_id,
        )

        failed = await store.fail_abandoned_run_children(
            idle_after=timedelta(minutes=90),
            queue_backend=_StubQueueBackend(),
        )
        await db_session.commit()

    assert {job.keeper_sync_run_id for job in failed} == {run_a_id, run_b_id}


@pytest.mark.asyncio
async def test_fail_abandoned_run_discovery_reaps_arq_lost_row(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A lost discovery row is failed with a discovery-specific message.

    The message must not read "Abandoned keeper-sync child": the row is
    the fan-out's parent, and a postmortem needs to tell "the run never
    started" apart from "one project of the run was lost".
    """
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="ks-rd-abandoned-1"
        )
        discovery_id = await _seed_abandoned_discovery_row(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="arq-rd-lost",
            created_offset=timedelta(hours=3),
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_run_discovery(
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == ["arq-rd-lost"]
    assert len(failed) == 1
    assert failed[0].id == discovery_id
    assert failed[0].status == JobStatus.failed
    assert failed[0].date_completed is not None
    assert failed[0].errors is not None
    assert failed[0].errors["type"] == "AbandonedQueueJob"
    assert "run discovery" in failed[0].errors["message"]
    assert "child" not in failed[0].errors["message"]


@pytest.mark.asyncio
async def test_fail_abandoned_run_discovery_spares_row_arq_still_knows(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A discovery job arq still knows about survives at any age."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="ks-rd-abandoned-2"
        )
        healthy_id = await _seed_abandoned_discovery_row(
            db_session,
            org_id=org_id,
            run_id=run_id,
            backend_job_id="arq-rd-alive",
            created_offset=timedelta(days=30),
        )

        backend = _StubQueueBackend(known_ids={"arq-rd-alive"})
        failed = await store.fail_abandoned_run_discovery(
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == ["arq-rd-alive"]
    assert failed == []

    async with db_session.begin():
        survivor = await store.get(healthy_id)
        assert survivor is not None
        assert survivor.status == JobStatus.queued


@pytest.mark.asyncio
async def test_fail_abandoned_run_discovery_skips_children_and_tier_cron(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Three-way scoping: discovery, run children, and tier-cron rows.

    Each keeper-sync abandoned sweep owns exactly one population, so a
    single tick cannot double-fail a row or leave one unclaimed.
    """
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(
            db_session, slug="ks-rd-abandoned-3"
        )
        child_id = await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-rd-child",
            created_offset=timedelta(hours=3),
            run_id=run_id,
        )
        tier_cron_id = await _seed_abandoned_keeper_sync_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-rd-tier-cron",
            created_offset=timedelta(hours=3),
            subject_label="phalanx",
        )

        backend = _StubQueueBackend()
        failed = await store.fail_abandoned_run_discovery(
            idle_after=timedelta(minutes=90),
            queue_backend=backend,
        )
        await db_session.commit()

    assert backend.queried == []
    assert failed == []

    async with db_session.begin():
        for job_id in (child_id, tier_cron_id):
            survivor = await store.get(job_id)
            assert survivor is not None
            assert survivor.status == JobStatus.queued


# ---------------------------------------------------------------------
# lifecycle_eval / git_ref_audit abandoned sweeps (PRD #538, task #541)
# ---------------------------------------------------------------------
#
# The two families ``lifecycle_reaper`` owns are structurally identical
# — each is scoped by ``kind`` alone and each guards a per-org active
# mutex (``idx_queue_jobs_lifecycle_eval_active_uq`` /
# ``idx_queue_jobs_git_ref_audit_active_uq``) that a wedged ``queued``
# row holds forever — so the predicate tests are parametrized over a
# two-row spec table rather than written twice.


_LifecycleSweep = Callable[
    [QueueJobStore, timedelta, QueueBackend], Awaitable[list[QueueJob]]
]


async def _sweep_abandoned_lifecycle_eval(
    store: QueueJobStore,
    idle_after: timedelta,
    queue_backend: QueueBackend,
) -> list[QueueJob]:
    return await store.fail_abandoned_lifecycle_eval_jobs(
        idle_after=idle_after, queue_backend=queue_backend
    )


async def _sweep_abandoned_git_ref_audit(
    store: QueueJobStore,
    idle_after: timedelta,
    queue_backend: QueueBackend,
) -> list[QueueJob]:
    return await store.fail_abandoned_git_ref_audit_jobs(
        idle_after=idle_after, queue_backend=queue_backend
    )


@dataclass(frozen=True)
class LifecycleAbandonedSpec:
    """One row per family the ``lifecycle_reaper`` abandoned sweeps own."""

    kind: JobKind
    other_kind: JobKind
    slug_prefix: str
    sweep: _LifecycleSweep

    @property
    def label(self) -> str:
        return self.kind.value


LIFECYCLE_ABANDONED_SPECS = [
    LifecycleAbandonedSpec(
        kind=JobKind.lifecycle_eval,
        other_kind=JobKind.git_ref_audit,
        slug_prefix="lce-ab",
        sweep=_sweep_abandoned_lifecycle_eval,
    ),
    LifecycleAbandonedSpec(
        kind=JobKind.git_ref_audit,
        other_kind=JobKind.lifecycle_eval,
        slug_prefix="gra-ab",
        sweep=_sweep_abandoned_git_ref_audit,
    ),
]


_lifecycle_abandoned_param = pytest.mark.parametrize(
    "spec",
    LIFECYCLE_ABANDONED_SPECS,
    ids=lambda s: s.label,
)


#: Stands in for ``config.lifecycle_reaper_threshold_seconds`` (6 h in
#: production), which both families use as their abandoned-sweep age
#: gate.
_LIFECYCLE_THRESHOLD = timedelta(hours=6)


@pytest.mark.asyncio
@_lifecycle_abandoned_param
async def test_fail_abandoned_lifecycle_row_reaped_when_arq_lost(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: LifecycleAbandonedSpec,
) -> None:
    """A ``queued`` row past the threshold that arq lost is failed."""
    backend_job_id = f"arq-{spec.slug_prefix}-lost"
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug=f"{spec.slug_prefix}-1")
        abandoned_id = await _seed_lifecycle_family_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=backend_job_id,
            date_created_offset=timedelta(hours=7),
        )

        backend = _StubQueueBackend()
        failed = await spec.sweep(store, _LIFECYCLE_THRESHOLD, backend)
        await db_session.commit()

    assert backend.queried == [backend_job_id]
    assert len(failed) == 1
    assert failed[0].id == abandoned_id
    assert failed[0].status == JobStatus.failed
    assert failed[0].date_completed is not None
    assert failed[0].errors is not None
    assert failed[0].errors["type"] == "AbandonedQueueJob"


@pytest.mark.asyncio
@_lifecycle_abandoned_param
async def test_fail_abandoned_lifecycle_row_spared_when_arq_knows(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: LifecycleAbandonedSpec,
) -> None:
    """A job arq still knows about survives regardless of age.

    Both families run on pools that can back up (the lifecycle
    dispatcher fans out one job per org), so age alone would cancel
    healthy work. The backend check is what separates "lost" from
    "waiting its turn".
    """
    backend_job_id = f"arq-{spec.slug_prefix}-alive"
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug=f"{spec.slug_prefix}-2")
        healthy_id = await _seed_lifecycle_family_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=backend_job_id,
            date_created_offset=timedelta(days=30),
        )

        backend = _StubQueueBackend(known_ids={backend_job_id})
        failed = await spec.sweep(store, _LIFECYCLE_THRESHOLD, backend)
        await db_session.commit()

    assert backend.queried == [backend_job_id]
    assert failed == []

    async with db_session.begin():
        survivor = await store.get(healthy_id)
        assert survivor is not None
        assert survivor.status == JobStatus.queued


@pytest.mark.asyncio
@_lifecycle_abandoned_param
async def test_fail_abandoned_lifecycle_row_inside_threshold_untouched(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: LifecycleAbandonedSpec,
) -> None:
    """Threshold boundary: a row younger than the gate is not a candidate.

    A just-dispatched row legitimately has no settled arq record yet, so
    it must not even be queried against the backend.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug=f"{spec.slug_prefix}-3")
        await _seed_lifecycle_family_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=f"arq-{spec.slug_prefix}-fresh",
            date_created_offset=_LIFECYCLE_THRESHOLD - timedelta(minutes=1),
        )

        backend = _StubQueueBackend()
        failed = await spec.sweep(store, _LIFECYCLE_THRESHOLD, backend)
        await db_session.commit()

    assert backend.queried == []
    assert failed == []


@pytest.mark.asyncio
@_lifecycle_abandoned_param
async def test_fail_abandoned_lifecycle_row_scoped_to_its_kind(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: LifecycleAbandonedSpec,
) -> None:
    """Kind scoping: the sibling family's rows belong to its own sweep.

    ``lifecycle_reaper`` runs both sweeps in one transaction, so a
    predicate that leaked across kinds would double-count reaped rows
    and misattribute them in the log context.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug=f"{spec.slug_prefix}-4")
        await _seed_lifecycle_family_row(
            db_session,
            kind=spec.other_kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=f"arq-{spec.slug_prefix}-sibling",
            date_created_offset=timedelta(hours=7),
        )
        await _seed_runless_row(
            db_session,
            kind=JobKind.dashboard_build,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=f"arq-{spec.slug_prefix}-runless",
            date_created_offset=timedelta(hours=7),
        )

        backend = _StubQueueBackend()
        failed = await spec.sweep(store, _LIFECYCLE_THRESHOLD, backend)
        await db_session.commit()

    assert backend.queried == []
    assert failed == []


@pytest.mark.asyncio
@_lifecycle_abandoned_param
async def test_fail_abandoned_lifecycle_leaves_silent_and_orphan_rows(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: LifecycleAbandonedSpec,
) -> None:
    """Sweep non-interference within the family.

    An ``in_progress`` row belongs to the silent sweep and a ``queued``
    row with no ``backend_job_id`` to the orphan sweep; claiming either
    here would make the three loss modes indistinguishable in a
    postmortem.
    """
    async with db_session.begin():
        org_a_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-5a"
        )
        org_b_id = await _seed_org_only(
            db_session, slug=f"{spec.slug_prefix}-5b"
        )
        silent_id = await _seed_lifecycle_family_row(
            db_session,
            kind=spec.kind,
            org_id=org_a_id,
            status=JobStatus.in_progress,
            backend_job_id=f"arq-{spec.slug_prefix}-silent",
            date_started=datetime.now(tz=UTC) - timedelta(hours=7),
            date_created_offset=timedelta(hours=7),
        )
        orphan_id = await _seed_lifecycle_family_row(
            db_session,
            kind=spec.kind,
            org_id=org_b_id,
            status=JobStatus.queued,
            backend_job_id=None,
            date_created_offset=timedelta(hours=7),
        )

        backend = _StubQueueBackend()
        failed = await spec.sweep(store, _LIFECYCLE_THRESHOLD, backend)
        await db_session.commit()

    assert backend.queried == []
    assert failed == []

    async with db_session.begin():
        silent = await store.get(silent_id)
        orphan = await store.get(orphan_id)
        assert silent is not None
        assert silent.status == JobStatus.in_progress
        assert orphan is not None
        assert orphan.status == JobStatus.queued


@pytest.mark.asyncio
@_lifecycle_abandoned_param
async def test_fail_abandoned_lifecycle_aborts_when_backend_unreachable(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: LifecycleAbandonedSpec,
) -> None:
    """An unreachable backend leaves every candidate alone."""
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug=f"{spec.slug_prefix}-6")
        candidate_id = await _seed_lifecycle_family_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=f"arq-{spec.slug_prefix}-unreachable",
            date_created_offset=timedelta(hours=7),
        )

        backend = _StubQueueBackend(error=ConnectionError("redis is down"))
        failed = await spec.sweep(store, _LIFECYCLE_THRESHOLD, backend)
        await db_session.commit()

    assert failed == []

    async with db_session.begin():
        candidate = await store.get(candidate_id)
        assert candidate is not None
        assert candidate.status == JobStatus.queued
        assert candidate.errors is None


@pytest.mark.asyncio
@_lifecycle_abandoned_param
async def test_fail_abandoned_lifecycle_frees_per_org_mutex(
    db_session: AsyncSession,
    store: QueueJobStore,
    spec: LifecycleAbandonedSpec,
) -> None:
    """Reaping releases the family's per-org active-job mutex.

    Both mutexes count a ``queued`` row as live work, so until the sweep
    fails the wedged row the next dispatcher / discovery tick cannot
    enqueue for that org at all.
    """
    async with db_session.begin():
        org_id = await _seed_org_only(db_session, slug=f"{spec.slug_prefix}-7")
        wedged_id = await _seed_lifecycle_family_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            status=JobStatus.queued,
            backend_job_id=f"arq-{spec.slug_prefix}-wedged",
            date_created_offset=timedelta(hours=7),
        )

        blocked = await store.create_unless_active(
            kind=spec.kind, org_id=org_id
        )
        assert blocked is None

        failed = await spec.sweep(
            store, _LIFECYCLE_THRESHOLD, _StubQueueBackend()
        )
        assert len(failed) == 1

        fresh = await store.create_unless_active(kind=spec.kind, org_id=org_id)
        await db_session.commit()

    assert fresh is not None
    assert fresh.id != wedged_id
    assert fresh.status == JobStatus.queued


@pytest.mark.asyncio
async def test_public_ids_are_time_ordered(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Jobs enqueued in succession carry time-ordered public IDs.

    A short real-time gap guarantees the two mints land in distinct
    milliseconds so the ordering comes from the time-ordered high bits.
    """
    async with db_session.begin():
        first = await store.create(kind=JobKind.build_processing, org_id=1)
        await asyncio.sleep(0.005)
        second = await store.create(kind=JobKind.build_processing, org_id=1)
        await db_session.commit()

    assert second.public_id > first.public_id


@pytest.mark.asyncio
async def test_create_retries_on_public_id_collision(
    db_session: AsyncSession,
    store: QueueJobStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A colliding public_id is re-minted with no error and no merged rows."""
    collision_id = 313131
    fresh_id = 888888

    def _fake_ids() -> Iterator[int]:
        yield from (collision_id, fresh_id)

    ids = _fake_ids()
    monkeypatch.setattr(
        "docverse_server.storage._public_id.generate_resource_id",
        lambda: next(ids),
    )

    async with db_session.begin():
        # Pre-insert a job occupying ``collision_id`` and flush it into the
        # outer transaction so the retried insert races a persistent row.
        existing = SqlQueueJob(
            public_id=collision_id,
            kind=JobKind.build_processing.value,
            status=JobStatus.queued.value,
            org_id=1,
            subject_label="pre-existing",
        )
        db_session.add(existing)
        await db_session.flush()

        created = await store.create(kind=JobKind.build_processing, org_id=1)
        await db_session.commit()

    # The retry minted the fresh id, leaving the pre-existing row untouched.
    assert created.public_id == fresh_id

    async with db_session.begin():
        total = await db_session.scalar(
            select(func.count()).select_from(SqlQueueJob)
        )
        preserved = await db_session.scalar(
            select(SqlQueueJob.subject_label).where(
                SqlQueueJob.public_id == collision_id
            )
        )
    assert total == 2
    assert preserved == "pre-existing"


async def _seed_org_project(
    db_session: AsyncSession, *, slug: str = "lbo-org"
) -> tuple[int, int]:
    """Seed an org with one project; return ``(org_id, project_id)``."""
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title="LBO Org",
            base_domain=f"{slug}.example.com",
        )
    )
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="lbo-proj",
            title="LBO Project",
            source_url="https://example.com/example/repo",
        ),
    )
    return org.id, project.id


@pytest.mark.asyncio
async def test_list_by_org_newest_first(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Jobs for an org are returned newest-first with a total count."""
    async with db_session.begin():
        org_id, _ = await _seed_org_project(db_session)
        first = await store.create(
            kind=JobKind.build_processing, org_id=org_id
        )
        second = await store.create(
            kind=JobKind.publish_edition, org_id=org_id
        )
        third = await store.create(kind=JobKind.dashboard_build, org_id=org_id)
        result = await store.list_by_org(org_id=org_id, limit=10)
        await db_session.commit()

    assert result.count == 3
    ids = [job.id for job in result.entries]
    assert ids == [third.id, second.id, first.id]


@pytest.mark.asyncio
async def test_list_by_org_excludes_other_orgs(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Jobs belonging to other orgs never appear in the listing."""
    async with db_session.begin():
        org_id, _ = await _seed_org_project(db_session, slug="lbo-mine")
        other_id, _ = await _seed_org_project(db_session, slug="lbo-other")
        mine = await store.create(kind=JobKind.build_processing, org_id=org_id)
        await store.create(kind=JobKind.build_processing, org_id=other_id)
        result = await store.list_by_org(org_id=org_id, limit=10)
        await db_session.commit()

    assert result.count == 1
    assert [job.id for job in result.entries] == [mine.id]


@pytest.mark.asyncio
async def test_list_by_org_filters_by_kind(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """The ``kind`` filter narrows results to a single JobKind."""
    async with db_session.begin():
        org_id, _ = await _seed_org_project(db_session)
        build = await store.create(
            kind=JobKind.build_processing, org_id=org_id
        )
        await store.create(kind=JobKind.publish_edition, org_id=org_id)
        result = await store.list_by_org(
            org_id=org_id, kind=JobKind.build_processing, limit=10
        )
        await db_session.commit()

    assert result.count == 1
    assert [job.id for job in result.entries] == [build.id]


@pytest.mark.asyncio
async def test_list_by_org_filters_by_status(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """The ``status`` filter narrows results to a single JobStatus."""
    async with db_session.begin():
        org_id, _ = await _seed_org_project(db_session)
        await store.create(kind=JobKind.build_processing, org_id=org_id)
        started = await store.create(
            kind=JobKind.build_processing, org_id=org_id
        )
        await store.start_if_queued(started.id)
        result = await store.list_by_org(
            org_id=org_id, status=JobStatus.in_progress, limit=10
        )
        await db_session.commit()

    assert result.count == 1
    assert [job.id for job in result.entries] == [started.id]


@pytest.mark.asyncio
async def test_list_by_org_filters_by_project(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """The ``project_id`` filter narrows results to one project."""
    async with db_session.begin():
        org_id, project_id = await _seed_org_project(db_session)
        scoped = await store.create(
            kind=JobKind.dashboard_build,
            org_id=org_id,
            project_id=project_id,
        )
        await store.create(kind=JobKind.build_processing, org_id=org_id)
        result = await store.list_by_org(
            org_id=org_id, project_id=project_id, limit=10
        )
        await db_session.commit()

    assert result.count == 1
    assert [job.id for job in result.entries] == [scoped.id]


@pytest.mark.asyncio
async def test_list_by_org_filters_by_run(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """The ``keeper_sync_run_id`` filter narrows results to one run."""
    async with db_session.begin():
        org_id, run_id = await _seed_org_and_run(db_session, slug="lbo-run")
        attributed = await store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
        )
        await store.create(kind=JobKind.build_processing, org_id=org_id)
        result = await store.list_by_org(
            org_id=org_id, keeper_sync_run_id=run_id, limit=10
        )
        await db_session.commit()

    assert result.count == 1
    assert [job.id for job in result.entries] == [attributed.id]


@pytest.mark.asyncio
async def test_list_by_org_filters_combine(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """Multiple filters combine conjunctively."""
    async with db_session.begin():
        org_id, project_id = await _seed_org_project(db_session)
        match = await store.create(
            kind=JobKind.dashboard_build,
            org_id=org_id,
            project_id=project_id,
        )
        # Same kind, wrong project.
        await store.create(kind=JobKind.dashboard_build, org_id=org_id)
        # Right project, wrong kind.
        await store.create(
            kind=JobKind.build_processing,
            org_id=org_id,
            project_id=project_id,
        )
        result = await store.list_by_org(
            org_id=org_id,
            kind=JobKind.dashboard_build,
            project_id=project_id,
            limit=10,
        )
        await db_session.commit()

    assert result.count == 1
    assert [job.id for job in result.entries] == [match.id]


@pytest.mark.asyncio
async def test_list_by_org_paginates(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A keyset cursor pages through the org's jobs newest-first."""
    async with db_session.begin():
        org_id, _ = await _seed_org_project(db_session)
        created = [
            await store.create(kind=JobKind.build_processing, org_id=org_id)
            for _ in range(5)
        ]
        first_page = await store.list_by_org(org_id=org_id, limit=2)
        await db_session.commit()

    assert first_page.count == 5
    newest = list(reversed(created))
    assert [job.id for job in first_page.entries] == [
        newest[0].id,
        newest[1].id,
    ]
    assert first_page.next_cursor is not None

    async with db_session.begin():
        second_page = await store.list_by_org(
            org_id=org_id, cursor=first_page.next_cursor, limit=2
        )
        await db_session.commit()

    assert [job.id for job in second_page.entries] == [
        newest[2].id,
        newest[3].id,
    ]


# ---------------------------------------------------------------------------
# create_unless_active — race-tolerant insert against the active-job
# partial unique indexes (issue #508)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_unless_active_returns_none_when_slot_taken(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """The loser of the active-job race gets ``None``, not an exception."""
    async with db_session.begin():
        org_id, _ = await _seed_org_project(db_session, slug="cua-taken")
        first = await store.create_unless_active(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            subject_label="pipelines",
        )
        second = await store.create_unless_active(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            subject_label="pipelines",
        )
        await db_session.commit()

    assert first is not None
    assert second is None


@pytest.mark.asyncio
async def test_create_unless_active_leaves_transaction_usable(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A lost race does not poison the caller's transaction.

    The whole point of absorbing the violation inside a savepoint: the
    tier-cron pass that loses the race on one slug must keep enqueuing
    the remaining slugs, and must be able to commit what it did.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org_project(db_session, slug="cua-usable")
        await store.create_unless_active(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            subject_label="pipelines",
        )
        lost = await store.create_unless_active(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            subject_label="pipelines",
        )
        # The transaction survived: a further read and a further write
        # both succeed, and the whole unit commits.
        assert lost is None
        assert await store.has_active_for_subject(
            org_id=org_id,
            kind=JobKind.keeper_sync_project,
            subject_label="pipelines",
        )
        next_slug = await store.create_unless_active(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            subject_label="afw",
        )
        await db_session.commit()

    assert next_slug is not None
    async with db_session.begin():
        rows = (
            (
                await db_session.execute(
                    select(SqlQueueJob).where(SqlQueueJob.org_id == org_id)
                )
            )
            .scalars()
            .all()
        )
    assert sorted(row.subject_label or "" for row in rows) == [
        "afw",
        "pipelines",
    ]


@pytest.mark.asyncio
async def test_create_unless_active_propagates_other_integrity_errors(
    db_session: AsyncSession,
    store: QueueJobStore,
) -> None:
    """A non-unique-violation IntegrityError is not swallowed.

    ``edition_id`` is a real foreign key; pointing it at a missing row
    raises SQLSTATE ``23503``, which the helper must let through rather
    than reporting as "someone else holds the slot".
    """
    async with db_session.begin():
        org_id, _ = await _seed_org_project(db_session, slug="cua-fk")
        with pytest.raises(IntegrityError):
            await store.create_unless_active(
                kind=JobKind.publish_edition,
                org_id=org_id,
                edition_id=987654321,
            )


@pytest.mark.asyncio
async def test_create_unless_active_dedups_concurrent_sessions(
    db_session: AsyncSession,
) -> None:
    """Two genuinely concurrent inserts settle on exactly one row.

    Each task runs on its own session/connection and both enter their
    transaction before either inserts, so the winner is decided by the
    ``idx_queue_jobs_keeper_sync_project_active_uq`` index rather than by
    the application-side pre-check. The loser blocks on the winner's row
    lock, then gets ``None`` once the winner commits — neither raises.
    """
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        org_id, _ = await _seed_org_project(db_session, slug="cua-concurrent")
        await db_session.commit()

    barrier = asyncio.Barrier(2)

    async def enqueue() -> QueueJob | None:
        async for session in db_session_dependency():
            store = QueueJobStore(session=session, logger=logger)
            async with session.begin():
                await barrier.wait()
                job = await store.create_unless_active(
                    kind=JobKind.keeper_sync_project,
                    org_id=org_id,
                    subject_label="pipelines",
                )
                await session.commit()
            return job
        msg = "No database session available"
        raise RuntimeError(msg)

    results = await asyncio.gather(enqueue(), enqueue())

    assert sum(job is not None for job in results) == 1
    assert sum(job is None for job in results) == 1
    async with db_session.begin():
        rows = (
            (
                await db_session.execute(
                    select(SqlQueueJob).where(SqlQueueJob.org_id == org_id)
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 1


def test_active_job_unique_indexes_cover_every_mutex() -> None:
    """The derived index set names every active-job mutex on the table.

    ``_ACTIVE_JOB_UNIQUE_INDEXES`` is derived from the table metadata by
    naming convention, so a mutex added later participates in
    ``create_unless_active`` for free — but a rename that breaks the
    convention would silently empty the set and turn the backstop back
    into an unhandled ``IntegrityError``. Pin the current membership so
    that regression fails here instead of in production.
    """
    assert {
        "idx_queue_jobs_keeper_sync_project_active_uq",
        "idx_queue_jobs_lifecycle_eval_active_uq",
        "idx_queue_jobs_git_ref_audit_active_uq",
        "idx_queue_jobs_dashboard_build_active_uq",
    } == _ACTIVE_JOB_UNIQUE_INDEXES
