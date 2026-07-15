"""Tests for ``GitRefAuditRunStore`` and git_ref_audit constraints.

Mirrors :mod:`tests.storage.lifecycle_eval_run_store_test`. The two
subsystems share the dispatcher / per-org / reaper pattern, so the
behaviour we pin here is symmetric: singleton non-terminal run via
the partial-unique index, per-org mutex via the
``idx_queue_jobs_git_ref_audit_active_uq`` index, forward-only
status transitions, and JSONB summary capture.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import structlog
from docverse.client.models import GitRefAuditRunStatus, OrganizationCreate
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.git_ref_audit_run import SqlGitRefAuditRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.exceptions import InvalidJobStateError, JobNotFoundError
from docverse.storage.git_ref_audit_run_store import GitRefAuditRunStore
from docverse.storage.organization_store import OrganizationStore


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(
    db_session: AsyncSession, *, slug: str = "gar-org"
) -> tuple[int, str]:
    org_store = OrganizationStore(session=db_session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"GAR Org {slug}",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id, org.slug


def _seed_queue_job(
    db_session: AsyncSession,
    *,
    org_id: int,
    run_id: int,
    status: JobStatus,
    date_created: datetime | None = None,
    date_started: datetime | None = None,
    date_completed: datetime | None = None,
    subject_label: str | None = None,
) -> None:
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        kind=JobKind.git_ref_audit.value,
        status=status.value,
        org_id=org_id,
        git_ref_audit_run_id=run_id,
        subject_label=subject_label,
        date_started=date_started,
        date_completed=date_completed,
    )
    if date_created is not None:
        row.date_created = date_created
    db_session.add(row)


@pytest.mark.asyncio
async def test_partial_unique_index_blocks_two_pending_runs(
    db_session: AsyncSession,
) -> None:
    """Two ``pending`` git_ref_audit rows are rejected by the partial UQ."""
    async with db_session.begin():
        db_session.add(SqlGitRefAuditRun(status="pending"))

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(SqlGitRefAuditRun(status="pending"))


@pytest.mark.asyncio
async def test_partial_unique_index_allows_terminal_alongside_pending(
    db_session: AsyncSession,
) -> None:
    """Terminal rows do not participate in the non-terminal uniqueness."""
    async with db_session.begin():
        db_session.add(SqlGitRefAuditRun(status="succeeded"))
        db_session.add(SqlGitRefAuditRun(status="failed"))
        db_session.add(SqlGitRefAuditRun(status="partial_failure"))

    async with db_session.begin():
        db_session.add(SqlGitRefAuditRun(status="pending"))


@pytest.mark.asyncio
async def test_git_ref_audit_runs_status_check_rejects_invalid(
    db_session: AsyncSession,
) -> None:
    """A status outside the allowed set fails the CHECK constraint."""
    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(SqlGitRefAuditRun(status="bogus"))


@pytest.mark.asyncio
async def test_queue_jobs_git_ref_audit_per_org_mutex(
    db_session: AsyncSession,
) -> None:
    """Two active ``git_ref_audit`` queue_jobs for one org collide."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        db_session.add(
            SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.git_ref_audit.value,
                status=JobStatus.queued.value,
                org_id=org_id,
                subject_label=org_slug,
            )
        )

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(
                SqlQueueJob(
                    public_id=validate_base32_id(generate_base32_id()),
                    kind=JobKind.git_ref_audit.value,
                    status=JobStatus.in_progress.value,
                    org_id=org_id,
                    subject_label=org_slug,
                )
            )


@pytest.mark.asyncio
async def test_queue_jobs_git_ref_audit_terminal_does_not_block_active(
    db_session: AsyncSession,
) -> None:
    """A terminal sibling does not block a fresh active row for the org."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        db_session.add(
            SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.git_ref_audit.value,
                status=JobStatus.completed.value,
                org_id=org_id,
                subject_label=org_slug,
            )
        )

    async with db_session.begin():
        db_session.add(
            SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.git_ref_audit.value,
                status=JobStatus.queued.value,
                org_id=org_id,
                subject_label=org_slug,
            )
        )


@pytest.mark.asyncio
async def test_create_returns_pending_run(
    db_session: AsyncSession,
) -> None:
    """``create`` inserts a pending row and returns the domain model."""
    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        run = await store.create()

    assert run.id > 0
    assert run.status is GitRefAuditRunStatus.pending
    assert run.date_finished is None
    assert run.summary is None


@pytest.mark.asyncio
async def test_create_raises_integrity_error_on_second_non_terminal(
    db_session: AsyncSession,
) -> None:
    """``create`` surfaces the partial-UQ violation as ``IntegrityError``."""
    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        await store.create()

    async def _create_again() -> None:
        async with db_session.begin():
            other = GitRefAuditRunStore(session=db_session, logger=_logger())
            await other.create()

    with pytest.raises(IntegrityError):
        await _create_again()


@pytest.mark.asyncio
async def test_has_non_terminal_run_lifecycle(
    db_session: AsyncSession,
) -> None:
    """has_non_terminal_run() flips false once the row finalises."""
    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        run = await store.create()
        assert await store.has_non_terminal_run() is True

    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        await store.transition_status(
            run_id=run.id, new_status=GitRefAuditRunStatus.succeeded
        )

    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        assert await store.has_non_terminal_run() is False


@pytest.mark.asyncio
async def test_transition_status_forward_progression(
    db_session: AsyncSession,
) -> None:
    """``pending → in_progress → partial_failure`` is the audit path."""
    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        run = await store.create()
        in_progress = await store.transition_status(
            run_id=run.id,
            new_status=GitRefAuditRunStatus.in_progress,
        )
        assert in_progress.status is GitRefAuditRunStatus.in_progress
        assert in_progress.date_finished is None

        terminal = await store.transition_status(
            run_id=run.id,
            new_status=GitRefAuditRunStatus.partial_failure,
        )
        assert terminal.status is GitRefAuditRunStatus.partial_failure
        assert terminal.date_finished is not None


@pytest.mark.asyncio
async def test_transition_status_rejects_backwards_move(
    db_session: AsyncSession,
) -> None:
    """Once terminal, a row cannot move back to in_progress."""
    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        run = await store.create()
        await store.transition_status(
            run_id=run.id,
            new_status=GitRefAuditRunStatus.succeeded,
        )

    async def _try_backwards() -> None:
        async with db_session.begin():
            store = GitRefAuditRunStore(session=db_session, logger=_logger())
            await store.transition_status(
                run_id=run.id,
                new_status=GitRefAuditRunStatus.in_progress,
            )

    with pytest.raises(InvalidJobStateError):
        await _try_backwards()


@pytest.mark.asyncio
async def test_missing_run_raises_job_not_found(
    db_session: AsyncSession,
) -> None:
    """A transition against a missing run id raises JobNotFoundError."""
    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        with pytest.raises(JobNotFoundError):
            await store.transition_status(
                run_id=99999,
                new_status=GitRefAuditRunStatus.in_progress,
            )


@pytest.mark.asyncio
async def test_set_summary_persists_jsonb(
    db_session: AsyncSession,
) -> None:
    """``set_summary`` writes the dict, ``get`` reads it back unchanged."""
    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        run = await store.create()
        await store.set_summary(
            run_id=run.id,
            summary={"orgs_enqueued": 3, "orgs_skipped": 1},
        )

    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        fetched = await store.get(run.id)

    assert fetched is not None
    assert fetched.summary == {"orgs_enqueued": 3, "orgs_skipped": 1}


@pytest.mark.asyncio
async def test_aggregate_activity_groups_jobs_by_status(
    db_session: AsyncSession,
) -> None:
    """``completed_with_errors`` buckets as failure (drives partial_failure).

    The git_ref_audit per-org worker transitions its row to
    ``completed_with_errors`` when one or more per-project fetches
    failed but the org's pass otherwise ran to completion; the
    aggregator must treat that bucket as a failure so the parent run
    finalises as ``partial_failure`` rather than ``succeeded``.
    """
    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        run = await store.create()
        all_statuses = [
            JobStatus.queued,
            JobStatus.in_progress,
            JobStatus.completed,
            JobStatus.failed,
            JobStatus.cancelled,
            JobStatus.completed_with_errors,
        ]
        for index, status in enumerate(all_statuses):
            row_org_id, row_org_slug = await _seed_org(
                db_session, slug=f"gar-bucket-{index}"
            )
            _seed_queue_job(
                db_session,
                org_id=row_org_id,
                run_id=run.id,
                status=status,
                subject_label=row_org_slug,
            )
        await db_session.commit()

    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        activity = await store.aggregate_activity(run_id=run.id)

    assert activity.pending_count == 2
    assert activity.succeeded_count == 1
    assert activity.failed_count == 3
    assert activity.total_count == 6


@pytest.mark.asyncio
async def test_aggregate_activity_null_when_no_jobs(
    db_session: AsyncSession,
) -> None:
    """An empty run has zero counters and no last-activity timestamp."""
    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        run = await store.create()
        await db_session.commit()

    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        activity = await store.aggregate_activity(run_id=run.id)

    assert activity.total_count == 0
    assert activity.date_last_activity is None


@pytest.mark.asyncio
async def test_aggregate_activity_picks_max_coalesced_timestamp(
    db_session: AsyncSession,
) -> None:
    """``date_last_activity`` is MAX(coalesce(completed, started, created))."""
    base = datetime(2026, 5, 26, 5, 17, tzinfo=UTC)
    async with db_session.begin():
        alpha_id, alpha_slug = await _seed_org(
            db_session, slug="gar-org-alpha"
        )
        beta_id, beta_slug = await _seed_org(db_session, slug="gar-org-beta")
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        run = await store.create()
        _seed_queue_job(
            db_session,
            org_id=alpha_id,
            run_id=run.id,
            status=JobStatus.queued,
            date_created=base,
            subject_label=alpha_slug,
        )
        latest = base + timedelta(minutes=30)
        _seed_queue_job(
            db_session,
            org_id=beta_id,
            run_id=run.id,
            status=JobStatus.completed,
            date_created=base,
            date_started=base + timedelta(minutes=5),
            date_completed=latest,
            subject_label=beta_slug,
        )
        await db_session.commit()

    async with db_session.begin():
        store = GitRefAuditRunStore(session=db_session, logger=_logger())
        activity = await store.aggregate_activity(run_id=run.id)

    assert activity.date_last_activity == latest
