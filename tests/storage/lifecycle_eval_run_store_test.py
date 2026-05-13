"""Tests for ``LifecycleEvalRunStore`` and lifecycle_eval constraints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import structlog
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import LifecycleEvalRunStatus, OrganizationCreate
from docverse.dbschema.lifecycle_eval_run import SqlLifecycleEvalRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.exceptions import InvalidJobStateError
from docverse.storage.lifecycle_eval_run_store import LifecycleEvalRunStore
from docverse.storage.organization_store import OrganizationStore


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(
    db_session: AsyncSession, *, slug: str = "ler-org"
) -> tuple[int, str]:
    org_store = OrganizationStore(session=db_session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"LER Org {slug}",
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
        kind=JobKind.lifecycle_eval.value,
        status=status.value,
        org_id=org_id,
        lifecycle_eval_run_id=run_id,
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
    """Two ``pending`` lifecycle_eval rows are rejected by the partial UQ."""
    async with db_session.begin():
        db_session.add(SqlLifecycleEvalRun(status="pending"))

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(SqlLifecycleEvalRun(status="pending"))


@pytest.mark.asyncio
async def test_partial_unique_index_blocks_pending_and_in_progress(
    db_session: AsyncSession,
) -> None:
    """An ``in_progress`` row blocks a second non-terminal row globally."""
    async with db_session.begin():
        db_session.add(SqlLifecycleEvalRun(status="in_progress"))

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(SqlLifecycleEvalRun(status="pending"))


@pytest.mark.asyncio
async def test_partial_unique_index_allows_terminal_alongside_pending(
    db_session: AsyncSession,
) -> None:
    """Terminal rows do not participate in the non-terminal uniqueness."""
    async with db_session.begin():
        db_session.add(SqlLifecycleEvalRun(status="succeeded"))
        db_session.add(SqlLifecycleEvalRun(status="failed"))
        db_session.add(SqlLifecycleEvalRun(status="partial_failure"))

    async with db_session.begin():
        db_session.add(SqlLifecycleEvalRun(status="pending"))


@pytest.mark.asyncio
async def test_lifecycle_eval_runs_status_check_rejects_invalid(
    db_session: AsyncSession,
) -> None:
    """A status outside the allowed set fails the CHECK constraint."""
    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(SqlLifecycleEvalRun(status="pendng"))


@pytest.mark.asyncio
async def test_queue_jobs_lifecycle_eval_per_org_mutex(
    db_session: AsyncSession,
) -> None:
    """Two active ``lifecycle_eval`` queue_jobs for one org collide."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        db_session.add(
            SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.lifecycle_eval.value,
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
                    kind=JobKind.lifecycle_eval.value,
                    status=JobStatus.in_progress.value,
                    org_id=org_id,
                    subject_label=org_slug,
                )
            )


@pytest.mark.asyncio
async def test_queue_jobs_lifecycle_eval_mutex_ignores_subject_label(
    db_session: AsyncSession,
) -> None:
    """Mutex is keyed on ``org_id`` alone — divergent labels still collide.

    The previous mutex shape included ``subject_label``; the index is
    now single-column on ``org_id``. This test pins that property so a
    future revision cannot silently restore ``subject_label`` to the
    index without flagging itself here.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(db_session)
        db_session.add(
            SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.lifecycle_eval.value,
                status=JobStatus.queued.value,
                org_id=org_id,
                subject_label="ler-org",
            )
        )

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(
                SqlQueueJob(
                    public_id=validate_base32_id(generate_base32_id()),
                    kind=JobKind.lifecycle_eval.value,
                    status=JobStatus.in_progress.value,
                    org_id=org_id,
                    subject_label="some-other-label",
                )
            )


@pytest.mark.asyncio
async def test_queue_jobs_lifecycle_eval_mutex_terminal_alongside_active(
    db_session: AsyncSession,
) -> None:
    """A terminal sibling does not block a fresh active row for the org."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        db_session.add(
            SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.lifecycle_eval.value,
                status=JobStatus.completed.value,
                org_id=org_id,
                subject_label=org_slug,
            )
        )
        db_session.add(
            SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.lifecycle_eval.value,
                status=JobStatus.failed.value,
                org_id=org_id,
                subject_label=org_slug,
            )
        )

    async with db_session.begin():
        db_session.add(
            SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.lifecycle_eval.value,
                status=JobStatus.queued.value,
                org_id=org_id,
                subject_label=org_slug,
            )
        )


@pytest.mark.asyncio
async def test_queue_jobs_lifecycle_eval_mutex_distinct_orgs_coexist(
    db_session: AsyncSession,
) -> None:
    """Two distinct orgs may each hold an active lifecycle_eval row."""
    async with db_session.begin():
        first_org, first_slug = await _seed_org(db_session, slug="ler-org-a")
        second_org, second_slug = await _seed_org(db_session, slug="ler-org-b")
        db_session.add(
            SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.lifecycle_eval.value,
                status=JobStatus.queued.value,
                org_id=first_org,
                subject_label=first_slug,
            )
        )
        db_session.add(
            SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.lifecycle_eval.value,
                status=JobStatus.queued.value,
                org_id=second_org,
                subject_label=second_slug,
            )
        )


@pytest.mark.asyncio
async def test_create_returns_pending_run(
    db_session: AsyncSession,
) -> None:
    """``create`` inserts a pending row and returns the domain model."""
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        run = await store.create()

    assert run.id > 0
    assert run.status is LifecycleEvalRunStatus.pending
    assert run.date_finished is None
    assert run.summary is None


@pytest.mark.asyncio
async def test_create_raises_integrity_error_on_second_non_terminal(
    db_session: AsyncSession,
) -> None:
    """``create`` surfaces the partial-UQ violation as ``IntegrityError``.

    The store does not translate the error; callers (the dispatcher
    pre-check + handler) own the policy for whether a concurrent tick
    is a 409 or a clean skip.
    """
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        await store.create()

    async def _create_again() -> None:
        async with db_session.begin():
            other = LifecycleEvalRunStore(session=db_session, logger=_logger())
            await other.create()

    with pytest.raises(IntegrityError):
        await _create_again()


@pytest.mark.asyncio
async def test_get_returns_none_when_missing(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        result = await store.get(99999)

    assert result is None


@pytest.mark.asyncio
async def test_get_returns_persisted_run(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        created = await store.create()

    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        fetched = await store.get(created.id)

    assert fetched is not None
    assert fetched.id == created.id
    assert fetched.status is LifecycleEvalRunStatus.pending


@pytest.mark.asyncio
async def test_has_non_terminal_run_true_for_pending(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        await store.create()

    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        assert await store.has_non_terminal_run() is True


@pytest.mark.asyncio
async def test_has_non_terminal_run_false_when_only_terminal(
    db_session: AsyncSession,
) -> None:
    """A terminal row alone is not non-terminal — singleton check is honest."""
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        run = await store.create()
        await store.transition_status(
            run_id=run.id,
            new_status=LifecycleEvalRunStatus.succeeded,
        )

    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        assert await store.has_non_terminal_run() is False


@pytest.mark.asyncio
async def test_has_non_terminal_run_false_when_empty(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        assert await store.has_non_terminal_run() is False


@pytest.mark.asyncio
async def test_transition_status_forward_progression(
    db_session: AsyncSession,
) -> None:
    """``pending → in_progress → succeeded`` is the dispatcher path."""
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        run = await store.create()
        in_progress = await store.transition_status(
            run_id=run.id,
            new_status=LifecycleEvalRunStatus.in_progress,
        )
        assert in_progress.status is LifecycleEvalRunStatus.in_progress
        assert in_progress.date_finished is None

        succeeded = await store.transition_status(
            run_id=run.id,
            new_status=LifecycleEvalRunStatus.succeeded,
        )
        assert succeeded.status is LifecycleEvalRunStatus.succeeded
        assert succeeded.date_finished is not None


@pytest.mark.asyncio
async def test_transition_status_idempotent(
    db_session: AsyncSession,
) -> None:
    """Same-status transition is a no-op, not an error."""
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        run = await store.create()
        same = await store.transition_status(
            run_id=run.id,
            new_status=LifecycleEvalRunStatus.pending,
        )

    assert same.status is LifecycleEvalRunStatus.pending


@pytest.mark.asyncio
async def test_transition_status_rejects_backwards_move(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        run = await store.create()
        await store.transition_status(
            run_id=run.id,
            new_status=LifecycleEvalRunStatus.succeeded,
        )

    async def _try_backwards() -> None:
        async with db_session.begin():
            store = LifecycleEvalRunStore(session=db_session, logger=_logger())
            await store.transition_status(
                run_id=run.id,
                new_status=LifecycleEvalRunStatus.in_progress,
            )

    with pytest.raises(InvalidJobStateError):
        await _try_backwards()


@pytest.mark.asyncio
async def test_transition_status_pending_can_go_terminal(
    db_session: AsyncSession,
) -> None:
    """``pending`` may transition directly to any terminal state.

    The dispatcher pre-flight finds zero orgs to enqueue and finalises
    the run as ``succeeded`` without ever transitioning through
    ``in_progress``; this path must be permitted.
    """
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        run = await store.create()
        terminal = await store.transition_status(
            run_id=run.id,
            new_status=LifecycleEvalRunStatus.succeeded,
        )

    assert terminal.status is LifecycleEvalRunStatus.succeeded
    assert terminal.date_finished is not None


@pytest.mark.asyncio
async def test_transition_status_raises_when_missing(
    db_session: AsyncSession,
) -> None:
    async def _try_transition_missing() -> None:
        async with db_session.begin():
            store = LifecycleEvalRunStore(session=db_session, logger=_logger())
            await store.transition_status(
                run_id=99999,
                new_status=LifecycleEvalRunStatus.in_progress,
            )

    with pytest.raises(InvalidJobStateError):
        await _try_transition_missing()


@pytest.mark.asyncio
async def test_aggregate_activity_groups_jobs_by_status(
    db_session: AsyncSession,
) -> None:
    """Counters bucket queued/in_progress as pending and completed as success.

    Mirrors ``KeeperSyncRunStore.aggregate_activity`` bucketing:
    ``completed_with_errors`` is failure, not success, so soft-failure
    distinguishes from clean success.
    """
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
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
            # One org per row so the per-``org_id`` active-status
            # mutex does not block the two non-terminal rows from
            # coexisting. The test exercises the aggregator's
            # bucketing, not the mutex.
            row_org_id, row_org_slug = await _seed_org(
                db_session, slug=f"ler-org-bucket-{index}"
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
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        activity = await store.aggregate_activity(run_id=run.id)

    assert activity.pending_count == 2
    assert activity.succeeded_count == 1
    assert activity.failed_count == 3
    assert activity.total_count == 6


@pytest.mark.asyncio
async def test_aggregate_activity_null_when_no_jobs(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        run = await store.create()
        await db_session.commit()

    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        activity = await store.aggregate_activity(run_id=run.id)

    assert activity.total_count == 0
    assert activity.date_last_activity is None


@pytest.mark.asyncio
async def test_aggregate_activity_picks_max_coalesced_timestamp(
    db_session: AsyncSession,
) -> None:
    """``date_last_activity`` is MAX(coalesce(completed, started, created))."""
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    async with db_session.begin():
        # One org per row so the two non-terminal rows (queued +
        # in_progress) do not collide on the per-``org_id`` mutex;
        # the test exercises the timestamp coalescer, not the mutex.
        alpha_id, alpha_slug = await _seed_org(
            db_session, slug="ler-org-alpha"
        )
        beta_id, beta_slug = await _seed_org(db_session, slug="ler-org-beta")
        gamma_id, gamma_slug = await _seed_org(
            db_session, slug="ler-org-gamma"
        )
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        run = await store.create()
        _seed_queue_job(
            db_session,
            org_id=alpha_id,
            run_id=run.id,
            status=JobStatus.queued,
            date_created=base,
            subject_label=alpha_slug,
        )
        _seed_queue_job(
            db_session,
            org_id=beta_id,
            run_id=run.id,
            status=JobStatus.in_progress,
            date_created=base,
            date_started=base + timedelta(minutes=10),
            subject_label=beta_slug,
        )
        latest = base + timedelta(minutes=30)
        _seed_queue_job(
            db_session,
            org_id=gamma_id,
            run_id=run.id,
            status=JobStatus.completed,
            date_created=base,
            date_started=base + timedelta(minutes=5),
            date_completed=latest,
            subject_label=gamma_slug,
        )
        await db_session.commit()

    async with db_session.begin():
        store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        activity = await store.aggregate_activity(run_id=run.id)

    assert activity.date_last_activity == latest
