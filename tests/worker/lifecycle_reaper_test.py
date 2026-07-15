"""Tests for the ``lifecycle_reaper`` cron worker function.

Mirrors :mod:`tests.worker.keeper_sync_reaper_test` for the
lifecycle_eval **and** git_ref_audit subsystems. The reaper is the
cron-driven backstop for the case where arq itself loses a per-org
job — a worker pod OOM-killed mid-job that never gets to surface a
timeout, or a dispatcher that crashed between the ``queue_jobs`` SQL
commit and ``arq_queue.enqueue``. It marks any silently-stuck child
as ``failed``, sweeps orphan queued rows the dispatcher never
finished enqueueing, and finalises the parent run (either
``lifecycle_eval_runs`` or ``git_ref_audit_runs``) so an operator
never sees a stuck run.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    GitRefAuditRunStatus,
    JobKind,
    LifecycleEvalRunStatus,
    OrganizationCreate,
)
from docverse.config import config as runtime_config
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobStatus
from docverse.storage.git_ref_audit_run_store import GitRefAuditRunStore
from docverse.storage.lifecycle_eval_run_store import LifecycleEvalRunStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.lifecycle_reaper import lifecycle_reaper
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(db_session: AsyncSession, *, slug: str) -> tuple[int, str]:
    org_store = OrganizationStore(session=db_session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"LCR Org {slug}",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id, org.slug


async def _seed_run(
    db_session: AsyncSession,
    *,
    transition_to_in_progress: bool = True,
) -> int:
    """Create a ``lifecycle_eval_runs`` row, optionally bumped to in_progress.

    The reaper acts on rows whose parent run is in any non-terminal
    state; the dispatcher transitions ``pending → in_progress`` atomically
    with its first child enqueue, so the realistic seed for a stuck
    child is a run that has already moved to ``in_progress``.
    """
    run_store = LifecycleEvalRunStore(session=db_session, logger=_logger())
    run = await run_store.create()
    if transition_to_in_progress:
        await run_store.transition_status(
            run_id=run.id, new_status=LifecycleEvalRunStatus.in_progress
        )
    return run.id


async def _seed_silent_child(
    db_session: AsyncSession,
    *,
    org_id: int,
    org_slug: str,
    run_id: int,
    backend_job_id: str,
    started_minutes_ago: int,
) -> int:
    """Insert a ``kind='lifecycle_eval'`` row stuck in ``in_progress``.

    Goes via direct ``SqlQueueJob`` insertion rather than
    ``QueueJobStore.create`` because the store's ``create`` signature
    does not expose ``lifecycle_eval_run_id`` (the dispatcher sibling
    task will add that — for now the reaper test mirrors the test-seed
    pattern already used in
    :mod:`tests.worker.lifecycle_eval_test`).
    """
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=backend_job_id,
        kind=JobKind.lifecycle_eval.value,
        status=JobStatus.in_progress.value,
        org_id=org_id,
        lifecycle_eval_run_id=run_id,
        subject_label=org_slug,
        date_started=(
            datetime.now(tz=UTC) - timedelta(minutes=started_minutes_ago)
        ),
    )
    db_session.add(row)
    await db_session.flush()
    await db_session.refresh(row)
    return row.id


async def _seed_orphan_child(
    db_session: AsyncSession,
    *,
    org_id: int,
    org_slug: str,
    run_id: int,
    created_minutes_ago: int,
) -> int:
    """Insert a ``kind='lifecycle_eval'`` orphan: queued, no backend_job_id."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=None,
        kind=JobKind.lifecycle_eval.value,
        status=JobStatus.queued.value,
        org_id=org_id,
        lifecycle_eval_run_id=run_id,
        subject_label=org_slug,
    )
    db_session.add(row)
    await db_session.flush()
    # ``date_created`` defaults to ``func.now()`` at the DB layer; backdate
    # it on the row after flush so the reaper's ``date_created < cutoff``
    # filter actually matches.
    row.date_created = datetime.now(tz=UTC) - timedelta(
        minutes=created_minutes_ago
    )
    await db_session.flush()
    return row.id


def _make_ctx(http_client: httpx.AsyncClient) -> dict[str, Any]:
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    return make_worker_ctx(http_client=http_client, arq_queue=mock_arq)


@pytest.mark.asyncio
async def test_reaper_fails_stuck_in_progress_and_finalises_run(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A stuck ``in_progress`` child rolls its parent to partial_failure."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="lcr-1")
        run_id = await _seed_run(db_session)
        stuck_id = await _seed_silent_child(
            db_session,
            org_id=org_id,
            org_slug=org_slug,
            run_id=run_id,
            backend_job_id="arq-stuck-1",
            started_minutes_ago=600,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await lifecycle_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(stuck_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert qj.errors["type"] == "SilentWorker"
            assert qj.date_completed is not None

            run_store = LifecycleEvalRunStore(
                session=session, logger=_logger()
            )
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status is LifecycleEvalRunStatus.partial_failure
            assert run.date_finished is not None


@pytest.mark.asyncio
async def test_reaper_skips_recent_in_progress_rows(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A child within the idle window leaves its parent run untouched."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="lcr-2")
        run_id = await _seed_run(db_session)
        # Started just now — well within any sane idle window.
        fresh_id = await _seed_silent_child(
            db_session,
            org_id=org_id,
            org_slug=org_slug,
            run_id=run_id,
            backend_job_id="arq-fresh",
            started_minutes_ago=0,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await lifecycle_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(fresh_id)
            assert qj is not None
            assert qj.status == JobStatus.in_progress

            run_store = LifecycleEvalRunStore(
                session=session, logger=_logger()
            )
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status is LifecycleEvalRunStatus.in_progress


@pytest.mark.asyncio
async def test_reaper_sweeps_orphan_queued_rows(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A ``queued`` orphan past the idle window is reaped to ``failed``.

    Models the dispatcher crash window: the per-org ``queue_jobs`` row
    is committed before ``arq_queue.enqueue`` is called, so a worker
    crash between those two operations leaves a row that no arq job
    will ever pick up. Without the orphan sweep the per-org mutex
    keeps observing the stale row and the next dispatcher tick can't
    enqueue fresh work for that org.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="lcr-3")
        run_id = await _seed_run(db_session)
        orphan_id = await _seed_orphan_child(
            db_session,
            org_id=org_id,
            org_slug=org_slug,
            run_id=run_id,
            created_minutes_ago=10,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await lifecycle_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(orphan_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert qj.errors["type"] == "OrphanedQueueJob"

            # Per-org mutex unblocked for this org: a fresh dispatcher
            # tick can now enqueue another lifecycle_eval row.
            active = await qj_store.has_active_for_subject(
                org_id=org_id,
                kind=JobKind.lifecycle_eval,
                subject_label=org_slug,
            )
            assert active is False

            run_store = LifecycleEvalRunStore(
                session=session, logger=_logger()
            )
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status is LifecycleEvalRunStatus.partial_failure


@pytest.mark.asyncio
async def test_reaper_finalises_each_distinct_parent_run(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Stuck children on multiple runs each trigger their own finalisation.

    The reaper collects ``lifecycle_eval_run_id`` across every reaped
    row and calls ``maybe_finalise_lifecycle_run`` once per distinct
    parent. Two separate non-terminal runs cannot coexist (the partial
    unique index on ``lifecycle_eval_runs`` forbids it), so we drive
    one stuck child through the reaper, observe its parent rolling to
    a terminal state, then seed a second run + stuck child and observe
    the same outcome — verifying the reaper does not lazily memoise
    the first run's id across invocations.
    """
    async with db_session.begin():
        org_a_id, org_a_slug = await _seed_org(db_session, slug="lcr-4a")
        run_a_id = await _seed_run(db_session)
        await _seed_silent_child(
            db_session,
            org_id=org_a_id,
            org_slug=org_a_slug,
            run_id=run_a_id,
            backend_job_id="arq-stuck-a",
            started_minutes_ago=600,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await lifecycle_reaper(ctx)

        async for session in db_session_dependency():
            async with session.begin():
                run_store = LifecycleEvalRunStore(
                    session=session, logger=_logger()
                )
                run_a = await run_store.get(run_a_id)
                assert run_a is not None
                assert run_a.status is LifecycleEvalRunStatus.partial_failure

        async for session in db_session_dependency():
            async with session.begin():
                org_b_id, org_b_slug = await _seed_org(session, slug="lcr-4b")
                run_b_id = await _seed_run(session)
                await _seed_silent_child(
                    session,
                    org_id=org_b_id,
                    org_slug=org_b_slug,
                    run_id=run_b_id,
                    backend_job_id="arq-stuck-b",
                    started_minutes_ago=600,
                )

        await lifecycle_reaper(ctx)

        async for session in db_session_dependency():
            async with session.begin():
                run_store = LifecycleEvalRunStore(
                    session=session, logger=_logger()
                )
                run_b = await run_store.get(run_b_id)
                assert run_b is not None
                assert run_b.status is LifecycleEvalRunStatus.partial_failure
    finally:
        await ctx["http_client"].aclose()


@pytest.mark.asyncio
async def test_reaper_no_op_when_nothing_stuck(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A clean tick with nothing to reap returns ``completed`` without error.

    The dispatcher's empty-orgs branch and the steady-state happy path
    both produce ticks where the reaper finds zero candidate rows.
    The function must return cleanly with no side effects.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="lcr-5")
        run_id = await _seed_run(db_session)
        # One child within the idle window — nothing to reap.
        fresh_id = await _seed_silent_child(
            db_session,
            org_id=org_id,
            org_slug=org_slug,
            run_id=run_id,
            backend_job_id="arq-clean",
            started_minutes_ago=0,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await lifecycle_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(fresh_id)
            assert qj is not None
            assert qj.status == JobStatus.in_progress


@pytest.mark.asyncio
async def test_reaper_threshold_is_configurable(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reducing the threshold shrinks the silent window.

    Operators in non-prod set ``DOCVERSE_LIFECYCLE_REAPER_THRESHOLD_
    SECONDS`` to a small value so a deliberately-wedged job surfaces
    in seconds rather than the production-default 6 hours. The reaper
    must observe the configured value at invocation time rather than
    a baked-in default.
    """
    monkeypatch.setattr(
        runtime_config, "lifecycle_reaper_threshold_seconds", 60
    )

    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="lcr-6")
        run_id = await _seed_run(db_session)
        stuck_id = await _seed_silent_child(
            db_session,
            org_id=org_id,
            org_slug=org_slug,
            run_id=run_id,
            backend_job_id="arq-shortwindow",
            started_minutes_ago=5,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await lifecycle_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(stuck_id)
            assert qj is not None
            assert qj.status == JobStatus.failed


# -- git_ref_audit reaper coverage --------------------
#
# The reaper is shared across the two lifecycle subsystems. The
# helpers below mirror the lifecycle_eval seed/probe shapes but
# target ``kind='git_ref_audit'`` and the ``git_ref_audit_runs``
# parent table.


async def _seed_git_ref_audit_run(
    db_session: AsyncSession,
    *,
    transition_to_in_progress: bool = True,
) -> int:
    """Create a ``git_ref_audit_runs`` row, optionally in_progress.

    Sibling of :func:`_seed_run` for the daily audit subsystem.
    """
    run_store = GitRefAuditRunStore(session=db_session, logger=_logger())
    run = await run_store.create()
    if transition_to_in_progress:
        await run_store.transition_status(
            run_id=run.id, new_status=GitRefAuditRunStatus.in_progress
        )
    return run.id


async def _seed_silent_git_ref_audit_child(
    db_session: AsyncSession,
    *,
    org_id: int,
    org_slug: str,
    run_id: int,
    backend_job_id: str,
    started_minutes_ago: int,
) -> int:
    """Insert a ``kind='git_ref_audit'`` row stuck in ``in_progress``."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=backend_job_id,
        kind=JobKind.git_ref_audit.value,
        status=JobStatus.in_progress.value,
        org_id=org_id,
        git_ref_audit_run_id=run_id,
        subject_label=org_slug,
        date_started=(
            datetime.now(tz=UTC) - timedelta(minutes=started_minutes_ago)
        ),
    )
    db_session.add(row)
    await db_session.flush()
    await db_session.refresh(row)
    return row.id


async def _seed_orphan_git_ref_audit_child(
    db_session: AsyncSession,
    *,
    org_id: int,
    org_slug: str,
    run_id: int,
    created_minutes_ago: int,
) -> int:
    """Insert a ``kind='git_ref_audit'`` orphan: queued, no backend_job_id."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=None,
        kind=JobKind.git_ref_audit.value,
        status=JobStatus.queued.value,
        org_id=org_id,
        git_ref_audit_run_id=run_id,
        subject_label=org_slug,
    )
    db_session.add(row)
    await db_session.flush()
    row.date_created = datetime.now(tz=UTC) - timedelta(
        minutes=created_minutes_ago
    )
    await db_session.flush()
    return row.id


@pytest.mark.asyncio
async def test_reaper_fails_stuck_git_ref_audit_and_finalises_run(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A stuck audit ``in_progress`` child rolls its parent run terminal."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="lcr-ga-1")
        run_id = await _seed_git_ref_audit_run(db_session)
        stuck_id = await _seed_silent_git_ref_audit_child(
            db_session,
            org_id=org_id,
            org_slug=org_slug,
            run_id=run_id,
            backend_job_id="arq-stuck-audit-1",
            started_minutes_ago=600,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await lifecycle_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(stuck_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert qj.errors["type"] == "SilentWorker"

            run_store = GitRefAuditRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status is GitRefAuditRunStatus.partial_failure
            assert run.date_finished is not None


@pytest.mark.asyncio
async def test_reaper_sweeps_orphan_git_ref_audit_queued_rows(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A queued audit orphan past the idle window is reaped to ``failed``.

    Acceptance criterion: the reaper "fails a stuck git_ref_audit
    per-org row past the threshold and finalises its parent run".
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="lcr-ga-2")
        run_id = await _seed_git_ref_audit_run(db_session)
        orphan_id = await _seed_orphan_git_ref_audit_child(
            db_session,
            org_id=org_id,
            org_slug=org_slug,
            run_id=run_id,
            created_minutes_ago=10,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await lifecycle_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(orphan_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert qj.errors["type"] == "OrphanedQueueJob"

            active = await qj_store.has_active_for_subject(
                org_id=org_id,
                kind=JobKind.git_ref_audit,
                subject_label=org_slug,
            )
            assert active is False

            run_store = GitRefAuditRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status is GitRefAuditRunStatus.partial_failure


@pytest.mark.asyncio
async def test_reaper_sweeps_both_subsystems_in_one_tick(
    app: None,
    db_session: AsyncSession,
) -> None:
    """One reaper tick finalises stuck rows in both subsystems together.

    Verifies the lifecycle_eval and git_ref_audit sweeps share one
    transaction: a stuck child in each subsystem rolls its respective
    parent to ``partial_failure`` on the same invocation.
    """
    async with db_session.begin():
        org_a_id, org_a_slug = await _seed_org(db_session, slug="lcr-both-le")
        org_b_id, org_b_slug = await _seed_org(
            db_session, slug="lcr-both-audit"
        )
        le_run_id = await _seed_run(db_session)
        await _seed_silent_child(
            db_session,
            org_id=org_a_id,
            org_slug=org_a_slug,
            run_id=le_run_id,
            backend_job_id="arq-stuck-le",
            started_minutes_ago=600,
        )
        audit_run_id = await _seed_git_ref_audit_run(db_session)
        await _seed_silent_git_ref_audit_child(
            db_session,
            org_id=org_b_id,
            org_slug=org_b_slug,
            run_id=audit_run_id,
            backend_job_id="arq-stuck-audit-2",
            started_minutes_ago=600,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await lifecycle_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            le_run_store = LifecycleEvalRunStore(
                session=session, logger=_logger()
            )
            le_run = await le_run_store.get(le_run_id)
            assert le_run is not None
            assert le_run.status is LifecycleEvalRunStatus.partial_failure

            audit_run_store = GitRefAuditRunStore(
                session=session, logger=_logger()
            )
            audit_run = await audit_run_store.get(audit_run_id)
            assert audit_run is not None
            assert audit_run.status is GitRefAuditRunStatus.partial_failure
