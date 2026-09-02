"""Tests for the four run-less reaper cron worker functions.

Covers ``dashboard_build_reaper``, ``publish_edition_reaper``,
``build_processing_reaper``, and ``dashboard_sync_reaper`` — the
cron-driven backstops for the case where arq itself loses a queue
job (worker pod OOM-killed mid-job that never gets to surface a
timeout, or dispatcher crashed between the ``queue_jobs`` SQL commit
and ``arq_queue.enqueue``). Each reaper marks any silently-stuck row
as ``failed``, sweeps orphan queued rows, and — per PRD #538 — fails
``queued`` rows that reached arq and were then lost by it, verified
against the queue backend first.

Parametrized over :data:`RUNLESS_REAPER_SPECS` so the same behaviors
are exercised against every run-less kind without copy-pasted per-kind
test modules. pytest IDs like ``[dashboard_build]`` keep per-kind
failure attribution. The end-to-end unwedge test is ``dashboard_build``
only, since that is the kind whose mutex actually froze a production
dashboard.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import BuildStatus, OrganizationCreate
from docverse_server.config import config as runtime_config
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import (
    generate_base32_id,
    serialize_base32_id,
    validate_base32_id,
)
from docverse_server.domain.queue import JobKind, JobStatus
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.build_processing_reaper import (
    build_processing_reaper,
)
from docverse_server.worker.functions.dashboard_build_reaper import (
    dashboard_build_reaper,
)
from docverse_server.worker.functions.dashboard_sync_reaper import (
    dashboard_sync_reaper,
)
from docverse_server.worker.functions.publish_edition_reaper import (
    publish_edition_reaper,
)
from tests.worker.conftest import make_worker_ctx


@dataclass(frozen=True)
class ReaperSpec:
    """One row per run-less reaper for parametrized worker tests."""

    name: str
    reaper: Callable[[dict[str, Any]], Awaitable[str]]
    kind: JobKind
    threshold_attr: str
    well_past_minutes: int
    slug_prefix: str


RUNLESS_REAPER_SPECS: list[ReaperSpec] = [
    ReaperSpec(
        name="build_processing",
        reaper=build_processing_reaper,
        kind=JobKind.build_processing,
        threshold_attr="build_processing_reaper_threshold_seconds",
        well_past_minutes=600,
        slug_prefix="bpr",
    ),
    ReaperSpec(
        name="dashboard_build",
        reaper=dashboard_build_reaper,
        kind=JobKind.dashboard_build,
        threshold_attr="dashboard_build_reaper_threshold_seconds",
        well_past_minutes=60,
        slug_prefix="dbr",
    ),
    ReaperSpec(
        name="publish_edition",
        reaper=publish_edition_reaper,
        kind=JobKind.publish_edition,
        threshold_attr="publish_edition_reaper_threshold_seconds",
        well_past_minutes=300,
        slug_prefix="per",
    ),
    ReaperSpec(
        name="dashboard_sync",
        reaper=dashboard_sync_reaper,
        kind=JobKind.dashboard_sync,
        threshold_attr="dashboard_sync_reaper_threshold_seconds",
        well_past_minutes=480,
        slug_prefix="dsr",
    ),
]


_reaper_param = pytest.mark.parametrize(
    "spec",
    RUNLESS_REAPER_SPECS,
    ids=lambda s: s.name,
)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(db_session: AsyncSession, *, slug: str) -> int:
    org_store = OrganizationStore(session=db_session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"Reaper Org {slug}",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


async def _seed_silent_row(
    db_session: AsyncSession,
    *,
    kind: JobKind,
    org_id: int,
    backend_job_id: str,
    started_minutes_ago: int,
    project_id: int | None = None,
    build_id: int | None = None,
) -> int:
    """Insert one row of ``kind`` stuck in ``in_progress``."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=backend_job_id,
        kind=kind.value,
        status=JobStatus.in_progress.value,
        org_id=org_id,
        project_id=project_id,
        build_id=build_id,
        date_started=(
            datetime.now(tz=UTC) - timedelta(minutes=started_minutes_ago)
        ),
    )
    db_session.add(row)
    await db_session.flush()
    await db_session.refresh(row)
    return row.id


async def _seed_orphan_row(
    db_session: AsyncSession,
    *,
    kind: JobKind,
    org_id: int,
    created_minutes_ago: int,
    project_id: int | None = None,
) -> int:
    """Insert one orphan row (queued, no backend_job_id) of ``kind``."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=None,
        kind=kind.value,
        status=JobStatus.queued.value,
        org_id=org_id,
        project_id=project_id,
    )
    db_session.add(row)
    await db_session.flush()
    row.date_created = datetime.now(tz=UTC) - timedelta(
        minutes=created_minutes_ago
    )
    await db_session.flush()
    return row.id


async def _seed_abandoned_row(
    db_session: AsyncSession,
    *,
    kind: JobKind,
    org_id: int,
    backend_job_id: str,
    created_minutes_ago: int,
    project_id: int | None = None,
) -> int:
    """Insert one abandoned row: queued, has an arq ID, past the window."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=backend_job_id,
        kind=kind.value,
        status=JobStatus.queued.value,
        org_id=org_id,
        project_id=project_id,
    )
    db_session.add(row)
    await db_session.flush()
    row.date_created = datetime.now(tz=UTC) - timedelta(
        minutes=created_minutes_ago
    )
    await db_session.flush()
    return row.id


async def _seed_processing_build(
    db_session: AsyncSession,
    *,
    project_id: int,
    uploaded_minutes_ago: int,
) -> tuple[int, str]:
    """Insert one build sitting in ``processing`` for the given age.

    Returns the row id and the base32 public id the reaper's warning
    payload is expected to carry.
    """
    public_id = validate_base32_id(generate_base32_id())
    base32 = serialize_base32_id(public_id)
    row = SqlBuild(
        public_id=public_id,
        project_id=project_id,
        git_ref="main",
        content_hash="sha256:" + "0" * 64,
        status=BuildStatus.processing,
        staging_key=f"__staging/{base32}.tar.gz",
        storage_prefix=f"reaper-proj/__builds/{base32}/",
        uploader="reaper-test",
        date_uploaded=(
            datetime.now(tz=UTC) - timedelta(minutes=uploaded_minutes_ago)
        ),
    )
    db_session.add(row)
    await db_session.flush()
    await db_session.refresh(row)
    return row.id, base32


async def _read_build_status(build_id: int) -> BuildStatus:
    """Read one build's status back through a fresh session."""
    async for session in db_session_dependency():
        async with session.begin():
            store = BuildStore(session=session, logger=_logger())
            build = await store.get_by_id(build_id)
            assert build is not None
            return build.status
    raise AssertionError("No database session available")


def _make_ctx(http_client: httpx.AsyncClient) -> dict[str, Any]:
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    return make_worker_ctx(http_client=http_client, arq_queue=mock_arq)


@pytest.mark.asyncio
@_reaper_param
async def test_reaper_fails_stuck_in_progress(
    app: None,
    db_session: AsyncSession,
    spec: ReaperSpec,
) -> None:
    """A stuck ``in_progress`` row is reaped to ``failed`` for each kind."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug=f"{spec.slug_prefix}-1")
        stuck_id = await _seed_silent_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-stuck-1",
            started_minutes_ago=spec.well_past_minutes,
            project_id=101,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await spec.reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(stuck_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert qj.errors["type"] == "SilentWorker"
            assert qj.date_completed is not None


@pytest.mark.asyncio
@_reaper_param
async def test_reaper_sweeps_orphan_queued_row(
    app: None,
    db_session: AsyncSession,
    spec: ReaperSpec,
) -> None:
    """A ``queued`` orphan past the idle window is reaped to ``failed``.

    Models the dispatcher crash window: the ``queue_jobs`` row is
    committed before ``arq_queue.enqueue`` is called, so a crash
    between those two operations leaves a row that no arq job will
    ever pick up. Without the orphan sweep the per-kind active mutex
    keeps the row visible forever.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug=f"{spec.slug_prefix}-2")
        orphan_id = await _seed_orphan_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            created_minutes_ago=10,
            project_id=202,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await spec.reaper(ctx)
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


@pytest.mark.asyncio
@_reaper_param
async def test_reaper_no_op_logs_debug_not_warning(
    app: None,
    db_session: AsyncSession,
    spec: ReaperSpec,
) -> None:
    """A clean tick with nothing to reap logs ``debug``, not ``warning``.

    A healthy steady-state system must not flood ``warning``-level
    logs every 30 minutes when the reaper finds zero candidate rows
    — PRD #367 user story 16.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug=f"{spec.slug_prefix}-3")
        # Fresh in_progress within idle window — must not be reaped.
        fresh_id = await _seed_silent_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-clean",
            started_minutes_ago=0,
            project_id=303,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            result = await spec.reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    assert result == "completed"

    reaper_events = [
        entry
        for entry in captured
        if spec.kind.value in str(entry.get("event", ""))
    ]
    warnings = [
        entry for entry in reaper_events if entry.get("log_level") == "warning"
    ]
    debugs = [
        entry for entry in reaper_events if entry.get("log_level") == "debug"
    ]
    assert warnings == []
    assert len(debugs) == 1
    # Pin the literal so log dashboards keying off the exact event
    # string keep working after the refactor.
    assert debugs[0]["event"] == (
        f"No stuck {spec.kind.value} queue jobs to reap"
    )

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(fresh_id)
            assert qj is not None
            assert qj.status == JobStatus.in_progress


@pytest.mark.asyncio
@_reaper_param
async def test_reaper_warning_includes_count_and_public_ids(
    app: None,
    db_session: AsyncSession,
    spec: ReaperSpec,
) -> None:
    """Reaping > 0 rows emits one ``warning`` with count + public IDs.

    PRD #367 user story 8: operators need a structured ``warning``
    log line whenever a reaper sweep actually reaps something, so
    reaper activity can be correlated with the underlying incident
    in logs without scanning the database.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug=f"{spec.slug_prefix}-4")
        silent_id = await _seed_silent_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-stuck-warn",
            started_minutes_ago=spec.well_past_minutes,
            project_id=404,
        )
        orphan_id = await _seed_orphan_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            created_minutes_ago=10,
            project_id=405,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            await spec.reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    warnings = [
        entry
        for entry in captured
        if entry.get("log_level") == "warning"
        and spec.kind.value in str(entry.get("event", ""))
    ]
    assert len(warnings) == 1
    # Pin the literal so log dashboards keying off the exact event
    # string keep working after the refactor.
    assert warnings[0]["event"] == f"Reaped stuck {spec.kind.value} queue jobs"
    assert warnings[0]["reaped_count"] == 2
    assert warnings[0]["silent_count"] == 1
    assert warnings[0]["orphan_count"] == 1

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            silent_qj = await qj_store.get(silent_id)
            orphan_qj = await qj_store.get(orphan_id)
            assert silent_qj is not None
            assert orphan_qj is not None
            assert {
                serialize_base32_id(silent_qj.public_id),
                serialize_base32_id(orphan_qj.public_id),
            } == set(warnings[0]["reaped_public_ids"])


@pytest.mark.asyncio
@_reaper_param
async def test_reaper_threshold_is_configurable(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    spec: ReaperSpec,
) -> None:
    """Reducing the threshold shrinks the silent window.

    Operators in non-prod set
    ``DOCVERSE_<KIND>_REAPER_THRESHOLD_SECONDS`` to a small value so a
    deliberately-wedged job surfaces in seconds rather than the
    production default. The reaper must observe the configured value
    at invocation time rather than a baked-in default.
    """
    monkeypatch.setattr(runtime_config, spec.threshold_attr, 60)

    async with db_session.begin():
        org_id = await _seed_org(db_session, slug=f"{spec.slug_prefix}-5")
        stuck_id = await _seed_silent_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-shortwindow",
            started_minutes_ago=5,
            project_id=505,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await spec.reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(stuck_id)
            assert qj is not None
            assert qj.status == JobStatus.failed


@pytest.mark.asyncio
@_reaper_param
async def test_reaper_isolates_other_main_pool_kinds(
    app: None,
    db_session: AsyncSession,
    spec: ReaperSpec,
) -> None:
    """Cross-kind isolation: only the target kind's rows are reaped.

    Seeds ``in_progress`` rows of every other run-less kind plus
    ``lifecycle_eval`` past the target kind's threshold. Only the
    target kind's row should move to ``failed`` — the rest must
    remain ``in_progress``.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug=f"{spec.slug_prefix}-iso")
        target_id = await _seed_silent_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-target",
            started_minutes_ago=spec.well_past_minutes,
            project_id=606,
        )
        other_kinds = [
            s.kind for s in RUNLESS_REAPER_SPECS if s.kind != spec.kind
        ] + [JobKind.lifecycle_eval]
        other_ids: list[int] = []
        for idx, kind in enumerate(other_kinds):
            other_row = SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                backend_job_id=f"arq-other-{idx}",
                kind=kind.value,
                status=JobStatus.in_progress.value,
                org_id=org_id,
                date_started=(
                    datetime.now(tz=UTC)
                    - timedelta(minutes=spec.well_past_minutes)
                ),
            )
            db_session.add(other_row)
            await db_session.flush()
            await db_session.refresh(other_row)
            other_ids.append(other_row.id)

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await spec.reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())

            target = await qj_store.get(target_id)
            assert target is not None
            assert target.status == JobStatus.failed
            assert target.errors is not None
            assert target.errors["type"] == "SilentWorker"

            for other_id in other_ids:
                other = await qj_store.get(other_id)
                assert other is not None
                assert other.status == JobStatus.in_progress


@pytest.mark.asyncio
@_reaper_param
async def test_reaper_fails_abandoned_queued_row(
    app: None,
    db_session: AsyncSession,
    spec: ReaperSpec,
) -> None:
    """A ``queued`` row arq has lost is reaped past the threshold.

    The PRD #538 wedge shape: the row reached arq (it has a
    ``backend_job_id``) and arq then lost the job, so neither the
    silent sweep (``in_progress`` only) nor the orphan sweep
    (``backend_job_id IS NULL`` only) can see it while the active-job
    partial unique index keeps counting it as live work.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug=f"{spec.slug_prefix}-ab1")
        abandoned_id = await _seed_abandoned_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-lost-forever",
            created_minutes_ago=spec.well_past_minutes,
            project_id=707,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await spec.reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(abandoned_id)
            assert qj is not None
            assert qj.status == JobStatus.failed
            assert qj.errors is not None
            assert qj.errors["type"] == "AbandonedQueueJob"
            assert qj.date_completed is not None


@pytest.mark.asyncio
@_reaper_param
async def test_reaper_spares_queued_row_arq_still_knows(
    app: None,
    db_session: AsyncSession,
    spec: ReaperSpec,
) -> None:
    """Healthy-job race: a live arq job survives ticks at any age.

    Pure age-based reaping would cancel a job merely backed up behind
    a saturated worker pool. Verifying against the backend first is
    what makes the abandoned sweep safe, so a row whose
    ``backend_job_id`` arq still knows must survive even when it is
    orders of magnitude older than the threshold.
    """
    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    live_metadata = await ctx["arq_queue"].enqueue(spec.kind.value)

    async with db_session.begin():
        org_id = await _seed_org(db_session, slug=f"{spec.slug_prefix}-ab2")
        healthy_id = await _seed_abandoned_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id=live_metadata.id,
            created_minutes_ago=spec.well_past_minutes * 100,
            project_id=708,
        )

    try:
        await spec.reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(healthy_id)
            assert qj is not None
            assert qj.status == JobStatus.queued
            assert qj.errors is None


@pytest.mark.asyncio
@_reaper_param
async def test_reaper_backend_unreachable_skips_only_abandoned_sweep(
    app: None,
    db_session: AsyncSession,
    spec: ReaperSpec,
) -> None:
    """An unreachable backend costs the tick only its abandoned sweep.

    With Redis down the reaper cannot tell a lost job from a live one,
    so it logs a warning and leaves every abandoned candidate alone —
    but the silent and orphan sweeps need no backend, so they must
    still reap that tick (PRD #538 §Scope, backend-unreachable
    bullet).
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug=f"{spec.slug_prefix}-ab3")
        silent_id = await _seed_silent_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-stuck-unreachable",
            started_minutes_ago=spec.well_past_minutes,
            project_id=709,
        )
        orphan_id = await _seed_orphan_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            created_minutes_ago=10,
            project_id=710,
        )
        abandoned_id = await _seed_abandoned_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-unverifiable",
            created_minutes_ago=spec.well_past_minutes,
            project_id=711,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)

    async def _explode(*args: Any, **kwargs: Any) -> None:
        raise ConnectionError("redis is unreachable")

    ctx["arq_queue"].get_job_metadata = _explode

    try:
        with capture_logs() as captured:
            result = await spec.reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    assert result == "completed"

    backend_warnings = [
        entry
        for entry in captured
        if entry.get("log_level") == "warning"
        and entry.get("event")
        == "Queue backend unreachable; skipping abandoned sweep"
    ]
    assert len(backend_warnings) == 1

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())

            silent = await qj_store.get(silent_id)
            assert silent is not None
            assert silent.status == JobStatus.failed
            assert silent.errors is not None
            assert silent.errors["type"] == "SilentWorker"

            orphan = await qj_store.get(orphan_id)
            assert orphan is not None
            assert orphan.status == JobStatus.failed
            assert orphan.errors is not None
            assert orphan.errors["type"] == "OrphanedQueueJob"

            abandoned = await qj_store.get(abandoned_id)
            assert abandoned is not None
            assert abandoned.status == JobStatus.queued
            assert abandoned.errors is None


@pytest.mark.asyncio
@_reaper_param
async def test_reaper_backend_stall_keeps_silent_and_orphan_reaps(
    app: None,
    db_session: AsyncSession,
    spec: ReaperSpec,
) -> None:
    """A stalled backend costs the tick only its abandoned sweep.

    The failure mode task #548 fixes: a hung Redis — exactly the
    post-outage scenario the abandoned sweep exists for — used to push
    the tick past arq's job timeout while the silent and orphan sweeps'
    updates sat uncommitted in the same transaction. The resulting
    ``CancelledError`` is a ``BaseException``, so the sweep's
    ``except Exception`` soft-abort never saw it: the transaction
    unwound and every reap that tick was lost, tick after tick.

    Modelled deterministically by cancelling the reaper only once the
    backend call is actually in flight. The silent and orphan reaps must
    already be committed by then.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug=f"{spec.slug_prefix}-ab5")
        silent_id = await _seed_silent_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-stuck-stall",
            started_minutes_ago=spec.well_past_minutes,
            project_id=715,
        )
        orphan_id = await _seed_orphan_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            created_minutes_ago=10,
            project_id=716,
        )
        abandoned_id = await _seed_abandoned_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-stalled",
            created_minutes_ago=spec.well_past_minutes,
            project_id=717,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    reached_backend = asyncio.Event()

    async def _hang(*args: Any, **kwargs: Any) -> None:
        reached_backend.set()
        await asyncio.Event().wait()

    ctx["arq_queue"].get_job_metadata = _hang

    async def _tick() -> str:
        return await spec.reaper(ctx)

    try:
        task = asyncio.create_task(_tick())
        await asyncio.wait_for(reached_backend.wait(), timeout=30)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())

            silent = await qj_store.get(silent_id)
            assert silent is not None
            assert silent.status == JobStatus.failed
            assert silent.errors is not None
            assert silent.errors["type"] == "SilentWorker"

            orphan = await qj_store.get(orphan_id)
            assert orphan is not None
            assert orphan.status == JobStatus.failed
            assert orphan.errors is not None
            assert orphan.errors["type"] == "OrphanedQueueJob"

            abandoned = await qj_store.get(abandoned_id)
            assert abandoned is not None
            assert abandoned.status == JobStatus.queued


@pytest.mark.asyncio
@_reaper_param
async def test_reaper_warning_names_sweep_and_backend_job_id(
    app: None,
    db_session: AsyncSession,
    spec: ReaperSpec,
) -> None:
    """Reaped-row log context carries the sweep name and arq job ID.

    PRD #538 §Summary ("Observability — logs only"): with three loss
    modes now sharing one warning line, an operator needs to see which
    sweep claimed each row and the ``backend_job_id`` that went
    missing, since that field is deliberately not on the jobs API.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug=f"{spec.slug_prefix}-ab4")
        silent_id = await _seed_silent_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-silent-ctx",
            started_minutes_ago=spec.well_past_minutes,
            project_id=712,
        )
        orphan_id = await _seed_orphan_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            created_minutes_ago=10,
            project_id=713,
        )
        abandoned_id = await _seed_abandoned_row(
            db_session,
            kind=spec.kind,
            org_id=org_id,
            backend_job_id="arq-abandoned-ctx",
            created_minutes_ago=spec.well_past_minutes,
            project_id=714,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            await spec.reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    warnings = [
        entry
        for entry in captured
        if entry.get("log_level") == "warning"
        and entry.get("event") == f"Reaped stuck {spec.kind.value} queue jobs"
    ]
    assert len(warnings) == 1
    assert warnings[0]["reaped_count"] == 3
    assert warnings[0]["silent_count"] == 1
    assert warnings[0]["orphan_count"] == 1
    assert warnings[0]["abandoned_count"] == 1

    public_ids: dict[str, str] = {}
    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            for label, job_id in (
                ("silent", silent_id),
                ("orphan", orphan_id),
                ("abandoned", abandoned_id),
            ):
                qj = await qj_store.get(job_id)
                assert qj is not None
                public_ids[label] = serialize_base32_id(qj.public_id)

    by_public_id = {
        entry["public_id"]: entry for entry in warnings[0]["reaped_jobs"]
    }
    assert by_public_id[public_ids["silent"]] == {
        "public_id": public_ids["silent"],
        "sweep": "silent",
        "backend_job_id": "arq-silent-ctx",
    }
    assert by_public_id[public_ids["orphan"]] == {
        "public_id": public_ids["orphan"],
        "sweep": "orphan",
        "backend_job_id": None,
    }
    assert by_public_id[public_ids["abandoned"]] == {
        "public_id": public_ids["abandoned"],
        "sweep": "abandoned",
        "backend_job_id": "arq-abandoned-ctx",
    }


@pytest.mark.asyncio
async def test_build_processing_reaper_fails_stranded_build(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A ``processing`` build with no live job self-heals to ``failed``.

    The #575 residue this sweep exists for: the stale guard completed
    the queue job but never moved the build, so the row reads "in
    flight" forever while nothing is working on it. One tick must
    retire it and name it in the warning payload, while a build a live
    job still vouches for is left strictly alone.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="bpr-stranded")
        stranded_id, stranded_public_id = await _seed_processing_build(
            db_session, project_id=8181, uploaded_minutes_ago=600
        )
        covered_id, _ = await _seed_processing_build(
            db_session, project_id=8182, uploaded_minutes_ago=600
        )
        await _seed_silent_row(
            db_session,
            kind=JobKind.build_processing,
            org_id=org_id,
            backend_job_id="arq-still-working",
            started_minutes_ago=0,
            project_id=8182,
            build_id=covered_id,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            result = await build_processing_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    assert result == "completed"

    warnings = [
        entry
        for entry in captured
        if entry.get("event") == "Reaped stuck build_processing queue jobs"
    ]
    assert len(warnings) == 1
    assert warnings[0]["stranded_builds"] == [stranded_public_id]
    assert warnings[0]["reaped_count"] == 1
    assert warnings[0]["silent_count"] == 0
    assert warnings[0]["orphan_count"] == 0
    assert warnings[0]["abandoned_count"] == 0

    assert await _read_build_status(stranded_id) == BuildStatus.failed
    assert await _read_build_status(covered_id) == BuildStatus.processing


@pytest.mark.asyncio
async def test_build_processing_reaper_sweeps_build_its_silent_sweep_freed(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A build freed by this tick's own silent sweep is swept too.

    The common shape after a worker pod dies: the ``queue_jobs`` row is
    still ``in_progress`` when the tick starts, so the build only looks
    stranded once the silent sweep has failed its job. That ordering is
    why the stranded sweep runs after the other two inside the same
    transaction rather than ahead of them.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="bpr-freed")
        build_id, build_public_id = await _seed_processing_build(
            db_session, project_id=8183, uploaded_minutes_ago=600
        )
        job_id = await _seed_silent_row(
            db_session,
            kind=JobKind.build_processing,
            org_id=org_id,
            backend_job_id="arq-oom-killed",
            started_minutes_ago=600,
            project_id=8183,
            build_id=build_id,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            await build_processing_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    warnings = [
        entry
        for entry in captured
        if entry.get("event") == "Reaped stuck build_processing queue jobs"
    ]
    assert len(warnings) == 1
    assert warnings[0]["silent_count"] == 1
    assert warnings[0]["stranded_builds"] == [build_public_id]
    assert warnings[0]["reaped_count"] == 2

    assert await _read_build_status(build_id) == BuildStatus.failed

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(job_id)
            assert qj is not None
            assert qj.status == JobStatus.failed


@pytest.mark.asyncio
async def test_build_processing_reaper_no_stranded_builds_logs_debug(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A healthy in-flight build keeps the tick at ``debug``.

    The stranded sweep must not turn the steady state into a warning
    every tick: a build whose worker is still holding it is not
    stranded, so nothing is reaped and the no-op path is unchanged.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="bpr-healthy")
        build_id, _ = await _seed_processing_build(
            db_session, project_id=8184, uploaded_minutes_ago=600
        )
        await _seed_silent_row(
            db_session,
            kind=JobKind.build_processing,
            org_id=org_id,
            backend_job_id="arq-healthy",
            started_minutes_ago=0,
            project_id=8184,
            build_id=build_id,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            result = await build_processing_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    assert result == "completed"

    reaper_events = [
        entry
        for entry in captured
        if "build_processing" in str(entry.get("event", ""))
    ]
    assert [
        entry for entry in reaper_events if entry.get("log_level") == "warning"
    ] == []
    debugs = [
        entry for entry in reaper_events if entry.get("log_level") == "debug"
    ]
    assert len(debugs) == 1
    assert debugs[0]["event"] == "No stuck build_processing queue jobs to reap"

    assert await _read_build_status(build_id) == BuildStatus.processing


@pytest.mark.asyncio
async def test_dashboard_build_reaper_unwedges_the_project(
    app: None,
    db_session: AsyncSession,
) -> None:
    """The production wedge, end to end, for ``dashboard_build``.

    Reproduces the roundtable-dev incident PRD #538 was filed for: one
    abandoned row holds ``idx_queue_jobs_dashboard_build_active_uq``,
    so ``has_active_dashboard_build`` keeps reporting an in-flight
    build and every publish cascade skips the enqueue, freezing the
    project's version dashboard. One reaper tick must both fail the
    row and let the next enqueue through.
    """
    project_id = 8080
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="dbr-wedge")
        wedged_id = await _seed_abandoned_row(
            db_session,
            kind=JobKind.dashboard_build,
            org_id=org_id,
            backend_job_id="arq-documenteer-lost",
            created_minutes_ago=60,
            project_id=project_id,
        )

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            assert await qj_store.has_active_dashboard_build(
                org_id=org_id, project_id=project_id
            )
            assert (
                await qj_store.create_unless_active(
                    kind=JobKind.dashboard_build,
                    org_id=org_id,
                    project_id=project_id,
                )
                is None
            )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await dashboard_build_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            wedged = await qj_store.get(wedged_id)
            assert wedged is not None
            assert wedged.status == JobStatus.failed
            assert wedged.errors is not None
            assert wedged.errors["type"] == "AbandonedQueueJob"

            assert not await qj_store.has_active_dashboard_build(
                org_id=org_id, project_id=project_id
            )
            fresh = await qj_store.create_unless_active(
                kind=JobKind.dashboard_build,
                org_id=org_id,
                project_id=project_id,
            )
            await session.commit()

    assert fresh is not None
    assert fresh.id != wedged_id
    assert fresh.status == JobStatus.queued


@pytest.mark.asyncio
async def test_reaped_jobs_ids_grep_against_the_skip_warning(
    app: None,
    db_session: AsyncSession,
) -> None:
    """The postmortem payload's ids match what a late delivery logs.

    ``reaped_jobs`` exists so an operator can cross-reference a job that
    quietly did nothing against the sweep that claimed it. That only
    works if both sides spell the id the same way: the pickup guard logs
    ``queue_job_id`` as a base32 string, so the reaper's payload must
    too. Logging the raw ``int`` behind
    :class:`~docverse_server.domain.base32id.Base32Id` — whose
    serializer only fires in Pydantic JSON dumps, not in structlog
    context — makes the grep come back empty (task #553).
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="dbr-grep")
        reaped_id = await _seed_abandoned_row(
            db_session,
            kind=JobKind.dashboard_build,
            org_id=org_id,
            backend_job_id="arq-grep-lost",
            created_minutes_ago=60,
            project_id=9090,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as reaper_logs:
            await dashboard_build_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    warnings = [
        entry
        for entry in reaper_logs
        if entry.get("event") == "Reaped stuck dashboard_build queue jobs"
    ]
    assert len(warnings) == 1
    assert len(warnings[0]["reaped_jobs"]) == 1
    reaped_public_id = warnings[0]["reaped_jobs"][0]["public_id"]

    # The late delivery arq never cancelled: the same row, handed to a
    # worker after the sweep failed it.
    row_public_id: int | None = None
    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            with capture_logs() as pickup_logs:
                assert await qj_store.start_if_queued(reaped_id) is None
            row = await qj_store.get(reaped_id)
            assert row is not None
            row_public_id = row.public_id

    skips = [
        entry
        for entry in pickup_logs
        if entry.get("event") == "Skipping late-delivered queue job"
    ]
    assert len(skips) == 1
    assert skips[0]["queue_job_id"] == reaped_public_id
    # Both sides carry the id an operator actually sees in the API, not
    # the internal integer.
    assert validate_base32_id(reaped_public_id) == row_public_id
