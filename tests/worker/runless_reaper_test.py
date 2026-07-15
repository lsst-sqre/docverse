"""Tests for the four run-less reaper cron worker functions.

Covers ``dashboard_build_reaper``, ``publish_edition_reaper``,
``build_processing_reaper``, and ``dashboard_sync_reaper`` — the
cron-driven backstops for the case where arq itself loses a queue
job (worker pod OOM-killed mid-job that never gets to surface a
timeout, or dispatcher crashed between the ``queue_jobs`` SQL commit
and ``arq_queue.enqueue``). Each reaper marks any silently-stuck row
as ``failed`` and sweeps orphan queued rows.

Parametrized over :data:`RUNLESS_REAPER_SPECS` so the same six
behaviors are exercised against every run-less kind without
copy-pasted per-kind test modules. pytest IDs like
``[dashboard_build]`` keep per-kind failure attribution.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import structlog
from docverse.client.models import OrganizationCreate
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.config import config as runtime_config
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.build_processing_reaper import (
    build_processing_reaper,
)
from docverse.worker.functions.dashboard_build_reaper import (
    dashboard_build_reaper,
)
from docverse.worker.functions.dashboard_sync_reaper import (
    dashboard_sync_reaper,
)
from docverse.worker.functions.publish_edition_reaper import (
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
) -> int:
    """Insert one row of ``kind`` stuck in ``in_progress``."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=backend_job_id,
        kind=kind.value,
        status=JobStatus.in_progress.value,
        org_id=org_id,
        project_id=project_id,
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
            assert {silent_qj.public_id, orphan_qj.public_id} == set(
                warnings[0]["reaped_public_ids"]
            )


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
