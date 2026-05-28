"""Tests for the ``publish_edition_reaper`` cron worker function.

Mirrors :mod:`tests.worker.dashboard_build_reaper_test` for the
``publish_edition`` subsystem. The reaper is the cron-driven backstop
for the case where arq itself loses a ``publish_edition`` job — a
worker pod OOM-killed mid-job that never gets to surface a timeout,
or a dispatcher that crashed between the ``queue_jobs`` SQL commit
and ``arq_queue.enqueue``. It marks any silently-stuck row as
``failed`` and sweeps orphan queued rows so an edition does not sit
in ``publishing`` indefinitely and the CDN does not silently stay
behind.
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
from structlog.testing import capture_logs

from docverse.client.models import JobKind, OrganizationCreate
from docverse.config import config as runtime_config
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobStatus
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.publish_edition_reaper import (
    publish_edition_reaper,
)
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(db_session: AsyncSession, *, slug: str) -> int:
    org_store = OrganizationStore(session=db_session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"PER Org {slug}",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


async def _seed_silent_row(
    db_session: AsyncSession,
    *,
    org_id: int,
    backend_job_id: str,
    started_minutes_ago: int,
    project_id: int | None = None,
) -> int:
    """Insert a ``kind='publish_edition'`` row stuck in ``in_progress``."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=backend_job_id,
        kind=JobKind.publish_edition.value,
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
    org_id: int,
    created_minutes_ago: int,
    project_id: int | None = None,
) -> int:
    """Insert a ``kind='publish_edition'`` orphan (queued, no backend id)."""
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        backend_job_id=None,
        kind=JobKind.publish_edition.value,
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
async def test_reaper_fails_stuck_in_progress(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A stuck ``in_progress`` row is reaped to ``failed``."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="per-1")
        stuck_id = await _seed_silent_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-stuck-1",
            started_minutes_ago=300,
            project_id=101,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await publish_edition_reaper(ctx)
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
async def test_reaper_sweeps_orphan_queued_row(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A ``queued`` orphan past the idle window is reaped to ``failed``.

    Models the dispatcher crash window: the ``queue_jobs`` row is
    committed before ``arq_queue.enqueue`` is called, so a crash
    between those two operations leaves a row that no arq job will
    ever pick up. Without the orphan sweep the edition stays in
    ``publishing`` forever and the CDN silently lags behind.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="per-2")
        orphan_id = await _seed_orphan_row(
            db_session,
            org_id=org_id,
            created_minutes_ago=10,
            project_id=202,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await publish_edition_reaper(ctx)
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
async def test_reaper_no_op_logs_debug_not_warning(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A clean tick with nothing to reap logs ``debug``, not ``warning``.

    A healthy steady-state system must not flood ``warning``-level
    logs every 30 minutes when the reaper finds zero candidate rows
    — PRD #367 user story 16.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="per-3")
        # Fresh in_progress within idle window — must not be reaped.
        fresh_id = await _seed_silent_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-clean",
            started_minutes_ago=0,
            project_id=303,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            result = await publish_edition_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    assert result == "completed"

    reaper_events = [
        entry
        for entry in captured
        if "publish_edition" in str(entry.get("event", ""))
    ]
    warnings = [
        entry for entry in reaper_events if entry.get("log_level") == "warning"
    ]
    debugs = [
        entry for entry in reaper_events if entry.get("log_level") == "debug"
    ]
    assert warnings == []
    assert len(debugs) == 1

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(fresh_id)
            assert qj is not None
            assert qj.status == JobStatus.in_progress


@pytest.mark.asyncio
async def test_reaper_warning_includes_count_and_public_ids(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Reaping > 0 rows emits one ``warning`` with count + public IDs.

    PRD #367 user story 8: operators need a structured ``warning``
    log line whenever a reaper sweep actually reaps something, so
    reaper activity can be correlated with the underlying incident
    in logs without scanning the database.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="per-4")
        silent_id = await _seed_silent_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-stuck-warn",
            started_minutes_ago=300,
            project_id=404,
        )
        orphan_id = await _seed_orphan_row(
            db_session,
            org_id=org_id,
            created_minutes_ago=10,
            project_id=405,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            await publish_edition_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    warnings = [
        entry
        for entry in captured
        if entry.get("log_level") == "warning"
        and "publish_edition" in str(entry.get("event", ""))
    ]
    assert len(warnings) == 1
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
async def test_reaper_threshold_is_configurable(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reducing the threshold shrinks the silent window.

    Operators in non-prod set
    ``DOCVERSE_PUBLISH_EDITION_REAPER_THRESHOLD_SECONDS`` to a small
    value so a deliberately-wedged job surfaces in seconds rather
    than the production-default 4 hours. The reaper must observe
    the configured value at invocation time rather than a baked-in
    default.
    """
    monkeypatch.setattr(
        runtime_config, "publish_edition_reaper_threshold_seconds", 60
    )

    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="per-5")
        stuck_id = await _seed_silent_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-shortwindow",
            started_minutes_ago=5,
            project_id=505,
        )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await publish_edition_reaper(ctx)
    finally:
        await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            qj_store = QueueJobStore(session=session, logger=_logger())
            qj = await qj_store.get(stuck_id)
            assert qj is not None
            assert qj.status == JobStatus.failed


@pytest.mark.asyncio
async def test_reaper_isolates_other_main_pool_kinds(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Cross-kind isolation: only ``publish_edition`` rows are reaped.

    Seeds ``in_progress`` rows of every other main-pool kind plus
    ``lifecycle_eval`` past the publish_edition threshold. Only the
    ``publish_edition`` row should move to ``failed`` — the rest
    must remain ``in_progress``.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="per-iso")
        target_id = await _seed_silent_row(
            db_session,
            org_id=org_id,
            backend_job_id="arq-target",
            started_minutes_ago=300,
            project_id=606,
        )
        other_kinds = [
            JobKind.dashboard_build,
            JobKind.build_processing,
            JobKind.dashboard_sync,
            JobKind.lifecycle_eval,
        ]
        other_ids: list[int] = []
        for idx, kind in enumerate(other_kinds):
            other_row = SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                backend_job_id=f"arq-other-{idx}",
                kind=kind.value,
                status=JobStatus.in_progress.value,
                org_id=org_id,
                date_started=(datetime.now(tz=UTC) - timedelta(minutes=300)),
            )
            db_session.add(other_row)
            await db_session.flush()
            await db_session.refresh(other_row)
            other_ids.append(other_row.id)

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        await publish_edition_reaper(ctx)
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
