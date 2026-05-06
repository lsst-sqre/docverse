"""Tests for the ``keeper_sync_run_discovery`` worker function."""

from __future__ import annotations

import json
from typing import Literal

import httpx
import pytest
import respx
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    JobKind,
    KeeperSyncConfig,
    KeeperSyncRunStatus,
    OrganizationCreate,
)
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.queue import JobStatus
from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.keeper_sync import keeper_sync_run_discovery
from tests.support.arq_testing import get_jobs_by_name, register_queue
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(
    db_session: AsyncSession,
    *,
    project_slugs: list[str] | Literal["*"] = "*",
) -> tuple[int, str]:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug="ks-org",
            title="KS Org",
            base_domain="ks.example.com",
        )
    )
    await org_store.update_keeper_sync_config(
        slug=org.slug,
        config=KeeperSyncConfig(
            enabled=True,
            project_slugs=project_slugs,
        ),
    )
    return org.id, org.slug


async def _seed_run(db_session: AsyncSession, *, org_id: int) -> int:
    row = SqlKeeperSyncRun(org_id=org_id, kind="backfill", status="pending")
    db_session.add(row)
    await db_session.flush()
    await db_session.refresh(row)
    return row.id


async def _seed_discovery_queue_job(
    db_session: AsyncSession, *, org_id: int, run_id: int
) -> int:
    queue_job_store = QueueJobStore(session=db_session, logger=_logger())
    queue_job = await queue_job_store.create(
        kind=JobKind.keeper_sync_run_discovery,
        org_id=org_id,
        keeper_sync_run_id=run_id,
        backend_job_id="test-arq-discovery",
    )
    return queue_job.id


def _mock_ltd_products(mock_discovery: respx.Router, slugs: list[str]) -> None:
    products = [f"https://keeper.lsst.codes/products/{s}/" for s in slugs]
    mock_discovery.get("https://keeper.lsst.codes/products/").mock(
        return_value=httpx.Response(
            status_code=200,
            content=json.dumps({"products": products}).encode(),
            headers={"content-type": "application/json"},
        )
    )


@pytest.mark.asyncio
async def test_discovery_fans_out_intersected_slugs(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Discovery enqueues one ``keeper_sync_project`` per allowlisted slug."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["dmtn-001", "sqr-112"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _mock_ltd_products(mock_discovery, ["dmtn-001", "dmtn-002", "sqr-112"])

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "completed"

    # Two child enqueues — one per intersected slug — landing on the
    # dedicated sync queue and not the default queue.
    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert len(project_jobs) == 2
    default_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name="docverse:queue"
    )
    assert default_jobs == []

    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.keeper_sync_run_id == run_id,
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            )
            child_rows = (await session.execute(stmt)).scalars().all()
            assert len(child_rows) == 2
            assert all(row.org_id == org_id for row in child_rows)
            assert all(row.backend_job_id is not None for row in child_rows)

            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.in_progress

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            disc = await queue_job_store.get(queue_job_id)
            assert disc is not None
            assert disc.status == JobStatus.completed
            assert disc.progress is not None
            assert disc.progress["in_scope_count"] == 2


@pytest.mark.asyncio
async def test_discovery_with_wildcard_uses_all_ltd_slugs(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """``project_slugs="*"`` keeps every LTD slug in scope."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, project_slugs="*")
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _mock_ltd_products(mock_discovery, ["dmtn-001", "dmtn-002", "sqr-112"])

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.keeper_sync_run_id == run_id,
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            )
            child_rows = (await session.execute(stmt)).scalars().all()
            assert len(child_rows) == 3


@pytest.mark.asyncio
async def test_discovery_with_empty_intersection_finalises_run(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An empty fan-out terminates the run as ``succeeded`` immediately."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["nonexistent"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _mock_ltd_products(mock_discovery, ["dmtn-001"])

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org_id,
            "org_slug": org_slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.succeeded


@pytest.mark.asyncio
async def test_discovery_marks_run_failed_when_disabled(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A disabled config aborts discovery and marks the run failed."""
    logger = _logger()
    async with db_session.begin():
        org_store = OrganizationStore(session=db_session, logger=logger)
        org = await org_store.create(
            OrganizationCreate(
                slug="ks-org",
                title="KS Org",
                base_domain="ks.example.com",
            )
        )
        # Persist a disabled config, then re-fetch the org id.
        await org_store.update_keeper_sync_config(
            slug=org.slug,
            config=KeeperSyncConfig(enabled=False),
        )
        run_id = await _seed_run(db_session, org_id=org.id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org.id, run_id=run_id
        )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await keeper_sync_run_discovery(
        ctx,
        {
            "org_id": org.id,
            "org_slug": org.slug,
            "run_id": run_id,
            "queue_job_id": queue_job_id,
        },
    )
    await ctx["http_client"].aclose()
    assert result == "failed"

    async for session in db_session_dependency():
        async with session.begin():
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.failed
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            disc = await queue_job_store.get(queue_job_id)
            assert disc is not None
            assert disc.status == JobStatus.failed
