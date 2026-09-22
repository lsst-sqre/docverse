"""Tests for the ``keeper_sync_run_discovery`` worker function."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import httpx
import pytest
import respx
import sentry_sdk
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from safir.testing.sentry import (
    TestTransport,
    capture_events_fixture,
    sentry_init_fixture,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import (
    JobKind,
    KeeperSyncConfig,
    KeeperSyncRunStatus,
    OrganizationCreate,
)
from docverse_server.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import (
    generate_base32_id,
    validate_base32_id,
)
from docverse_server.domain.queue import JobStatus
from docverse_server.sentry import initialize_sentry
from docverse_server.services.keeper_sync_config import KeeperSyncConfigService
from docverse_server.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse_server.services.keeper_sync_scope_preview import (
    KeeperSyncScopePreviewService,
)
from docverse_server.services.keeper_sync_tombstone import (
    KeeperSyncTombstoneService,
)
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse_server.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse_server.storage.ltd.products_client import LtdProductsClient
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.keeper_sync import (
    keeper_sync_run_discovery,
)
from tests.support.arq_testing import get_jobs_by_name, register_queue
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(
    db_session: AsyncSession,
    *,
    project_slugs: list[str] | Literal["*"] = "*",
    project_slug_patterns: list[str] | None = None,
    exclude_project_slugs: list[str] | None = None,
    exclude_project_slug_patterns: list[str] | None = None,
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
            project_slug_patterns=project_slug_patterns or [],
            exclude_project_slugs=exclude_project_slugs or [],
            exclude_project_slug_patterns=(
                exclude_project_slug_patterns or []
            ),
        ),
    )
    return org.id, org.slug


async def _seed_run(db_session: AsyncSession, *, org_id: int) -> int:
    row = SqlKeeperSyncRun(
        public_id=validate_base32_id(generate_base32_id()),
        org_id=org_id,
        kind="backfill",
        status="pending",
    )
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

    # Each child payload carries the snapshot ``ltd_base_url`` so the
    # per-project worker can construct its KeeperSyncService without
    # re-reading the org config (which may have changed mid-run).
    payloads = [job.kwargs["payload"] for job in project_jobs]
    assert {p["ltd_slug"] for p in payloads} == {"dmtn-001", "sqr-112"}
    for payload in payloads:
        assert payload["ltd_base_url"] == "https://keeper.lsst.codes/"

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
async def test_discovery_drops_excluded_slugs(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """``exclude_project_slugs`` wins over the ``"*"`` wildcard.

    PRD #667: excludes always win, so ``www`` stays out of the fan-out
    even though every other LTD slug is in scope.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session,
            project_slugs="*",
            exclude_project_slugs=["www"],
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _mock_ltd_products(mock_discovery, ["dmtn-001", "www", "sqr-112"])

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

    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert [j.kwargs["payload"]["ltd_slug"] for j in project_jobs] == [
        "dmtn-001",
        "sqr-112",
    ]

    async for session in db_session_dependency():
        async with session.begin():
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            activity = await run_store.aggregate_activity(run_id=run_id)
            # The discovery job is attributed to the run alongside its
            # children, so the run totals one row per in-scope slug
            # plus the discovery row itself.
            assert activity.total_count == 3


@pytest.mark.asyncio
async def test_discovery_fans_out_pattern_matches_in_ltd_order(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An include pattern brings a whole document series into scope.

    PRD #667: with an empty ``project_slugs`` the fan-out is exactly
    the slugs the pattern fully matches, in LTD listing order, so
    ``sqr-1`` never matches ``sqr-100``.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session,
            project_slugs=[],
            project_slug_patterns=[r"sqr-\d+"],
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _mock_ltd_products(
        mock_discovery, ["sqr-112", "dmtn-001", "sqr-060", "sqr-alpha"]
    )

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

    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert [j.kwargs["payload"]["ltd_slug"] for j in project_jobs] == [
        "sqr-112",
        "sqr-060",
    ]

    async for session in db_session_dependency():
        async with session.begin():
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            activity = await run_store.aggregate_activity(run_id=run_id)
            # One row per in-scope slug plus the discovery row itself.
            assert activity.total_count == 3


@pytest.mark.asyncio
async def test_discovery_logs_excluded_count(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """The run-scope log event reports how many slugs an exclude removed."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session,
            project_slugs="*",
            exclude_project_slugs=["www"],
            exclude_project_slug_patterns=[r"test-.*"],
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    _mock_ltd_products(
        mock_discovery, ["dmtn-001", "www", "test-one", "test-two"]
    )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    with capture_logs() as captured:
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

    scope_events = [
        e for e in captured if e["event"] == "Resolved keeper-sync run scope"
    ]
    assert len(scope_events) == 1
    assert scope_events[0]["ltd_count"] == 4
    assert scope_events[0]["in_scope_count"] == 1
    assert scope_events[0]["excluded_count"] == 3


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


@pytest.mark.asyncio
async def test_discovery_fails_and_alerts_on_a_malformed_ltd_payload(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreadable LTD 200 fails the run and reaches Sentry.

    Issue #675 moved the malformed-payload failure into the
    ``LtdClientError`` taxonomy so the *attended* scope-preview endpoint
    can answer 502 instead of 500. This pins the other half of that
    change: the worker is unattended, so the same payload must still
    fail the run loudly rather than being quietly mapped to "nothing in
    scope" — which is what a Docverse-side ``except`` around the new
    exception type would have cost us.

    An HTML maintenance page served as 200 is the shape that broke: a
    proxy in front of LTD answers 200, so nothing in the status check
    notices, and the listing silently has no products in it.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    mock_discovery.get("https://keeper.lsst.codes/products/").mock(
        return_value=httpx.Response(
            200,
            content=b"<html><body>LTD is down for maintenance</body></html>",
            headers={"content-type": "text/html"},
        )
    )

    monkeypatch.setenv("SENTRY_DSN", "https://test@example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "test")
    real_init = sentry_sdk.init

    def _init_with_test_transport(*args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("transport", TestTransport())
        return real_init(*args, **kwargs)

    monkeypatch.setattr(sentry_sdk, "init", _init_with_test_transport)

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    with sentry_init_fixture():
        initialize_sentry(component="worker-keeper-sync")
        captured = capture_events_fixture(monkeypatch)()

        result = await keeper_sync_run_discovery(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )

        assert result == "failed"
        assert len(captured.errors) == 1
        exc_values = captured.errors[0]["exception"]["values"]
        assert any(exc["type"] == "LtdProductsError" for exc in exc_values)
    await ctx["http_client"].aclose()

    # No children fanned out, and both the job and the run are failed —
    # an unreadable listing must never look like an empty one, which
    # would have rolled the run up green with zero projects synced.
    assert (
        get_jobs_by_name(
            mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
        )
        == []
    )
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
            assert disc.errors is not None
            assert disc.errors["type"] == "LtdProductsError"


@pytest.mark.asyncio
async def test_discovery_reconciles_orphan_children_from_prior_attempt(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An orphan child from a crashed prior discovery is failed at start.

    Reproduces the race window where ``_enqueue_children`` committed a
    child ``queue_jobs`` row but died before ``arq_queue.enqueue``: the
    row is queued, has no ``backend_job_id``, and would block run
    finalisation forever. The next discovery attempt should sweep it.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["dmtn-001"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        # Pre-seed an orphan child older than the 5-minute idle window.
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        orphan = await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
        )
        orphan_row = await db_session.get(SqlQueueJob, orphan.id)
        assert orphan_row is not None
        orphan_row.date_created = datetime.now(tz=UTC) - timedelta(minutes=10)
        await db_session.flush()

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
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            reaped = await queue_job_store.get(orphan.id)
            assert reaped is not None
            assert reaped.status == JobStatus.failed
            assert reaped.errors is not None
            assert "orphan" in reaped.errors["message"].lower()


@pytest.mark.asyncio
async def test_discovery_skips_slug_with_active_keeper_sync_project(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An active ``keeper_sync_project`` row blocks the per-slug enqueue.

    Reproduces the QA race: a tier-cron-enqueued (run-less) job for
    ``pipelines`` is already queued; a subsequent operator-triggered
    backfill discovery must not enqueue a second job for the same slug,
    because the two would race through ``_ensure_edition`` and one
    would lose the ``uq_editions_project_lower_slug`` race.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["pipelines", "dmtn-001"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        # Pre-seed a tier-cron-style active row (no run attribution) for
        # ``pipelines``. Discovery must skip this slug.
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        existing = await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="pipelines",
            backend_job_id="arq-job-tier-cron",
        )

    _mock_ltd_products(mock_discovery, ["pipelines", "dmtn-001"])

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

    # Only ``dmtn-001`` got enqueued; ``pipelines`` was skipped.
    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert len(project_jobs) == 1
    assert project_jobs[0].kwargs["payload"]["ltd_slug"] == "dmtn-001"

    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
                SqlQueueJob.subject_label == "pipelines",
                SqlQueueJob.org_id == org_id,
            )
            rows = (await session.execute(stmt)).scalars().all()
            # Exactly one active ``pipelines`` row — the tier-cron's
            # original — survives. Discovery did not insert a duplicate.
            assert len(rows) == 1
            assert rows[0].id == existing.id
            assert rows[0].keeper_sync_run_id is None

            # The skipped slug does NOT count toward the run's progress
            # (its row stays attached to no run). Run-attributed children
            # = 1 (just dmtn-001).
            run_stmt = select(SqlQueueJob).where(
                SqlQueueJob.keeper_sync_run_id == run_id,
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            )
            attributed = (await session.execute(run_stmt)).scalars().all()
            assert len(attributed) == 1


@pytest.mark.asyncio
async def test_discovery_leaves_recent_unenqueued_children_alone(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A queued child younger than the idle window is not reaped.

    Guards against a discovery worker reaping rows that a concurrent
    healthy discovery worker just committed but hasn't yet had a chance
    to write a ``backend_job_id`` back onto.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["dmtn-001"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        # Fresh child with no backend_job_id — within the idle window.
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        recent = await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
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
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            untouched = await queue_job_store.get(recent.id)
            assert untouched is not None
            assert untouched.status == JobStatus.queued


async def _seed_abandoned_child(
    db_session: AsyncSession,
    *,
    org_id: int,
    run_id: int,
    subject_label: str,
    backend_job_id: str,
    created_minutes_ago: int,
) -> int:
    """Seed a run child that reached arq and was then lost by it.

    The PRD #538 shape, one level down from the reaper tests: the row
    has a ``backend_job_id`` (so the orphan sweep, which requires it to
    be ``NULL``, cannot see it) and never started (so the silent sweep,
    which requires ``in_progress``, cannot either). ``backend_job_id``
    is one the mock queue has never issued, so verification reads it
    back as lost.
    """
    queue_job_store = QueueJobStore(session=db_session, logger=_logger())
    child = await queue_job_store.create(
        kind=JobKind.keeper_sync_project,
        org_id=org_id,
        keeper_sync_run_id=run_id,
        subject_label=subject_label,
        backend_job_id=backend_job_id,
    )
    row = await db_session.get(SqlQueueJob, child.id)
    assert row is not None
    row.date_created = datetime.now(tz=UTC) - timedelta(
        minutes=created_minutes_ago
    )
    await db_session.flush()
    return child.id


@pytest.mark.asyncio
async def test_discovery_reconciles_abandoned_child_and_refans_its_slug(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A re-delivered discovery reaps an arq-lost child and re-fans its slug.

    The gap task #552 closes: reconciliation only swept orphans
    (``backend_job_id IS NULL``), so a child that *did* reach arq before
    arq lost it kept holding
    ``idx_queue_jobs_keeper_sync_project_subject_active_uq``. The
    re-fan-out's ``has_active_for_subject`` pre-check therefore skipped
    that slug and the run stayed a child short until the 30-minute cron
    reaper caught up. The discovery-side pass must fail the row and let
    the same run enqueue the slug again.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["dmtn-001"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        abandoned_id = await _seed_abandoned_child(
            db_session,
            org_id=org_id,
            run_id=run_id,
            subject_label="dmtn-001",
            backend_job_id="arq-child-lost",
            created_minutes_ago=600,
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

    # The slug is back in the fan-out rather than skipped as active.
    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert [j.kwargs["payload"]["ltd_slug"] for j in project_jobs] == [
        "dmtn-001"
    ]

    async for session in db_session_dependency():
        async with session.begin():
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            reaped = await queue_job_store.get(abandoned_id)
            assert reaped is not None
            assert reaped.status == JobStatus.failed
            assert reaped.errors is not None
            assert reaped.errors["type"] == "AbandonedQueueJob"

            # The replacement child belongs to the same run, so the
            # run's aggregate covers the slug again.
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.keeper_sync_run_id == run_id,
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
                SqlQueueJob.status == JobStatus.queued.value,
            )
            fresh = (await session.execute(stmt)).scalars().all()
            assert len(fresh) == 1
            assert fresh[0].id != abandoned_id
            assert fresh[0].subject_label == "dmtn-001"
            assert fresh[0].backend_job_id is not None


@pytest.mark.asyncio
async def test_discovery_soft_skips_abandoned_pass_when_backend_is_down(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An unreachable backend costs the discovery only its abandoned pass.

    With Redis down the sweep cannot tell a lost child from a live one,
    so it must leave every candidate alone and warn — but the discovery
    itself has to finish: the orphan pass still reaps, and every slug
    whose subject is free still fans out.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["dmtn-001", "sqr-112"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        abandoned_id = await _seed_abandoned_child(
            db_session,
            org_id=org_id,
            run_id=run_id,
            subject_label="dmtn-001",
            backend_job_id="arq-unverifiable",
            created_minutes_ago=600,
        )
        # An orphan too: the backend-free pass must still reap it.
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        orphan = await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=run_id,
            subject_label="sqr-112",
        )
        orphan_row = await db_session.get(SqlQueueJob, orphan.id)
        assert orphan_row is not None
        orphan_row.date_created = datetime.now(tz=UTC) - timedelta(minutes=10)
        await db_session.flush()

    _mock_ltd_products(mock_discovery, ["dmtn-001", "sqr-112"])

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    async def _explode(*args: Any, **kwargs: Any) -> None:
        raise ConnectionError("redis is unreachable")

    ctx["arq_queue"].get_job_metadata = _explode

    with capture_logs() as captured:
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
    assert any(
        entry["event"] == "Queue backend unreachable; skipping abandoned sweep"
        for entry in captured
    )

    # ``sqr-112``'s orphan was reaped, so that slug re-fans out;
    # ``dmtn-001`` keeps its unverifiable child and stays skipped.
    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert [j.kwargs["payload"]["ltd_slug"] for j in project_jobs] == [
        "sqr-112"
    ]

    async for session in db_session_dependency():
        async with session.begin():
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            spared = await queue_job_store.get(abandoned_id)
            assert spared is not None
            assert spared.status == JobStatus.queued
            assert spared.errors is None

            reaped_orphan = await queue_job_store.get(orphan.id)
            assert reaped_orphan is not None
            assert reaped_orphan.status == JobStatus.failed

            disc = await queue_job_store.get(queue_job_id)
            assert disc is not None
            assert disc.status == JobStatus.completed


@pytest.mark.asyncio
async def test_discovery_skips_tombstoned_project_slugs(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Tombstoned project slugs drop out of the fan-out candidate set.

    Issue #396 acceptance criterion: an org with a tombstoned project
    produces no ``keeper_sync_project`` child job for that resource.
    Without the filter, ``run_discovery`` would fan out a child per
    in-scope slug and the per-slug ``keeper_sync_project`` worker
    would short-circuit inside ``sync_project`` — wasted queue and DB
    work the filter avoids.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["dmtn-001", "sqr-112"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        # Tombstone one of the two in-scope project slugs.
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        tombstone_service = KeeperSyncTombstoneService(
            session=db_session,
            state_store=state_store,
            logger=_logger(),
        )
        await tombstone_service.record(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="dmtn-001",
            reason=TombstoneReason.manual_delete,
        )

    _mock_ltd_products(mock_discovery, ["dmtn-001", "sqr-112"])

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

    # Only the non-tombstoned slug is enqueued.
    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert len(project_jobs) == 1
    assert project_jobs[0].kwargs["payload"]["ltd_slug"] == "sqr-112"

    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.keeper_sync_run_id == run_id,
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
            )
            child_rows = (await session.execute(stmt)).scalars().all()
            assert len(child_rows) == 1
            assert child_rows[0].subject_label == "sqr-112"


# ---------------------------------------------------------------------------
# Lost active-job race in the discovery fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discovery_lost_race_does_not_truncate_the_fanout(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Losing the per-slug race skips that slug, not the rest of the run.

    The ``has_active_for_subject`` pre-check is stubbed to always miss,
    which is exactly what a genuine race looks like from the enqueuing
    worker's point of view: the ``SELECT`` sees no active row, but by the
    time the ``INSERT`` lands another worker (the 5-minute
    ``keeper_sync_tier`` cron, typically) holds
    ``idx_queue_jobs_keeper_sync_project_active_uq``. The lost race must
    skip that one slug — not escape to
    ``keeper_sync_run_discovery``'s outer ``except``, which would fail
    the discovery job *and* the whole run while silently dropping every
    remaining in-scope project.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, project_slugs=["dmtn-001", "sqr-112"]
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )
        # Another worker already holds the mutex for ``dmtn-001``.
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        existing = await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="dmtn-001",
            backend_job_id="arq-job-tier-cron",
        )

    async def always_miss(*args: Any, **kwargs: Any) -> bool:
        return False

    monkeypatch.setattr(QueueJobStore, "has_active_for_subject", always_miss)

    _mock_ltd_products(mock_discovery, ["dmtn-001", "sqr-112"])

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

    # ``dmtn-001`` lost the race and was skipped; ``sqr-112`` — which
    # comes after it in the per-slug loop — is still enqueued.
    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert [j.kwargs["payload"]["ltd_slug"] for j in project_jobs] == [
        "sqr-112"
    ]

    async for session in db_session_dependency():
        async with session.begin():
            # Exactly one active ``dmtn-001`` row — the other worker's —
            # survives; the lost insert left no duplicate behind.
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
                SqlQueueJob.subject_label == "dmtn-001",
                SqlQueueJob.org_id == org_id,
            )
            rows = (await session.execute(stmt)).scalars().all()
            assert len(rows) == 1
            assert rows[0].id == existing.id

            # The skipped slug is not attributed to this run, so the run's
            # child count matches what actually got enqueued and
            # ``maybe_finalise_run`` can still reach a terminal status.
            attributed = (
                (
                    await session.execute(
                        select(SqlQueueJob).where(
                            SqlQueueJob.keeper_sync_run_id == run_id,
                            SqlQueueJob.kind
                            == JobKind.keeper_sync_project.value,
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(attributed) == 1
            assert attributed[0].subject_label == "sqr-112"

            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status == KeeperSyncRunStatus.in_progress

            disc_store = QueueJobStore(session=session, logger=_logger())
            disc = await disc_store.get(queue_job_id)
            assert disc is not None
            assert disc.status == JobStatus.completed
            assert disc.progress is not None
            assert disc.progress["enqueued_count"] == 1


def _preview_service(
    session: AsyncSession, http_client: httpx.AsyncClient
) -> KeeperSyncScopePreviewService:
    """Build the preview service against a live session + HTTP client."""
    org_store = OrganizationStore(session=session, logger=_logger())
    return KeeperSyncScopePreviewService(
        org_store=org_store,
        config_service=KeeperSyncConfigService(
            org_store=org_store, logger=_logger()
        ),
        state_store=KeeperSyncStateStore(session=session, logger=_logger()),
        products_client_factory=lambda *, base_url: LtdProductsClient(
            http_client=http_client, base_url=base_url, logger=_logger()
        ),
        logger=_logger(),
    )


@pytest.mark.asyncio
async def test_preview_predicts_the_backfill_a_widened_scope_launches(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """The preview is an honest dry run of the backfill it precedes.

    PRD #667's wave workflow is preview → ``PATCH`` → launch backfill,
    which is only trustworthy if the preview reports what the backfill
    then does. Against one LTD listing and one widened scope this pins
    both halves of that promise: ``in_scope_count`` equals the run's
    ``total_count``, and ``new_slugs`` equals the set of projects the
    backfill imports for the *first* time — the in-scope slugs that had
    no keeper-sync state row when the preview ran.
    """
    async with db_session.begin():
        # The scope is already widened to both series; ``sqr-112`` has
        # been imported by an earlier wave, so it is in scope but not
        # new.
        org_id, org_slug = await _seed_org(
            db_session,
            project_slugs=[],
            project_slug_patterns=[r"sqr-\d+", r"dmtn-\d+"],
        )
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="sqr-112",
        )
        run_id = await _seed_run(db_session, org_id=org_id)
        queue_job_id = await _seed_discovery_queue_job(
            db_session, org_id=org_id, run_id=run_id
        )

    ltd_slugs = ["sqr-112", "www", "dmtn-201", "sqr-060"]
    _mock_ltd_products(mock_discovery, ltd_slugs)

    async with httpx.AsyncClient() as preview_http_client:
        async for session in db_session_dependency():
            # Two short transactions with the LTD fetch in the gap, as
            # the handler drives it — no transaction is held open across
            # the third-party call.
            service = _preview_service(session, preview_http_client)
            async with session.begin():
                plan = await service.load_plan(org_slug=org_slug)
            fetched = await service.fetch_ltd_product_slugs(plan)
            async with session.begin():
                preview = await service.report(plan=plan, ltd_slugs=fetched)
            break

    # The resolved scope follows the LTD listing order, not the config's.
    assert preview.in_scope_slugs == ["sqr-112", "dmtn-201", "sqr-060"]
    assert preview.new_slugs == ["dmtn-201", "sqr-060"]

    # Snapshot what keeper-sync already tracks, before the backfill: the
    # slugs the run fans out that are *not* in here are the ones it
    # imports for the first time.
    async for session in db_session_dependency():
        async with session.begin():
            tracked_before = {
                row.ltd_slug
                for row in await KeeperSyncStateStore(
                    session=session, logger=_logger()
                ).list_for_org(
                    org_id=org_id,
                    resource_type=ResourceType.project,
                    include_tombstoned=True,
                )
            }
        break
    assert tracked_before == {"sqr-112"}

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

    project_jobs = get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    fanned_out = [j.kwargs["payload"]["ltd_slug"] for j in project_jobs]
    assert fanned_out == preview.in_scope_slugs

    async for session in db_session_dependency():
        async with session.begin():
            run_store = KeeperSyncRunStore(session=session, logger=_logger())
            activity = await run_store.aggregate_activity(run_id=run_id)
            # The run's ``total_count`` aggregates every queue job
            # attributed to the run, and the discovery job attributes
            # itself — so the promise ``in_scope_count`` makes is about
            # the *children*, one per in-scope slug, and the run's total
            # is that plus the one discovery job.
            assert len(fanned_out) == preview.in_scope_count
            assert activity.total_count == preview.in_scope_count + 1
        break

    # The backfill's first-time imports — fanned-out slugs keeper-sync
    # was not already tracking — are exactly ``new_slugs``.
    first_time = [s for s in fanned_out if s not in tracked_before]
    assert first_time == preview.new_slugs
