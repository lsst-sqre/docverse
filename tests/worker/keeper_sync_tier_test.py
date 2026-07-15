"""Integration tests for the keeper-sync tier-cron worker functions.

The three cron functions (``keeper_sync_tier_main``,
``keeper_sync_tier_discovery``, ``keeper_sync_tier_other``) are the
steady-state reconciliation pass that keeps Docverse in step with LTD
between operator-triggered backfills (PRD #275 §"Reconciliation
cadence (steady state, run-independent)"). Each test seeds an LTD
fixture via ``respx``, calls one cron tick directly with a fake
``ctx``, and asserts on the resulting queue-job rows + arq enqueues.

The single shared invariant — verified across all three tiers — is
that tier-cron-enqueued ``queue_jobs`` rows have
``keeper_sync_run_id IS NULL`` and so do not pollute any operator-
triggered run's progress aggregation.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
import structlog
from arq.cron import CronJob
from docverse.client.models import (
    JobKind,
    KeeperSyncConfig,
    OrganizationCreate,
)
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.queue_job import SqlQueueJob
from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.services.keeper_sync_tombstone import KeeperSyncTombstoneService
from docverse.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.keeper_sync import (
    keeper_sync_tier_discovery,
    keeper_sync_tier_main,
    keeper_sync_tier_other,
)
from docverse.worker.main import KeeperSyncWorkerSettings
from tests.support.arq_testing import get_jobs_by_name, register_queue
from tests.worker.conftest import make_worker_ctx

LTD_BASE = "https://keeper.lsst.codes"

#: ``date_rebuilt`` for the canonical ``main`` edition fixture. Used by
#: tests that need to compare LTD's published timestamp against state.
_FIXTURE_MAIN_DATE_REBUILT = datetime(2026, 4, 30, 18, 30, tzinfo=UTC)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


class _StateStoreCallRecorder:
    """Records ``KeeperSyncStateStore`` method calls per tier-cron tick.

    The batched-read refactor (issue #310) replaces N per-edition
    ``get`` round-trips with one ``list_for_org`` call. The recorder is
    installed via ``monkeypatch`` on the class, so every store created
    by the factory shares the same counters.
    """

    def __init__(self) -> None:
        self.list_for_org_calls = 0
        self.get_calls = 0
        self.list_for_org_edition_calls = 0


def _install_state_store_recorder(
    monkeypatch: pytest.MonkeyPatch,
) -> _StateStoreCallRecorder:
    recorder = _StateStoreCallRecorder()
    real_list = KeeperSyncStateStore.list_for_org
    real_get = KeeperSyncStateStore.get

    async def counting_list(
        self: KeeperSyncStateStore, **kwargs: Any
    ) -> list[Any]:
        recorder.list_for_org_calls += 1
        if kwargs.get("resource_type") is ResourceType.edition:
            recorder.list_for_org_edition_calls += 1
        return await real_list(self, **kwargs)

    async def counting_get(self: KeeperSyncStateStore, **kwargs: Any) -> Any:
        recorder.get_calls += 1
        return await real_get(self, **kwargs)

    monkeypatch.setattr(KeeperSyncStateStore, "list_for_org", counting_list)
    monkeypatch.setattr(KeeperSyncStateStore, "get", counting_get)
    return recorder


async def _seed_org(
    db_session: AsyncSession,
    *,
    slug: str = "ks-tier",
    project_slugs: list[str] | str = "*",
    enabled: bool = True,
) -> tuple[int, str]:
    """Seed an org with the given keeper-sync config."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"Tier {slug}",
            base_domain=f"{slug}.example.com",
        )
    )
    await org_store.update_keeper_sync_config(
        slug=org.slug,
        config=KeeperSyncConfig(
            enabled=enabled,
            project_slugs=project_slugs,  # type: ignore[arg-type]
        ),
    )
    return org.id, org.slug


def _stub_products(
    mock_discovery: respx.Router, slugs: list[str], *, base_url: str = LTD_BASE
) -> None:
    """Stub ``GET /products/`` to return a flat list of product URLs."""
    products = [f"{base_url}/products/{s}/" for s in slugs]
    mock_discovery.get(f"{base_url}/products/").mock(
        return_value=httpx.Response(
            200,
            content=json.dumps({"products": products}).encode(),
            headers={"content-type": "application/json"},
        )
    )


def _stub_editions_listing(
    mock_discovery: respx.Router,
    *,
    product_slug: str,
    edition_ids: list[int],
    base_url: str = LTD_BASE,
) -> None:
    """Stub ``GET /products/<slug>/editions/`` to return edition URLs.

    LTD lists editions newest-first (descending by id). ``main`` is
    typically the oldest edition for a product, so it appears at the
    end of the listing; ``tier_main`` iterates in reverse to hit it
    first. Tests should pass ``edition_ids`` in newest-first order to
    mirror LTD's behavior.
    """
    urls = [f"{base_url}/editions/{i}" for i in edition_ids]
    mock_discovery.get(f"{base_url}/products/{product_slug}/editions/").mock(
        return_value=httpx.Response(200, json={"editions": urls})
    )


def _stub_edition(
    mock_discovery: respx.Router,
    *,
    edition_id: int,
    slug: str,
    date_rebuilt: datetime | None = None,
    has_build: bool = True,
    base_url: str = LTD_BASE,
) -> None:
    payload: dict[str, Any] = {
        "self_url": f"{base_url}/editions/{edition_id}",
        "product_url": f"{base_url}/products/pipelines",
        "build_url": (
            f"{base_url}/builds/{edition_id * 100}" if has_build else None
        ),
        "published_url": f"{base_url}/{slug}/",
        "slug": slug,
        "title": slug,
        "date_created": "2024-01-01T00:00:00+00:00",
        "date_rebuilt": (
            date_rebuilt.isoformat() if date_rebuilt is not None else None
        ),
        "date_ended": None,
        "tracked_refs": ["main" if slug == "main" else slug],
        "mode": "git_refs",
        "pending_rebuild": False,
    }
    mock_discovery.get(f"{base_url}/editions/{edition_id}").mock(
        return_value=httpx.Response(200, json=payload)
    )


def _make_ctx(http_client: httpx.AsyncClient) -> dict[str, Any]:
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    return make_worker_ctx(http_client=http_client, arq_queue=mock_arq)


async def _seed_state(
    db_session: AsyncSession,
    *,
    org_id: int,
    resource_type: ResourceType,
    ltd_id: int | None,
    ltd_slug: str,
    docverse_id: int | None = 99,
    date_last_synced: datetime | None = None,
    date_rebuilt_seen: datetime | None = None,
) -> None:
    state_store = KeeperSyncStateStore(session=db_session, logger=_logger())
    await state_store.upsert(
        org_id=org_id,
        resource_type=resource_type,
        ltd_id=ltd_id,
        ltd_slug=ltd_slug,
        docverse_id=docverse_id,
        date_last_synced=date_last_synced,
        date_rebuilt_seen=date_rebuilt_seen,
    )


async def _seed_tombstone(
    db_session: AsyncSession,
    *,
    org_id: int,
    resource_type: ResourceType,
    ltd_id: int | None = None,
    ltd_slug: str | None = None,
    reason: TombstoneReason = TombstoneReason.manual_delete,
) -> None:
    """Stamp a tombstone on the matching ``keeper_sync_state`` row.

    Uses the same service entrypoint the production deletion paths
    will use (PRD #332 §"Centralized edition soft-delete"), so the
    tier-cron filter behavior is exercised against rows produced the
    same way operators and lifecycle workers produce them.
    """
    state_store = KeeperSyncStateStore(session=db_session, logger=_logger())
    service = KeeperSyncTombstoneService(
        session=db_session, state_store=state_store, logger=_logger()
    )
    await service.record(
        org_id=org_id,
        resource_type=resource_type,
        ltd_id=ltd_id,
        ltd_slug=ltd_slug,
        reason=reason,
    )


# ---------------------------------------------------------------------------
# tier_main
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tier_main_enqueues_when_ltd_rebuilt_advanced(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """LTD's ``date_rebuilt`` is newer than state — enqueue refresh.

    Also locks the no-run-attribution invariant: the resulting
    ``queue_jobs`` row has ``keeper_sync_run_id IS NULL`` and the arq
    payload has no ``run_id`` key.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-main-1", project_slugs=["pipelines"]
        )
        # State row records an older date_rebuilt — LTD has moved on.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            date_rebuilt_seen=_FIXTURE_MAIN_DATE_REBUILT - timedelta(hours=2),
        )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    arq_queue = ctx["arq_queue"]
    children = get_jobs_by_name(
        arq_queue, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert len(children) == 1
    payload = children[0].kwargs["payload"]
    assert payload["ltd_slug"] == "pipelines"
    assert payload["org_id"] == org_id
    # Key invariant: tier-cron payloads carry no run attribution.
    assert "run_id" not in payload

    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
                SqlQueueJob.org_id == org_id,
            )
            rows = (await session.execute(stmt)).scalars().all()
            assert len(rows) == 1
            row = rows[0]
            # Acceptance criterion: tier-cron-enqueued queue_jobs rows
            # have keeper_sync_run_id IS NULL.
            assert row.keeper_sync_run_id is None
            assert row.subject_label == "pipelines"
            assert row.backend_job_id is not None


@pytest.mark.asyncio
async def test_tier_main_skips_when_state_matches_ltd(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """LTD's ``date_rebuilt`` equals state — no enqueue."""
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-main-2", project_slugs=["pipelines"]
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            # Identical to fixture: nothing for tier_main to chase.
            date_rebuilt_seen=_FIXTURE_MAIN_DATE_REBUILT,
        )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    arq_queue = ctx["arq_queue"]
    assert (
        get_jobs_by_name(
            arq_queue,
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )


@pytest.mark.asyncio
async def test_tier_main_enqueues_when_state_missing(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """No state row for the main edition — discovery has not yet run."""
    async with db_session.begin():
        await _seed_org(
            db_session, slug="ks-tier-main-3", project_slugs=["pipelines"]
        )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[2, 1]
    )
    # tier_main walks the URL list in reverse looking for slug=="main".
    # Fixture orders [2, 1] so the reverse iteration hits 1 first.
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    arq_queue = ctx["arq_queue"]
    children = get_jobs_by_name(
        arq_queue, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    assert len(children) == 1
    assert children[0].kwargs["payload"]["ltd_slug"] == "pipelines"


@pytest.mark.asyncio
async def test_tier_main_caches_main_edition_pointer_after_walk(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """First successful resolve writes the cached pointer onto project state.

    Locks the cold-cache half of the contract: after ``_find_main_edition``
    walks the URL list to locate ``main``, the project-resource state row
    carries ``main_edition_url`` / ``main_edition_ltd_id`` annotations so
    the next tick can skip the walk.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-main-cache-cold",
            project_slugs=["pipelines"],
        )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[2, 1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    _stub_edition(
        mock_discovery,
        edition_id=2,
        slug="u-jsick-feature",
        date_rebuilt=datetime(2026, 4, 29, tzinfo=UTC),
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            project_state = await state_store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug="pipelines",
            )
    assert project_state is not None
    assert project_state.annotations is not None
    assert project_state.annotations["main_edition_ltd_id"] == 1
    assert (
        project_state.annotations["main_edition_url"]
        == f"{LTD_BASE}/editions/1"
    )


@pytest.mark.asyncio
async def test_tier_main_uses_cached_pointer_to_skip_walk(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Cached pointer resolves to ``main`` — only the cached fetch fires.

    Acceptance criterion: in the steady-state common case
    ``_find_main_edition`` issues exactly **one** LTD HTTP call per
    project per tick. Verified by ``respx`` route counters: the
    editions-listing endpoint is never hit, only the cached
    ``/editions/1`` URL is.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-main-cache-hit",
            project_slugs=["pipelines"],
        )
        # Project state seeded with a cached pointer at ltd_id=1.
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
            docverse_id=99,
            annotations={
                "main_edition_ltd_id": 1,
                "main_edition_url": f"{LTD_BASE}/editions/1",
            },
        )
        # Edition state lags LTD: triggers an enqueue on cache hit.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            date_rebuilt_seen=_FIXTURE_MAIN_DATE_REBUILT - timedelta(hours=2),
        )

    _stub_products(mock_discovery, ["pipelines"])
    listing_route = mock_discovery.get(
        f"{LTD_BASE}/products/pipelines/editions/"
    ).mock(return_value=httpx.Response(200, json={"editions": []}))
    edition_route = mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(
            200,
            json={
                "self_url": f"{LTD_BASE}/editions/1",
                "product_url": f"{LTD_BASE}/products/pipelines",
                "build_url": f"{LTD_BASE}/builds/100",
                "published_url": f"{LTD_BASE}/main/",
                "slug": "main",
                "title": "main",
                "date_created": "2024-01-01T00:00:00+00:00",
                "date_rebuilt": _FIXTURE_MAIN_DATE_REBUILT.isoformat(),
                "date_ended": None,
                "tracked_refs": ["main"],
                "mode": "git_refs",
                "pending_rebuild": False,
            },
        )
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    # The cached path bypasses the URL listing entirely.
    assert listing_route.call_count == 0
    assert edition_route.call_count == 1

    # And the lagging edition state still triggers an enqueue.
    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert len(children) == 1


@pytest.mark.asyncio
async def test_tier_main_falls_back_to_walk_on_cached_404(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Cached pointer 404s — walk runs, annotation is overwritten."""
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-main-cache-404",
            project_slugs=["pipelines"],
        )
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
            docverse_id=99,
            annotations={
                "main_edition_ltd_id": 99,
                "main_edition_url": f"{LTD_BASE}/editions/99",
            },
        )

    _stub_products(mock_discovery, ["pipelines"])
    cached_route = mock_discovery.get(f"{LTD_BASE}/editions/99").mock(
        return_value=httpx.Response(404)
    )
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    assert cached_route.call_count == 1

    async for session in db_session_dependency():
        async with session.begin():
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            project_state = await state_store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug="pipelines",
            )
    assert project_state is not None
    assert project_state.annotations is not None
    assert project_state.annotations["main_edition_ltd_id"] == 1
    assert (
        project_state.annotations["main_edition_url"]
        == f"{LTD_BASE}/editions/1"
    )


@pytest.mark.asyncio
async def test_tier_main_falls_back_to_walk_on_slug_mismatch(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Cached edition exists but is no longer ``main`` — walk + rewrite."""
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-main-cache-slug",
            project_slugs=["pipelines"],
        )
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
            docverse_id=99,
            annotations={
                "main_edition_ltd_id": 99,
                "main_edition_url": f"{LTD_BASE}/editions/99",
            },
        )

    _stub_products(mock_discovery, ["pipelines"])
    # Cached edition still exists, but its slug has been changed by a
    # maintainer: the cache is stale and must be rewritten.
    _stub_edition(
        mock_discovery,
        edition_id=99,
        slug="renamed-edition",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            project_state = await state_store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug="pipelines",
            )
    assert project_state is not None
    assert project_state.annotations is not None
    assert project_state.annotations["main_edition_ltd_id"] == 1
    assert (
        project_state.annotations["main_edition_url"]
        == f"{LTD_BASE}/editions/1"
    )


@pytest.mark.asyncio
async def test_tier_main_polls_only_hot_and_due_dormant_projects(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Mixed cohort: hot, dormant-skippable, and dormant-due projects.

    Acceptance criterion (issue #312): on a single tier_main tick the
    cron must call ``_find_main_edition`` only for projects the
    planner declares hot or dormant-due. The dormant-skippable project
    keeps the same cached pointer it started with and the LTD edition
    endpoint for it is never hit, verified by ``respx`` route counters.
    """
    now = datetime.now(tz=UTC)
    fresh_main_rebuilt = now - timedelta(minutes=15)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-main-cohort",
            project_slugs=["hot-proj", "skip-proj", "due-proj"],
        )
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        # Hot: rebuilt 2 days ago. Planner returns True regardless of
        # any last-polled annotation.
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="hot-proj",
            docverse_id=1,
            date_rebuilt_seen=now - timedelta(days=2),
            annotations={
                "main_edition_ltd_id": 1,
                "main_edition_url": f"{LTD_BASE}/editions/1",
            },
        )
        # Dormant-skippable: rebuilt 30 days ago, polled 1h ago — well
        # within the 24h dormant interval.
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="skip-proj",
            docverse_id=2,
            date_rebuilt_seen=now - timedelta(days=30),
            annotations={
                "main_edition_ltd_id": 2,
                "main_edition_url": f"{LTD_BASE}/editions/2",
                "date_main_last_polled": (
                    now - timedelta(hours=1)
                ).isoformat(),
            },
        )
        # Dormant-due: rebuilt 30 days ago, polled 49h ago — past the
        # full 48h jittered dormant ceiling (24h interval + up to 24h
        # slug-keyed jitter), so the planner re-polls regardless of
        # how the LTD slug hashes.
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="due-proj",
            docverse_id=3,
            date_rebuilt_seen=now - timedelta(days=30),
            annotations={
                "main_edition_ltd_id": 3,
                "main_edition_url": f"{LTD_BASE}/editions/3",
                "date_main_last_polled": (
                    now - timedelta(hours=49)
                ).isoformat(),
            },
        )
        # Edition state for each polled project: date_rebuilt_seen older
        # than what LTD will return so should_refresh_main_edition
        # triggers an enqueue. Skip-proj's edition row is never read.
        for ltd_id in (1, 2, 3):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug="main",
                date_rebuilt_seen=fresh_main_rebuilt - timedelta(hours=2),
            )

    _stub_products(mock_discovery, ["hot-proj", "skip-proj", "due-proj"])
    # Per-project respx routes pinned to the cached edition URL each
    # project advertises in its annotations. Cache-hit path means the
    # listings endpoints are never touched.
    hot_route = mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(
            200,
            json={
                "self_url": f"{LTD_BASE}/editions/1",
                "product_url": f"{LTD_BASE}/products/hot-proj",
                "build_url": f"{LTD_BASE}/builds/100",
                "published_url": f"{LTD_BASE}/main/",
                "slug": "main",
                "title": "main",
                "date_created": "2024-01-01T00:00:00+00:00",
                "date_rebuilt": fresh_main_rebuilt.isoformat(),
                "date_ended": None,
                "tracked_refs": ["main"],
                "mode": "git_refs",
                "pending_rebuild": False,
            },
        )
    )
    skip_route = mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(
            200,
            json={
                "self_url": f"{LTD_BASE}/editions/2",
                "product_url": f"{LTD_BASE}/products/skip-proj",
                "build_url": f"{LTD_BASE}/builds/200",
                "published_url": f"{LTD_BASE}/main/",
                "slug": "main",
                "title": "main",
                "date_created": "2024-01-01T00:00:00+00:00",
                "date_rebuilt": fresh_main_rebuilt.isoformat(),
                "date_ended": None,
                "tracked_refs": ["main"],
                "mode": "git_refs",
                "pending_rebuild": False,
            },
        )
    )
    due_route = mock_discovery.get(f"{LTD_BASE}/editions/3").mock(
        return_value=httpx.Response(
            200,
            json={
                "self_url": f"{LTD_BASE}/editions/3",
                "product_url": f"{LTD_BASE}/products/due-proj",
                "build_url": f"{LTD_BASE}/builds/300",
                "published_url": f"{LTD_BASE}/main/",
                "slug": "main",
                "title": "main",
                "date_created": "2024-01-01T00:00:00+00:00",
                "date_rebuilt": fresh_main_rebuilt.isoformat(),
                "date_ended": None,
                "tracked_refs": ["main"],
                "mode": "git_refs",
                "pending_rebuild": False,
            },
        )
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    # Acceptance: LTD HTTP fired exactly for the polled cohort.
    assert hot_route.call_count == 1
    assert skip_route.call_count == 0
    assert due_route.call_count == 1

    # Both polled projects' main editions advance state, so each
    # enqueues one keeper_sync_project child. The skipped project does
    # not.
    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    enqueued_slugs = {c.kwargs["payload"]["ltd_slug"] for c in children}
    assert enqueued_slugs == {"hot-proj", "due-proj"}

    # Acceptance: date_main_last_polled is updated on every polled
    # visit. Verified via the project state row's annotations.
    async for session in db_session_dependency():
        async with session.begin():
            store = KeeperSyncStateStore(session=session, logger=_logger())
            for slug, expected_polled in (
                ("hot-proj", True),
                ("due-proj", True),
                ("skip-proj", False),
            ):
                row = await store.get(
                    org_id=org_id,
                    resource_type=ResourceType.project,
                    ltd_slug=slug,
                )
                assert row is not None
                assert row.annotations is not None
                if expected_polled:
                    # Stamp is "now-ish" (within a small slop), proving
                    # the polled visit overwrote the stale value.
                    raw = row.annotations["date_main_last_polled"]
                    assert isinstance(raw, str)
                    stamped = datetime.fromisoformat(raw)
                    assert (now - stamped) < timedelta(minutes=5)
                else:
                    # Skipped project's annotation reflects the seeded
                    # 1h-ago value, untouched by this tick.
                    raw = row.annotations["date_main_last_polled"]
                    assert isinstance(raw, str)
                    stamped = datetime.fromisoformat(raw)
                    assert timedelta(minutes=30) < (now - stamped)


@pytest.mark.asyncio
async def test_tier_main_records_polled_annotation_on_ltd_error(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """LtdClientError on a dormant-due project still updates the annotation.

    Otherwise a flaky LTD endpoint would defeat the dormancy rate
    limiter — the project would re-poll on every 5-min tick instead of
    waiting out the dormant interval. The error is logged and the next
    project continues, but the polled timestamp advances either way.
    """
    now = datetime.now(tz=UTC)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-main-error",
            project_slugs=["flaky-proj"],
        )
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="flaky-proj",
            docverse_id=1,
            date_rebuilt_seen=now - timedelta(days=30),
            annotations={
                "main_edition_ltd_id": 9,
                "main_edition_url": f"{LTD_BASE}/editions/9",
                "date_main_last_polled": (
                    now - timedelta(hours=49)
                ).isoformat(),
            },
        )

    _stub_products(mock_discovery, ["flaky-proj"])
    # The cached edition fetch fails, then the walk also fails.
    mock_discovery.get(f"{LTD_BASE}/editions/9").mock(
        return_value=httpx.Response(500)
    )
    mock_discovery.get(f"{LTD_BASE}/products/flaky-proj/editions/").mock(
        return_value=httpx.Response(500)
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            store = KeeperSyncStateStore(session=session, logger=_logger())
            row = await store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug="flaky-proj",
            )
    assert row is not None
    assert row.annotations is not None
    raw = row.annotations["date_main_last_polled"]
    assert isinstance(raw, str)
    stamped = datetime.fromisoformat(raw)
    # Update fired during this tick, not 49h ago.
    assert (now - stamped) < timedelta(minutes=5)


@pytest.mark.asyncio
async def test_tier_main_skips_disabled_orgs(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An org with ``keeper_sync_config.enabled=False`` is left alone."""
    async with db_session.begin():
        await _seed_org(
            db_session,
            slug="ks-tier-disabled",
            project_slugs=["pipelines"],
            enabled=False,
        )

    # LTD should not be queried at all when no orgs are enabled, but
    # the cron tolerates either outcome — assert by counting enqueues.

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )


@pytest.mark.asyncio
async def test_tier_main_skips_when_active_keeper_sync_project_exists(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An active ``keeper_sync_project`` row blocks tier_main's enqueue.

    Reproduces the QA race: a prior unattributed ``keeper_sync_project``
    is still queued (or a discovery fan-out attributed one). Tier_main's
    pre-check must skip rather than enqueue a duplicate, even when
    ``should_refresh_main_edition`` says LTD has advanced — the in-
    flight job will catch up.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-main-mux", project_slugs=["pipelines"]
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            date_rebuilt_seen=_FIXTURE_MAIN_DATE_REBUILT - timedelta(hours=2),
        )
        # Pre-seed an active ``keeper_sync_project`` row for the same
        # subject_label. tier_main's pre-check must observe this and
        # skip the enqueue.
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        existing = await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label="pipelines",
            backend_job_id="arq-job-prior-tier",
        )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    # No new arq enqueues — the pre-check fired.
    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )

    # Exactly one ``pipelines`` row in the DB — the original. No duplicate.
    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.keeper_sync_project.value,
                SqlQueueJob.subject_label == "pipelines",
                SqlQueueJob.org_id == org_id,
            )
            rows = (await session.execute(stmt)).scalars().all()
    assert len(rows) == 1
    assert rows[0].id == existing.id


@pytest.mark.asyncio
async def test_tier_main_skips_tombstoned_project_slug(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A tombstoned project state row keeps its slug out of the candidate set.

    Issue #396: the tier crons must filter tombstoned state rows out
    of their candidate set up front so they do not enqueue child jobs
    that ``sync_project`` would only short-circuit. The non-tombstoned
    slug in the same org still gets its enqueue, locking the
    "non-tombstoned resources are still discovered" half of the
    acceptance criterion.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-main-tomb",
            project_slugs=["pipelines", "live-proj"],
        )
        # Tombstoned project: the per-slug pre-check must skip without
        # touching LTD.
        await _seed_tombstone(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
        )
        # Live project: the main edition state lags LTD so the normal
        # enqueue path fires.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            ltd_slug="main",
            date_rebuilt_seen=_FIXTURE_MAIN_DATE_REBUILT - timedelta(hours=2),
        )

    _stub_products(mock_discovery, ["pipelines", "live-proj"])
    # Live-proj's main edition listing + edition payload — ``pipelines``
    # is tombstoned and must never be polled, so no stubs for it.
    _stub_editions_listing(
        mock_discovery, product_slug="live-proj", edition_ids=[2]
    )
    _stub_edition(
        mock_discovery,
        edition_id=2,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    slugs = {c.kwargs["payload"]["ltd_slug"] for c in children}
    assert slugs == {"live-proj"}


@pytest.mark.asyncio
async def test_tier_main_skips_tombstoned_main_edition(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A tombstoned main-edition state row keeps the project from enqueueing.

    Issue #396: even when the project itself is not tombstoned, a
    tombstoned main edition must not produce a ``keeper_sync_project``
    enqueue — the resulting ``sync_edition`` would only short-circuit.
    The seeded ``date_rebuilt_seen`` would otherwise lag LTD and trip
    :func:`should_refresh_main_edition` into enqueueing, so the only
    reason no enqueue lands is the tombstone filter.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-main-edition-tomb",
            project_slugs=["pipelines"],
        )
        # Edition state lags LTD but is tombstoned — must not enqueue.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            date_rebuilt_seen=_FIXTURE_MAIN_DATE_REBUILT - timedelta(hours=2),
        )
        await _seed_tombstone(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            reason=TombstoneReason.lifecycle_delete,
        )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )


# ---------------------------------------------------------------------------
# tier_discovery
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tier_discovery_enqueues_when_project_state_missing(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """No project state row — enqueue immediately and skip edition walk."""
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-disc-1", project_slugs=["pipelines"]
        )

    _stub_products(mock_discovery, ["pipelines"])
    # No editions listing stub — the project-state short-circuit must
    # skip the edition walk entirely.

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert len(children) == 1
    assert children[0].kwargs["payload"]["ltd_slug"] == "pipelines"

    async for session in db_session_dependency():
        async with session.begin():
            row = (
                await session.execute(
                    select(SqlQueueJob).where(SqlQueueJob.org_id == org_id)
                )
            ).scalar_one()
            assert row.keeper_sync_run_id is None


@pytest.mark.asyncio
async def test_tier_discovery_enqueues_when_edition_state_missing(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Project known, but a child edition has no state row — enqueue."""
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-disc-2", project_slugs=["pipelines"]
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_id=None,
            ltd_slug="pipelines",
        )
        # Edition 1 has state, edition 2 does not.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            date_rebuilt_seen=_FIXTURE_MAIN_DATE_REBUILT,
        )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[2, 1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    _stub_edition(
        mock_discovery,
        edition_id=2,
        slug="u-jsick-feature",
        date_rebuilt=datetime(2026, 4, 29, tzinfo=UTC),
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    # Single enqueue covers the project; the unseen edition gets
    # imported as a side effect of ``KeeperSyncService.sync_project``.
    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert len(children) == 1


@pytest.mark.asyncio
async def test_tier_discovery_batches_edition_state_lookups(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One ``list_for_org`` call per project regardless of edition count.

    Replaces the prior N-per-edition ``get`` round-trips so a project
    with 5 editions issues exactly one batched read for the
    edition-state dictionary plus one ``get`` for the project-state
    short-circuit. Locks the new contract from issue #310.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-disc-batch", project_slugs=["pipelines"]
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_id=None,
            ltd_slug="pipelines",
        )
        # Every LTD edition has a state row — ``is_unknown_resource``
        # returns ``False`` for each, so the loop must consult every
        # one before deciding not to enqueue. With per-edition ``get``
        # this would be 5 round-trips; with the batched read it is 1.
        for ltd_id, slug in (
            (1, "main"),
            (2, "branch-a"),
            (3, "branch-b"),
            (4, "branch-c"),
            (5, "branch-d"),
        ):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug=slug,
            )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[5, 4, 3, 2, 1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    for edition_id, slug in (
        (2, "branch-a"),
        (3, "branch-b"),
        (4, "branch-c"),
        (5, "branch-d"),
    ):
        _stub_edition(
            mock_discovery,
            edition_id=edition_id,
            slug=slug,
            date_rebuilt=datetime(2026, 4, 29, tzinfo=UTC),
        )

    recorder = _install_state_store_recorder(monkeypatch)
    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    # Fully-known project — no enqueue.
    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )
    # Two ``get`` calls: one for the dormancy planner / project-state
    # short-circuit (now shared between :func:`should_poll_for_tier`
    # and :func:`_project_needs_discovery`), and one inside
    # :func:`_record_tier_polled` to merge with prior annotations
    # before stamping ``date_discovery_last_polled``. Two
    # ``list_for_org`` calls: one for the project-tombstone filter
    # (issue #396) and one for the org-wide edition lookup hoisted
    # out of :func:`_project_needs_discovery` (see
    # :func:`test_tier_discovery_batches_edition_state_lookups_across_slugs`
    # for the cross-slug lock). Five-edition fixture still proves the
    # count is independent of edition cardinality.
    assert recorder.get_calls == 2
    assert recorder.list_for_org_calls == 2


@pytest.mark.asyncio
async def test_tier_discovery_batches_edition_state_lookups_across_slugs(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One ``list_for_org(resource_type=edition)`` per tick, not per slug.

    Locks the org-wide edition-state read out of the per-slug loop.
    Before this contract, ``_project_needs_discovery`` issued one
    ``list_for_org`` per in-scope slug, so 1500 in-scope projects ran
    1500 unscoped scans of ~15 000 rows each. After, the cron loads
    the org's edition rows once before the loop and consults the
    indexed dict in memory per slug.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-disc-batch-org",
            project_slugs=["proj-a", "proj-b", "proj-c"],
        )
        for slug in ("proj-a", "proj-b", "proj-c"):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_id=None,
                ltd_slug=slug,
            )
        # Every LTD edition has a state row, so each project's
        # ``_project_needs_discovery`` walks the full per-LTD-id dict
        # before deciding not to enqueue. Without the hoist this is
        # three unscoped ``list_for_org`` calls; with the hoist, one.
        for ltd_id, slug in (
            (1, "main-a"),
            (2, "main-b"),
            (3, "main-c"),
        ):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug=slug,
            )

    _stub_products(mock_discovery, ["proj-a", "proj-b", "proj-c"])
    for slug, edition_id in (
        ("proj-a", 1),
        ("proj-b", 2),
        ("proj-c", 3),
    ):
        _stub_editions_listing(
            mock_discovery, product_slug=slug, edition_ids=[edition_id]
        )
        _stub_edition(
            mock_discovery,
            edition_id=edition_id,
            slug=f"main-{slug[-1]}",
            date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
        )

    recorder = _install_state_store_recorder(monkeypatch)
    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    # Fully-known: no enqueues across all three slugs.
    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )
    # The acceptance criterion: exactly one
    # ``list_for_org(resource_type=edition)`` per tier_discovery
    # tick, regardless of how many in-scope slugs the tick processes.
    assert recorder.list_for_org_edition_calls == 1


@pytest.mark.asyncio
async def test_tier_discovery_skips_fully_known_project(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Every LTD resource has a state row — no enqueue."""
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-disc-3", project_slugs=["pipelines"]
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_id=None,
            ltd_slug="pipelines",
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
        )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )


@pytest.mark.asyncio
async def test_tier_discovery_polls_only_hot_and_due_dormant_projects(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Mixed cohort: hot, dormant-skippable, and dormant-due projects.

    Acceptance criterion: on a single tier_discovery tick the cron must
    issue ``GET /products/<slug>/editions/`` only for projects the
    planner declares hot or dormant-due. The dormant-skippable
    project's listing endpoint is never hit, verified by ``respx``
    route counters; its ``date_discovery_last_polled`` annotation is
    untouched. Polled projects (hot + due) write a fresh stamp
    regardless of whether ``_project_needs_discovery`` decided to
    enqueue, matching :func:`_record_tier_polled`'s clamp shape.
    """
    now = datetime.now(tz=UTC)
    fresh_rebuild = now - timedelta(days=2)
    old_rebuild = now - timedelta(days=30)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-disc-cohort",
            project_slugs=["hot-proj", "skip-proj", "due-proj"],
        )
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        # Hot: rebuilt 2 days ago. Planner returns True regardless of
        # any last-polled annotation; LTD HTTP fires.
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="hot-proj",
            docverse_id=1,
            date_rebuilt_seen=fresh_rebuild,
        )
        # Dormant-skippable: rebuilt 30 days ago, polled 1h ago — well
        # within the 24h dormant interval. Planner skips, so the
        # listing endpoint must not be touched.
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="skip-proj",
            docverse_id=2,
            date_rebuilt_seen=old_rebuild,
            annotations={
                "date_discovery_last_polled": (
                    now - timedelta(hours=1)
                ).isoformat()
            },
        )
        # Dormant-due: rebuilt 30 days ago, polled 49h ago — past the
        # full 48h jittered dormant ceiling (24h interval + up to 24h
        # slug-keyed jitter), so the planner re-polls regardless of
        # how the LTD slug hashes.
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="due-proj",
            docverse_id=3,
            date_rebuilt_seen=old_rebuild,
            annotations={
                "date_discovery_last_polled": (
                    now - timedelta(hours=49)
                ).isoformat()
            },
        )
        # No edition state for any project — polled projects enqueue
        # because LTD lists an edition we have not seen.

    _stub_products(mock_discovery, ["hot-proj", "skip-proj", "due-proj"])
    hot_listing = mock_discovery.get(
        f"{LTD_BASE}/products/hot-proj/editions/"
    ).mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/1"]}
        )
    )
    skip_listing = mock_discovery.get(
        f"{LTD_BASE}/products/skip-proj/editions/"
    ).mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/2"]}
        )
    )
    due_listing = mock_discovery.get(
        f"{LTD_BASE}/products/due-proj/editions/"
    ).mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/3"]}
        )
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    _stub_edition(
        mock_discovery,
        edition_id=2,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    _stub_edition(
        mock_discovery,
        edition_id=3,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    # Acceptance: LTD HTTP fired exactly for the polled cohort.
    assert hot_listing.call_count == 1
    assert skip_listing.call_count == 0
    assert due_listing.call_count == 1

    # Hot and due both have unseen editions, so each enqueues one
    # ``keeper_sync_project`` child. Skip-proj is not visited.
    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    enqueued_slugs = {c.kwargs["payload"]["ltd_slug"] for c in children}
    assert enqueued_slugs == {"hot-proj", "due-proj"}

    # Acceptance: ``date_discovery_last_polled`` is stamped on every
    # polled visit, regardless of enqueue. The skipped project's
    # annotation reflects its seeded value, untouched by this tick.
    async for session in db_session_dependency():
        async with session.begin():
            store = KeeperSyncStateStore(session=session, logger=_logger())
            for slug, expected_polled in (
                ("hot-proj", True),
                ("due-proj", True),
                ("skip-proj", False),
            ):
                row = await store.get(
                    org_id=org_id,
                    resource_type=ResourceType.project,
                    ltd_slug=slug,
                )
                assert row is not None
                assert row.annotations is not None
                raw = row.annotations["date_discovery_last_polled"]
                assert isinstance(raw, str)
                stamped = datetime.fromisoformat(raw)
                if expected_polled:
                    assert (now - stamped) < timedelta(minutes=5)
                else:
                    assert timedelta(minutes=30) < (now - stamped)


@pytest.mark.asyncio
async def test_tier_discovery_records_polled_annotation_on_ltd_error(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """LtdClientError still updates ``date_discovery_last_polled``.

    Mirrors :func:`test_tier_main_records_polled_annotation_on_ltd_error`:
    a flaky LTD endpoint must not defeat the dormancy rate-limiter for
    the discovery tier. The error is logged and the loop continues to
    the next project, but the polled timestamp advances either way so
    the project waits out ``TIER_DISCOVERY_DORMANT_INTERVAL`` instead
    of re-polling on every 30-min tick.
    """
    now = datetime.now(tz=UTC)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-disc-error",
            project_slugs=["flaky-proj"],
        )
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="flaky-proj",
            docverse_id=1,
            date_rebuilt_seen=now - timedelta(days=30),
            annotations={
                "date_discovery_last_polled": (
                    now - timedelta(hours=49)
                ).isoformat()
            },
        )

    _stub_products(mock_discovery, ["flaky-proj"])
    mock_discovery.get(f"{LTD_BASE}/products/flaky-proj/editions/").mock(
        return_value=httpx.Response(500)
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            store = KeeperSyncStateStore(session=session, logger=_logger())
            row = await store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug="flaky-proj",
            )
    assert row is not None
    assert row.annotations is not None
    raw = row.annotations["date_discovery_last_polled"]
    assert isinstance(raw, str)
    stamped = datetime.fromisoformat(raw)
    # Update fired during this tick, not 49h ago.
    assert (now - stamped) < timedelta(minutes=5)


@pytest.mark.asyncio
async def test_tier_discovery_skips_tombstoned_project_slug(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A tombstoned project state row keeps its slug out of the candidate set.

    Issue #396 acceptance criterion: an org with a tombstoned project
    produces no ``keeper_sync_project`` child jobs for that resource.
    The non-tombstoned slug in the same org still enqueues — its
    project has no state row, so ``_project_needs_discovery``'s
    cheap-path short-circuit fires.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-disc-tomb",
            project_slugs=["pipelines", "live-proj"],
        )
        await _seed_tombstone(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
        )

    _stub_products(mock_discovery, ["pipelines", "live-proj"])
    # No editions stub for ``pipelines`` — the per-slug pre-check
    # must skip without touching LTD.

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    slugs = {c.kwargs["payload"]["ltd_slug"] for c in children}
    assert slugs == {"live-proj"}


@pytest.mark.asyncio
async def test_tier_discovery_does_not_treat_tombstoned_edition_as_unknown(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A tombstoned edition counts as known, not unknown.

    Issue #396: without the filter, ``_project_needs_discovery`` would
    consult the org-wide edition-state dict, find no entry for the
    tombstoned edition (default ``include_tombstoned=False`` filters
    it out), call :func:`is_unknown_resource` which returns True for a
    ``None`` state, and enqueue ``keeper_sync_project`` — which would
    then iterate LTD editions, hit the tombstoned edition, and have
    ``sync_edition`` short-circuit. The fix keeps tombstoned editions
    visible to the dict so they read as known, not unknown.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-disc-tomb-ed",
            project_slugs=["pipelines"],
        )
        # Project state exists — no cheap-path enqueue.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_id=None,
            ltd_slug="pipelines",
        )
        # Main edition state present + a tombstoned non-main edition.
        # Without the filter, the tombstoned edition reads as unknown
        # and ``_project_needs_discovery`` returns True.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            date_rebuilt_seen=_FIXTURE_MAIN_DATE_REBUILT,
        )
        await _seed_tombstone(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            reason=TombstoneReason.lifecycle_delete,
        )

    _stub_products(mock_discovery, ["pipelines"])
    # LTD still lists the tombstoned edition (id=2) — it has not been
    # deleted on the LTD side, only Docverse-side.
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[2, 1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    _stub_edition(
        mock_discovery,
        edition_id=2,
        slug="u-jsick-feature",
        date_rebuilt=datetime(2026, 4, 29, tzinfo=UTC),
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )


# ---------------------------------------------------------------------------
# tier_other
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tier_other_enqueues_for_stale_non_main_edition(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A non-main edition past the threshold — enqueue refresh.

    Also asserts the queue_jobs row carries ``keeper_sync_run_id IS
    NULL`` and the payload lacks ``run_id``.
    """
    stale = datetime.now(tz=UTC) - timedelta(hours=2)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-other-1", project_slugs=["pipelines"]
        )
        # Branch edition (ltd_id=2): stale.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            ltd_slug="u-jsick-feature",
            date_last_synced=stale,
        )
        # Main edition (ltd_id=1): tier_other ignores main entirely.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            date_last_synced=stale,
        )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[2, 1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    _stub_edition(
        mock_discovery,
        edition_id=2,
        slug="u-jsick-feature",
        date_rebuilt=datetime(2026, 4, 29, tzinfo=UTC),
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert len(children) == 1
    payload = children[0].kwargs["payload"]
    assert "run_id" not in payload

    async for session in db_session_dependency():
        async with session.begin():
            row = (
                await session.execute(
                    select(SqlQueueJob).where(SqlQueueJob.org_id == org_id)
                )
            ).scalar_one()
            assert row.keeper_sync_run_id is None


@pytest.mark.asyncio
async def test_tier_other_skips_when_only_main_is_stale(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """``main`` editions belong to tier_main; tier_other ignores them."""
    stale = datetime.now(tz=UTC) - timedelta(hours=4)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-other-2", project_slugs=["pipelines"]
        )
        # Only main is stale.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            date_last_synced=stale,
        )
        # Branch edition is fresh.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            ltd_slug="u-jsick-feature",
            date_last_synced=datetime.now(tz=UTC),
        )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[2, 1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    _stub_edition(
        mock_discovery,
        edition_id=2,
        slug="u-jsick-feature",
        date_rebuilt=datetime(2026, 5, 7, tzinfo=UTC),
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )


@pytest.mark.asyncio
async def test_tier_other_skips_edition_with_no_state(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Editions without state are tier_discovery's domain.

    Decoupling the two crons means a single missing-state row never
    causes both tiers to enqueue for the same project on the same
    hour. tier_other consults state only.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-other-3", project_slugs=["pipelines"]
        )
        # Only the main edition has state; the branch edition does not.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            date_last_synced=datetime.now(tz=UTC),
        )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[2, 1]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    _stub_edition(
        mock_discovery,
        edition_id=2,
        slug="u-jsick-feature",
        date_rebuilt=datetime(2026, 4, 29, tzinfo=UTC),
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )


@pytest.mark.asyncio
async def test_tier_other_batches_edition_state_lookups(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One ``list_for_org`` per project; no per-edition ``get`` calls.

    Issue #310: tier_other walks every non-``main`` edition LTD lists.
    With per-edition ``get`` the cost grew with edition count; the
    batched read makes it constant per project. The fixture lists five
    branch editions plus ``main`` so a regression to the old shape would
    show up as ``get_calls == 5``.
    """
    fresh = datetime.now(tz=UTC)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-other-batch", project_slugs=["pipelines"]
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            date_last_synced=fresh,
        )
        # All branch editions are fresh — no enqueue, but every state
        # row must be consulted before that decision.
        for ltd_id, slug in (
            (2, "branch-a"),
            (3, "branch-b"),
            (4, "branch-c"),
            (5, "branch-d"),
            (6, "branch-e"),
        ):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug=slug,
                date_last_synced=fresh,
            )

    _stub_products(mock_discovery, ["pipelines"])
    _stub_editions_listing(
        mock_discovery,
        product_slug="pipelines",
        edition_ids=[6, 5, 4, 3, 2, 1],
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    for edition_id, slug in (
        (2, "branch-a"),
        (3, "branch-b"),
        (4, "branch-c"),
        (5, "branch-d"),
        (6, "branch-e"),
    ):
        _stub_edition(
            mock_discovery,
            edition_id=edition_id,
            slug=slug,
            date_rebuilt=datetime(2026, 5, 7, tzinfo=UTC),
        )

    recorder = _install_state_store_recorder(monkeypatch)
    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )
    # Two ``get`` calls per project per tick: one for the dormancy
    # planner read at the top of the loop and one inside
    # :func:`_record_tier_polled` to merge with prior annotations
    # before stamping ``date_other_last_polled``. Two
    # ``list_for_org`` calls: one for the project-tombstone filter
    # (issue #396) and one for the non-main edition staleness scan
    # in :func:`_has_stale_non_main_edition`. The per-project
    # edition-state cost stays independent of the branch count.
    assert recorder.get_calls == 2
    assert recorder.list_for_org_calls == 2


@pytest.mark.asyncio
async def test_tier_other_polls_only_hot_and_due_dormant_projects(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Mixed cohort for tier_other: only the polled cohort hits LTD.

    Acceptance criterion (issue #314): tier_other must skip dormant
    projects whose ``date_other_last_polled`` is within the dormant
    interval, and must stamp the annotation fresh on every polled
    visit even when the staleness check decides not to enqueue.
    """
    now = datetime.now(tz=UTC)
    fresh_rebuild = now - timedelta(days=2)
    old_rebuild = now - timedelta(days=30)
    stale_synced = now - timedelta(hours=2)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-other-cohort",
            project_slugs=["hot-proj", "skip-proj", "due-proj"],
        )
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        # Hot: rebuilt 2 days ago, no last_polled annotation.
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="hot-proj",
            docverse_id=1,
            date_rebuilt_seen=fresh_rebuild,
        )
        # Dormant-skippable: rebuilt 30 days ago, polled 1h ago — well
        # within the 24h dormant interval.
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="skip-proj",
            docverse_id=2,
            date_rebuilt_seen=old_rebuild,
            annotations={
                "date_other_last_polled": (
                    now - timedelta(hours=1)
                ).isoformat()
            },
        )
        # Dormant-due: rebuilt 30 days ago, polled 49h ago — past the
        # full 48h jittered dormant ceiling (24h interval + up to 24h
        # slug-keyed jitter), so the planner re-polls regardless of
        # how the LTD slug hashes.
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="due-proj",
            docverse_id=3,
            date_rebuilt_seen=old_rebuild,
            annotations={
                "date_other_last_polled": (
                    now - timedelta(hours=49)
                ).isoformat()
            },
        )
        # Stale branch edition state for hot and due so each
        # ``_has_stale_non_main_edition`` call returns True and
        # triggers an enqueue. Skip-proj's edition state would also
        # be stale, but the planner skips before LTD is even queried.
        for ltd_id in (10, 20, 30):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug="u-jsick-feature",
                date_last_synced=stale_synced,
            )

    _stub_products(mock_discovery, ["hot-proj", "skip-proj", "due-proj"])
    hot_listing = mock_discovery.get(
        f"{LTD_BASE}/products/hot-proj/editions/"
    ).mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/10"]}
        )
    )
    skip_listing = mock_discovery.get(
        f"{LTD_BASE}/products/skip-proj/editions/"
    ).mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/20"]}
        )
    )
    due_listing = mock_discovery.get(
        f"{LTD_BASE}/products/due-proj/editions/"
    ).mock(
        return_value=httpx.Response(
            200, json={"editions": [f"{LTD_BASE}/editions/30"]}
        )
    )
    _stub_edition(
        mock_discovery,
        edition_id=10,
        slug="u-jsick-feature",
        date_rebuilt=datetime(2026, 4, 29, tzinfo=UTC),
    )
    _stub_edition(
        mock_discovery,
        edition_id=20,
        slug="u-jsick-feature",
        date_rebuilt=datetime(2026, 4, 29, tzinfo=UTC),
    )
    _stub_edition(
        mock_discovery,
        edition_id=30,
        slug="u-jsick-feature",
        date_rebuilt=datetime(2026, 4, 29, tzinfo=UTC),
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    # Acceptance: LTD HTTP fired exactly for the polled cohort.
    assert hot_listing.call_count == 1
    assert skip_listing.call_count == 0
    assert due_listing.call_count == 1

    # Hot and due each enqueue one ``keeper_sync_project`` child.
    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    enqueued_slugs = {c.kwargs["payload"]["ltd_slug"] for c in children}
    assert enqueued_slugs == {"hot-proj", "due-proj"}

    # Acceptance: ``date_other_last_polled`` is stamped on every polled
    # visit. The skipped project's annotation is untouched.
    async for session in db_session_dependency():
        async with session.begin():
            store = KeeperSyncStateStore(session=session, logger=_logger())
            for slug, expected_polled in (
                ("hot-proj", True),
                ("due-proj", True),
                ("skip-proj", False),
            ):
                row = await store.get(
                    org_id=org_id,
                    resource_type=ResourceType.project,
                    ltd_slug=slug,
                )
                assert row is not None
                assert row.annotations is not None
                raw = row.annotations["date_other_last_polled"]
                assert isinstance(raw, str)
                stamped = datetime.fromisoformat(raw)
                if expected_polled:
                    assert (now - stamped) < timedelta(minutes=5)
                else:
                    assert timedelta(minutes=30) < (now - stamped)


@pytest.mark.asyncio
async def test_tier_other_records_polled_annotation_on_ltd_error(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """LtdClientError still updates ``date_other_last_polled``.

    Same rationale as the tier_main and tier_discovery error tests:
    if we skipped the annotation update on errors, a flaky LTD endpoint
    would re-poll on every cron tick instead of waiting out the
    dormant interval.
    """
    now = datetime.now(tz=UTC)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-other-error",
            project_slugs=["flaky-proj"],
        )
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="flaky-proj",
            docverse_id=1,
            date_rebuilt_seen=now - timedelta(days=30),
            annotations={
                "date_other_last_polled": (
                    now - timedelta(hours=49)
                ).isoformat()
            },
        )

    _stub_products(mock_discovery, ["flaky-proj"])
    mock_discovery.get(f"{LTD_BASE}/products/flaky-proj/editions/").mock(
        return_value=httpx.Response(500)
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            store = KeeperSyncStateStore(session=session, logger=_logger())
            row = await store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug="flaky-proj",
            )
    assert row is not None
    assert row.annotations is not None
    raw = row.annotations["date_other_last_polled"]
    assert isinstance(raw, str)
    stamped = datetime.fromisoformat(raw)
    # Update fired during this tick, not 49h ago.
    assert (now - stamped) < timedelta(minutes=5)


@pytest.mark.asyncio
async def test_tier_other_skips_tombstoned_project_slug(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A tombstoned project state row keeps its slug out of the candidate set.

    Issue #396: a tier_other tick must not call
    ``GET /products/<slug>/editions/`` for a tombstoned project, nor
    enqueue a ``keeper_sync_project`` child for it. The non-tombstoned
    slug in the same org still enqueues from its stale non-main
    edition.
    """
    stale = datetime.now(tz=UTC) - timedelta(hours=2)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-other-tomb",
            project_slugs=["pipelines", "live-proj"],
        )
        # Tombstoned project: must be skipped entirely.
        await _seed_tombstone(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
        )
        # Live project: a stale non-main edition triggers the normal
        # enqueue path.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=20,
            ltd_slug="u-jsick-feature",
            date_last_synced=stale,
        )

    _stub_products(mock_discovery, ["pipelines", "live-proj"])
    # Only stub live-proj's editions — the tombstoned ``pipelines``
    # must not hit LTD.
    _stub_editions_listing(
        mock_discovery, product_slug="live-proj", edition_ids=[20, 10]
    )
    _stub_edition(
        mock_discovery,
        edition_id=10,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    _stub_edition(
        mock_discovery,
        edition_id=20,
        slug="u-jsick-feature",
        date_rebuilt=datetime(2026, 4, 29, tzinfo=UTC),
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    slugs = {c.kwargs["payload"]["ltd_slug"] for c in children}
    assert slugs == {"live-proj"}


# ---------------------------------------------------------------------------
# Cron registration
# ---------------------------------------------------------------------------


def test_cron_registration_matches_documented_cadence() -> None:
    """Lock the documented cadences: 5 min / 30 min / hourly.

    PRD #275 §"Reconciliation cadence" defines:
    * tier_main — every 5 min (user story 10's main-edition SLO)
    * tier_discovery — every 30 min
    * tier_other — hourly

    A drift in either the cron registration or the docstring of the
    relevant function should fail this test so the cadence stays
    aligned with the user-visible SLO.
    """
    by_name: dict[str, CronJob] = {
        cj.coroutine.__qualname__: cj
        for cj in KeeperSyncWorkerSettings.cron_jobs
    }
    assert "keeper_sync_tier_main" in by_name
    assert "keeper_sync_tier_discovery" in by_name
    assert "keeper_sync_tier_other" in by_name

    # tier_main fires every 5 min on the dot — each :MM that's a
    # multiple of 5 from :00.
    assert by_name["keeper_sync_tier_main"].minute == {
        0,
        5,
        10,
        15,
        20,
        25,
        30,
        35,
        40,
        45,
        50,
        55,
    }
    # tier_discovery fires twice an hour at :00 / :30.
    assert by_name["keeper_sync_tier_discovery"].minute == {0, 30}
    # tier_other fires once an hour at :00.
    assert by_name["keeper_sync_tier_other"].minute == {0}
