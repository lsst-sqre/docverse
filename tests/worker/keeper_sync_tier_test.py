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

import asyncio
import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
import structlog
from arq.cron import CronJob
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs
from structlog.typing import EventDict, WrappedLogger

from docverse.models import JobKind, KeeperSyncConfig, OrganizationCreate
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.queue import JobStatus
from docverse_server.services.keeper_sync import mappers
from docverse_server.services.keeper_sync.scheduler import (
    Tier,
    tier_cron_timeout,
)
from docverse_server.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse_server.services.keeper_sync_tombstone import (
    KeeperSyncTombstoneService,
)
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.keeper_sync import (
    _MAX_RECORDED_EDITION_FAILURES,
    keeper_sync_tier_discovery,
    keeper_sync_tier_main,
    keeper_sync_tier_other,
)
from docverse_server.worker.main import KeeperSyncWorkerSettings
from tests.support.arq_cancel import HangUntilCancelled, cancel_when_reached
from tests.support.arq_testing import get_jobs_by_name, register_queue
from tests.worker.conftest import make_worker_ctx

LTD_BASE = "https://keeper.lsst.codes"

#: ``date_rebuilt`` for the canonical ``main`` edition fixture. Used by
#: tests that need to compare LTD's published timestamp against state.
_FIXTURE_MAIN_DATE_REBUILT = datetime(2026, 4, 30, 18, 30, tzinfo=UTC)

#: Path of one LTD edition resource, ``GET /editions/<id>``: the
#: per-edition payload fetch the discovery and other tiers must never
#: make.
_EDITION_RESOURCE_PATH = re.compile(r"/editions/\d+/?")


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
    project_slug_patterns: list[str] | None = None,
    exclude_project_slugs: list[str] | None = None,
    exclude_project_slug_patterns: list[str] | None = None,
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
            project_slug_patterns=project_slug_patterns or [],
            exclude_project_slugs=exclude_project_slugs or [],
            exclude_project_slug_patterns=(
                exclude_project_slug_patterns or []
            ),
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


def _ltd_request_paths(mock_discovery: respx.Router) -> list[str]:
    """Return the path of every request the tick sent to LTD, in order.

    ``respx`` records every call on the router, matched or not, so a
    request to an unstubbed LTD URL shows up here too.
    """
    return [
        call.request.url.path
        for call in mock_discovery.calls
        if str(call.request.url).startswith(LTD_BASE)
    ]


def _assert_no_edition_fetches(mock_discovery: respx.Router) -> None:
    """Fail when the tick fetched any edition payload from LTD.

    ``tier_discovery`` and ``tier_other`` decide from each listed
    edition's LTD id alone, which the edition URL carries, so they list
    a product's edition URLs and never follow them: following them is
    one ``GET /editions/<id>`` per edition, which on ``pipelines``
    (2,938 editions) pushed both crons past arq's cron timeout.
    """
    fetched = [
        path
        for path in _ltd_request_paths(mock_discovery)
        if _EDITION_RESOURCE_PATH.fullmatch(path)
    ]
    assert fetched == []


#: An edition URL whose last path segment is no integer id, so
#: :func:`~docverse_server.storage.ltd.models.parse_ltd_id` raises on it.
_UNPARSABLE_EDITION_URL = f"{LTD_BASE}/editions/latest"

#: The warning the discovery and other tiers log for a project whose
#: edition listing carries URLs they cannot read an id from.
_UNPARSABLE_URLS_EVENT = "Keeper-sync tier skipped unparsable LTD edition URLs"


def _stub_edition_url_listing(
    mock_discovery: respx.Router, *, product_slug: str, urls: list[str]
) -> None:
    """Stub ``GET /products/<slug>/editions/`` to list ``urls`` verbatim.

    Unlike :func:`_stub_editions_listing`, which builds well-formed URLs
    from ids, this lets a test list a URL with no trailing id.
    """
    mock_discovery.get(f"{LTD_BASE}/products/{product_slug}/editions/").mock(
        return_value=httpx.Response(200, json={"editions": urls})
    )


async def _read_polled_annotation(
    *, org_id: int, ltd_slug: str, key: str
) -> datetime:
    """Return a project's ``date_<tier>_last_polled`` annotation."""
    stamped: datetime | None = None
    async for session in db_session_dependency():
        async with session.begin():
            store = KeeperSyncStateStore(session=session, logger=_logger())
            row = await store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug=ltd_slug,
            )
        assert row is not None
        assert row.annotations is not None
        raw = row.annotations[key]
        assert isinstance(raw, str)
        stamped = datetime.fromisoformat(raw)
    assert stamped is not None
    return stamped


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
    annotations: dict[str, Any] | None = None,
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
        annotations=annotations,
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
    carries a ``main_edition_url`` annotation so the next tick can skip
    the walk. The pointer is the URL alone: no ``main_edition_ltd_id``
    companion is written, since nothing reads one.
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
    assert "main_edition_ltd_id" not in project_state.annotations
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
    assert (
        project_state.annotations["main_edition_url"]
        == f"{LTD_BASE}/editions/1"
    )


def _fold_default_onto_main(
    monkeypatch: pytest.MonkeyPatch, *, ltd_slug: str
) -> None:
    """Make the edition-slug mapper fold ``ltd_slug`` onto ``__main`` too.

    ``mappers.is_ltd_main`` is defined by what
    ``mappers.derive_edition_slug`` folds onto Docverse's default
    edition, so widening the mapper is how a test shows a caller follows
    that shared rule rather than a hard-coded ``"main"``.
    """
    real = mappers.derive_edition_slug

    def folding(slug: str) -> str:
        return real(mappers.LTD_MAIN_SLUG if slug == ltd_slug else slug)

    monkeypatch.setattr(mappers, "derive_edition_slug", folding)


@pytest.mark.asyncio
async def test_tier_main_walk_follows_is_ltd_main(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The URL walk recognises any slug ``is_ltd_main`` accepts as ``main``.

    tier_main and tier_other share one definition of ``main``: whatever
    LTD slug the mapper folds onto Docverse's ``__main``. Here the
    mapper also folds ``default``, so the walk resolves edition 1 (slug
    ``default``) as the project's ``main``, caches its URL, and
    enqueues a refresh because no edition state exists yet.
    """
    _fold_default_onto_main(monkeypatch, ltd_slug="default")
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-main-folded-walk",
            project_slugs=["pipelines"],
        )

    _stub_products(mock_discovery, ["pipelines"])
    # No edition carries the literal ``main`` slug, so only the shared
    # rule can pick ``default`` out of the walk.
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[1, 2]
    )
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="default",
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

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == ["pipelines"]
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
    assert (
        project_state.annotations["main_edition_url"]
        == f"{LTD_BASE}/editions/1"
    )


@pytest.mark.asyncio
async def test_tier_main_cached_pointer_follows_is_ltd_main(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cached pointer to any slug ``is_ltd_main`` accepts stays valid.

    The cached fetch's slug check uses the same shared rule as the walk:
    with the mapper folding ``default`` onto ``__main``, a cached
    pointer whose edition is slugged ``default`` is a hit, so the tick
    never falls back to listing the project's edition URLs.
    """
    _fold_default_onto_main(monkeypatch, ltd_slug="default")
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-main-folded-cache",
            project_slugs=["pipelines"],
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_id=None,
            ltd_slug="pipelines",
            annotations={"main_edition_url": f"{LTD_BASE}/editions/1"},
        )

    _stub_products(mock_discovery, ["pipelines"])
    listing_route = mock_discovery.get(
        f"{LTD_BASE}/products/pipelines/editions/"
    ).mock(return_value=httpx.Response(200, json={"editions": []}))
    _stub_edition(
        mock_discovery,
        edition_id=1,
        slug="default",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    assert listing_route.call_count == 0
    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert len(children) == 1


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


@pytest.mark.asyncio
async def test_tier_main_honours_scope_patterns_and_excludes(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """tier_main resolves its candidates through the config scope rule.

    PRD #667: ``sqr-100`` is in scope only because of the include
    pattern — it never appeared in ``project_slugs``, which is how a
    product created on LTD after the config was written joins the
    cadence with no config change. ``sqr-999`` matches the same
    pattern but is excluded, and ``www`` is not included at all;
    neither is stubbed on LTD, so a scope leak fails the tick.
    """
    async with db_session.begin():
        await _seed_org(
            db_session,
            slug="ks-tier-main-scope",
            project_slugs=[],
            project_slug_patterns=[r"sqr-\d+"],
            exclude_project_slugs=["sqr-999"],
        )

    _stub_products(mock_discovery, ["sqr-100", "sqr-999", "www"])
    _stub_editions_listing(
        mock_discovery, product_slug="sqr-100", edition_ids=[2, 1]
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

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == ["sqr-100"]


@pytest.mark.asyncio
async def test_tier_main_logs_scope_counts(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """The tier scope log event reports the same counts as run discovery."""
    async with db_session.begin():
        await _seed_org(
            db_session,
            slug="ks-tier-main-scope-log",
            project_slugs="*",
            exclude_project_slugs=["www"],
            exclude_project_slug_patterns=[r"test-.*"],
        )

    _stub_products(mock_discovery, ["sqr-100", "www", "test-one"])
    _stub_editions_listing(
        mock_discovery, product_slug="sqr-100", edition_ids=[1]
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
        with capture_logs() as captured:
            result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    scope_events = [
        e
        for e in captured
        if e["event"] == "Resolved keeper-sync tier scope"
        and e["org"] == "ks-tier-main-scope-log"
    ]
    assert len(scope_events) == 1
    assert scope_events[0]["ltd_count"] == 3
    assert scope_events[0]["in_scope_count"] == 1
    assert scope_events[0]["excluded_count"] == 2
    assert scope_events[0]["tombstoned_count"] == 0
    assert scope_events[0]["fan_out_count"] == 1


@pytest.mark.asyncio
async def test_tier_main_scope_counts_split_tombstones_from_fan_out(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """The tier scope counts obey ``in_scope - tombstoned = fan_out``.

    Issue #680: the tier event uses the preview's field names, so it
    has to use the preview's definitions — ``in_scope_count`` before
    the tombstone subtraction, ``tombstoned_count`` scoped to it. The
    org carries a second tombstone on a slug the config never admitted,
    so a whole-state-table count would report ``2`` here.
    """
    org_slug = "ks-tier-main-scope-tombstones"
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug=org_slug,
            project_slugs=["sqr-100", "sqr-200"],
        )
        for slug in ("sqr-200", "www"):
            await _seed_tombstone(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug=slug,
            )

    _stub_products(mock_discovery, ["sqr-100", "sqr-200", "www"])
    _stub_editions_listing(
        mock_discovery, product_slug="sqr-100", edition_ids=[1]
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
        with capture_logs() as captured:
            result = await keeper_sync_tier_main(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    scope_events = [
        e
        for e in captured
        if e["event"] == "Resolved keeper-sync tier scope"
        and e["org"] == org_slug
    ]
    assert len(scope_events) == 1
    event = scope_events[0]
    assert event["ltd_count"] == 3
    assert event["in_scope_count"] == 2
    assert event["excluded_count"] == 0
    assert event["tombstoned_count"] == 1
    assert event["fan_out_count"] == 1
    assert (
        event["in_scope_count"] - event["tombstoned_count"]
        == event["fan_out_count"]
    )

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == ["sqr-100"]


@pytest.mark.asyncio
async def test_tier_main_leaves_newly_excluded_project_rows_alone(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """Excluding a synced project stops its jobs without touching its rows.

    PRD #667: falling out of scope is not a delete. ``old-proj`` was
    synced before the exclude was added — its state rows lag LTD, so a
    scope leak would enqueue it — and after the tick those rows are
    still there, un-tombstoned, ready to resume if the exclude is
    lifted.
    """
    now = datetime.now(tz=UTC)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-main-excluded",
            project_slugs="*",
            exclude_project_slugs=["old-proj"],
        )
        # ``old-proj`` looks freshly synced and hot, so nothing but the
        # exclude keeps it out of this tick.
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_id=None,
            ltd_slug="old-proj",
            date_last_synced=now - timedelta(minutes=1),
            date_rebuilt_seen=now - timedelta(minutes=1),
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            ltd_slug="main",
            date_rebuilt_seen=_FIXTURE_MAIN_DATE_REBUILT - timedelta(hours=2),
        )

    _stub_products(mock_discovery, ["old-proj", "live-proj"])
    # Both products are stubbed on LTD: a scope leak would enqueue
    # ``old-proj`` rather than error out.
    _stub_editions_listing(
        mock_discovery, product_slug="old-proj", edition_ids=[2]
    )
    _stub_editions_listing(
        mock_discovery, product_slug="live-proj", edition_ids=[12]
    )
    _stub_edition(
        mock_discovery,
        edition_id=2,
        slug="main",
        date_rebuilt=_FIXTURE_MAIN_DATE_REBUILT,
    )
    _stub_edition(
        mock_discovery,
        edition_id=12,
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
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == ["live-proj"]

    async for session in db_session_dependency():
        async with session.begin():
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            project_state = await state_store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug="old-proj",
                include_tombstoned=True,
            )
            assert project_state is not None
            assert project_state.date_tombstoned is None
            edition_state = await state_store.get(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=2,
                include_tombstoned=True,
            )
            assert edition_state is not None
            assert edition_state.date_tombstoned is None
            assert edition_state.date_rebuilt_seen == (
                _FIXTURE_MAIN_DATE_REBUILT - timedelta(hours=2)
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
    _assert_no_edition_fetches(mock_discovery)

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

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

    # Single enqueue covers the project; the unseen edition gets
    # imported as a side effect of ``KeeperSyncService.sync_project``.
    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert len(children) == 1


@pytest.mark.asyncio
async def test_tier_discovery_reads_only_the_edition_url_listing(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """One LTD call per project with a state row: its edition URL list.

    Discovery needs only each listed edition's LTD id, which the URL
    carries, so it compares the parsed ids against the edition-state
    map and never fetches an edition payload. ``sqr-001`` lists an id
    with no state row and enqueues; every id ``pipelines`` lists is
    known, so it does not. No edition resource is stubbed, so a
    regression to following the URLs also shows up as an
    ``/editions/<id>`` request.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-disc-urls",
            project_slugs=["pipelines", "sqr-001"],
        )
        for slug in ("pipelines", "sqr-001"):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_id=None,
                ltd_slug=slug,
            )
        for ltd_id, slug in ((1, "main"), (2, "branch-a"), (3, "branch-b")):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug=slug,
            )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=10,
            ltd_slug="main",
        )

    _stub_products(mock_discovery, ["pipelines", "sqr-001"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[3, 2, 1]
    )
    _stub_editions_listing(
        mock_discovery, product_slug="sqr-001", edition_ids=[11, 10]
    )

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
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == ["sqr-001"]
    _assert_no_edition_fetches(mock_discovery)
    assert sorted(_ltd_request_paths(mock_discovery)) == [
        "/products/",
        "/products/pipelines/editions/",
        "/products/sqr-001/editions/",
    ]


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

    recorder = _install_state_store_recorder(monkeypatch)
    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

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

    recorder = _install_state_store_recorder(monkeypatch)
    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

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

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)
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

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

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
    _assert_no_edition_fetches(mock_discovery)

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
    _assert_no_edition_fetches(mock_discovery)

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

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )


@pytest.mark.asyncio
async def test_tier_discovery_honours_scope_patterns_and_excludes(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """tier_discovery resolves its candidates through the config scope rule.

    ``sqr-100`` reaches the cron through the include pattern alone —
    the shape a product created on LTD after the config was written
    takes — while the excluded ``sqr-999`` and the unmatched ``www``
    never do. Neither is stubbed on LTD, so a scope leak fails the
    tick.
    """
    async with db_session.begin():
        await _seed_org(
            db_session,
            slug="ks-tier-disc-scope",
            project_slugs=[],
            project_slug_patterns=[r"sqr-\d+"],
            exclude_project_slugs=["sqr-999"],
        )

    _stub_products(mock_discovery, ["sqr-100", "sqr-999", "www"])
    # No editions stub: ``sqr-100`` has no project state row, so
    # discovery enqueues on the cheap path without walking editions.

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == ["sqr-100"]


@pytest.mark.asyncio
async def test_tier_discovery_skips_unparsable_edition_url(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An edition URL with no trailing id costs that URL, not the org's tick.

    ``aaa`` lists a URL :func:`parse_ltd_id` cannot read ahead of two
    well-formed ones. The tick logs one warning naming the org, the slug
    and the URL, still decides ``aaa`` from the ids that did parse —
    edition 2 has no state row, so it enqueues — and still stamps its
    polled annotation. ``bbb``, later in the same scope, is still
    visited: a raise on the URL would have dropped it for the tick.
    """
    now = datetime.now(tz=UTC)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-disc-badurl",
            project_slugs=["aaa", "bbb"],
        )
        for slug in ("aaa", "bbb"):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_id=None,
                ltd_slug=slug,
            )
        for ltd_id in (1, 3):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug="main",
            )

    _stub_products(mock_discovery, ["aaa", "bbb"])
    _stub_edition_url_listing(
        mock_discovery,
        product_slug="aaa",
        urls=[
            _UNPARSABLE_EDITION_URL,
            f"{LTD_BASE}/editions/2",
            f"{LTD_BASE}/editions/1",
        ],
    )
    _stub_editions_listing(
        mock_discovery, product_slug="bbb", edition_ids=[4, 3]
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == [
        "aaa",
        "bbb",
    ]

    warnings = [e for e in captured if e["event"] == _UNPARSABLE_URLS_EVENT]
    assert len(warnings) == 1
    assert warnings[0]["log_level"] == "warning"
    assert warnings[0]["tier"] == "discovery"
    assert warnings[0]["org"] == "ks-tier-disc-badurl"
    assert warnings[0]["ltd_slug"] == "aaa"
    assert warnings[0]["unparsable_urls"] == [_UNPARSABLE_EDITION_URL]

    stamped = await _read_polled_annotation(
        org_id=org_id, ltd_slug="aaa", key="date_discovery_last_polled"
    )
    assert (now - stamped) < timedelta(minutes=5)


@pytest.mark.asyncio
async def test_tier_unparsable_edition_url_warning_is_capped(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """The unparsable-URL warning names a bounded sample, counted exactly.

    ``aaa`` lists more unreadable URLs than
    ``_MAX_RECORDED_EDITION_FAILURES`` ahead of one well-formed id: the
    shape every edition of ``pipelines`` (2,938) would take if LTD
    changed its URL scheme. The warning carries only the first
    ``_MAX_RECORDED_EDITION_FAILURES`` URLs, in listing order, while
    ``skipped_count`` stays the exact total, so one tick cannot emit a
    multi-megabyte log line per project. The parsed id is still decided
    on — edition 2 has no state row, so ``aaa`` enqueues — and ``aaa``
    is still stamped polled.
    """
    now = datetime.now(tz=UTC)
    unparsable_urls = [
        f"{LTD_BASE}/editions/latest-{n}"
        for n in range(_MAX_RECORDED_EDITION_FAILURES + 5)
    ]
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-disc-badurl-cap",
            project_slugs=["aaa"],
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_id=None,
            ltd_slug="aaa",
        )

    _stub_products(mock_discovery, ["aaa"])
    _stub_edition_url_listing(
        mock_discovery,
        product_slug="aaa",
        urls=[*unparsable_urls, f"{LTD_BASE}/editions/2"],
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            result = await keeper_sync_tier_discovery(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == ["aaa"]

    warnings = [e for e in captured if e["event"] == _UNPARSABLE_URLS_EVENT]
    assert len(warnings) == 1
    assert (
        warnings[0]["unparsable_urls"]
        == (unparsable_urls[:_MAX_RECORDED_EDITION_FAILURES])
    )
    assert warnings[0]["skipped_count"] == len(unparsable_urls)
    assert warnings[0]["parsed_count"] == 1

    stamped = await _read_polled_annotation(
        org_id=org_id, ltd_slug="aaa", key="date_discovery_last_polled"
    )
    assert (now - stamped) < timedelta(minutes=5)


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

    ``main`` is stale too, but its state row records the ``main`` slug,
    so tier_other leaves it out and the enqueue comes from the branch
    edition alone. Also asserts the queue_jobs row carries
    ``keeper_sync_run_id IS NULL`` and the payload lacks ``run_id``.
    """
    stale = datetime.now(tz=UTC) - timedelta(hours=2)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-other-1", project_slugs=["pipelines"]
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_id=None,
            ltd_slug="pipelines",
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

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

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
    """``main`` editions belong to tier_main; tier_other ignores them.

    tier_other never fetches an edition payload, so it recognises
    ``main`` by the LTD slug its ``keeper_sync_state`` row records. It
    needs nothing tier_main caches on the project's state row (this
    project has no annotations at all), and an old ``main`` alone never
    enqueues.
    """
    stale = datetime.now(tz=UTC) - timedelta(hours=4)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-other-2", project_slugs=["pipelines"]
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_id=None,
            ltd_slug="pipelines",
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

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)
    assert (
        get_jobs_by_name(
            ctx["arq_queue"],
            "keeper_sync_project",
            queue_name=KEEPER_SYNC_QUEUE_NAME,
        )
        == []
    )


@pytest.mark.asyncio
async def test_tier_other_reads_only_the_edition_url_listing(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """One LTD call per polled project: its edition URL list.

    tier_other needs only each listed edition's LTD id (to read its
    state row, which also names ``main`` by its LTD slug), so it never
    fetches an edition payload. ``pipelines`` has a stale
    branch edition and enqueues; every edition ``sqr-001`` lists is
    fresh, so it does not. No edition resource is stubbed, so a
    regression to following the URLs also shows up as an
    ``/editions/<id>`` request.
    """
    now = datetime.now(tz=UTC)
    stale = now - timedelta(hours=2)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-other-urls",
            project_slugs=["pipelines", "sqr-001"],
        )
        for slug in ("pipelines", "sqr-001"):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_id=None,
                ltd_slug=slug,
            )
        for ltd_id, slug, synced in (
            (1, "main", now),
            (2, "branch-a", now),
            (3, "branch-b", stale),
            (10, "main", now),
            (11, "branch-c", now),
        ):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug=slug,
                date_last_synced=synced,
            )

    _stub_products(mock_discovery, ["pipelines", "sqr-001"])
    _stub_editions_listing(
        mock_discovery, product_slug="pipelines", edition_ids=[3, 2, 1]
    )
    _stub_editions_listing(
        mock_discovery, product_slug="sqr-001", edition_ids=[11, 10]
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
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == ["pipelines"]
    _assert_no_edition_fetches(mock_discovery)
    assert sorted(_ltd_request_paths(mock_discovery)) == [
        "/products/",
        "/products/pipelines/editions/",
        "/products/sqr-001/editions/",
    ]


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

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)
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

    recorder = _install_state_store_recorder(monkeypatch)
    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

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

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

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
    _assert_no_edition_fetches(mock_discovery)

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

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    slugs = {c.kwargs["payload"]["ltd_slug"] for c in children}
    assert slugs == {"live-proj"}


@pytest.mark.asyncio
async def test_tier_other_honours_scope_patterns_and_excludes(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """tier_other resolves its candidates through the config scope rule.

    ``sqr-100`` is in scope by pattern only and its non-main edition is
    stale, so it enqueues; the excluded ``sqr-999`` and the unmatched
    ``www`` are never fetched from LTD, so a scope leak fails the tick.
    """
    stale = datetime.now(tz=UTC) - timedelta(hours=2)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-other-scope",
            project_slugs=[],
            project_slug_patterns=[r"sqr-\d+"],
            exclude_project_slugs=["sqr-999"],
        )
        await _seed_state(
            db_session,
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            ltd_slug="u-jsick-feature",
            date_last_synced=stale,
        )

    _stub_products(mock_discovery, ["sqr-100", "sqr-999", "www"])
    _stub_editions_listing(
        mock_discovery, product_slug="sqr-100", edition_ids=[2, 1]
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == ["sqr-100"]


@pytest.mark.asyncio
async def test_tier_other_skips_unparsable_edition_url(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """An edition URL with no trailing id costs that URL, not the org's tick.

    ``aaa`` lists a URL :func:`parse_ltd_id` cannot read alongside two
    well-formed ones. The tick logs one warning naming the org, the slug
    and the URL, still scans the ids that did parse — branch edition 2
    is stale, so ``aaa`` enqueues — and still stamps its polled
    annotation. ``bbb``, later in the same scope, is still visited: a
    raise on the URL would have dropped it for the tick.
    """
    now = datetime.now(tz=UTC)
    stale = now - timedelta(hours=2)
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-other-badurl",
            project_slugs=["aaa", "bbb"],
        )
        for slug in ("aaa", "bbb"):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_id=None,
                ltd_slug=slug,
            )
        for ltd_id in (2, 4):
            await _seed_state(
                db_session,
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug="u-jsick-feature",
                date_last_synced=stale,
            )

    _stub_products(mock_discovery, ["aaa", "bbb"])
    _stub_edition_url_listing(
        mock_discovery,
        product_slug="aaa",
        urls=[
            f"{LTD_BASE}/editions/2",
            _UNPARSABLE_EDITION_URL,
            f"{LTD_BASE}/editions/1",
        ],
    )
    _stub_editions_listing(
        mock_discovery, product_slug="bbb", edition_ids=[4, 3]
    )

    http_client = httpx.AsyncClient()
    ctx = _make_ctx(http_client)
    try:
        with capture_logs() as captured:
            result = await keeper_sync_tier_other(ctx)
    finally:
        await ctx["http_client"].aclose()
    assert result == "completed"
    _assert_no_edition_fetches(mock_discovery)

    children = get_jobs_by_name(
        ctx["arq_queue"],
        "keeper_sync_project",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == [
        "aaa",
        "bbb",
    ]

    warnings = [e for e in captured if e["event"] == _UNPARSABLE_URLS_EVENT]
    assert len(warnings) == 1
    assert warnings[0]["log_level"] == "warning"
    assert warnings[0]["tier"] == "other"
    assert warnings[0]["org"] == "ks-tier-other-badurl"
    assert warnings[0]["ltd_slug"] == "aaa"
    assert warnings[0]["unparsable_urls"] == [_UNPARSABLE_EDITION_URL]

    stamped = await _read_polled_annotation(
        org_id=org_id, ltd_slug="aaa", key="date_other_last_polled"
    )
    assert (now - stamped) < timedelta(minutes=5)


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


# ---------------------------------------------------------------------------
# Lost active-job race (issue #508)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tier_lost_race_does_not_truncate_the_org_pass(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Losing the per-slug race skips that slug, not the rest of the org.

    The ``has_active_for_subject`` pre-check is stubbed to always miss,
    which is exactly what a genuine race looks like from the enqueuing
    worker's point of view: the ``SELECT`` sees no active row, but by the
    time the ``INSERT`` lands another worker holds
    ``idx_queue_jobs_keeper_sync_project_active_uq``. The first slug is
    already taken, so its insert loses; the second slug must still be
    enqueued rather than being dropped along with the rest of the tick.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session,
            slug="ks-tier-race",
            project_slugs=["aaa", "bbb"],
        )
        # Another worker already holds the mutex for "aaa".
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        await queue_job_store.create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            subject_label="aaa",
        )

    async def always_miss(*args: Any, **kwargs: Any) -> bool:
        return False

    monkeypatch.setattr(QueueJobStore, "has_active_for_subject", always_miss)

    _stub_products(mock_discovery, ["aaa", "bbb"])
    for product_slug, edition_id in (("aaa", 1), ("bbb", 2)):
        _stub_editions_listing(
            mock_discovery, product_slug=product_slug, edition_ids=[edition_id]
        )
        _stub_edition(
            mock_discovery,
            edition_id=edition_id,
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
    # "aaa" lost the race and was skipped; "bbb" — which comes after it
    # in the per-slug loop — is still enqueued.
    assert [c.kwargs["payload"]["ltd_slug"] for c in children] == ["bbb"]

    async for session in db_session_dependency():
        async with session.begin():
            rows = (
                (
                    await session.execute(
                        select(SqlQueueJob).where(
                            SqlQueueJob.org_id == org_id,
                            SqlQueueJob.kind
                            == JobKind.keeper_sync_project.value,
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert sorted(row.subject_label or "" for row in rows) == [
                "aaa",
                "bbb",
            ]
        break


@pytest.mark.asyncio
async def test_tier_cancel_mid_handoff_fails_the_orphaned_child(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tier tick cancelled mid-hand-off fails the child it orphaned.

    The cron is cancelled after committing a ``keeper_sync_project`` row
    but before arq accepted the job, as a rolling deploy's SIGTERM would
    catch it. Left alone, that ``queued`` row with no ``backend_job_id``
    holds the slug's active-job mutex — so every tick and run skips the
    project — until ``keeper_sync_reaper``'s orphan sweep reaches it.
    Instead it fails at once with a ``CancelledError`` payload naming the
    cron, read against the timeout the cron is registered with, and the
    slug is free for the next tick.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="ks-tier-cancel", project_slugs=["pipelines"]
        )
    _stub_products(mock_discovery, ["pipelines"])
    ctx = _make_ctx(httpx.AsyncClient())
    hang = HangUntilCancelled()
    monkeypatch.setattr(ctx["arq_queue"], "enqueue", hang)

    task = asyncio.create_task(keeper_sync_tier_discovery(ctx))
    await cancel_when_reached(task, hang.reached)
    await ctx["http_client"].aclose()

    async for session in db_session_dependency():
        async with session.begin():
            row = (
                await session.execute(
                    select(SqlQueueJob).where(SqlQueueJob.org_id == org_id)
                )
            ).scalar_one()
            assert row.status == JobStatus.failed.value
            assert row.errors is not None
            assert row.errors["type"] == "CancelledError"
            assert row.errors["reason"] == "worker_shutdown"
            assert row.errors["job_function"] == "keeper_sync_tier_discovery"
            assert row.errors["timeout_seconds"] == (
                tier_cron_timeout(Tier.discovery).total_seconds()
            )
            assert not await QueueJobStore(
                session=session, logger=_logger()
            ).has_active_for_subject(
                org_id=org_id,
                kind=JobKind.keeper_sync_project,
                subject_label="pipelines",
            )


@pytest.mark.asyncio
async def test_tier_pass_cancelled_mid_scope_logs_how_far_it_got(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A tier pass arq cancels logs one warning saying how far it got.

    The pass holds no ``queue_jobs`` row of its own, so the log line is
    its whole record. The first org finishes its one project; the second
    is cancelled while LTD lists the editions of its second of three
    projects, as a timeout or a rolling deploy would catch it. The
    warning names the tier, the time the pass ran, the orgs it finished,
    and the in-flight org's position in its scope, and the cancel still
    reaches arq.
    """
    async with db_session.begin():
        await _seed_org(
            db_session, slug="ks-tier-cut-a", project_slugs=["aaa"]
        )
        await _seed_org(
            db_session,
            slug="ks-tier-cut-b",
            project_slugs=["aaa", "bbb", "ccc"],
        )
    _stub_products(mock_discovery, ["aaa", "bbb", "ccc"])
    _stub_editions_listing(mock_discovery, product_slug="aaa", edition_ids=[1])
    hang = HangUntilCancelled()
    mock_discovery.get(f"{LTD_BASE}/products/bbb/editions/").mock(
        side_effect=hang
    )
    ctx = _make_ctx(httpx.AsyncClient())

    with capture_logs() as captured:
        task = asyncio.create_task(keeper_sync_tier_other(ctx))
        await cancel_when_reached(task, hang.reached)
    await ctx["http_client"].aclose()

    cancelled = [
        e for e in captured if e["event"] == "Keeper-sync tier pass cancelled"
    ]
    assert len(cancelled) == 1
    event = cancelled[0]
    assert event["log_level"] == "warning"
    assert event["tier"] == "other"
    assert event["reason"] == "worker_shutdown"
    assert event["elapsed_seconds"] >= 0
    assert event["timeout_seconds"] == (
        tier_cron_timeout(Tier.other).total_seconds()
    )
    assert event["orgs_completed"] == 1
    assert event["orgs_total"] == 2
    assert event["org"] == "ks-tier-cut-b"
    assert event["projects_visited"] == 1
    assert event["projects_total"] == 3
    assert event["ltd_slug"] == "bbb"
    assert not any(
        e["event"] == "Keeper-sync tier pass complete" for e in captured
    )


@pytest.mark.asyncio
async def test_tier_cancel_log_failure_still_propagates_the_cancel(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A failure while logging a tier pass's cancel never replaces it.

    The warning is the cancelled pass's whole record, but rendering it
    runs every structlog processor (and any Sentry hook), and one of
    those raising must not swap the ``CancelledError`` for an unrelated
    exception: arq would record the pass failed with the wrong
    traceback. The failure is logged with its traceback instead and the
    original cancel still reaches arq.
    """
    async with db_session.begin():
        await _seed_org(
            db_session, slug="ks-tier-cut-log", project_slugs=["aaa"]
        )
    _stub_products(mock_discovery, ["aaa"])
    hang = HangUntilCancelled()
    mock_discovery.get(f"{LTD_BASE}/products/aaa/editions/").mock(
        side_effect=hang
    )
    ctx = _make_ctx(httpx.AsyncClient())

    def _fail_on_cancel_warning(
        _logger: WrappedLogger, _method: str, event_dict: EventDict
    ) -> EventDict:
        if event_dict["event"] == "Keeper-sync tier pass cancelled":
            msg = "log processor failed"
            raise RuntimeError(msg)
        return event_dict

    with capture_logs(processors=[_fail_on_cancel_warning]) as captured:
        task = asyncio.create_task(keeper_sync_tier_other(ctx))
        await cancel_when_reached(task, hang.reached)
    await ctx["http_client"].aclose()

    failures = [
        e
        for e in captured
        if e["event"] == "Failed to log the tier pass's cancellation"
    ]
    assert len(failures) == 1
    assert failures[0]["log_level"] == "error"
    assert failures[0]["exc_info"] is True
    assert failures[0]["tier"] == "other"


@pytest.mark.asyncio
async def test_tier_cancel_propagates_when_every_log_call_fails(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """A cancel still escapes a pass whose logging fails on every call.

    The fallback that reports a failed cancellation log runs through the
    same structlog processors as the warning it reports on, so a
    processor or Sentry hook that fails on *every* event raises again
    from the fallback. That second failure is swallowed too: the
    ``CancelledError`` is the only exception that may leave the pass, or
    arq records it failed with an unrelated traceback and no cancel.
    Logging breaks just as the cancel lands, so the pass reaches its
    hang the normal way.
    """
    async with db_session.begin():
        await _seed_org(
            db_session, slug="ks-tier-cut-log-all", project_slugs=["aaa"]
        )
    _stub_products(mock_discovery, ["aaa"])
    hang = HangUntilCancelled()
    mock_discovery.get(f"{LTD_BASE}/products/aaa/editions/").mock(
        side_effect=hang
    )
    ctx = _make_ctx(httpx.AsyncClient())
    logging_broken = False
    attempted: list[str] = []

    def _fail_every_event(
        _logger: WrappedLogger, _method: str, event_dict: EventDict
    ) -> EventDict:
        if logging_broken:
            attempted.append(event_dict["event"])
            msg = "log processor failed"
            raise RuntimeError(msg)
        return event_dict

    async def _break_logging() -> None:
        nonlocal logging_broken
        logging_broken = True

    with capture_logs(processors=[_fail_every_event]):
        task = asyncio.create_task(keeper_sync_tier_other(ctx))
        # Asserts the ``CancelledError``, and nothing else, leaves the task.
        await cancel_when_reached(
            task, hang.reached, before_cancel=_break_logging
        )
    await ctx["http_client"].aclose()

    assert attempted == [
        "Keeper-sync tier pass cancelled",
        "Failed to log the tier pass's cancellation",
    ]
