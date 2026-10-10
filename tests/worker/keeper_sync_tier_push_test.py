"""Integration tests for ``tier_main``'s check of pushed refs (PRD #803).

A GitHub ``push`` stamps the pushed ref onto an LTD-synced project's
``keeper_sync_state`` row (the ``github_pushed_refs`` annotation). On
each pass, ``keeper_sync_tier_main`` visits every stamped ref: it finds
the Docverse edition tracking the ref, fetches that edition from LTD,
and enqueues the project's ``keeper_sync_project`` when LTD has rebuilt
it, or when the ref has no edition yet and LTD lists one keeper-sync has
not seen. These tests seed the stamp directly, run one tick against the
``respx`` LTD mock, and assert on the enqueued jobs and the stamps left.

Unless a test says otherwise, the seeded project is *dormant* on
``main``: its LTD ``main`` rebuilt a month ago and was polled an hour
ago, so ``tier_main``'s own ``main``-edition check skips it and every
LTD call the tick makes is the push check's.
"""

from __future__ import annotations

import asyncio
import importlib
import json
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
import sentry_sdk
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from docverse.models import (
    EditionKind,
    JobKind,
    KeeperSyncConfig,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse_server.config import config
from docverse_server.services.keeper_sync.push_hints import (
    ANNOTATION_GITHUB_PUSHED_REFS,
)
from docverse_server.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
)
from docverse_server.storage.ltd import LtdClientError
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.keeper_sync import (
    keeper_sync_tier_discovery,
    keeper_sync_tier_main,
    keeper_sync_tier_other,
)
from tests.support.arq_testing import get_jobs_by_name, register_queue
from tests.worker.conftest import make_worker_ctx

LTD_BASE = "https://keeper.lsst.codes"

_SLUG = "sqr-112"
"""The seeded project's slug, which is also its LTD product slug."""

_MAIN_LTD_ID = 1
"""LTD id of the seeded project's ``main`` edition."""

_SYNCED_AT = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
"""When the seeded editions were last synced, and the rebuild they saw."""


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _now() -> datetime:
    return datetime.now(tz=UTC)


async def _seed_org(session: AsyncSession, slug: str) -> int:
    """Create an org syncing every LTD product, and return its id."""
    store = OrganizationStore(session=session, logger=_logger())
    org = await store.create(
        OrganizationCreate(
            slug=slug, title=f"Org {slug}", base_domain=f"{slug}.example.com"
        )
    )
    await store.update_keeper_sync_config(
        slug=org.slug,
        config=KeeperSyncConfig(enabled=True, project_slugs="*"),
    )
    return org.id


async def _seed_pushed_project(
    session: AsyncSession,
    *,
    org_id: int,
    pushed_refs: dict[str, datetime],
    dormant: bool = True,
) -> int:
    """Create a synced project stamped with ``pushed_refs``.

    Returns the Docverse project id. A ``dormant`` project's LTD
    ``main`` rebuilt a month ago and was polled an hour ago, so
    ``tier_main``'s ``main``-edition check skips it; otherwise its
    ``main`` rebuilt a day ago and the check runs.
    """
    project = await ProjectStore(session=session, logger=_logger()).create(
        org_id=org_id,
        data=ProjectCreate(
            slug=_SLUG,
            title=_SLUG,
            source_url=f"https://example.com/lsst-sqre/{_SLUG}",
        ),
    )
    now = _now()
    await KeeperSyncStateStore(session=session, logger=_logger()).upsert(
        org_id=org_id,
        resource_type=ResourceType.project,
        ltd_slug=_SLUG,
        docverse_id=project.id,
        date_last_synced=_SYNCED_AT,
        date_rebuilt_seen=(
            now - timedelta(days=30) if dormant else now - timedelta(days=1)
        ),
        annotations={
            "main_edition_url": f"{LTD_BASE}/editions/{_MAIN_LTD_ID}",
            "date_main_last_polled": (now - timedelta(hours=1)).isoformat(),
            ANNOTATION_GITHUB_PUSHED_REFS: {
                ref: pushed_at.isoformat()
                for ref, pushed_at in pushed_refs.items()
            },
        },
    )
    return project.id


async def _seed_synced_edition(
    session: AsyncSession,
    *,
    org_id: int,
    project_id: int,
    slug: str,
    git_ref: str,
    ltd_id: int,
    kind: EditionKind = EditionKind.draft,
    date_rebuilt_seen: datetime = _SYNCED_AT,
) -> None:
    """Create a Docverse edition tracking ``git_ref`` and its state row."""
    edition = await EditionStore(
        session=session, logger=_logger()
    ).create_internal(
        project_id=project_id,
        slug=slug,
        title=slug,
        kind=kind,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": git_ref},
    )
    await KeeperSyncStateStore(session=session, logger=_logger()).upsert(
        org_id=org_id,
        resource_type=ResourceType.edition,
        ltd_id=ltd_id,
        ltd_slug="main" if slug == "__main" else slug,
        docverse_id=edition.id,
        date_last_synced=_SYNCED_AT,
        date_rebuilt_seen=date_rebuilt_seen,
    )


def _stub_products(mock: respx.Router) -> None:
    mock.get(f"{LTD_BASE}/products/").mock(
        return_value=httpx.Response(
            200,
            content=json.dumps(
                {"products": [f"{LTD_BASE}/products/{_SLUG}/"]}
            ).encode(),
            headers={"content-type": "application/json"},
        )
    )


def _stub_edition(
    mock: respx.Router,
    *,
    ltd_id: int,
    slug: str,
    tracked_ref: str,
    date_rebuilt: datetime | None,
) -> respx.Route:
    payload: dict[str, Any] = {
        "self_url": f"{LTD_BASE}/editions/{ltd_id}",
        "product_url": f"{LTD_BASE}/products/{_SLUG}",
        "build_url": f"{LTD_BASE}/builds/{ltd_id * 100}",
        "published_url": f"https://{_SLUG}.lsst.io/v/{slug}/",
        "slug": slug,
        "title": slug,
        "date_created": "2026-01-01T00:00:00+00:00",
        "date_rebuilt": (
            date_rebuilt.isoformat() if date_rebuilt is not None else None
        ),
        "date_ended": None,
        "tracked_refs": [tracked_ref],
        "mode": "git_refs",
        "pending_rebuild": False,
    }
    return mock.get(f"{LTD_BASE}/editions/{ltd_id}").mock(
        return_value=httpx.Response(200, json=payload)
    )


def _stub_listing(mock: respx.Router, ltd_ids: list[int]) -> respx.Route:
    return mock.get(f"{LTD_BASE}/products/{_SLUG}/editions/").mock(
        return_value=httpx.Response(
            200,
            json={"editions": [f"{LTD_BASE}/editions/{i}" for i in ltd_ids]},
        )
    )


def _ltd_paths(mock: respx.Router) -> list[str]:
    """Every LTD path the tick requested, except the product listing."""
    return [
        call.request.url.path
        for call in mock.calls
        if str(call.request.url).startswith(LTD_BASE)
        and call.request.url.path != "/products/"
    ]


async def _run_tier_main() -> list[Any]:
    """Run one ``tier_main`` tick; return its ``keeper_sync_project`` jobs."""
    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)
    try:
        assert await keeper_sync_tier_main(ctx) == "completed"
    finally:
        await http_client.aclose()
    return get_jobs_by_name(
        mock_arq, "keeper_sync_project", queue_name=KEEPER_SYNC_QUEUE_NAME
    )


async def _stamps(org_id: int) -> dict[str, str]:
    """Return the project's ``github_pushed_refs`` map as stored."""
    async for session in db_session_dependency():
        async with session.begin():
            state = await KeeperSyncStateStore(
                session=session, logger=_logger()
            ).get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug=_SLUG,
            )
        assert state is not None
        assert state.annotations is not None
        stamps: dict[str, str] = state.annotations[
            ANNOTATION_GITHUB_PUSHED_REFS
        ]
        return stamps
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_rebuilt_pushed_ref_enqueues_and_clears_its_stamp(
    app: None, db_session: AsyncSession, mock_discovery: respx.Router
) -> None:
    """LTD rebuilt the pushed branch's edition: one sync, stamp cleared.

    The project is dormant on ``main``, so the sync comes from the push
    check alone, through one ``GET /editions/<id>`` of the edition the
    ref's Docverse edition maps to.
    """
    pushed_at = _now() - timedelta(minutes=10)
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-rebuilt")
        project_id = await _seed_pushed_project(
            db_session, org_id=org_id, pushed_refs={"tickets/DM-1": pushed_at}
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="tickets-DM-1",
            git_ref="tickets/DM-1",
            ltd_id=2,
        )
    _stub_products(mock_discovery)
    _stub_edition(
        mock_discovery,
        ltd_id=2,
        slug="tickets-DM-1",
        tracked_ref="tickets/DM-1",
        date_rebuilt=_now() - timedelta(minutes=1),
    )

    jobs = await _run_tier_main()

    assert len(jobs) == 1
    assert jobs[0].kwargs["payload"]["ltd_slug"] == _SLUG
    assert "run_id" not in jobs[0].kwargs["payload"]
    assert _ltd_paths(mock_discovery) == ["/editions/2"]
    assert await _stamps(org_id) == {}


@pytest.mark.asyncio
async def test_pushed_ref_without_edition_enqueues_on_unseen_ltd_edition(
    app: None, db_session: AsyncSession, mock_discovery: respx.Router
) -> None:
    """No edition tracks the ref yet, and LTD lists one not seen: sync.

    The discovery fallback lists the project's edition URLs and reads
    their ids; it fetches no edition payload.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-new")
        project_id = await _seed_pushed_project(
            db_session,
            org_id=org_id,
            pushed_refs={"tickets/DM-2": _now() - timedelta(minutes=10)},
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="__main",
            git_ref="main",
            ltd_id=_MAIN_LTD_ID,
            kind=EditionKind.main,
        )
    _stub_products(mock_discovery)
    _stub_listing(mock_discovery, [3, _MAIN_LTD_ID])

    jobs = await _run_tier_main()

    assert len(jobs) == 1
    assert _ltd_paths(mock_discovery) == [f"/products/{_SLUG}/editions/"]
    assert await _stamps(org_id) == {}


@pytest.mark.asyncio
async def test_pushed_ref_without_edition_or_unseen_edition_keeps_stamp(
    app: None, db_session: AsyncSession, mock_discovery: respx.Router
) -> None:
    """LTD has not created the ref's edition yet: no sync, stamp kept."""
    pushed_at = _now() - timedelta(minutes=10)
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-not-found")
        project_id = await _seed_pushed_project(
            db_session, org_id=org_id, pushed_refs={"tickets/DM-2": pushed_at}
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="__main",
            git_ref="main",
            ltd_id=_MAIN_LTD_ID,
            kind=EditionKind.main,
        )
    _stub_products(mock_discovery)
    _stub_listing(mock_discovery, [_MAIN_LTD_ID])

    jobs = await _run_tier_main()

    assert jobs == []
    assert await _stamps(org_id) == {"tickets/DM-2": pushed_at.isoformat()}


@pytest.mark.asyncio
async def test_unchanged_pushed_ref_keeps_its_stamp(
    app: None, db_session: AsyncSession, mock_discovery: respx.Router
) -> None:
    """LTD has not rebuilt the ref's edition yet: no sync, stamp kept."""
    pushed_at = _now() - timedelta(minutes=10)
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-unchanged")
        project_id = await _seed_pushed_project(
            db_session, org_id=org_id, pushed_refs={"tickets/DM-1": pushed_at}
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="tickets-DM-1",
            git_ref="tickets/DM-1",
            ltd_id=2,
        )
    _stub_products(mock_discovery)
    _stub_edition(
        mock_discovery,
        ltd_id=2,
        slug="tickets-DM-1",
        tracked_ref="tickets/DM-1",
        date_rebuilt=_SYNCED_AT,
    )

    jobs = await _run_tier_main()

    assert jobs == []
    assert _ltd_paths(mock_discovery) == ["/editions/2"]
    assert await _stamps(org_id) == {"tickets/DM-1": pushed_at.isoformat()}


@pytest.mark.asyncio
async def test_expired_pushed_ref_is_pruned_without_ltd_call(
    app: None, db_session: AsyncSession, mock_discovery: respx.Router
) -> None:
    """A ref past its window is pruned: no LTD call, no sync."""
    live_at = _now() - timedelta(minutes=10)
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-expired")
        project_id = await _seed_pushed_project(
            db_session,
            org_id=org_id,
            pushed_refs={
                "tickets/DM-1": _now() - timedelta(hours=2),
                "tickets/DM-3": live_at,
            },
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="tickets-DM-3",
            git_ref="tickets/DM-3",
            ltd_id=4,
        )
    _stub_products(mock_discovery)
    _stub_edition(
        mock_discovery,
        ltd_id=4,
        slug="tickets-DM-3",
        tracked_ref="tickets/DM-3",
        date_rebuilt=_SYNCED_AT,
    )

    jobs = await _run_tier_main()

    assert jobs == []
    assert _ltd_paths(mock_discovery) == ["/editions/4"]
    assert await _stamps(org_id) == {"tickets/DM-3": live_at.isoformat()}


@pytest.mark.asyncio
async def test_ltd_error_keeps_the_stamp_and_reports_to_sentry(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An LTD failure keeps the stamp for the next tick and goes to Sentry.

    The failure stops the project's visit: its other live ref is left
    unchecked for the next tick rather than spending another LTD call.
    """
    captured: list[BaseException] = []
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)
    first_at = _now() - timedelta(minutes=20)
    second_at = _now() - timedelta(minutes=10)
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-error")
        project_id = await _seed_pushed_project(
            db_session,
            org_id=org_id,
            pushed_refs={"tickets/DM-1": first_at, "tickets/DM-3": second_at},
        )
        for ref, ltd_id in (("tickets/DM-1", 2), ("tickets/DM-3", 4)):
            await _seed_synced_edition(
                db_session,
                org_id=org_id,
                project_id=project_id,
                slug=ref.replace("/", "-"),
                git_ref=ref,
                ltd_id=ltd_id,
            )
    _stub_products(mock_discovery)
    # 403 is not retried, so the failure surfaces without backoff.
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(403)
    )

    jobs = await _run_tier_main()

    assert jobs == []
    assert len(captured) == 1
    assert isinstance(captured[0], LtdClientError)
    assert _ltd_paths(mock_discovery) == ["/editions/2"]
    assert await _stamps(org_id) == {
        "tickets/DM-1": first_at.isoformat(),
        "tickets/DM-3": second_at.isoformat(),
    }


@pytest.mark.asyncio
async def test_main_check_ltd_failure_defers_the_pushed_refs(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When LTD fails the ``main`` check, the pushed refs wait a tick.

    The ``main`` check's failure has already ridden out the LTD client's
    retries; the project's refs keep their stamps and cost no call.
    """
    captured: list[BaseException] = []
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)
    pushed_at = _now() - timedelta(minutes=10)
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-main-down")
        project_id = await _seed_pushed_project(
            db_session,
            org_id=org_id,
            pushed_refs={"tickets/DM-1": pushed_at},
            dormant=False,
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="tickets-DM-1",
            git_ref="tickets/DM-1",
            ltd_id=2,
        )
    _stub_products(mock_discovery)
    mock_discovery.get(f"{LTD_BASE}/editions/{_MAIN_LTD_ID}").mock(
        return_value=httpx.Response(403)
    )

    jobs = await _run_tier_main()

    assert jobs == []
    assert len(captured) == 1
    assert _ltd_paths(mock_discovery) == [f"/editions/{_MAIN_LTD_ID}"]
    assert await _stamps(org_id) == {"tickets/DM-1": pushed_at.isoformat()}


@pytest.mark.asyncio
async def test_hot_path_off_leaves_tier_main_as_it_was(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the hot path off, stamps are neither checked nor settled.

    The project is hot, so ``tier_main``'s ``main`` check runs and makes
    its one LTD call; a rebuilt pushed ref and an expired one cost
    nothing, enqueue nothing, and stay on the row as they were.
    """
    monkeypatch.setattr(config, "keeper_sync_push_hot_path_enabled", False)
    stamps = {
        "tickets/DM-1": _now() - timedelta(minutes=10),
        "old": _now() - timedelta(hours=3),
    }
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-off")
        project_id = await _seed_pushed_project(
            db_session, org_id=org_id, pushed_refs=stamps, dormant=False
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="__main",
            git_ref="main",
            ltd_id=_MAIN_LTD_ID,
            kind=EditionKind.main,
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="tickets-DM-1",
            git_ref="tickets/DM-1",
            ltd_id=2,
        )
    _stub_products(mock_discovery)
    _stub_edition(
        mock_discovery,
        ltd_id=_MAIN_LTD_ID,
        slug="main",
        tracked_ref="main",
        date_rebuilt=_SYNCED_AT,
    )
    _stub_edition(
        mock_discovery,
        ltd_id=2,
        slug="tickets-DM-1",
        tracked_ref="tickets/DM-1",
        date_rebuilt=_now(),
    )

    jobs = await _run_tier_main()

    assert jobs == []
    assert _ltd_paths(mock_discovery) == [f"/editions/{_MAIN_LTD_ID}"]
    assert await _stamps(org_id) == {
        ref: pushed_at.isoformat() for ref, pushed_at in stamps.items()
    }


@pytest.mark.asyncio
async def test_active_job_skips_the_enqueue_and_keeps_the_stamp(
    app: None, db_session: AsyncSession, mock_discovery: respx.Router
) -> None:
    """A sync already queued for the project holds its slot.

    The push check's enqueue goes through the same per-project mutex as
    every tier's, so no second job is created, and the ref keeps its
    stamp to be checked again once that job has run.
    """
    pushed_at = _now() - timedelta(minutes=10)
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-mutex")
        project_id = await _seed_pushed_project(
            db_session, org_id=org_id, pushed_refs={"tickets/DM-1": pushed_at}
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="tickets-DM-1",
            git_ref="tickets/DM-1",
            ltd_id=2,
        )
        await QueueJobStore(session=db_session, logger=_logger()).create(
            kind=JobKind.keeper_sync_project,
            org_id=org_id,
            keeper_sync_run_id=None,
            subject_label=_SLUG,
            backend_job_id="arq-job-prior",
        )
    _stub_products(mock_discovery)
    _stub_edition(
        mock_discovery,
        ltd_id=2,
        slug="tickets-DM-1",
        tracked_ref="tickets/DM-1",
        date_rebuilt=_now() - timedelta(minutes=1),
    )

    jobs = await _run_tier_main()

    assert jobs == []
    assert await _stamps(org_id) == {"tickets/DM-1": pushed_at.isoformat()}


@pytest.mark.asyncio
async def test_rebuilt_tag_release_edition_enqueues(
    app: None, db_session: AsyncSession, mock_discovery: respx.Router
) -> None:
    """A tag push is checked against the release edition tracking it."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-tag")
        project_id = await _seed_pushed_project(
            db_session,
            org_id=org_id,
            pushed_refs={"v1.2.0": _now() - timedelta(minutes=10)},
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="v1.2.0",
            git_ref="v1.2.0",
            ltd_id=5,
            kind=EditionKind.release,
        )
    _stub_products(mock_discovery)
    _stub_edition(
        mock_discovery,
        ltd_id=5,
        slug="v1.2.0",
        tracked_ref="v1.2.0",
        date_rebuilt=_now() - timedelta(minutes=1),
    )

    jobs = await _run_tier_main()

    assert len(jobs) == 1
    assert _ltd_paths(mock_discovery) == ["/editions/5"]
    assert await _stamps(org_id) == {}


@pytest.mark.asyncio
async def test_pushed_main_shares_the_main_check(
    app: None, db_session: AsyncSession, mock_discovery: respx.Router
) -> None:
    """A push to ``main`` on a hot project rides ``tier_main``'s own check.

    The ``main`` check fetches LTD's ``main`` edition and finds it
    rebuilt; the pushed ref resolves to the same edition, reuses that
    fetch, and is cleared by the project's one enqueue.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-main")
        project_id = await _seed_pushed_project(
            db_session,
            org_id=org_id,
            pushed_refs={"main": _now() - timedelta(minutes=10)},
            dormant=False,
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="__main",
            git_ref="main",
            ltd_id=_MAIN_LTD_ID,
            kind=EditionKind.main,
        )
    _stub_products(mock_discovery)
    _stub_edition(
        mock_discovery,
        ltd_id=_MAIN_LTD_ID,
        slug="main",
        tracked_ref="main",
        date_rebuilt=_now() - timedelta(minutes=1),
    )

    jobs = await _run_tier_main()

    assert len(jobs) == 1
    assert _ltd_paths(mock_discovery) == [f"/editions/{_MAIN_LTD_ID}"]
    assert await _stamps(org_id) == {}


@pytest.mark.asyncio
async def test_push_during_the_visit_keeps_its_newer_stamp(
    app: None,
    db_session: AsyncSession,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ref pushed again while the tick ran keeps its newer stamp.

    The new push lands between the visit's read of the stamp and the
    settling write, as a webhook can at any time: the enqueue covers the
    push the visit saw, not the newer one, so the ref stays stamped.
    """
    module = importlib.import_module(
        "docverse_server.worker.functions.keeper_sync"
    )
    real_enqueue = module._enqueue_tier_project_sync
    pushed_again = _now()

    async def enqueue_then_push(**kwargs: Any) -> bool:
        enqueued = await real_enqueue(**kwargs)
        async for session in db_session_dependency():
            async with session.begin():
                store = KeeperSyncStateStore(session=session, logger=_logger())
                state = await store.get(
                    org_id=kwargs["org_id"],
                    resource_type=ResourceType.project,
                    ltd_slug=_SLUG,
                    for_update=True,
                )
                assert state is not None
                assert state.annotations is not None
                await store.upsert(
                    org_id=kwargs["org_id"],
                    resource_type=ResourceType.project,
                    ltd_slug=_SLUG,
                    annotations={
                        **state.annotations,
                        ANNOTATION_GITHUB_PUSHED_REFS: {
                            "tickets/DM-1": pushed_again.isoformat()
                        },
                    },
                )
        return bool(enqueued)

    monkeypatch.setattr(
        module, "_enqueue_tier_project_sync", enqueue_then_push
    )
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-again")
        project_id = await _seed_pushed_project(
            db_session,
            org_id=org_id,
            pushed_refs={"tickets/DM-1": _now() - timedelta(minutes=10)},
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="tickets-DM-1",
            git_ref="tickets/DM-1",
            ltd_id=2,
        )
    _stub_products(mock_discovery)
    _stub_edition(
        mock_discovery,
        ltd_id=2,
        slug="tickets-DM-1",
        tracked_ref="tickets/DM-1",
        date_rebuilt=_now() - timedelta(minutes=1),
    )

    jobs = await _run_tier_main()

    assert len(jobs) == 1
    assert await _stamps(org_id) == {"tickets/DM-1": pushed_again.isoformat()}


@pytest.mark.asyncio
async def test_edition_gone_from_ltd_falls_back_to_the_listing(
    app: None, db_session: AsyncSession, mock_discovery: respx.Router
) -> None:
    """LTD lost the ref's synced edition: the listing check decides.

    A branch deleted and pushed again gets a new LTD edition; the old
    one answers 404, and the listing shows the new id unseen.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-gone")
        project_id = await _seed_pushed_project(
            db_session,
            org_id=org_id,
            pushed_refs={"tickets/DM-1": _now() - timedelta(minutes=10)},
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="tickets-DM-1",
            git_ref="tickets/DM-1",
            ltd_id=2,
        )
    _stub_products(mock_discovery)
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(404)
    )
    _stub_listing(mock_discovery, [6])

    jobs = await _run_tier_main()

    assert len(jobs) == 1
    assert _ltd_paths(mock_discovery) == [
        "/editions/2",
        f"/products/{_SLUG}/editions/",
    ]
    assert await _stamps(org_id) == {}


@pytest.mark.asyncio
async def test_visit_and_enqueue_are_logged(
    app: None, db_session: AsyncSession, mock_discovery: respx.Router
) -> None:
    """Each visited ref logs its outcome; an enqueue logs the push lag."""
    pushed_at = _now() - timedelta(minutes=10)
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-logs")
        project_id = await _seed_pushed_project(
            db_session,
            org_id=org_id,
            pushed_refs={
                "tickets/DM-1": pushed_at,
                "old": _now() - timedelta(hours=2),
            },
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="tickets-DM-1",
            git_ref="tickets/DM-1",
            ltd_id=2,
        )
    _stub_products(mock_discovery)
    _stub_edition(
        mock_discovery,
        ltd_id=2,
        slug="tickets-DM-1",
        tracked_ref="tickets/DM-1",
        date_rebuilt=_now() - timedelta(minutes=1),
    )

    with capture_logs() as captured:
        await _run_tier_main()

    visits = {
        entry["github_ref"]: entry
        for entry in captured
        if entry["event"] == "Tier-main: checked pushed ref"
    }
    assert set(visits) == {"tickets/DM-1", "old"}
    assert visits["old"]["outcome"] == "expired"
    assert visits["old"]["enqueued"] is False
    assert visits["tickets/DM-1"]["outcome"] == "rebuilt"
    assert visits["tickets/DM-1"]["enqueued"] is True
    assert visits["tickets/DM-1"]["org"] == "ks-push-logs"
    assert visits["tickets/DM-1"]["project"] == _SLUG
    assert visits["tickets/DM-1"]["pushed_at"] == pushed_at.isoformat()
    enqueues = [
        entry
        for entry in captured
        if entry["event"] == "Tier-main: enqueued project sync for pushed ref"
    ]
    assert len(enqueues) == 1
    assert enqueues[0]["github_ref"] == "tickets/DM-1"
    assert enqueues[0]["outcome"] == "rebuilt"
    assert enqueues[0]["push_lag_seconds"] >= 600


async def _wait_until_a_backend_blocks(
    maker: async_sessionmaker[AsyncSession], *, timeout: float = 10.0
) -> None:
    """Block until some backend in this test's database waits on a lock.

    Test databases are per xdist worker and a worker runs its tests
    serially, so the only backends this sees are the test's own.
    """
    query = text(
        "SELECT count(*) FROM pg_stat_activity"
        " WHERE datname = current_database()"
        " AND cardinality(pg_blocking_pids(pid)) > 0"
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        async with maker() as session:
            blocked = (await session.execute(query)).scalar_one()
        if blocked:
            return
        if loop.time() >= deadline:
            msg = "the tier pass never blocked on the project row lock"
            raise AssertionError(msg)
        await asyncio.sleep(0.02)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tier_cron",
    [
        keeper_sync_tier_main,
        keeper_sync_tier_discovery,
        keeper_sync_tier_other,
    ],
)
async def test_tier_poll_stamp_keeps_a_concurrent_push(
    app: None,
    db_session: AsyncSession,
    db_session_factory: async_sessionmaker[AsyncSession],
    mock_discovery: respx.Router,
    tier_cron: Callable[[dict[str, Any]], Coroutine[Any, Any, str]],
) -> None:
    """A push stamped while a tier records its poll is not overwritten.

    Each tier rewrites the project row's annotations to record its poll.
    A push delivery holding the row's lock stamps a ref mid-pass; the
    tier waits for it and merges the stamp in, rather than writing back
    the annotations it read before the push.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ks-push-race")
        project_id = await _seed_pushed_project(
            db_session, org_id=org_id, pushed_refs={}, dormant=False
        )
        await _seed_synced_edition(
            db_session,
            org_id=org_id,
            project_id=project_id,
            slug="__main",
            git_ref="main",
            ltd_id=_MAIN_LTD_ID,
            kind=EditionKind.main,
        )
    _stub_products(mock_discovery)
    _stub_listing(mock_discovery, [_MAIN_LTD_ID])
    _stub_edition(
        mock_discovery,
        ltd_id=_MAIN_LTD_ID,
        slug="main",
        tracked_ref="main",
        date_rebuilt=_SYNCED_AT,
    )
    pushed_at = _now()

    store = KeeperSyncStateStore(session=db_session, logger=_logger())
    async with db_session.begin():
        locked = await store.get(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug=_SLUG,
            for_update=True,
        )
        assert locked is not None
        assert locked.annotations is not None
        http_client = httpx.AsyncClient()
        mock_arq = MockArqQueue(default_queue_name="docverse:queue")
        register_queue(mock_arq, KEEPER_SYNC_QUEUE_NAME)
        ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)
        tick = asyncio.create_task(tier_cron(ctx))
        try:
            await _wait_until_a_backend_blocks(db_session_factory)
            await store.upsert(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug=_SLUG,
                annotations={
                    **locked.annotations,
                    ANNOTATION_GITHUB_PUSHED_REFS: {
                        "tickets/DM-1": pushed_at.isoformat()
                    },
                },
            )
        except BaseException:
            tick.cancel()
            raise
    try:
        assert await tick == "completed"
    finally:
        await http_client.aclose()

    assert await _stamps(org_id) == {"tickets/DM-1": pushed_at.isoformat()}
