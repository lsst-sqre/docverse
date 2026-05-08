"""Tests for ``KeeperSyncStateStore``."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate
from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
from docverse.storage.organization_store import OrganizationStore


async def _seed_org(session: AsyncSession, *, slug: str = "ks-state") -> int:
    logger = structlog.get_logger("test")
    store = OrganizationStore(session=session, logger=logger)
    org = await store.create(
        OrganizationCreate(
            slug=slug,
            title="ks-state",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


@pytest.mark.asyncio
async def test_get_returns_none_when_no_row(db_session: AsyncSession) -> None:
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        got = await store.get(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="missing",
        )
    assert got is None


@pytest.mark.asyncio
async def test_upsert_inserts_row_first_time(
    db_session: AsyncSession,
) -> None:
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    rebuilt = datetime(2026, 4, 30, 18, 30, tzinfo=UTC)
    async with db_session.begin():
        state = await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.build,
            ltd_id=42,
            ltd_slug="42",
            docverse_id=7,
            date_last_synced=datetime(2026, 5, 1, tzinfo=UTC),
            date_rebuilt_seen=rebuilt,
            content_hash="sha256:" + "a" * 64,
        )
    assert state.id > 0
    assert state.docverse_id == 7
    assert state.date_rebuilt_seen == rebuilt
    assert state.content_hash is not None
    assert state.content_hash.startswith("sha256:")


@pytest.mark.asyncio
async def test_upsert_updates_only_non_none_fields(
    db_session: AsyncSession,
) -> None:
    """A second upsert with ``None`` for a field preserves the prior value."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
            docverse_id=11,
            content_hash="sha256:" + "b" * 64,
        )
    async with db_session.begin():
        state = await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
            date_last_synced=datetime(2026, 5, 2, tzinfo=UTC),
        )
    assert state.docverse_id == 11
    assert state.content_hash == "sha256:" + "b" * 64
    assert state.date_last_synced is not None
    assert state.ltd_id is None


@pytest.mark.asyncio
async def test_get_rejects_wrong_key_for_resource_type(
    db_session: AsyncSession,
) -> None:
    """Passing the wrong key variant for a resource type raises ValueError."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        with pytest.raises(ValueError, match="ltd_slug is required"):
            await store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_id=1,
            )
        with pytest.raises(ValueError, match="ltd_id must be None"):
            await store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_id=1,
                ltd_slug="pipelines",
            )
        with pytest.raises(ValueError, match="ltd_id is required"):
            await store.get(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_slug="main",
            )


@pytest.mark.asyncio
async def test_upsert_round_trips_via_get(db_session: AsyncSession) -> None:
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=5,
            ltd_slug="main",
            docverse_id=33,
            annotations={"tracked_refs": ["main"]},
        )
    async with db_session.begin():
        got = await store.get(
            org_id=org_id, resource_type=ResourceType.edition, ltd_id=5
        )
    assert got is not None
    assert got.docverse_id == 33
    assert got.ltd_slug == "main"
    assert got.annotations == {"tracked_refs": ["main"]}


@pytest.mark.asyncio
async def test_list_for_org_returns_empty_when_no_rows(
    db_session: AsyncSession,
) -> None:
    """No matching rows returns an empty list, not ``None``."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-list-empty")
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        rows = await store.list_for_org(
            org_id=org_id, resource_type=ResourceType.edition
        )
    assert rows == []


@pytest.mark.asyncio
async def test_list_for_org_returns_only_matching_resource_type(
    db_session: AsyncSession,
) -> None:
    """Project / build rows must not leak into an edition listing.

    The new batched read replaces N per-edition ``get`` calls in the
    tier-cron worker, so any leakage between resource types would let a
    project state row spoof an edition state row in the dict lookup.
    """
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-list-isolate")
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        # One project row, two edition rows, one build row — only the
        # editions should come back.
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
        )
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
        )
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            ltd_slug="u-jsick-feature",
        )
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.build,
            ltd_id=99,
            ltd_slug="99",
        )
    async with db_session.begin():
        rows = await store.list_for_org(
            org_id=org_id, resource_type=ResourceType.edition
        )
    by_ltd_id = {r.ltd_id: r for r in rows}
    assert set(by_ltd_id) == {1, 2}
    assert by_ltd_id[1].ltd_slug == "main"
    assert by_ltd_id[2].ltd_slug == "u-jsick-feature"


@pytest.mark.asyncio
async def test_list_for_org_returns_only_matching_org(
    db_session: AsyncSession,
) -> None:
    """An org's editions must not include another org's rows."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_a = await _seed_org(db_session, slug="ks-list-org-a")
        org_b = await _seed_org(db_session, slug="ks-list-org-b")
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await store.upsert(
            org_id=org_a,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
        )
        await store.upsert(
            org_id=org_b,
            resource_type=ResourceType.edition,
            ltd_id=2,
            ltd_slug="main",
        )
    async with db_session.begin():
        rows = await store.list_for_org(
            org_id=org_a, resource_type=ResourceType.edition
        )
    assert [r.ltd_id for r in rows] == [1]


@pytest.mark.asyncio
async def test_list_for_org_filters_by_ltd_ids(
    db_session: AsyncSession,
) -> None:
    """``ltd_ids`` narrows the result to the supplied LTD ids only.

    Used by ``_has_stale_non_main_edition`` so the worker only pays for
    rows tied to editions LTD currently lists.
    """
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-list-ltd-ids")
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        for ltd_id, slug in ((1, "main"), (2, "branch-a"), (3, "branch-b")):
            await store.upsert(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug=slug,
            )
    async with db_session.begin():
        rows = await store.list_for_org(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_ids=[1, 3],
        )
    assert sorted(r.ltd_id for r in rows if r.ltd_id is not None) == [1, 3]

    async with db_session.begin():
        empty = await store.list_for_org(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_ids=[],
        )
    assert empty == []
