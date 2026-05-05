"""Tests for ``KeeperSyncStateStore``."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate
from docverse.keeper_sync.state_store import KeeperSyncStateStore, ResourceType
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
