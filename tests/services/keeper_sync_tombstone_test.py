"""Tests for :class:`KeeperSyncTombstoneService`."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.client.models import OrganizationCreate
from docverse.services.keeper_sync_tombstone import KeeperSyncTombstoneService
from docverse.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse.storage.organization_store import OrganizationStore


async def _seed_org(session: AsyncSession, *, slug: str = "ks-tomb") -> int:
    logger = structlog.get_logger("test")
    store = OrganizationStore(session=session, logger=logger)
    org = await store.create(
        OrganizationCreate(
            slug=slug,
            title="ks-tomb",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


def _build_service(
    session: AsyncSession,
    *,
    logger: structlog.stdlib.BoundLogger | None = None,
) -> KeeperSyncTombstoneService:
    log = logger or structlog.get_logger("test")
    state_store = KeeperSyncStateStore(session=session, logger=log)
    return KeeperSyncTombstoneService(
        session=session, state_store=state_store, logger=log
    )


@pytest.mark.asyncio
async def test_record_writes_tombstone_on_existing_edition_row(
    db_session: AsyncSession,
) -> None:
    """Existing state row picks up tombstone fields on ``record``."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=42,
            ltd_slug="main",
            docverse_id=7,
        )

    service = KeeperSyncTombstoneService(
        session=db_session, state_store=state_store, logger=logger
    )
    async with db_session.begin():
        state = await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=42,
            reason=TombstoneReason.lifecycle_delete,
            note="aged-out draft",
        )

    assert state.date_tombstoned is not None
    assert state.tombstone_reason == "lifecycle_delete"
    assert state.tombstone_note == "aged-out draft"
    assert state.docverse_id == 7


@pytest.mark.asyncio
async def test_record_creates_row_when_none_exists_for_preemptive(
    db_session: AsyncSession,
) -> None:
    """``lifecycle_preemptive`` writes a new row with ``docverse_id=NULL``."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-preemptive")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        state = await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=99,
            reason=TombstoneReason.lifecycle_preemptive,
        )

    assert state.id > 0
    assert state.docverse_id is None
    assert state.date_tombstoned is not None
    assert state.tombstone_reason == "lifecycle_preemptive"
    assert state.tombstone_note is None
    assert state.ltd_id == 99


@pytest.mark.asyncio
async def test_record_creates_project_row_with_slug_key(
    db_session: AsyncSession,
) -> None:
    """Project rows are slug-keyed; ``record`` honours the slug variant."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-project")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        state = await service.record(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
            reason=TombstoneReason.manual_delete,
        )

    assert state.ltd_slug == "pipelines"
    assert state.tombstone_reason == "manual_delete"
    assert state.date_tombstoned is not None


@pytest.mark.asyncio
async def test_is_tombstoned_returns_true_only_when_stamped(
    db_session: AsyncSession,
) -> None:
    """``is_tombstoned`` is true iff ``date_tombstoned IS NOT NULL``."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-check")
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
        )

    service = KeeperSyncTombstoneService(
        session=db_session, state_store=state_store, logger=logger
    )
    async with db_session.begin():
        assert (
            await service.is_tombstoned(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=1,
            )
            is False
        )
        await service.record(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            reason=TombstoneReason.manual_delete,
        )
        assert (
            await service.is_tombstoned(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=1,
            )
            is True
        )


@pytest.mark.asyncio
async def test_is_tombstoned_false_for_missing_row(
    db_session: AsyncSession,
) -> None:
    """Absence of a state row is "not tombstoned", not an error."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-missing")

    service = _build_service(db_session, logger=logger)
    async with db_session.begin():
        assert (
            await service.is_tombstoned(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=12345,
            )
            is False
        )


@pytest.mark.asyncio
async def test_record_emits_structured_log(
    db_session: AsyncSession,
) -> None:
    """``record`` emits a log line with org, resource_type, ltd_id, reason."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-log")

    service = _build_service(db_session, logger=logger)
    with capture_logs() as captured:
        async with db_session.begin():
            await service.record(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=42,
                reason=TombstoneReason.lifecycle_preemptive,
            )

    events = [
        e for e in captured if e.get("event") == "Sync tombstone recorded"
    ]
    assert events, "expected a Sync tombstone recorded log line"
    event = events[-1]
    assert event["org_id"] == org_id
    assert event["resource_type"] == "edition"
    assert event["ltd_id"] == 42
    assert event["reason"] == "lifecycle_preemptive"
