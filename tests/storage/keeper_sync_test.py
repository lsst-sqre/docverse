"""Tests for the keeper_sync_runs and keeper_sync_state tables."""

from __future__ import annotations

import pytest
import structlog
from docverse.client.models import OrganizationCreate
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.keeper_sync_state import SqlKeeperSyncState
from docverse.storage.organization_store import OrganizationStore


async def _seed_org(session: AsyncSession, *, slug: str = "ks-org") -> int:
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title="KS Org",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


@pytest.mark.asyncio
async def test_partial_unique_index_blocks_two_pending_runs(
    db_session: AsyncSession,
) -> None:
    """Two ``pending`` runs for the same org are rejected by the partial UQ."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        db_session.add(
            SqlKeeperSyncRun(org_id=org_id, kind="backfill", status="pending")
        )

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(
                SqlKeeperSyncRun(
                    org_id=org_id, kind="backfill", status="pending"
                )
            )


@pytest.mark.asyncio
async def test_partial_unique_index_blocks_pending_and_in_progress(
    db_session: AsyncSession,
) -> None:
    """An ``in_progress`` row blocks a second non-terminal row for the org."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        db_session.add(
            SqlKeeperSyncRun(
                org_id=org_id, kind="backfill", status="in_progress"
            )
        )

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(
                SqlKeeperSyncRun(
                    org_id=org_id, kind="backfill", status="pending"
                )
            )


@pytest.mark.asyncio
async def test_partial_unique_index_allows_terminal_alongside_pending(
    db_session: AsyncSession,
) -> None:
    """Terminal-status runs do not participate in non-terminal uniqueness."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        db_session.add(
            SqlKeeperSyncRun(
                org_id=org_id, kind="backfill", status="succeeded"
            )
        )
        db_session.add(
            SqlKeeperSyncRun(org_id=org_id, kind="backfill", status="failed")
        )
        db_session.add(
            SqlKeeperSyncRun(
                org_id=org_id, kind="backfill", status="partial_failure"
            )
        )

    async with db_session.begin():
        db_session.add(
            SqlKeeperSyncRun(org_id=org_id, kind="backfill", status="pending")
        )


@pytest.mark.asyncio
async def test_partial_unique_index_allows_pending_for_distinct_orgs(
    db_session: AsyncSession,
) -> None:
    """Two distinct orgs may each hold a ``pending`` run concurrently."""
    async with db_session.begin():
        first_org = await _seed_org(db_session, slug="ks-org-a")
        second_org = await _seed_org(db_session, slug="ks-org-b")
        db_session.add(
            SqlKeeperSyncRun(
                org_id=first_org, kind="backfill", status="pending"
            )
        )
        db_session.add(
            SqlKeeperSyncRun(
                org_id=second_org, kind="backfill", status="pending"
            )
        )


@pytest.mark.asyncio
async def test_keeper_sync_state_unique_constraint(
    db_session: AsyncSession,
) -> None:
    """``(org_id, resource_type, ltd_id)`` is unique on the state table."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        db_session.add(
            SqlKeeperSyncState(
                org_id=org_id,
                resource_type="project",
                ltd_id=42,
                ltd_slug="dmtn-001",
            )
        )

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(
                SqlKeeperSyncState(
                    org_id=org_id,
                    resource_type="project",
                    ltd_id=42,
                    ltd_slug="dmtn-001",
                )
            )


@pytest.mark.asyncio
async def test_keeper_sync_state_allows_distinct_resource_types_same_id(
    db_session: AsyncSession,
) -> None:
    """``ltd_id`` is namespaced by ``resource_type`` (project vs edition)."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        db_session.add(
            SqlKeeperSyncState(
                org_id=org_id,
                resource_type="project",
                ltd_id=1,
                ltd_slug="dmtn-001",
            )
        )
        db_session.add(
            SqlKeeperSyncState(
                org_id=org_id,
                resource_type="edition",
                ltd_id=1,
                ltd_slug="main",
            )
        )


@pytest.mark.asyncio
async def test_keeper_sync_runs_status_check_rejects_invalid(
    db_session: AsyncSession,
) -> None:
    """A ``status`` outside the allowed set fails the CHECK constraint."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(
                SqlKeeperSyncRun(
                    org_id=org_id, kind="backfill", status="pendng"
                )
            )


@pytest.mark.asyncio
async def test_keeper_sync_runs_kind_check_rejects_invalid(
    db_session: AsyncSession,
) -> None:
    """A ``kind`` value outside the allowed set fails the CHECK constraint."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(
                SqlKeeperSyncRun(
                    org_id=org_id, kind="garbage", status="pending"
                )
            )


@pytest.mark.asyncio
async def test_keeper_sync_state_resource_type_check_rejects_invalid(
    db_session: AsyncSession,
) -> None:
    """A ``resource_type`` outside the allowed set fails the CHECK."""
    async with db_session.begin():
        org_id = await _seed_org(db_session)
        db_session.add(
            SqlKeeperSyncState(
                org_id=org_id,
                resource_type="build",
                ltd_id=7,
                ltd_slug="b-7",
            )
        )

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(
                SqlKeeperSyncState(
                    org_id=org_id,
                    resource_type="garbage",
                    ltd_id=99,
                    ltd_slug="g-99",
                )
            )


@pytest.mark.asyncio
async def test_keeper_sync_state_tombstone_reason_check_rejects_invalid(
    db_session: AsyncSession,
) -> None:
    """A ``tombstone_reason`` outside the allowed set fails the CHECK."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-check")

    with pytest.raises(IntegrityError):
        async with db_session.begin():
            db_session.add(
                SqlKeeperSyncState(
                    org_id=org_id,
                    resource_type="edition",
                    ltd_id=1,
                    ltd_slug="main",
                    tombstone_reason="garbage",
                )
            )


@pytest.mark.asyncio
async def test_keeper_sync_state_tombstone_reason_check_allows_null(
    db_session: AsyncSession,
) -> None:
    """``tombstone_reason=NULL`` (the not-tombstoned default) is accepted."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-check-null")
        db_session.add(
            SqlKeeperSyncState(
                org_id=org_id,
                resource_type="edition",
                ltd_id=1,
                ltd_slug="main",
            )
        )
