"""Tests for ``KeeperSyncStateStore``."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate
from docverse.dbschema.keeper_sync_state import SqlKeeperSyncState
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
async def test_upsert_mints_unique_public_id(
    db_session: AsyncSession,
) -> None:
    """Every newly upserted row gets a distinct time-ordered ``public_id``."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        first = await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="1",
        )
        second = await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            ltd_slug="2",
        )
    async with db_session.begin():
        public_ids = list(
            (
                await db_session.execute(
                    select(SqlKeeperSyncState.public_id)
                    .where(SqlKeeperSyncState.org_id == org_id)
                    .order_by(SqlKeeperSyncState.id)
                )
            )
            .scalars()
            .all()
        )
    assert first.id != second.id
    # Both inserts minted a populated, non-null, unique public_id.
    assert len(public_ids) == 2
    assert all(pid is not None and pid > 0 for pid in public_ids)
    assert len(set(public_ids)) == 2


@pytest.mark.asyncio
async def test_upsert_reuses_public_id_on_update(
    db_session: AsyncSession,
) -> None:
    """A second upsert on the same key does not re-mint ``public_id``."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session)
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        inserted = await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
            docverse_id=1,
        )

    async def _public_id() -> int:
        async with db_session.begin():
            return (
                await db_session.execute(
                    select(SqlKeeperSyncState.public_id).where(
                        SqlKeeperSyncState.id == inserted.id
                    )
                )
            ).scalar_one()

    first_public_id = await _public_id()
    async with db_session.begin():
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
            docverse_id=2,
        )
    assert await _public_id() == first_public_id


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


@pytest.mark.asyncio
async def test_list_for_org_filters_by_docverse_ids(
    db_session: AsyncSession,
) -> None:
    """``docverse_ids`` narrows the result by the Docverse-side ids.

    Used by the per-project read paths
    (``KeeperSyncProjectService._scoped_edition_states`` and
    ``list_project_editions``) to bound the query cost on the
    state-row read to the project's edition count rather than the
    org-wide row count.
    """
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-list-docverse-ids")
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        for ltd_id, slug, docverse_id in (
            (1, "main", 11),
            (2, "branch-a", 22),
            (3, "branch-b", 33),
        ):
            await store.upsert(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_id,
                ltd_slug=slug,
                docverse_id=docverse_id,
            )
    async with db_session.begin():
        rows = await store.list_for_org(
            org_id=org_id,
            resource_type=ResourceType.edition,
            docverse_ids=[11, 33],
        )
    assert sorted(
        r.docverse_id for r in rows if r.docverse_id is not None
    ) == [
        11,
        33,
    ]

    async with db_session.begin():
        empty = await store.list_for_org(
            org_id=org_id,
            resource_type=ResourceType.edition,
            docverse_ids=[],
        )
    assert empty == []


@pytest.mark.asyncio
async def test_get_by_docverse_id_returns_matching_row(
    db_session: AsyncSession,
) -> None:
    """Indexed single-row lookup keyed on the per-resource Docverse id.

    Replaces the in-memory filter loop in
    :meth:`KeeperSyncProjectService._lookup_main_edition_row` so the
    per-project GET no longer pulls every edition state row for the
    org just to find the one matching the ``__main`` edition's id.
    """
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-get-by-docverse")
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            docverse_id=42,
        )
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            ltd_slug="branch-a",
            docverse_id=43,
        )
    async with db_session.begin():
        row = await store.get_by_docverse_id(
            org_id=org_id,
            resource_type=ResourceType.edition,
            docverse_id=42,
        )
    assert row is not None
    assert row.docverse_id == 42
    assert row.ltd_slug == "main"


@pytest.mark.asyncio
async def test_get_by_docverse_id_returns_none_when_no_match(
    db_session: AsyncSession,
) -> None:
    """No row with the supplied ``docverse_id`` returns ``None``."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-get-by-docverse-miss")
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            docverse_id=42,
        )
    async with db_session.begin():
        row = await store.get_by_docverse_id(
            org_id=org_id,
            resource_type=ResourceType.edition,
            docverse_id=999,
        )
    assert row is None


@pytest.mark.asyncio
async def test_get_by_docverse_id_isolates_by_org_and_resource_type(
    db_session: AsyncSession,
) -> None:
    """A different org or resource type with the same docverse_id is ignored.

    ``docverse_id`` is not globally unique across orgs or resource
    types (a project and an edition can both have id 42 in their
    respective tables), so the lookup must filter by all three key
    columns.
    """
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_a = await _seed_org(db_session, slug="ks-get-by-docverse-org-a")
        org_b = await _seed_org(db_session, slug="ks-get-by-docverse-org-b")
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        # Org A: edition with docverse_id=42
        await store.upsert(
            org_id=org_a,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            docverse_id=42,
        )
        # Org B: edition with the same docverse_id=42 (different org).
        await store.upsert(
            org_id=org_b,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
            docverse_id=42,
        )
        # Org A: build with docverse_id=42 (different resource type).
        await store.upsert(
            org_id=org_a,
            resource_type=ResourceType.build,
            ltd_id=99,
            ltd_slug="99",
            docverse_id=42,
        )
    async with db_session.begin():
        row = await store.get_by_docverse_id(
            org_id=org_a,
            resource_type=ResourceType.edition,
            docverse_id=42,
        )
    assert row is not None
    assert row.org_id == org_a
    assert row.resource_type == ResourceType.edition.value


async def _set_tombstone(
    session: AsyncSession,
    *,
    row_id: int,
    reason: str = "manual_delete",
    note: str | None = None,
) -> None:
    """Stamp tombstone fields directly on a state row via the ORM."""
    stmt = select(SqlKeeperSyncState).where(SqlKeeperSyncState.id == row_id)
    row = (await session.execute(stmt)).scalar_one()
    row.date_tombstoned = datetime(2026, 5, 27, tzinfo=UTC)
    row.tombstone_reason = reason
    row.tombstone_note = note


@pytest.mark.asyncio
async def test_get_hides_tombstoned_row_by_default(
    db_session: AsyncSession,
) -> None:
    """A tombstoned row is invisible to the default ``get`` read."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-get")
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        state = await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
        )
        await _set_tombstone(db_session, row_id=state.id)

    async with db_session.begin():
        hidden = await store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
        )
        visible = await store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            include_tombstoned=True,
        )
    assert hidden is None
    assert visible is not None
    assert visible.date_tombstoned is not None
    assert visible.tombstone_reason == "manual_delete"


@pytest.mark.asyncio
async def test_list_for_org_hides_tombstoned_rows_by_default(
    db_session: AsyncSession,
) -> None:
    """``list_for_org`` filters tombstoned rows out unless asked."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-list")
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=1,
            ltd_slug="main",
        )
        tombstoned = await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=2,
            ltd_slug="u-jsick-feature",
        )
        await _set_tombstone(
            db_session, row_id=tombstoned.id, reason="lifecycle_delete"
        )

    async with db_session.begin():
        default_rows = await store.list_for_org(
            org_id=org_id, resource_type=ResourceType.edition
        )
        all_rows = await store.list_for_org(
            org_id=org_id,
            resource_type=ResourceType.edition,
            include_tombstoned=True,
        )
    assert sorted(r.ltd_id for r in default_rows if r.ltd_id is not None) == [
        1
    ]
    assert sorted(r.ltd_id for r in all_rows if r.ltd_id is not None) == [1, 2]


@pytest.mark.asyncio
async def test_list_project_resources_hides_tombstoned_by_default(
    db_session: AsyncSession,
) -> None:
    """``list_project_resources_for_org`` skips tombstoned rows by default."""
    logger = structlog.get_logger("test")
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="ks-tomb-projects")
    store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="pipelines",
        )
        deleted = await store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="dmtn-123",
        )
        await _set_tombstone(
            db_session, row_id=deleted.id, reason="manual_delete"
        )

    async with db_session.begin():
        default_page = await store.list_project_resources_for_org(
            org_id=org_id, cursor=None, limit=50
        )
        all_page = await store.list_project_resources_for_org(
            org_id=org_id, cursor=None, limit=50, include_tombstoned=True
        )
    assert sorted(r.ltd_slug for r in default_page.entries) == ["pipelines"]
    assert sorted(r.ltd_slug for r in all_page.entries) == [
        "dmtn-123",
        "pipelines",
    ]
