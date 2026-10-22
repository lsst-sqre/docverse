"""Test the ``editions.delete_reason`` migration (``d1e2f3a4b5c6``).

PRD #830 (DM-56771) records why an edition was soft-deleted on the
edition row itself, because a native (direct-upload) edition has no
``keeper_sync_state`` row to carry a tombstone reason. The migration
adds the nullable, CHECK-constrained column and backfills existing
soft-deleted editions from the tombstone on their keeper-sync state row,
joined on ``(org_id, resource_type='edition', docverse_id)``. A row with
nothing to copy from stays ``NULL``, which every consumer reads as
"revivable". This test steps a fresh schema across the migration with
each backfill case seeded beforehand, pins the constraint, and pins the
downgrade so a rollback is safe.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from tests.support.migrations import alembic_downgrade, alembic_upgrade

# Revision immediately before this migration (projects default branch).
PRE_DELETE_REASON_REVISION = "c0d1e2f3a4b5"

# The migration under test.
DELETE_REASON_REVISION = "d1e2f3a4b5c6"


@dataclass(frozen=True, slots=True)
class _Seeded:
    """Edition ids for each backfill case, keyed by what they exercise."""

    manual: int
    lifecycle: int
    preemptive: int
    no_state_row: int
    untombstoned_state_row: int
    other_org_state_row: int
    live_with_tombstone: int
    retombstoned: int


async def _delete_reason_column(
    engine: AsyncEngine,
) -> tuple[str, int | None, str] | None:
    """Return ``(data_type, max_length, is_nullable)``, or ``None``."""
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT data_type, character_maximum_length, is_nullable"
                    " FROM information_schema.columns"
                    " WHERE table_name = 'editions'"
                    " AND column_name = 'delete_reason'"
                )
            )
        ).first()
    if row is None:
        return None
    return row.data_type, row.character_maximum_length, row.is_nullable


async def _insert_org(
    conn: AsyncConnection, *, public_id: int, slug: str
) -> int:
    org_id: int = (
        await conn.execute(
            text(
                "INSERT INTO organizations"
                " (public_id, slug, title, base_domain, url_scheme,"
                "  root_path_prefix, purgatory_retention_seconds)"
                " VALUES (:public_id, :slug, :slug, :domain, 'subdomain',"
                "  '/', 2592000)"
                " RETURNING id"
            ),
            {
                "public_id": public_id,
                "slug": slug,
                "domain": f"{slug}.example.com",
            },
        )
    ).scalar_one()
    return org_id


async def _insert_edition(
    conn: AsyncConnection, *, project_id: int, slug: str, deleted: bool
) -> int:
    edition_id: int = (
        await conn.execute(
            text(
                "INSERT INTO editions"
                " (slug, title, project_id, kind, tracking_mode,"
                "  lifecycle_exempt, date_deleted)"
                " VALUES (:slug, :slug, :project_id, 'draft', 'git_ref',"
                "  false,"
                "  CASE WHEN :deleted THEN NOW() ELSE NULL END)"
                " RETURNING id"
            ),
            {"slug": slug, "project_id": project_id, "deleted": deleted},
        )
    ).scalar_one()
    return edition_id


async def _insert_state(
    conn: AsyncConnection,
    *,
    org_id: int,
    ltd_id: int,
    docverse_id: int,
    reason: str | None,
    tombstoned_minutes_ago: int = 1,
) -> None:
    await conn.execute(
        text(
            "INSERT INTO keeper_sync_state"
            " (public_id, org_id, resource_type, ltd_id, ltd_slug,"
            "  docverse_id, date_tombstoned, tombstone_reason)"
            " VALUES (:public_id, :org_id, 'edition', :ltd_id, :ltd_slug,"
            "  :docverse_id,"
            "  CASE WHEN CAST(:reason AS VARCHAR) IS NULL THEN NULL"
            "   ELSE NOW() - make_interval(mins => :minutes) END,"
            "  :reason)"
        ),
        {
            # ``ltd_id`` is unique per seed, so it doubles as the key.
            "public_id": 1000 + ltd_id,
            "org_id": org_id,
            "ltd_id": ltd_id,
            "ltd_slug": f"ltd-{ltd_id}",
            "docverse_id": docverse_id,
            "reason": reason,
            "minutes": tombstoned_minutes_ago,
        },
    )


async def _seed(engine: AsyncEngine) -> _Seeded:
    """Seed one edition per backfill case at the pre-migration revision."""
    async with engine.begin() as conn:
        org_id = await _insert_org(conn, public_id=1, slug="dr-org")
        other_org_id = await _insert_org(conn, public_id=2, slug="dr-other")
        project_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO projects"
                    " (public_id, slug, title, org_id, source_url)"
                    " VALUES (1, 'dr-proj', 'DR Proj', :org,"
                    "  'https://github.com/example/dr-proj')"
                    " RETURNING id"
                ),
                {"org": org_id},
            )
        ).scalar_one()

        async def edition(slug: str, *, deleted: bool = True) -> int:
            return await _insert_edition(
                conn, project_id=project_id, slug=slug, deleted=deleted
            )

        seeded = _Seeded(
            manual=await edition("manual"),
            lifecycle=await edition("lifecycle"),
            preemptive=await edition("preemptive"),
            no_state_row=await edition("native"),
            untombstoned_state_row=await edition("untombstoned"),
            other_org_state_row=await edition("other-org"),
            live_with_tombstone=await edition("live", deleted=False),
            retombstoned=await edition("retombstoned"),
        )
        await _insert_state(
            conn,
            org_id=org_id,
            ltd_id=1,
            docverse_id=seeded.manual,
            reason="manual_delete",
        )
        await _insert_state(
            conn,
            org_id=org_id,
            ltd_id=2,
            docverse_id=seeded.lifecycle,
            reason="lifecycle_delete",
        )
        await _insert_state(
            conn,
            org_id=org_id,
            ltd_id=3,
            docverse_id=seeded.preemptive,
            reason="lifecycle_preemptive",
        )
        await _insert_state(
            conn,
            org_id=org_id,
            ltd_id=4,
            docverse_id=seeded.untombstoned_state_row,
            reason=None,
        )
        # The state row names this edition's id but belongs to another
        # org, so the join's ``org_id`` leg must not match it.
        await _insert_state(
            conn,
            org_id=other_org_id,
            ltd_id=5,
            docverse_id=seeded.other_org_state_row,
            reason="manual_delete",
        )
        await _insert_state(
            conn,
            org_id=org_id,
            ltd_id=6,
            docverse_id=seeded.live_with_tombstone,
            reason="manual_delete",
        )
        # Two state rows point at one edition: the most recent
        # tombstone is the one that describes its delete.
        await _insert_state(
            conn,
            org_id=org_id,
            ltd_id=7,
            docverse_id=seeded.retombstoned,
            reason="manual_delete",
            tombstoned_minutes_ago=60,
        )
        await _insert_state(
            conn,
            org_id=org_id,
            ltd_id=8,
            docverse_id=seeded.retombstoned,
            reason="lifecycle_delete",
            tombstoned_minutes_ago=5,
        )
    return seeded


async def _delete_reasons(engine: AsyncEngine) -> dict[int, str | None]:
    """Return ``{edition id: delete_reason}`` for every edition."""
    async with engine.connect() as conn:
        rows = (
            await conn.execute(text("SELECT id, delete_reason FROM editions"))
        ).all()
    return {row.id: row.delete_reason for row in rows}


@pytest.mark.asyncio
async def test_migration_adds_nullable_delete_reason(
    fresh_engine: AsyncEngine,
) -> None:
    """The column arrives as a nullable ``VARCHAR(32)``."""
    await alembic_upgrade(PRE_DELETE_REASON_REVISION)
    assert await _delete_reason_column(fresh_engine) is None

    await alembic_upgrade(DELETE_REASON_REVISION)

    assert await _delete_reason_column(fresh_engine) == (
        "character varying",
        32,
        "YES",
    )


@pytest.mark.asyncio
async def test_migration_backfills_from_keeper_sync_tombstones(
    fresh_engine: AsyncEngine,
) -> None:
    """Soft-deleted editions inherit their state row's tombstone reason."""
    await alembic_upgrade(PRE_DELETE_REASON_REVISION)
    seeded = await _seed(fresh_engine)

    await alembic_upgrade(DELETE_REASON_REVISION)

    reasons = await _delete_reasons(fresh_engine)
    assert reasons[seeded.manual] == "manual_delete"
    assert reasons[seeded.lifecycle] == "lifecycle_delete"
    # A preemptive tombstone is still a lifecycle deletion.
    assert reasons[seeded.preemptive] == "lifecycle_delete"
    assert reasons[seeded.retombstoned] == "lifecycle_delete"


@pytest.mark.asyncio
async def test_migration_leaves_rows_without_a_tombstone_null(
    fresh_engine: AsyncEngine,
) -> None:
    """No tombstone to copy from — or no deletion — means ``NULL``."""
    await alembic_upgrade(PRE_DELETE_REASON_REVISION)
    seeded = await _seed(fresh_engine)

    await alembic_upgrade(DELETE_REASON_REVISION)

    reasons = await _delete_reasons(fresh_engine)
    assert reasons[seeded.no_state_row] is None
    assert reasons[seeded.untombstoned_state_row] is None
    assert reasons[seeded.other_org_state_row] is None
    assert reasons[seeded.live_with_tombstone] is None


@pytest.mark.asyncio
async def test_check_constraint_rejects_unknown_reasons(
    fresh_engine: AsyncEngine,
) -> None:
    """Only ``manual_delete`` and ``lifecycle_delete`` are storable."""
    await alembic_upgrade(PRE_DELETE_REASON_REVISION)
    seeded = await _seed(fresh_engine)
    await alembic_upgrade(DELETE_REASON_REVISION)

    with pytest.raises(IntegrityError, match="ck_editions_delete_reason"):
        async with fresh_engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE editions"
                    " SET delete_reason = 'lifecycle_preemptive'"
                    " WHERE id = :id"
                ),
                {"id": seeded.no_state_row},
            )


@pytest.mark.asyncio
async def test_downgrade_drops_delete_reason(
    fresh_engine: AsyncEngine,
) -> None:
    """Rolling back removes the column and its constraint."""
    await alembic_upgrade(DELETE_REASON_REVISION)
    assert await _delete_reason_column(fresh_engine) is not None

    await alembic_downgrade(PRE_DELETE_REASON_REVISION)

    assert await _delete_reason_column(fresh_engine) is None
    async with fresh_engine.connect() as conn:
        constraint = (
            await conn.execute(
                text(
                    "SELECT 1 FROM pg_constraint"
                    " WHERE conname = 'ck_editions_delete_reason'"
                )
            )
        ).first()
    assert constraint is None
