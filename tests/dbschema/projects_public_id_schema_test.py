"""Test the ``projects.public_id`` migration (``f7a8b9c0d1e2``).

Mirrors :mod:`tests.dbschema.keeper_sync_runs_public_id_schema_test`: the
``fresh_engine`` fixture brings the schema up to the revision **before**
this migration, the test seeds populated ``projects`` rows, then steps
forward to the migration under test. The migration adds a ``public_id``
column and backfills it from each row's ``date_created`` in ascending
order, so the assertions pin that the backfilled IDs are unique and sort
in ``date_created`` order regardless of primary-key order. The downgrade
is exercised to confirm the column and its unique constraint drop
cleanly.

A project's public ID appears in no object-store key — builds carry
their own — so re-minting IDs for existing rows is safe.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.support.migrations import alembic_downgrade, alembic_upgrade

# Revision immediately before this PR's projects public_id migration.
PRE_PUBLIC_ID_REVISION = "e6f7a8b9c0d1"

# The migration under test.
PUBLIC_ID_REVISION = "f7a8b9c0d1e2"


async def _seed_projects(engine: AsyncEngine) -> int:
    """Seed one org and three projects with distinct ``date_created``.

    The ``date_created`` order deliberately differs from primary-key
    order so the backfill's "order by ``date_created``" behaviour is
    observable: the earliest-created project is inserted second and the
    latest-created one is inserted first.

    Returns the org id.
    """
    async with engine.begin() as conn:
        org_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO organizations"
                    " (slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES ('proj-pid-org', 'Project PID Org',"
                    "  'proj-pid.example.com', 'subdomain', '/', 2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
        await conn.execute(
            text(
                """
                INSERT INTO projects (slug, title, org_id, date_created,
                    date_updated)
                VALUES
                    ('newest', 'Newest', :org,
                     NOW() - INTERVAL '1 minute', NOW()),
                    ('oldest', 'Oldest', :org,
                     NOW() - INTERVAL '3 minutes', NOW()),
                    ('middle', 'Middle', :org,
                     NOW() - INTERVAL '2 minutes', NOW())
                """
            ),
            {"org": org_id},
        )
    return org_id


@pytest.mark.asyncio
async def test_migration_backfills_ordered_unique_public_ids(
    fresh_engine: AsyncEngine,
) -> None:
    """Existing rows get unique public IDs sorted in ``date_created`` order."""
    await alembic_upgrade(PRE_PUBLIC_ID_REVISION)
    await _seed_projects(fresh_engine)
    await alembic_upgrade(PUBLIC_ID_REVISION)

    async with fresh_engine.connect() as conn:
        column = (
            await conn.execute(
                text(
                    "SELECT data_type, is_nullable"
                    " FROM information_schema.columns"
                    " WHERE table_name = 'projects'"
                    " AND column_name = 'public_id'"
                )
            )
        ).one()
        assert (column.data_type, column.is_nullable) == ("bigint", "NO")

        rows = (
            await conn.execute(
                text(
                    "SELECT slug, public_id FROM projects"
                    " ORDER BY date_created ASC, id ASC"
                )
            )
        ).all()

    assert [row.slug for row in rows] == ["oldest", "middle", "newest"]
    public_ids = [row.public_id for row in rows]
    assert all(pid is not None for pid in public_ids)
    assert len(set(public_ids)) == 3
    assert public_ids[0] < public_ids[1] < public_ids[2]


@pytest.mark.asyncio
async def test_unique_constraint_rejects_duplicate_public_id(
    fresh_engine: AsyncEngine,
) -> None:
    """The backfilled column carries a working unique constraint."""
    await alembic_upgrade(PRE_PUBLIC_ID_REVISION)
    org_id = await _seed_projects(fresh_engine)
    await alembic_upgrade(PUBLIC_ID_REVISION)

    async with fresh_engine.connect() as conn:
        existing = (
            await conn.execute(text("SELECT public_id FROM projects LIMIT 1"))
        ).scalar_one()

    with pytest.raises(IntegrityError):
        async with fresh_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO projects (public_id, slug, title, org_id)"
                    " VALUES (:pid, 'duplicate', 'Duplicate', :org)"
                ),
                {"pid": existing, "org": org_id},
            )


@pytest.mark.asyncio
async def test_downgrade_drops_public_id_column(
    fresh_engine: AsyncEngine,
) -> None:
    """The downgrade removes the column and its unique constraint cleanly."""
    await alembic_upgrade(PRE_PUBLIC_ID_REVISION)
    await _seed_projects(fresh_engine)
    await alembic_upgrade(PUBLIC_ID_REVISION)
    await alembic_downgrade(PRE_PUBLIC_ID_REVISION)

    async with fresh_engine.connect() as conn:
        columns = {
            row.column_name
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name"
                        " FROM information_schema.columns"
                        " WHERE table_name = 'projects'"
                    )
                )
            ).all()
        }
        assert "public_id" not in columns

        constraints = {
            row.conname
            for row in (
                await conn.execute(
                    text(
                        "SELECT conname FROM pg_constraint"
                        " WHERE conrelid = 'projects'::regclass"
                    )
                )
            ).all()
        }
        assert "projects_public_id_key" not in constraints
