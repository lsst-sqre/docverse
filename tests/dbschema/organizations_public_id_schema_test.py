"""Test the ``organizations.public_id`` migration (``a8b9c0d1e2f3``).

The sibling of :mod:`tests.dbschema.projects_public_id_schema_test`: the
``fresh_engine`` fixture brings the schema up to the revision **before**
this migration (the ``projects`` public-ID backfill), the test seeds
populated ``organizations`` rows, then steps forward to the migration
under test. The migration adds a ``public_id`` column and backfills it
from each row's ``date_created`` in ascending order, so the assertions
pin that the backfilled IDs are unique and sort in ``date_created``
order regardless of primary-key order. The downgrade is exercised to
confirm the column and its unique constraint drop cleanly.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.support.migrations import alembic_downgrade, alembic_upgrade

# Revision immediately before this PR's organizations public_id migration
# (the sibling ``projects`` backfill).
PRE_PUBLIC_ID_REVISION = "f7a8b9c0d1e2"

# The migration under test.
PUBLIC_ID_REVISION = "a8b9c0d1e2f3"


async def _seed_orgs(engine: AsyncEngine) -> None:
    """Seed three organizations with distinct ``date_created``.

    The ``date_created`` order deliberately differs from primary-key
    order so the backfill's "order by ``date_created``" behaviour is
    observable: the earliest-created org is inserted second and the
    latest-created one is inserted first.
    """
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO organizations (slug, title, base_domain,
                    url_scheme, root_path_prefix,
                    purgatory_retention_seconds, date_created, date_updated)
                VALUES
                    ('org-newest', 'Newest', 'newest.example.com',
                     'subdomain', '/', 2592000,
                     NOW() - INTERVAL '1 minute', NOW()),
                    ('org-oldest', 'Oldest', 'oldest.example.com',
                     'subdomain', '/', 2592000,
                     NOW() - INTERVAL '3 minutes', NOW()),
                    ('org-middle', 'Middle', 'middle.example.com',
                     'subdomain', '/', 2592000,
                     NOW() - INTERVAL '2 minutes', NOW())
                """
            )
        )


@pytest.mark.asyncio
async def test_migration_backfills_ordered_unique_public_ids(
    fresh_engine: AsyncEngine,
) -> None:
    """Existing rows get unique public IDs sorted in ``date_created`` order."""
    await alembic_upgrade(PRE_PUBLIC_ID_REVISION)
    await _seed_orgs(fresh_engine)
    await alembic_upgrade(PUBLIC_ID_REVISION)

    async with fresh_engine.connect() as conn:
        column = (
            await conn.execute(
                text(
                    "SELECT data_type, is_nullable"
                    " FROM information_schema.columns"
                    " WHERE table_name = 'organizations'"
                    " AND column_name = 'public_id'"
                )
            )
        ).one()
        assert (column.data_type, column.is_nullable) == ("bigint", "NO")

        rows = (
            await conn.execute(
                text(
                    "SELECT slug, public_id FROM organizations"
                    " ORDER BY date_created ASC, id ASC"
                )
            )
        ).all()

    assert [row.slug for row in rows] == [
        "org-oldest",
        "org-middle",
        "org-newest",
    ]
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
    await _seed_orgs(fresh_engine)
    await alembic_upgrade(PUBLIC_ID_REVISION)

    async with fresh_engine.connect() as conn:
        existing = (
            await conn.execute(
                text("SELECT public_id FROM organizations LIMIT 1")
            )
        ).scalar_one()

    with pytest.raises(IntegrityError):
        async with fresh_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO organizations (public_id, slug, title,"
                    " base_domain, url_scheme, root_path_prefix,"
                    " purgatory_retention_seconds)"
                    " VALUES (:pid, 'org-duplicate', 'Duplicate',"
                    " 'duplicate.example.com', 'subdomain', '/', 2592000)"
                ),
                {"pid": existing},
            )


@pytest.mark.asyncio
async def test_downgrade_drops_public_id_column(
    fresh_engine: AsyncEngine,
) -> None:
    """The downgrade removes the column and its unique constraint cleanly."""
    await alembic_upgrade(PRE_PUBLIC_ID_REVISION)
    await _seed_orgs(fresh_engine)
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
                        " WHERE table_name = 'organizations'"
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
                        " WHERE conrelid = 'organizations'::regclass"
                    )
                )
            ).all()
        }
        assert "organizations_public_id_key" not in constraints
