"""Test the ``keeper_sync_state.public_id`` migration (``d8e9f0a1b2c3``).

Mirrors :mod:`tests.dbschema.keeper_sync_runs_public_id_schema_test`: the
``fresh_engine`` fixture brings the schema up to the revision **before**
this migration, the test seeds populated ``keeper_sync_state`` rows, then
steps forward to the migration under test. The migration adds a
``public_id`` column and backfills it from each row's
``COALESCE(date_tombstoned, date_last_synced, now())`` in ascending
order, so the assertions pin that the backfilled IDs are unique and sort
in that order regardless of primary-key order — including rows where
every timestamp is NULL, which fall back to ``now()`` and order last. The
downgrade is exercised to confirm the column and its unique constraint
drop cleanly.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
import structlog
from alembic.config import Config
from safir.database import (
    create_database_engine,
    drop_database,
    initialize_database,
    stamp_database_async,
)
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from alembic import command
from docverse.config import config
from docverse.dbschema import Base

# Revision immediately before this PR's public_id migration.
PRE_PUBLIC_ID_REVISION = "c7d8e9f0a1b2"

# The migration under test.
PUBLIC_ID_REVISION = "d8e9f0a1b2c3"


@pytest_asyncio.fixture
async def fresh_engine() -> AsyncGenerator[AsyncEngine]:
    """Yield an engine pointing at a freshly-dropped DB."""
    logger = structlog.get_logger("docverse")
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    await drop_database(engine, Base.metadata)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    try:
        yield engine
    finally:
        await drop_database(engine, Base.metadata)
        async with engine.begin() as conn:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        await initialize_database(
            engine, logger, schema=Base.metadata, reset=True
        )
        await stamp_database_async(engine)
        await engine.dispose()


def _alembic_config() -> Config:
    return Config("alembic.ini")


async def _alembic_upgrade(target: str) -> None:
    await asyncio.to_thread(command.upgrade, _alembic_config(), target)


async def _alembic_downgrade(target: str) -> None:
    await asyncio.to_thread(command.downgrade, _alembic_config(), target)


async def _seed_states(engine: AsyncEngine) -> int:
    """Seed one org and four state rows with mixed timestamp coverage.

    The backfill orders by ``COALESCE(date_tombstoned, date_last_synced,
    now())``. The seeded rows deliberately exercise every branch of that
    expression, and their primary-key order differs from their expected
    backfill order so the "order by the coalesced timestamp" behaviour is
    observable:

    1. inserted first, coalesces to ``date_tombstoned`` (2 minutes ago) →
       expected backfill order 2.
    2. inserted second, coalesces to ``date_last_synced`` (5 minutes ago,
       no tombstone) → expected backfill order 1 (earliest).
    3. inserted third, coalesces to ``date_tombstoned`` (1 minute ago) →
       expected backfill order 3.
    4. inserted fourth, all timestamps NULL → coalesces to ``now()`` →
       expected backfill order 4 (latest).

    Returns the org id.
    """
    async with engine.begin() as conn:
        org_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO organizations"
                    " (slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES ('kss-pid-org', 'KSS PID Org',"
                    "  'kss.example.com', 'subdomain', '/', 2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
        await conn.execute(
            text(
                """
                INSERT INTO keeper_sync_state (org_id, resource_type,
                    ltd_id, ltd_slug, date_last_synced, date_tombstoned)
                VALUES
                    (:org, 'edition', 1, 'e-1',
                     NULL, NOW() - INTERVAL '2 minutes'),
                    (:org, 'edition', 2, 'e-2',
                     NOW() - INTERVAL '5 minutes', NULL),
                    (:org, 'edition', 3, 'e-3',
                     NULL, NOW() - INTERVAL '1 minute'),
                    (:org, 'edition', 4, 'e-4',
                     NULL, NULL)
                """
            ),
            {"org": org_id},
        )
    return org_id


@pytest.mark.asyncio
async def test_migration_backfills_ordered_unique_public_ids(
    fresh_engine: AsyncEngine,
) -> None:
    """Existing rows get unique public IDs sorted in coalesced-ts order."""
    await _alembic_upgrade(PRE_PUBLIC_ID_REVISION)
    await _seed_states(fresh_engine)
    await _alembic_upgrade(PUBLIC_ID_REVISION)

    async with fresh_engine.connect() as conn:
        columns = {
            row.column_name: (row.data_type, row.is_nullable)
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name, data_type, is_nullable"
                        " FROM information_schema.columns"
                        " WHERE table_name = 'keeper_sync_state'"
                        " AND column_name = 'public_id'"
                    )
                )
            ).all()
        }
        assert columns["public_id"] == ("bigint", "NO")

        # Public IDs, read back in the same coalesced-timestamp order the
        # backfill used, which also places the all-NULL row (row 4) last.
        rows = (
            await conn.execute(
                text(
                    "SELECT ltd_slug, public_id FROM keeper_sync_state"
                    " ORDER BY"
                    " COALESCE(date_tombstoned, date_last_synced, now())"
                    " ASC, id ASC"
                )
            )
        ).all()

    slugs = [row.ltd_slug for row in rows]
    public_ids = [row.public_id for row in rows]

    assert len(public_ids) == 4
    # The all-NULL row sorts last (now() beats every past timestamp).
    assert slugs == ["e-2", "e-1", "e-3", "e-4"]
    # Every ID is populated, unique, and strictly increasing in order.
    assert all(pid is not None for pid in public_ids)
    assert len(set(public_ids)) == 4
    assert public_ids == sorted(public_ids)
    assert public_ids[0] < public_ids[1] < public_ids[2] < public_ids[3]


@pytest.mark.asyncio
async def test_unique_constraint_rejects_duplicate_public_id(
    fresh_engine: AsyncEngine,
) -> None:
    """The backfilled column carries a working unique constraint."""
    await _alembic_upgrade(PRE_PUBLIC_ID_REVISION)
    org_id = await _seed_states(fresh_engine)
    await _alembic_upgrade(PUBLIC_ID_REVISION)

    async with fresh_engine.connect() as conn:
        existing = (
            await conn.execute(
                text("SELECT public_id FROM keeper_sync_state LIMIT 1")
            )
        ).scalar_one()

    with pytest.raises(IntegrityError):
        async with fresh_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO keeper_sync_state (public_id, org_id,"
                    " resource_type, ltd_id, ltd_slug)"
                    " VALUES (:pid, :org, 'edition', 99, 'e-99')"
                ),
                {"pid": existing, "org": org_id},
            )


@pytest.mark.asyncio
async def test_downgrade_drops_public_id_column(
    fresh_engine: AsyncEngine,
) -> None:
    """The downgrade removes the column and its unique constraint cleanly."""
    await _alembic_upgrade(PRE_PUBLIC_ID_REVISION)
    await _seed_states(fresh_engine)
    await _alembic_upgrade(PUBLIC_ID_REVISION)
    await _alembic_downgrade(PRE_PUBLIC_ID_REVISION)

    async with fresh_engine.connect() as conn:
        columns = {
            row.column_name
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name"
                        " FROM information_schema.columns"
                        " WHERE table_name = 'keeper_sync_state'"
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
                        " WHERE conrelid = 'keeper_sync_state'::regclass"
                    )
                )
            ).all()
        }
        assert "keeper_sync_state_public_id_key" not in constraints
