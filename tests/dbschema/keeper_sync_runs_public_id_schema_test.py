"""Test the ``keeper_sync_runs.public_id`` migration (``c7d8e9f0a1b2``).

Mirrors :mod:`tests.dbschema.git_ref_audit_runs_schema_test`: the
``fresh_engine`` fixture brings the schema up to the revision **before**
this migration, the test seeds populated ``keeper_sync_runs`` rows, then
steps forward to the migration under test. The migration adds a
``public_id`` column and backfills it from each row's ``date_started`` in
ascending order, so the assertions pin that the backfilled IDs are unique
and sort in ``date_started`` order regardless of primary-key order. The
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
PRE_PUBLIC_ID_REVISION = "b6c7d8e9f0a1"

# The migration under test.
PUBLIC_ID_REVISION = "c7d8e9f0a1b2"


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


async def _seed_runs(engine: AsyncEngine) -> int:
    """Seed one org and three terminal runs with distinct ``date_started``.

    The ``date_started`` order deliberately differs from primary-key order
    so the backfill's "order by ``date_started``" behaviour is observable:
    the earliest-started run is inserted second, and the latest-started run
    is inserted first. All three are seeded in a terminal status so the
    one-non-terminal-run-per-org partial unique index tolerates the trio.

    Returns the org id.
    """
    async with engine.begin() as conn:
        org_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO organizations"
                    " (slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES ('ksr-pid-org', 'KSR PID Org',"
                    "  'ksr.example.com', 'subdomain', '/', 2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
        await conn.execute(
            text(
                """
                INSERT INTO keeper_sync_runs (org_id, kind, status,
                    date_started)
                VALUES
                    (:org, 'backfill', 'succeeded',
                     NOW() - INTERVAL '1 minute'),
                    (:org, 'backfill', 'succeeded',
                     NOW() - INTERVAL '3 minutes'),
                    (:org, 'backfill', 'succeeded',
                     NOW() - INTERVAL '2 minutes')
                """
            ),
            {"org": org_id},
        )
    return org_id


@pytest.mark.asyncio
async def test_migration_backfills_ordered_unique_public_ids(
    fresh_engine: AsyncEngine,
) -> None:
    """Existing rows get unique public IDs sorted in ``date_started`` order."""
    await _alembic_upgrade(PRE_PUBLIC_ID_REVISION)
    await _seed_runs(fresh_engine)
    await _alembic_upgrade(PUBLIC_ID_REVISION)

    async with fresh_engine.connect() as conn:
        columns = {
            row.column_name: (row.data_type, row.is_nullable)
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name, data_type, is_nullable"
                        " FROM information_schema.columns"
                        " WHERE table_name = 'keeper_sync_runs'"
                        " AND column_name = 'public_id'"
                    )
                )
            ).all()
        }
        assert columns["public_id"] == ("bigint", "NO")

        # Public IDs, read back in date_started order.
        public_ids = [
            row.public_id
            for row in (
                await conn.execute(
                    text(
                        "SELECT public_id FROM keeper_sync_runs"
                        " ORDER BY date_started ASC, id ASC"
                    )
                )
            ).all()
        ]

    assert len(public_ids) == 3
    # Every ID is populated, unique, and strictly increasing in
    # date_started order.
    assert all(pid is not None for pid in public_ids)
    assert len(set(public_ids)) == 3
    assert public_ids == sorted(public_ids)
    assert public_ids[0] < public_ids[1] < public_ids[2]


@pytest.mark.asyncio
async def test_unique_constraint_rejects_duplicate_public_id(
    fresh_engine: AsyncEngine,
) -> None:
    """The backfilled column carries a working unique constraint."""
    await _alembic_upgrade(PRE_PUBLIC_ID_REVISION)
    org_id = await _seed_runs(fresh_engine)
    await _alembic_upgrade(PUBLIC_ID_REVISION)

    async with fresh_engine.connect() as conn:
        existing = (
            await conn.execute(
                text("SELECT public_id FROM keeper_sync_runs LIMIT 1")
            )
        ).scalar_one()

    with pytest.raises(IntegrityError):
        async with fresh_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO keeper_sync_runs (public_id, org_id, kind,"
                    " status) VALUES (:pid, :org, 'backfill', 'succeeded')"
                ),
                {"pid": existing, "org": org_id},
            )


@pytest.mark.asyncio
async def test_downgrade_drops_public_id_column(
    fresh_engine: AsyncEngine,
) -> None:
    """The downgrade removes the column and its unique constraint cleanly."""
    await _alembic_upgrade(PRE_PUBLIC_ID_REVISION)
    await _seed_runs(fresh_engine)
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
                        " WHERE table_name = 'keeper_sync_runs'"
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
                        " WHERE conrelid = 'keeper_sync_runs'::regclass"
                    )
                )
            ).all()
        }
        assert "keeper_sync_runs_public_id_key" not in constraints
