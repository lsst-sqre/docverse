"""Test the ``queue_jobs.backend_queue_name`` migration (``a2b3c4d5e6f7``).

The abandoned reaper sweep verifies a stranded ``queued`` row against
the arq queue it was enqueued onto, so the column has to exist *and* be
nullable: rows written before this migration have no way to say which
pool holds their job, and the sweep falls back to probing every pool
queue for exactly those. This test pins both properties by stepping a
fresh schema from the revision before the migration to the migration
itself, and pins the downgrade so a rollback is safe.
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
from sqlalchemy.ext.asyncio import AsyncEngine

from alembic import command
from docverse_server.config import config
from docverse_server.dbschema import Base

# Revision immediately before the backend_queue_name migration.
PRE_QUEUE_NAME_REVISION = "f1a2b3c4d5e6"

# The migration under test.
QUEUE_NAME_REVISION = "a2b3c4d5e6f7"


@pytest_asyncio.fixture
async def fresh_engine() -> AsyncGenerator[AsyncEngine]:
    """Yield an engine pointing at a freshly-dropped DB.

    Mirrors the other ``tests/dbschema`` migration tests: the ``app``
    conftest fixture jumps straight to head, but this test needs to step
    the schema forward from a known earlier revision. On teardown the
    schema is restored to head so later tests' ``app`` fixtures find a
    clean DB.
    """
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
    # ``run_migrations_online`` calls ``asyncio.run`` internally, so the
    # alembic command has to run on a thread that owns its own loop.
    await asyncio.to_thread(command.upgrade, _alembic_config(), target)


async def _alembic_downgrade(target: str) -> None:
    await asyncio.to_thread(command.downgrade, _alembic_config(), target)


async def _backend_queue_name_column(
    engine: AsyncEngine,
) -> tuple[str, str] | None:
    """Return ``(data_type, is_nullable)`` for the column, or ``None``."""
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT data_type, is_nullable"
                    " FROM information_schema.columns"
                    " WHERE table_name = 'queue_jobs'"
                    " AND column_name = 'backend_queue_name'"
                )
            )
        ).first()
    if row is None:
        return None
    return row.data_type, row.is_nullable


@pytest.mark.asyncio
async def test_migration_adds_nullable_backend_queue_name(
    fresh_engine: AsyncEngine,
) -> None:
    """The column arrives nullable, and rows seeded before it stay valid."""
    await _alembic_upgrade(PRE_QUEUE_NAME_REVISION)
    assert await _backend_queue_name_column(fresh_engine) is None

    async with fresh_engine.begin() as conn:
        org_id = (
            await conn.execute(
                text(
                    "INSERT INTO organizations"
                    " (slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES ('bqn-org', 'BQN Org',"
                    "  'bqn.example.com', 'subdomain', '/', 2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
        await conn.execute(
            text(
                "INSERT INTO queue_jobs"
                " (public_id, backend_job_id, kind, status, org_id)"
                " VALUES (1, 'arq-legacy', 'dashboard_build',"
                "  'queued', :org)"
            ),
            {"org": org_id},
        )

    await _alembic_upgrade(QUEUE_NAME_REVISION)

    assert await _backend_queue_name_column(fresh_engine) == (
        "character varying",
        "YES",
    )

    # No backfill: the pre-existing row reads as "queue unknown", which
    # is what routes it to the sweep's multi-queue fallback probe.
    async with fresh_engine.connect() as conn:
        legacy = (
            await conn.execute(
                text(
                    "SELECT backend_job_id, backend_queue_name FROM queue_jobs"
                )
            )
        ).one()
    assert legacy.backend_job_id == "arq-legacy"
    assert legacy.backend_queue_name is None


@pytest.mark.asyncio
async def test_downgrade_drops_backend_queue_name(
    fresh_engine: AsyncEngine,
) -> None:
    """Rolling back removes the column without touching the rest."""
    await _alembic_upgrade(QUEUE_NAME_REVISION)
    assert await _backend_queue_name_column(fresh_engine) is not None

    await _alembic_downgrade(PRE_QUEUE_NAME_REVISION)

    assert await _backend_queue_name_column(fresh_engine) is None
    async with fresh_engine.connect() as conn:
        still_there = (
            await conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns"
                    " WHERE table_name = 'queue_jobs'"
                    " AND column_name = 'backend_job_id'"
                )
            )
        ).first()
    assert still_there is not None
