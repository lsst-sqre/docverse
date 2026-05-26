"""Test the ``git_ref_audit_runs`` schema migration (``z4a5b6c7d8e9``).

Mirrors :mod:`tests.dbschema.projects_github_binding_test`: the
``fresh_engine`` fixture brings the schema up to the revision
**before** the audit-table migration and then steps forward to the
audit migration itself. The test pins both directions of the
upgrade against PostgreSQL — table presence, FK on ``queue_jobs``,
both new indexes (mutex + secondary), and the partial-unique
non-terminal index on ``git_ref_audit_runs``.
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

# Revision immediately before this PR's git_ref_audit_runs migration.
PRE_AUDIT_REVISION = "y3z4a5b6c7d8"

# The new audit-table migration under test.
AUDIT_REVISION = "z4a5b6c7d8e9"


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


@pytest.mark.asyncio
async def test_migration_creates_table_columns_and_indexes(
    fresh_engine: AsyncEngine,
) -> None:
    """``git_ref_audit_runs`` exists with the expected columns and indexes."""
    await _alembic_upgrade(PRE_AUDIT_REVISION)
    await _alembic_upgrade(AUDIT_REVISION)

    async with fresh_engine.connect() as conn:
        columns = {
            row.column_name: (row.data_type, row.is_nullable)
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name, data_type, is_nullable"
                        " FROM information_schema.columns"
                        " WHERE table_name = 'git_ref_audit_runs'"
                    )
                )
            ).all()
        }
        assert "id" in columns
        assert "status" in columns
        assert columns["status"][1] == "NO"
        assert "date_started" in columns
        assert "date_finished" in columns
        assert columns["date_finished"][1] == "YES"
        assert "summary" in columns
        assert columns["summary"][0] == "jsonb"

        # FK column on queue_jobs.
        qj_columns = {
            row.column_name: row.is_nullable
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name, is_nullable"
                        " FROM information_schema.columns"
                        " WHERE table_name = 'queue_jobs'"
                        " AND column_name = 'git_ref_audit_run_id'"
                    )
                )
            ).all()
        }
        assert qj_columns == {"git_ref_audit_run_id": "YES"}

        indexes = {
            row.indexname
            for row in (
                await conn.execute(
                    text(
                        "SELECT indexname FROM pg_indexes"
                        " WHERE schemaname = 'public'"
                    )
                )
            ).all()
        }
        assert "idx_git_ref_audit_runs_non_terminal_uq" in indexes
        assert "idx_queue_jobs_git_ref_audit_active_uq" in indexes
        assert "idx_queue_jobs_git_ref_audit_run_id" in indexes


@pytest.mark.asyncio
async def test_partial_unique_non_terminal_index_enforces_singleton(
    fresh_engine: AsyncEngine,
) -> None:
    """Two non-terminal rows are rejected; terminal rows do not collide."""
    await _alembic_upgrade(AUDIT_REVISION)
    async with fresh_engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO git_ref_audit_runs (status) VALUES ('pending')")
        )

    with pytest.raises(IntegrityError):
        async with fresh_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO git_ref_audit_runs (status)"
                    " VALUES ('in_progress')"
                )
            )

    # Terminal rows do not participate in the unique constraint, so
    # several terminal rows can coexist with one non-terminal row.
    async with fresh_engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO git_ref_audit_runs (status) VALUES"
                " ('succeeded'), ('failed'), ('partial_failure')"
            )
        )


@pytest.mark.asyncio
async def test_status_check_rejects_invalid_value(
    fresh_engine: AsyncEngine,
) -> None:
    """A status outside the allowed set fails the CHECK constraint."""
    await _alembic_upgrade(AUDIT_REVISION)
    with pytest.raises(IntegrityError):
        async with fresh_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO git_ref_audit_runs (status) VALUES ('bogus')"
                )
            )


@pytest.mark.asyncio
async def test_downgrade_removes_table_and_fk_column(
    fresh_engine: AsyncEngine,
) -> None:
    """The downgrade path is symmetric with the upgrade.

    Pins that an operator can roll forward and back through this
    revision without leaving dead rows or stale FK constraints.
    """
    await _alembic_upgrade(AUDIT_REVISION)
    await _alembic_downgrade(PRE_AUDIT_REVISION)

    async with fresh_engine.connect() as conn:
        tables = {
            row.table_name
            for row in (
                await conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables"
                        " WHERE table_schema = 'public'"
                    )
                )
            ).all()
        }
        assert "git_ref_audit_runs" not in tables

        qj_columns = {
            row.column_name
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name"
                        " FROM information_schema.columns"
                        " WHERE table_name = 'queue_jobs'"
                    )
                )
            ).all()
        }
        assert "git_ref_audit_run_id" not in qj_columns
