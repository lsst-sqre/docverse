"""Test the embedded duplicate-cleanup in the partial-UQ migrations.

The two ``20260509_*`` migrations
(``t8u9v0w1x2y3_add_queue_jobs_keeper_sync_project_active_uq`` and
``u9v0w1x2y3z4_add_queue_jobs_dashboard_build_active_uq``) each prepend
an ``UPDATE`` that fails any duplicate ``(queued|in_progress)`` rows
that accumulated under the pre-mutex / pre-dedup code path before
creating the partial unique index. The cleanup is what unblocked the
staging deploy at revision ``s7t8u9v0w1x2`` after a
``UniqueViolationError`` on ``(org_id=2, subject_label='phalanx')``.

This test seeds duplicate rows of both kinds at the pre-cleanup
revision, runs ``alembic upgrade head``, and asserts the survivor-
selection invariant (``MAX(id)`` keeps its active status, the rest are
``failed`` with ``date_completed`` populated) plus the existence of both
partial unique indexes with their documented predicates. A second
``alembic upgrade head`` proves the cleanup is idempotent on a clean
DB.
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
from docverse.config import config
from docverse.dbschema import Base

# Revision immediately before the two migrations under test.
PRE_CLEANUP_REVISION = "s7t8u9v0w1x2"


@pytest_asyncio.fixture
async def fresh_engine() -> AsyncGenerator[AsyncEngine]:
    """Yield an engine pointing at a freshly-dropped DB.

    The ``app`` conftest fixture is intentionally not used: this test
    needs to run alembic migrations forward over a known earlier
    revision, not jump straight to head via ``initialize_database`` +
    ``stamp_database_async``.

    On fixture teardown the schema is restored to head with a fresh
    ORM-driven create + stamp so subsequent tests in the suite see a
    clean DB regardless of whether this test left partial state behind.
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
        # Restore a head-stamped DB so the shared test database is in
        # a state subsequent tests' ``app`` fixtures can reset cleanly.
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


async def _seed_duplicates(engine: AsyncEngine) -> tuple[int, int]:
    """Insert two pairs of duplicates: one per kind under test.

    Returns ``(org_id, project_id)`` so the assertions can refer to the
    DB-generated keys.
    """
    async with engine.begin() as conn:
        # An org and a project are required to satisfy the
        # ``fk_queue_jobs_project_id`` FK that the dashboard_build
        # duplicates seed against. ``queue_jobs.org_id`` has no FK so
        # the same org is reused for the keeper_sync_project pair.
        org_id = (
            await conn.execute(
                text(
                    "INSERT INTO organizations"
                    " (slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES ('cleanuporg', 'Cleanup Org',"
                    "  'cleanup.example.com', 'subdomain', '/', 2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
        project_id = (
            await conn.execute(
                text(
                    "INSERT INTO projects"
                    " (slug, title, org_id, doc_repo)"
                    " VALUES ('cleanup-proj', 'Cleanup Project',"
                    " :org_id, 'https://github.com/example/cleanup-proj')"
                    " RETURNING id"
                ),
                {"org_id": org_id},
            )
        ).scalar_one()
        # keeper_sync_project pair: same (org_id, subject_label).
        await conn.execute(
            text(
                """
                INSERT INTO queue_jobs (
                    public_id, kind, status, org_id, subject_label,
                    date_created
                )
                VALUES
                    (:pid1, 'keeper_sync_project', 'queued', :org,
                     :slug, NOW() - INTERVAL '5 minutes'),
                    (:pid2, 'keeper_sync_project', 'queued', :org,
                     :slug, NOW())
                """
            ),
            {
                "pid1": 1001,
                "pid2": 1002,
                "org": org_id,
                "slug": "phalanx",
            },
        )
        # dashboard_build pair: same (org_id, project_id), one queued
        # and one in_progress to exercise both active-status branches.
        await conn.execute(
            text(
                """
                INSERT INTO queue_jobs (
                    public_id, kind, status, org_id, project_id,
                    date_created
                )
                VALUES
                    (:pid1, 'dashboard_build', 'in_progress', :org,
                     :project, NOW() - INTERVAL '5 minutes'),
                    (:pid2, 'dashboard_build', 'queued', :org,
                     :project, NOW())
                """
            ),
            {
                "pid1": 2001,
                "pid2": 2002,
                "org": org_id,
                "project": project_id,
            },
        )
    return org_id, project_id


@pytest.mark.asyncio
async def test_active_uq_migrations_clean_up_duplicates_and_create_indexes(
    fresh_engine: AsyncEngine,
) -> None:
    # Bring the DB to the revision *before* the two cleanup+index
    # migrations so we can seed dirty rows the way they would have
    # accumulated under the pre-mutex code path.
    await _alembic_upgrade(PRE_CLEANUP_REVISION)
    org_id, project_id = await _seed_duplicates(fresh_engine)

    # The migration under test would raise ``UniqueViolationError`` here
    # without the embedded cleanup; this call is the regression check.
    await _alembic_upgrade("head")

    async with fresh_engine.connect() as conn:
        # Survivor selection — keeper_sync_project pair.
        rows = (
            await conn.execute(
                text(
                    "SELECT id, status, date_completed"
                    " FROM queue_jobs"
                    " WHERE kind = 'keeper_sync_project'"
                    " ORDER BY id"
                )
            )
        ).all()
        assert len(rows) == 2
        loser, winner = rows
        assert loser.status == "failed"
        assert loser.date_completed is not None
        assert winner.status == "queued"
        assert winner.date_completed is None

        # Survivor selection — dashboard_build pair.
        rows = (
            await conn.execute(
                text(
                    "SELECT id, status, date_completed"
                    " FROM queue_jobs"
                    " WHERE kind = 'dashboard_build'"
                    " ORDER BY id"
                )
            )
        ).all()
        assert len(rows) == 2
        loser, winner = rows
        assert loser.status == "failed"
        assert loser.date_completed is not None
        # The MAX(id) row was seeded as 'queued'; confirm its active
        # status survived the cleanup.
        assert winner.status == "queued"
        assert winner.date_completed is None

        # Each duplicate group has exactly one row in an active status.
        active_keeper = (
            await conn.execute(
                text(
                    "SELECT COUNT(*) FROM queue_jobs"
                    " WHERE kind = 'keeper_sync_project'"
                    " AND org_id = :org"
                    " AND subject_label = 'phalanx'"
                    " AND status IN ('queued', 'in_progress')"
                ),
                {"org": org_id},
            )
        ).scalar_one()
        assert active_keeper == 1

        active_dashboard = (
            await conn.execute(
                text(
                    "SELECT COUNT(*) FROM queue_jobs"
                    " WHERE kind = 'dashboard_build'"
                    " AND org_id = :org"
                    " AND project_id = :project"
                    " AND status IN ('queued', 'in_progress')"
                ),
                {"org": org_id, "project": project_id},
            )
        ).scalar_one()
        assert active_dashboard == 1

        # Both partial unique indexes exist with their documented
        # predicates. ``pg_get_indexdef`` returns the canonical form
        # Postgres parsed the ``WHERE`` predicate into.
        keeper_def = (
            await conn.execute(
                text(
                    "SELECT pg_get_indexdef(c.oid)"
                    " FROM pg_class c"
                    " JOIN pg_namespace n ON n.oid = c.relnamespace"
                    " WHERE n.nspname = 'public'"
                    " AND c.relname ="
                    " 'idx_queue_jobs_keeper_sync_project_active_uq'"
                )
            )
        ).scalar_one()
        assert "UNIQUE INDEX" in keeper_def
        assert "org_id" in keeper_def
        assert "subject_label" in keeper_def
        assert "keeper_sync_project" in keeper_def
        assert "queued" in keeper_def
        assert "in_progress" in keeper_def

        dashboard_def = (
            await conn.execute(
                text(
                    "SELECT pg_get_indexdef(c.oid)"
                    " FROM pg_class c"
                    " JOIN pg_namespace n ON n.oid = c.relnamespace"
                    " WHERE n.nspname = 'public'"
                    " AND c.relname ="
                    " 'idx_queue_jobs_dashboard_build_active_uq'"
                )
            )
        ).scalar_one()
        assert "UNIQUE INDEX" in dashboard_def
        assert "org_id" in dashboard_def
        assert "project_id" in dashboard_def
        assert "dashboard_build" in dashboard_def
        assert "queued" in dashboard_def
        assert "in_progress" in dashboard_def

    # Idempotency: re-running ``alembic upgrade head`` on a DB already
    # at head must be a no-op. Independently re-running just the
    # cleanup ``UPDATE`` against the post-cleanup state must match
    # zero rows.
    await _alembic_upgrade("head")
    async with fresh_engine.connect() as conn:
        keeper_failed = (
            await conn.execute(
                text(
                    "SELECT COUNT(*) FROM queue_jobs"
                    " WHERE kind = 'keeper_sync_project'"
                    " AND status = 'failed'"
                )
            )
        ).scalar_one()
        assert keeper_failed == 1  # unchanged

        dashboard_failed = (
            await conn.execute(
                text(
                    "SELECT COUNT(*) FROM queue_jobs"
                    " WHERE kind = 'dashboard_build'"
                    " AND status = 'failed'"
                )
            )
        ).scalar_one()
        assert dashboard_failed == 1  # unchanged


@pytest.mark.asyncio
async def test_active_uq_migrations_no_op_on_clean_db(
    fresh_engine: AsyncEngine,
) -> None:
    """A clean DB upgrades to head without touching any rows."""
    await _alembic_upgrade("head")

    async with fresh_engine.connect() as conn:
        failed_count = (
            await conn.execute(
                text("SELECT COUNT(*) FROM queue_jobs WHERE status = 'failed'")
            )
        ).scalar_one()
        assert failed_count == 0
        idx_names = {
            row[0]
            for row in (
                await conn.execute(
                    text(
                        "SELECT indexname FROM pg_indexes"
                        " WHERE schemaname = 'public'"
                        " AND tablename = 'queue_jobs'"
                    )
                )
            ).all()
        }
        assert "idx_queue_jobs_keeper_sync_project_active_uq" in idx_names
        assert "idx_queue_jobs_dashboard_build_active_uq" in idx_names
