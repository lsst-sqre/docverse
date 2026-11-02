"""Test the ``excluded_path_scrub`` mutex migration (``e2f3a4b5c6d7``).

A project runs one excluded-path scrub at a time. The endpoint's
read-then-insert check cannot guarantee that on its own under concurrent
requests, so the mutex is a partial unique index on
``(org_id, subject_label)`` for active ``excluded_path_scrub`` rows,
with the project slug as the subject.

Pinned by stepping a fresh schema from the revision before the migration
to the migration itself, then back down again so a rollback is safe.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.support.migrations import alembic_downgrade, alembic_upgrade

# Revision immediately before the excluded_path_scrub mutex migration.
PRE_SCRUB_MUTEX_REVISION = "d1e2f3a4b5c6"

# The migration under test.
SCRUB_MUTEX_REVISION = "e2f3a4b5c6d7"

INDEX_NAME = "idx_queue_jobs_excluded_path_scrub_active_uq"


async def _index_definition(engine: AsyncEngine, name: str) -> str | None:
    """Return ``pg_get_indexdef`` for an index, or ``None`` if absent."""
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text(
                    "SELECT pg_get_indexdef(c.oid)"
                    " FROM pg_class c"
                    " JOIN pg_namespace n ON n.oid = c.relnamespace"
                    " WHERE n.nspname = 'public' AND c.relname = :name"
                ),
                {"name": name},
            )
        ).scalar_one_or_none()


async def _seed_org(engine: AsyncEngine) -> int:
    """Insert one organization and return its id."""
    async with engine.begin() as conn:
        org_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO organizations"
                    " (public_id, slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES (1, 'scrub-org', 'Scrub Org',"
                    "  'scrub.example.com', 'subdomain', '/', 2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
    return org_id


async def _insert_job(
    engine: AsyncEngine,
    *,
    org_id: int,
    public_id: int,
    status: str,
    subject_label: str = "docs",
) -> None:
    """Insert one ``excluded_path_scrub`` queue-job row for a project."""
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO queue_jobs"
                " (public_id, kind, status, org_id, subject_label)"
                " VALUES (:public_id, 'excluded_path_scrub', :status,"
                "  :org_id, :subject_label)"
            ),
            {
                "org_id": org_id,
                "public_id": public_id,
                "status": status,
                "subject_label": subject_label,
            },
        )


@pytest.mark.asyncio
async def test_migration_creates_excluded_path_scrub_mutex(
    fresh_engine: AsyncEngine,
) -> None:
    """The per-project mutex is unique and scoped to active rows only."""
    await alembic_upgrade(SCRUB_MUTEX_REVISION)

    definition = await _index_definition(fresh_engine, INDEX_NAME)

    assert definition is not None
    assert "UNIQUE INDEX" in definition
    assert "org_id" in definition
    assert "subject_label" in definition
    assert "excluded_path_scrub" in definition
    assert "queued" in definition
    assert "in_progress" in definition


@pytest.mark.asyncio
async def test_mutex_rejects_a_second_active_scrub_of_one_project(
    fresh_engine: AsyncEngine,
) -> None:
    """Two active scrubs of one project cannot coexist."""
    await alembic_upgrade(SCRUB_MUTEX_REVISION)
    org_id = await _seed_org(fresh_engine)
    await _insert_job(
        fresh_engine, org_id=org_id, public_id=5001, status="queued"
    )

    with pytest.raises(IntegrityError):
        await _insert_job(
            fresh_engine, org_id=org_id, public_id=5002, status="in_progress"
        )


@pytest.mark.asyncio
async def test_mutex_ignores_terminal_rows_and_other_projects(
    fresh_engine: AsyncEngine,
) -> None:
    """Finished scrubs and other projects' scrubs never block a new one."""
    await alembic_upgrade(SCRUB_MUTEX_REVISION)
    org_id = await _seed_org(fresh_engine)
    await _insert_job(
        fresh_engine, org_id=org_id, public_id=5003, status="completed"
    )
    await _insert_job(
        fresh_engine, org_id=org_id, public_id=5004, status="failed"
    )
    await _insert_job(
        fresh_engine,
        org_id=org_id,
        public_id=5005,
        status="queued",
        subject_label="other",
    )

    await _insert_job(
        fresh_engine, org_id=org_id, public_id=5006, status="queued"
    )

    async with fresh_engine.connect() as conn:
        count = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM queue_jobs"
                    " WHERE org_id = :org_id"
                    " AND kind = 'excluded_path_scrub'"
                ),
                {"org_id": org_id},
            )
        ).scalar_one()
    assert count == 4


@pytest.mark.asyncio
async def test_downgrade_drops_the_mutex(
    fresh_engine: AsyncEngine,
) -> None:
    """Rolling back removes the index and leaves the table usable."""
    await alembic_upgrade(SCRUB_MUTEX_REVISION)

    await alembic_downgrade(PRE_SCRUB_MUTEX_REVISION)

    assert await _index_definition(fresh_engine, INDEX_NAME) is None
