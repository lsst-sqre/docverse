"""Test the ``edition_reconcile`` per-org mutex migration (``d5e6f7a8b9c0``).

The reconciliation loop (PRD #612) fans out one ``queue_jobs`` row per
organization, exactly as ``lifecycle_eval``, ``git_ref_audit`` and
``purgatory_cleanup`` do. A second active row for one org would run two
reconcilers over the same editions at once and re-drive every publish
twice, so the mutex is a partial unique index on ``org_id`` rather than
anything the dispatcher's read-then-write could guarantee on its own.

Pinned by stepping a fresh schema from the revision before the migration
to the migration itself, then back down again so a rollback is safe.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.support.migrations import alembic_downgrade, alembic_upgrade

# Revision immediately before the edition_reconcile mutex migration.
PRE_RECONCILE_REVISION = "c4d5e6f7a8b9"

# The migration under test.
RECONCILE_REVISION = "d5e6f7a8b9c0"

INDEX_NAME = "idx_queue_jobs_edition_reconcile_active_uq"


async def _index_definition(engine: AsyncEngine, name: str) -> str | None:
    """Return ``pg_get_indexdef`` for an index, or ``None`` if absent.

    ``pg_get_indexdef`` returns the canonical form Postgres parsed the
    index into, so asserting on it pins the predicate the planner will
    actually use rather than the SQL the migration happened to write.
    """
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
                    " (slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES ('recon-org', 'Recon Org',"
                    "  'recon.example.com', 'subdomain', '/', 2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
    return org_id


async def _insert_job(
    engine: AsyncEngine, *, org_id: int, public_id: int, status: str
) -> None:
    """Insert one ``edition_reconcile`` queue-job row for an org."""
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO queue_jobs"
                " (public_id, kind, status, org_id, subject_label)"
                " VALUES (:public_id, 'edition_reconcile', :status,"
                "  :org_id, 'recon-org')"
            ),
            {"org_id": org_id, "public_id": public_id, "status": status},
        )


@pytest.mark.asyncio
async def test_migration_creates_edition_reconcile_mutex(
    fresh_engine: AsyncEngine,
) -> None:
    """The per-org mutex is unique and scoped to active rows only."""
    await alembic_upgrade(RECONCILE_REVISION)

    definition = await _index_definition(fresh_engine, INDEX_NAME)

    assert definition is not None
    assert "UNIQUE INDEX" in definition
    assert "org_id" in definition
    assert "edition_reconcile" in definition
    assert "queued" in definition
    assert "in_progress" in definition


@pytest.mark.asyncio
async def test_mutex_rejects_a_second_active_job_for_one_org(
    fresh_engine: AsyncEngine,
) -> None:
    """Two queued ``edition_reconcile`` rows for one org cannot coexist.

    This is the backstop behind the dispatcher's ``create_unless_active``
    check: a second reconciler for the same org would plan against the
    same editions and re-drive every drifted publish twice, so the
    database refuses the row rather than trusting the read-then-write to
    have been atomic.
    """
    await alembic_upgrade(RECONCILE_REVISION)
    org_id = await _seed_org(fresh_engine)
    await _insert_job(
        fresh_engine, org_id=org_id, public_id=4001, status="queued"
    )

    with pytest.raises(IntegrityError):
        await _insert_job(
            fresh_engine, org_id=org_id, public_id=4002, status="in_progress"
        )


@pytest.mark.asyncio
async def test_mutex_ignores_terminal_rows(
    fresh_engine: AsyncEngine,
) -> None:
    """A finished tick never blocks the next one for the same org."""
    await alembic_upgrade(RECONCILE_REVISION)
    org_id = await _seed_org(fresh_engine)
    await _insert_job(
        fresh_engine, org_id=org_id, public_id=4003, status="completed"
    )
    await _insert_job(
        fresh_engine, org_id=org_id, public_id=4004, status="failed"
    )

    await _insert_job(
        fresh_engine, org_id=org_id, public_id=4005, status="queued"
    )

    async with fresh_engine.connect() as conn:
        count = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM queue_jobs"
                    " WHERE org_id = :org_id"
                    " AND kind = 'edition_reconcile'"
                ),
                {"org_id": org_id},
            )
        ).scalar_one()
    assert count == 3


@pytest.mark.asyncio
async def test_downgrade_drops_the_mutex(
    fresh_engine: AsyncEngine,
) -> None:
    """Rolling back removes the index and leaves the table usable."""
    await alembic_upgrade(RECONCILE_REVISION)

    await alembic_downgrade(PRE_RECONCILE_REVISION)

    assert await _index_definition(fresh_engine, INDEX_NAME) is None
