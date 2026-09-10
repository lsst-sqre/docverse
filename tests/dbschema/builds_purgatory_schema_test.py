"""Test the ``builds.date_purged`` migration (``c4d5e6f7a8b9``).

The purgatory sweep needs three things from the schema before any of
its behaviour can be written: somewhere to record that a build's
objects are gone, an index that makes "which builds are reap-pending"
cheap to ask, and a mutex that stops two sweeps running against one org
at once. This module pins all three by stepping a fresh schema from the
revision before the migration to the migration itself, and pins the
downgrade so a rollback is safe.

The column must arrive nullable *and* stay null over existing rows: a
backfilled timestamp would claim the content of every already-deleted
build had been reclaimed, which is exactly backwards — those builds are
the sweep's first work items, and stamping them would strand their
objects on the store forever with nothing left pointing at them.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.support.migrations import alembic_downgrade, alembic_upgrade

# Revision immediately before the purgatory migration.
PRE_PURGATORY_REVISION = "b3c4d5e6f7a8"

# The migration under test.
PURGATORY_REVISION = "c4d5e6f7a8b9"


async def _date_purged_column(engine: AsyncEngine) -> tuple[str, str] | None:
    """Return ``(data_type, is_nullable)`` for the column, or ``None``."""
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT data_type, is_nullable"
                    " FROM information_schema.columns"
                    " WHERE table_name = 'builds'"
                    " AND column_name = 'date_purged'"
                )
            )
        ).first()
    if row is None:
        return None
    return row.data_type, row.is_nullable


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


async def _seed_soft_deleted_build(engine: AsyncEngine) -> int:
    """Insert an org, a project, and one soft-deleted build.

    Returns the ``organizations.id`` so the mutex assertions can reuse
    it.
    """
    async with engine.begin() as conn:
        org_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO organizations"
                    " (slug, title, base_domain, url_scheme,"
                    "  root_path_prefix, purgatory_retention_seconds)"
                    " VALUES ('purge-org', 'Purge Org',"
                    "  'purge.example.com', 'subdomain', '/', 2592000)"
                    " RETURNING id"
                )
            )
        ).scalar_one()
        project_id = (
            await conn.execute(
                text(
                    "INSERT INTO projects (slug, title, org_id, source_url)"
                    " VALUES ('purge-proj', 'Purge Project', :org,"
                    "  'https://github.com/example/purge-proj')"
                    " RETURNING id"
                ),
                {"org": org_id},
            )
        ).scalar_one()
        await conn.execute(
            text(
                "INSERT INTO builds"
                " (public_id, project_id, git_ref, content_hash, status,"
                "  staging_key, storage_prefix, uploader, date_deleted)"
                " VALUES (9001, :project, 'main', :digest, 'cancelled',"
                "  'uploads/9001.tar.gz', 'purge-proj/__builds/9001/',"
                "  'jdoe', NOW() - INTERVAL '60 days')"
            ),
            {"project": project_id, "digest": "sha256:" + "a" * 64},
        )
    return org_id


@pytest.mark.asyncio
async def test_migration_adds_nullable_date_purged(
    fresh_engine: AsyncEngine,
) -> None:
    """The column arrives nullable and leaves existing rows unstamped."""
    await alembic_upgrade(PRE_PURGATORY_REVISION)
    assert await _date_purged_column(fresh_engine) is None
    await _seed_soft_deleted_build(fresh_engine)

    await alembic_upgrade(PURGATORY_REVISION)

    assert await _date_purged_column(fresh_engine) == (
        "timestamp with time zone",
        "YES",
    )
    async with fresh_engine.connect() as conn:
        unstamped = (
            await conn.execute(
                text(
                    "SELECT COUNT(*) FROM builds"
                    " WHERE date_deleted IS NOT NULL"
                    " AND date_purged IS NULL"
                )
            )
        ).scalar_one()
    # The build deleted before the migration is reap-pending, not
    # already reclaimed.
    assert unstamped == 1


@pytest.mark.asyncio
async def test_migration_creates_purgatory_work_list_index(
    fresh_engine: AsyncEngine,
) -> None:
    """``idx_builds_purgatory`` covers only the reap-pending rows."""
    await alembic_upgrade(PURGATORY_REVISION)

    definition = await _index_definition(fresh_engine, "idx_builds_purgatory")

    assert definition is not None
    assert "ON public.builds" in definition
    assert "date_deleted" in definition
    # The partial predicate is the point: a build that was never
    # deleted, or one already purged, must not be in the index.
    assert "date_deleted IS NOT NULL" in definition
    assert "date_purged IS NULL" in definition


@pytest.mark.asyncio
async def test_migration_creates_purgatory_cleanup_mutex(
    fresh_engine: AsyncEngine,
) -> None:
    """The per-org mutex is unique and scoped to active rows only."""
    await alembic_upgrade(PURGATORY_REVISION)

    definition = await _index_definition(
        fresh_engine, "idx_queue_jobs_purgatory_cleanup_active_uq"
    )

    assert definition is not None
    assert "UNIQUE INDEX" in definition
    assert "org_id" in definition
    assert "purgatory_cleanup" in definition
    assert "queued" in definition
    assert "in_progress" in definition


@pytest.mark.asyncio
async def test_mutex_rejects_a_second_active_job_for_one_org(
    fresh_engine: AsyncEngine,
) -> None:
    """Two queued ``purgatory_cleanup`` rows for one org cannot coexist.

    This is the backstop behind the dispatcher's ``create_unless_active``
    check: a second sweep for the same org would plan against the same
    builds and race the first one's reclaim, so the database refuses the
    row rather than trusting the read-then-write to have been atomic.
    """
    await alembic_upgrade(PURGATORY_REVISION)
    org_id = await _seed_soft_deleted_build(fresh_engine)

    async with fresh_engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO queue_jobs"
                " (public_id, kind, status, org_id, subject_label)"
                " VALUES (3001, 'purgatory_cleanup', 'queued', :org,"
                "  'purge-org')"
            ),
            {"org": org_id},
        )

    with pytest.raises(IntegrityError):
        async with fresh_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO queue_jobs"
                    " (public_id, kind, status, org_id, subject_label)"
                    " VALUES (3002, 'purgatory_cleanup', 'queued', :org,"
                    "  'purge-org')"
                ),
                {"org": org_id},
            )


@pytest.mark.asyncio
async def test_mutex_admits_a_new_job_once_the_last_one_finished(
    fresh_engine: AsyncEngine,
) -> None:
    """A terminal row does not hold the org's slot.

    The predicate is what makes the mutex a mutex rather than a
    one-sweep-ever rule: yesterday's completed job must not block
    today's tick.
    """
    await alembic_upgrade(PURGATORY_REVISION)
    org_id = await _seed_soft_deleted_build(fresh_engine)

    async with fresh_engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO queue_jobs"
                " (public_id, kind, status, org_id, subject_label)"
                " VALUES (3003, 'purgatory_cleanup', 'completed', :org,"
                "  'purge-org')"
            ),
            {"org": org_id},
        )
        await conn.execute(
            text(
                "INSERT INTO queue_jobs"
                " (public_id, kind, status, org_id, subject_label)"
                " VALUES (3004, 'purgatory_cleanup', 'queued', :org,"
                "  'purge-org')"
            ),
            {"org": org_id},
        )

    async with fresh_engine.connect() as conn:
        count = (
            await conn.execute(
                text(
                    "SELECT COUNT(*) FROM queue_jobs"
                    " WHERE kind = 'purgatory_cleanup' AND org_id = :org"
                ),
                {"org": org_id},
            )
        ).scalar_one()
    assert count == 2


@pytest.mark.asyncio
async def test_downgrade_drops_the_column_and_both_indexes(
    fresh_engine: AsyncEngine,
) -> None:
    """Rolling back removes everything the upgrade added, and no more."""
    await alembic_upgrade(PURGATORY_REVISION)
    assert await _date_purged_column(fresh_engine) is not None

    await alembic_downgrade(PRE_PURGATORY_REVISION)

    assert await _date_purged_column(fresh_engine) is None
    assert (
        await _index_definition(fresh_engine, "idx_builds_purgatory") is None
    )
    assert (
        await _index_definition(
            fresh_engine, "idx_queue_jobs_purgatory_cleanup_active_uq"
        )
        is None
    )
    # The sibling mutexes the migration did not touch are still there.
    assert (
        await _index_definition(
            fresh_engine, "idx_queue_jobs_lifecycle_eval_active_uq"
        )
        is not None
    )
