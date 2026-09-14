"""Test the projects ``date_updated`` listing index (``b9c0d1e2f3a4``).

``GET /orgs/{org}/projects`` gained ``order=date_updated`` and an
``updated_since`` filter (PRD #634 / DM-56083), both of which read
``projects`` by ``org_id`` and then by the clock. The pre-existing
``idx_projects_org_id`` stops at the org, so every poll would sort or
filter the org's whole project set in memory. This migration adds the
composite the keyset cursor actually walks — ``(org_id, date_updated,
id)``, matching its ``date_updated DESC, id DESC`` ordering and its
``id`` tiebreak.

The assertions read ``pg_get_indexdef``, which reports the canonical
form Postgres parsed the index into, so they pin the column list the
planner will use rather than the SQL the migration happened to write.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.support.migrations import alembic_downgrade, alembic_upgrade

# Revision immediately before this migration (organizations public_id).
PRE_INDEX_REVISION = "a8b9c0d1e2f3"

# The migration under test.
INDEX_REVISION = "b9c0d1e2f3a4"

INDEX_NAME = "idx_projects_org_date_updated"


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


@pytest.mark.asyncio
async def test_migration_creates_org_date_updated_index(
    fresh_engine: AsyncEngine,
) -> None:
    """The upgrade leaves a ``(org_id, date_updated, id)`` btree behind."""
    await alembic_upgrade(INDEX_REVISION)

    definition = await _index_definition(fresh_engine, INDEX_NAME)

    assert definition is not None
    assert "ON public.projects" in definition
    assert "(org_id, date_updated, id)" in definition


@pytest.mark.asyncio
async def test_downgrade_drops_org_date_updated_index(
    fresh_engine: AsyncEngine,
) -> None:
    """The downgrade removes the index cleanly."""
    await alembic_upgrade(INDEX_REVISION)
    await alembic_downgrade(PRE_INDEX_REVISION)

    assert await _index_definition(fresh_engine, INDEX_NAME) is None
