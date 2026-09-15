"""Test the projects ``date_updated`` listing index (``b9c0d1e2f3a4``).

``GET /orgs/{org}/projects`` gained ``order=date_updated`` and an
``updated_since`` filter (PRD #634 / DM-56083), both of which read
``projects`` by ``org_id`` and then by the clock. The pre-existing
``idx_projects_org_id`` stopped at the org, so every poll would sort or
filter the org's whole project set in memory. This migration adds the
composite the keyset cursor actually walks — ``(org_id, date_updated,
id)``, matching its ``date_updated DESC, id DESC`` ordering and its
``id`` tiebreak — and drops ``idx_projects_org_id``, which the
composite subsumes as its leading column.

The assertions read ``pg_get_indexdef``, which reports the canonical
form Postgres parsed the index into, so they pin the column list the
planner will use rather than the SQL the migration happened to write.

The last test guards the drift this migration makes easy to introduce:
the rest of the suite runs on a schema built from
:class:`~docverse_server.dbschema.project.SqlProject` rather than from
the migrations, so an index dropped on one side only would go unnoticed
everywhere except production. It compares the two schemas directly.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from docverse_server.dbschema import Base
from tests.support.migrations import alembic_downgrade, alembic_upgrade

# Revision immediately before this migration (organizations public_id).
PRE_INDEX_REVISION = "a8b9c0d1e2f3"

# The migration under test.
INDEX_REVISION = "b9c0d1e2f3a4"

INDEX_NAME = "idx_projects_org_date_updated"

# The single-column prefix index the composite makes redundant.
ORG_ID_INDEX_NAME = "idx_projects_org_id"


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


async def _project_index_definitions(engine: AsyncEngine) -> dict[str, str]:
    """Return every ``projects`` index, keyed by name."""
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT c.relname, pg_get_indexdef(c.oid)"
                    " FROM pg_class c"
                    " JOIN pg_index i ON i.indexrelid = c.oid"
                    " JOIN pg_class t ON t.oid = i.indrelid"
                    " JOIN pg_namespace n ON n.oid = c.relnamespace"
                    " WHERE n.nspname = 'public' AND t.relname = 'projects'"
                )
            )
        ).all()
    return {str(name): str(definition) for name, definition in rows}


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


@pytest.mark.asyncio
async def test_migration_drops_redundant_org_id_index(
    fresh_engine: AsyncEngine,
) -> None:
    """The upgrade drops the prefix index the composite subsumes."""
    await alembic_upgrade(PRE_INDEX_REVISION)
    assert await _index_definition(fresh_engine, ORG_ID_INDEX_NAME) is not None

    await alembic_upgrade(INDEX_REVISION)

    assert await _index_definition(fresh_engine, ORG_ID_INDEX_NAME) is None


@pytest.mark.asyncio
async def test_downgrade_restores_org_id_index(
    fresh_engine: AsyncEngine,
) -> None:
    """The downgrade puts the prefix index back for earlier revisions."""
    await alembic_upgrade(INDEX_REVISION)
    await alembic_downgrade(PRE_INDEX_REVISION)

    definition = await _index_definition(fresh_engine, ORG_ID_INDEX_NAME)

    assert definition is not None
    assert "(org_id)" in definition


@pytest.mark.asyncio
async def test_migrated_project_indexes_match_orm_metadata(
    fresh_engine: AsyncEngine,
) -> None:
    """Migrations and :class:`SqlProject` build the same index set."""
    await alembic_upgrade("head")
    migrated = await _project_index_definitions(fresh_engine)

    async with fresh_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    declared = await _project_index_definitions(fresh_engine)

    assert migrated == declared
