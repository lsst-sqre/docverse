"""Test the widened ``builds_status_check`` migration (``b3c4d5e6f7a8``).

``builds.status`` is a non-native enum: the values it may hold live in a
CHECK constraint, not in a Postgres type, so adding ``superseded`` and
``cancelled`` to :class:`~docverse.models.BuildStatus` is inert until the
constraint is replaced. This test pins that the widened constraint
accepts both new values, still rejects an unknown one, and that the
downgrade folds the new rows into ``failed`` *before* restoring the
five-value constraint — otherwise the rollback would fail on its own
data.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.support.migrations import alembic_downgrade, alembic_upgrade

# Revision immediately before the widened-status migration.
PRE_STATUSES_REVISION = "a2b3c4d5e6f7"

# The migration under test.
STATUSES_REVISION = "b3c4d5e6f7a8"


async def _insert_build(
    engine: AsyncEngine, *, public_id: int, status: str
) -> None:
    """Insert one ``builds`` row in the given status.

    ``builds.project_id`` carries no foreign key, so a schema-level test
    can seed a build without standing up an organization and project.
    """
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO builds"
                " (public_id, project_id, git_ref, content_hash, status,"
                "  staging_key, storage_prefix, uploader)"
                " VALUES (:public_id, 1, 'main', :hash, :status,"
                "  :staging_key, :prefix, 'testuser')"
            ),
            {
                "public_id": public_id,
                "hash": "sha256:" + "0" * 64,
                "status": status,
                "staging_key": f"__staging/{public_id}.tar.gz",
                "prefix": f"proj/__builds/{public_id}/",
            },
        )


async def _statuses(engine: AsyncEngine) -> dict[int, str]:
    """Return ``{public_id: status}`` for every build row."""
    async with engine.connect() as conn:
        rows = (
            await conn.execute(text("SELECT public_id, status FROM builds"))
        ).all()
    return {row.public_id: row.status for row in rows}


@pytest.mark.asyncio
async def test_migration_accepts_superseded_and_cancelled(
    fresh_engine: AsyncEngine,
) -> None:
    """The widened constraint takes both new values and no others."""
    await alembic_upgrade(PRE_STATUSES_REVISION)

    # Before the migration the constraint is the five-value one, so a
    # build can never be recorded as superseded.
    with pytest.raises(IntegrityError):
        await _insert_build(fresh_engine, public_id=1, status="superseded")

    # An existing row in one of today's five statuses survives the
    # widening untouched.
    await _insert_build(fresh_engine, public_id=2, status="processing")

    await alembic_upgrade(STATUSES_REVISION)

    await _insert_build(fresh_engine, public_id=3, status="superseded")
    await _insert_build(fresh_engine, public_id=4, status="cancelled")

    with pytest.raises(IntegrityError):
        await _insert_build(fresh_engine, public_id=5, status="abandoned")

    assert await _statuses(fresh_engine) == {
        2: "processing",
        3: "superseded",
        4: "cancelled",
    }


@pytest.mark.asyncio
async def test_downgrade_folds_new_statuses_into_failed(
    fresh_engine: AsyncEngine,
) -> None:
    """Rolling back rewrites the new statuses rather than breaking."""
    await alembic_upgrade(STATUSES_REVISION)
    await _insert_build(fresh_engine, public_id=1, status="superseded")
    await _insert_build(fresh_engine, public_id=2, status="cancelled")
    await _insert_build(fresh_engine, public_id=3, status="completed")

    await alembic_downgrade(PRE_STATUSES_REVISION)

    assert await _statuses(fresh_engine) == {
        1: "failed",
        2: "failed",
        3: "completed",
    }

    # The five-value constraint is back in force.
    with pytest.raises(IntegrityError):
        await _insert_build(fresh_engine, public_id=4, status="cancelled")
