"""Test the widened ``builds_status_check`` migration (``b3c4d5e6f7a8``).

``builds.status`` is a non-native enum: the values it may hold live in a
CHECK constraint, not in a Postgres type, so adding ``superseded`` and
``cancelled`` to :class:`~docverse.models.BuildStatus` is inert until the
constraint is replaced. This test pins that the widened constraint
accepts both new values, still rejects an unknown one, and that the
downgrade folds the new rows into ``failed`` *before* restoring the
five-value constraint — otherwise the rollback would fail on its own
data. It also pins the upgrade's backfill: builds soft-deleted while
still ``pending`` or ``processing`` are retired to ``cancelled``, and
nothing else is touched.
"""

from __future__ import annotations

from datetime import UTC, datetime

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
    engine: AsyncEngine, *, public_id: int, status: str, deleted: bool = False
) -> None:
    """Insert one ``builds`` row in the given status.

    ``builds.project_id`` carries no foreign key, so a schema-level test
    can seed a build without standing up an organization and project.
    ``deleted`` stamps ``date_deleted``, making the row soft-deleted.
    """
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO builds"
                " (public_id, project_id, git_ref, content_hash, status,"
                "  staging_key, storage_prefix, uploader, date_deleted)"
                " VALUES (:public_id, 1, 'main', :hash, :status,"
                "  :staging_key, :prefix, 'testuser', :date_deleted)"
            ),
            {
                "public_id": public_id,
                "hash": "sha256:" + "0" * 64,
                "status": status,
                "staging_key": f"__staging/{public_id}.tar.gz",
                "prefix": f"proj/__builds/{public_id}/",
                "date_deleted": datetime.now(tz=UTC) if deleted else None,
            },
        )


async def _statuses(engine: AsyncEngine) -> dict[int, str]:
    """Return ``{public_id: status}`` for every build row."""
    async with engine.connect() as conn:
        rows = (
            await conn.execute(text("SELECT public_id, status FROM builds"))
        ).all()
    return {row.public_id: row.status for row in rows}


async def _completed_stamps(engine: AsyncEngine) -> dict[int, bool]:
    """Return ``{public_id: date_completed is set}`` for every build row."""
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text("SELECT public_id, date_completed FROM builds")
            )
        ).all()
    return {row.public_id: row.date_completed is not None for row in rows}


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


@pytest.mark.asyncio
async def test_upgrade_cancels_soft_deleted_in_flight_builds(
    fresh_engine: AsyncEngine,
) -> None:
    """The upgrade retires builds soft-deleted while still in flight."""
    await alembic_upgrade(PRE_STATUSES_REVISION)

    # Two rows the pre-#577 code could leave behind: deleted while the
    # build was still waiting for, or being worked by, a worker.
    await _insert_build(
        fresh_engine, public_id=1, status="pending", deleted=True
    )
    await _insert_build(
        fresh_engine, public_id=2, status="processing", deleted=True
    )
    # A live build a worker really is on, and a deleted row that already
    # reached a terminal status: neither is the backfill's business.
    await _insert_build(fresh_engine, public_id=3, status="processing")
    await _insert_build(
        fresh_engine, public_id=4, status="completed", deleted=True
    )

    await alembic_upgrade(STATUSES_REVISION)

    assert await _statuses(fresh_engine) == {
        1: "cancelled",
        2: "cancelled",
        3: "processing",
        4: "completed",
    }

    # Entering a terminal status stamps ``date_completed``, and the
    # backfill is an entry like any other. Rows it left alone keep the
    # ``NULL`` they were seeded with.
    assert await _completed_stamps(fresh_engine) == {
        1: True,
        2: True,
        3: False,
        4: False,
    }
