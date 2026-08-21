"""Pin that the schema tests are isolated onto the DDL database.

Every module in this package drops the schema and steps Alembic through
individual revisions. Before the DDL database existed they did that to
the database the whole suite shares — which was survivable only because
each test also rebuilt the schema on the way out, and only as long as
nothing else was mid-transaction on it. With a session-scoped engine and
application lifespan it is not survivable at all, so these tests pin the
routing rather than leaving it to a fixture's docstring.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine

from docverse_server.config import config
from tests.support.database import DDL_DATABASE_SUFFIX

SHARED_DATABASE = make_url(config.database_url).database
"""Database the rest of the suite shares.

Captured at import, because ``fresh_engine`` repoints ``config`` at the
DDL database for the duration of each test.
"""


@pytest.mark.asyncio
async def test_fresh_engine_targets_the_ddl_database(
    fresh_engine: AsyncEngine, ddl_database_url: str
) -> None:
    """The schema harness runs on the sibling database, not the shared one."""
    assert fresh_engine.url.database == make_url(ddl_database_url).database
    assert (
        fresh_engine.url.database == f"{SHARED_DATABASE}{DDL_DATABASE_SUFFIX}"
    )
    assert fresh_engine.url.database != SHARED_DATABASE


@pytest.mark.asyncio
async def test_fresh_engine_repoints_alembic(
    fresh_engine: AsyncEngine,
) -> None:
    """Alembic follows the fixture onto the DDL database.

    ``alembic/env.py`` builds its own engine from ``config.database_url``,
    so pointing the fixture's engine at the DDL database is only half the
    isolation: without the config patch, ``command.upgrade`` would still
    migrate the shared database.
    """
    assert make_url(config.database_url).database == fresh_engine.url.database


@pytest.mark.asyncio
async def test_schema_tests_leave_the_shared_database_alone(
    database_engine: AsyncEngine, fresh_engine: AsyncEngine
) -> None:
    """Emptying the DDL database does not disturb the shared schema."""
    assert database_engine.url.database == SHARED_DATABASE

    async with fresh_engine.connect() as conn:
        dropped = await conn.scalar(
            text("SELECT to_regclass('organizations')")
        )
    assert dropped is None

    async with database_engine.connect() as conn:
        intact = await conn.scalar(text("SELECT to_regclass('organizations')"))
    assert intact is not None
