"""Fixtures for the Alembic schema tests.

These tests are the one corner of the suite that cannot run on the
shared, session-scoped database: they need an empty schema they can step
Alembic forward through revision by revision, and the rest of the suite
needs a schema at head with a live connection pool and application
lifespan on top of it. They run on the dedicated DDL database instead —
see :mod:`tests.support.database`.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine

from docverse_server.config import config
from tests.support.database import ddl_database


@pytest_asyncio.fixture
async def fresh_engine(
    ddl_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> AsyncGenerator[AsyncEngine]:
    """Yield an engine on an emptied DDL database.

    ``config.database_url`` is repointed for the duration of the test
    because ``alembic/env.py`` reads it to build the engine the
    migrations run on; without that, ``command.upgrade`` would migrate
    the shared database no matter which engine the test itself holds.

    Teardown does not restore the schema. It marks the database as no
    longer canonical instead, so the rebuild is paid lazily by the next
    test that actually asks for a schema — and is paid even when this
    test failed partway through a migration, which is exactly when the
    leftover schema is least trustworthy.
    """
    monkeypatch.setattr(config, "database_url", ddl_database_url)
    async with ddl_database(
        ddl_database_url, config.database_password
    ) as engine:
        yield engine
