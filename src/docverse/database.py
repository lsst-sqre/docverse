"""Database management functions."""

from __future__ import annotations

from typing import NamedTuple

from alembic.runtime.migration import MigrationContext
from safir.database import create_database_engine, initialize_database
from sqlalchemy import Connection, text
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.asyncio import AsyncEngine
from structlog.stdlib import BoundLogger

from .config import Configuration
from .dbschema import Base

__all__ = [
    "DatabaseState",
    "check_database_state",
    "get_current_revision",
    "init_database",
]


class DatabaseState(NamedTuple):
    """State of the database for guarding initialization."""

    has_orm_tables: bool
    """Whether the database has ORM-managed tables."""

    has_alembic_version: bool
    """Whether the database has an ``alembic_version`` table."""


async def get_current_revision(engine: AsyncEngine) -> str | None:
    """Get the current Alembic revision from the database.

    Parameters
    ----------
    engine
        Database engine.

    Returns
    -------
    str or None
        The current revision string, or `None` if no revision is stamped.
    """

    def _get_heads(connection: Connection) -> set[str]:
        context = MigrationContext.configure(connection)
        return set(context.get_current_heads())

    async with engine.begin() as connection:
        heads = await connection.run_sync(_get_heads)

    if not heads:
        return None
    return ",".join(sorted(heads))


async def check_database_state(config: Configuration) -> DatabaseState:
    """Inspect the database to determine its current state.

    Parameters
    ----------
    config
        Application configuration.

    Returns
    -------
    DatabaseState
        The current state of the database.
    """
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    try:
        async with engine.connect() as connection:
            table_names = await connection.run_sync(
                lambda conn: sa_inspect(conn).get_table_names()
            )
    finally:
        await engine.dispose()
    return DatabaseState(
        has_orm_tables="organizations" in table_names,
        has_alembic_version="alembic_version" in table_names,
    )


async def init_database(
    config: Configuration,
    logger: BoundLogger,
    engine: AsyncEngine | None = None,
    *,
    reset: bool = False,
) -> None:
    """Initialize the database.

    This is the internal async implementation of the ``init`` command,
    except for the Alembic parts. Alembic has to run outside of a running
    asyncio loop, hence this separation. Always stamp the database with
    Alembic after calling this function.

    Parameters
    ----------
    config
        Application configuration.
    logger
        Logger to use for status reporting.
    engine
        If given, database engine to use, which avoids the need to create
        another one.
    reset
        Whether to reset the database.
    """
    engine_created = False
    if not engine:
        engine = create_database_engine(
            config.database_url, config.database_password
        )
        engine_created = True
    # Ensure required PostgreSQL extensions exist before creating tables
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    await initialize_database(
        engine, logger, schema=Base.metadata, reset=reset
    )
    if engine_created:
        await engine.dispose()
