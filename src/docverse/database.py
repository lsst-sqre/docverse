"""Database management functions."""

from __future__ import annotations

from typing import NamedTuple

from safir.database import create_database_engine, initialize_database
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.asyncio import AsyncEngine
from structlog.stdlib import BoundLogger

from .config import Configuration
from .dbschema import Base

__all__ = [
    "DatabaseState",
    "check_database_state",
    "init_database",
]


class DatabaseState(NamedTuple):
    """State of the database for guarding initialization."""

    has_orm_tables: bool
    """Whether the database has ORM-managed tables."""

    has_alembic_version: bool
    """Whether the database has an ``alembic_version`` table."""


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
    await initialize_database(
        engine, logger, schema=Base.metadata, reset=reset
    )
    if engine_created:
        await engine.dispose()
