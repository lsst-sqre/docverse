"""Database management functions."""

from __future__ import annotations

from safir.database import create_database_engine, initialize_database
from sqlalchemy.ext.asyncio import AsyncEngine
from structlog.stdlib import BoundLogger

from .config import Configuration
from .dbschema import Base

__all__ = ["init_database"]


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
