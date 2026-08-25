"""Helpers for driving Alembic from the schema tests.

``alembic/env.py`` builds its own engine from ``config.database_url``, so
these helpers migrate whichever database that names at the time they run
— which the ``fresh_engine`` fixture in :mod:`tests.dbschema.conftest`
points at the DDL database.
"""

from __future__ import annotations

import asyncio

from alembic.config import Config

from alembic import command

__all__ = [
    "ALEMBIC_CONFIG_PATH",
    "alembic_downgrade",
    "alembic_upgrade",
]

ALEMBIC_CONFIG_PATH = "alembic.ini"
"""Alembic configuration, relative to the repository root pytest runs in."""


def _alembic_config() -> Config:
    """Build a fresh Alembic config for one command."""
    return Config(ALEMBIC_CONFIG_PATH)


async def alembic_upgrade(target: str) -> None:
    """Upgrade the configured database to a revision.

    Parameters
    ----------
    target
        Revision to upgrade to, or ``"head"``.
    """
    # ``run_migrations_online`` calls ``asyncio.run`` internally, so the
    # Alembic command has to run on a thread that owns its own loop.
    await asyncio.to_thread(command.upgrade, _alembic_config(), target)


async def alembic_downgrade(target: str) -> None:
    """Downgrade the configured database to a revision.

    Parameters
    ----------
    target
        Revision to downgrade to.
    """
    await asyncio.to_thread(command.downgrade, _alembic_config(), target)
