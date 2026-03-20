"""Administrative command-line interface."""

from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from typing import Any

import click
import structlog
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from safir.database import (
    create_database_engine,
    is_database_current,
    stamp_database,
)
from safir.logging import configure_logging
from sqlalchemy import Connection

from .config import config
from .database import init_database

__all__ = ["help", "main"]

# Add -h as a help shortcut option
CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(message="%(version)s")
def main() -> None:
    """Docverse.

    Administrative command-line interface for Docverse.
    """
    configure_logging(
        profile=config.log_profile,
        log_level=config.log_level,
        name="docverse",
    )


@main.command()
@click.argument("topic", default=None, required=False, nargs=1)
@click.pass_context
def help(ctx: click.Context, /, topic: None | str, **kw: Any) -> None:
    """Show help for any command."""
    if topic:
        if topic in main.commands:
            click.echo(main.commands[topic].get_help(ctx))
        else:
            msg = f"Unknown help topic {topic}"
            raise click.UsageError(msg, ctx)
    else:
        if not ctx.parent:
            msg = "help called without topic or parent"
            raise RuntimeError(msg)
        click.echo(ctx.parent.get_help())


@main.command()
@click.option(
    "--alembic-config-path",
    envvar="DOCVERSE_ALEMBIC_CONFIG_PATH",
    type=click.Path(path_type=Path),
    help="Alembic configuration file.",
)
@click.option(
    "--reset", is_flag=True, help="Delete all existing database data."
)
def init(*, alembic_config_path: Path, reset: bool) -> None:
    """Initialize the SQL database storage."""
    logger = structlog.get_logger("docverse")
    logger.debug("Initializing database")
    asyncio.run(init_database(config, logger, reset=reset))
    stamp_database(alembic_config_path)
    logger.debug("Finished initializing data stores")


@main.command()
@click.option(
    "--alembic-config-path",
    envvar="DOCVERSE_ALEMBIC_CONFIG_PATH",
    type=click.Path(path_type=Path),
    help="Alembic configuration file.",
)
def update_db_schema(*, alembic_config_path: Path) -> None:
    """Update the SQL database schema."""
    logger = structlog.get_logger("docverse")

    alembic_config = Config(str(alembic_config_path))
    alembic_scripts = ScriptDirectory.from_config(alembic_config)
    head_rev = alembic_scripts.get_current_head()
    current_rev = asyncio.run(_get_current_revision())

    logger.info(
        "Starting database schema update",
        current_revision=current_rev,
        target_revision=head_rev,
    )

    subprocess.run(
        ["alembic", "upgrade", "head"],
        check=True,
        cwd=str(alembic_config_path.parent),
    )

    new_rev = asyncio.run(_get_current_revision())

    logger.info(
        "Database schema update complete",
        previous_revision=current_rev,
        current_revision=new_rev,
    )


@main.command()
@click.option(
    "--alembic-config-path",
    envvar="DOCVERSE_ALEMBIC_CONFIG_PATH",
    type=click.Path(path_type=Path),
    help="Alembic configuration file.",
)
def validate_db_schema(*, alembic_config_path: Path) -> None:
    """Validate that the SQL database schema is current."""
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    logger = structlog.get_logger("docverse")
    if not asyncio.run(
        is_database_current(engine, logger, alembic_config_path)
    ):
        msg = "Database schema is not current"
        raise click.ClickException(msg)


async def _get_current_revision() -> str | None:
    """Get the current Alembic revision from the database."""
    engine = create_database_engine(
        config.database_url, config.database_password
    )

    def _get_heads(connection: Connection) -> set[str]:
        context = MigrationContext.configure(connection)
        return set(context.get_current_heads())

    try:
        async with engine.begin() as connection:
            heads = await connection.run_sync(_get_heads)
    finally:
        await engine.dispose()

    if not heads:
        return None
    return ",".join(sorted(heads))
