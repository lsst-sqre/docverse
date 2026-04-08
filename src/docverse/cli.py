"""Administrative command-line interface."""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import click
import structlog
from alembic.config import Config
from alembic.script import ScriptDirectory
from safir.database import (
    create_database_engine,
    is_database_current,
    stamp_database,
)
from safir.logging import configure_logging

from .config import config
from .database import check_database_state, get_current_revision, init_database

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

    app_version = importlib.metadata.version("docverse")

    if reset:
        logger.debug("Reinitializing database (reset)")
        asyncio.run(init_database(config, logger, reset=True))
        stamp_database(alembic_config_path)
        revision = asyncio.run(_log_version_and_revision(logger, app_version))
        logger.debug(
            "Finished reinitializing database",
            app_version=app_version,
            db_revision=revision,
        )
        return

    db_state = asyncio.run(check_database_state(config))

    if db_state.has_orm_tables and db_state.has_alembic_version:
        msg = (
            "Database already initialized and tracked by Alembic. "
            "Use 'docverse-admin update-db-schema' to apply migrations, "
            "or 'docverse-admin init --reset' to reinitialize."
        )
        raise click.ClickException(msg)

    if db_state.has_orm_tables and not db_state.has_alembic_version:
        msg = (
            "Database has existing tables but no Alembic tracking. "
            "Use 'docverse-admin init --reset' to reinitialize."
        )
        raise click.ClickException(msg)

    # Fresh database — create tables and stamp
    logger.debug("Initializing database")
    asyncio.run(init_database(config, logger, reset=False))
    stamp_database(alembic_config_path)
    revision = asyncio.run(_log_version_and_revision(logger, app_version))
    logger.debug(
        "Finished initializing database",
        app_version=app_version,
        db_revision=revision,
    )


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
    app_version = importlib.metadata.version("docverse")

    alembic_config = Config(str(alembic_config_path))
    alembic_scripts = ScriptDirectory.from_config(alembic_config)
    head_rev = alembic_scripts.get_current_head()
    current_rev = asyncio.run(_cli_get_current_revision())

    logger.info(
        "Starting database schema update",
        app_version=app_version,
        current_revision=current_rev,
        target_revision=head_rev,
    )

    subprocess.run(
        ["alembic", "upgrade", "head"],
        check=True,
        cwd=str(alembic_config_path.parent),
    )

    new_rev = asyncio.run(_cli_get_current_revision())

    logger.info(
        "Database schema update complete",
        app_version=app_version,
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


@main.command()
@click.option(
    "--docverse-repo",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Path to the docverse monorepo root.",
)
@click.option(
    "--deployments-repo",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Path to local docverse-cloudflare-deployments checkout.",
)
@click.option(
    "--env",
    "wrangler_env",
    required=True,
    help="Wrangler environment name (e.g., dev, production).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Build the worker bundle without deploying.",
)
def deploy_worker(
    *,
    docverse_repo: Path,
    deployments_repo: Path,
    wrangler_env: str,
    dry_run: bool,
) -> None:
    """Pack and deploy the Cloudflare Worker."""
    logger = structlog.get_logger("docverse")

    if not re.match(r"^[a-zA-Z0-9_-]+$", wrangler_env):
        msg = (
            f"Invalid environment name {wrangler_env!r}: "
            "must contain only alphanumeric characters, hyphens, "
            "and underscores"
        )
        raise click.ClickException(msg)

    worker_dir = docverse_repo.resolve() / "cloudflare-worker"
    if not worker_dir.is_dir():
        msg = f"cloudflare-worker/ not found in {docverse_repo.resolve()}"
        raise click.ClickException(msg)

    deployments_repo = deployments_repo.resolve()
    dest_dir = deployments_repo / "worker"
    dest_dir.mkdir(exist_ok=True)

    # Phase 1: npm pack
    logger.info("Packing cloudflare worker", worker_dir=str(worker_dir))
    try:
        pack_result = subprocess.run(
            ["npm", "pack", "--json"],
            check=True,
            capture_output=True,
            text=True,
            cwd=str(worker_dir),
        )
    except subprocess.CalledProcessError as exc:
        msg = f"npm pack failed: {exc.stderr}"
        raise click.ClickException(msg) from exc
    try:
        tarball_name = json.loads(pack_result.stdout)[0]["filename"]
    except (json.JSONDecodeError, KeyError, IndexError) as exc:
        msg = f"Failed to parse npm pack output: {pack_result.stdout!r}"
        raise click.ClickException(msg) from exc

    # Phase 2: copy and unpack into deployments repo
    logger.info(
        "Unpacking worker into deployments repo",
        tarball=tarball_name,
        dest=str(dest_dir),
    )
    shutil.copy2(worker_dir / tarball_name, dest_dir / tarball_name)
    try:
        subprocess.run(
            ["tar", "xzf", tarball_name, "--strip-components=1"],
            check=True,
            cwd=str(dest_dir),
        )
    except subprocess.CalledProcessError as exc:
        msg = f"Failed to unpack worker tarball: {exc}"
        raise click.ClickException(msg) from exc
    finally:
        for tgz in dest_dir.glob("*.tgz"):
            tgz.unlink()
    (worker_dir / tarball_name).unlink(missing_ok=True)

    # Phase 3: wrangler deploy
    wrangler_cmd = [
        "npx",
        "wrangler",
        "deploy",
        "--env",
        wrangler_env,
    ]
    if dry_run:
        outdir = deployments_repo / "dist"
        wrangler_cmd.extend(["--dry-run", f"--outdir={outdir}"])
    logger.info("Deploying worker", env=wrangler_env, dry_run=dry_run)
    try:
        subprocess.run(
            wrangler_cmd,
            check=True,
            cwd=str(deployments_repo),
        )
    except subprocess.CalledProcessError as exc:
        msg = f"wrangler deploy failed: {exc}"
        raise click.ClickException(msg) from exc

    logger.info(
        "Worker deployed successfully",
        env=wrangler_env,
        dry_run=dry_run,
    )


async def _cli_get_current_revision() -> str | None:
    """Get the current Alembic revision, creating a temporary engine."""
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    try:
        return await get_current_revision(engine)
    finally:
        await engine.dispose()


async def _log_version_and_revision(
    logger: structlog.stdlib.BoundLogger,
    app_version: str,
) -> str | None:
    """Log app version and DB schema revision."""
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    try:
        revision = await get_current_revision(engine)
    finally:
        await engine.dispose()
    logger.info(
        "Docverse initialized",
        app_version=app_version,
        db_revision=revision,
    )
    return revision
