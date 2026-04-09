"""Administrative command-line interface."""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import click
import httpx
import structlog
from alembic.config import Config
from alembic.script import ScriptDirectory
from safir.database import (
    create_database_engine,
    is_database_current,
    stamp_database,
)
from safir.logging import configure_logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_scoped_session, async_sessionmaker

from .config import config
from .database import check_database_state, get_current_revision, init_database
from .dbschema.build import SqlBuild
from .dbschema.edition import SqlEdition
from .dbschema.organization import SqlOrganization
from .dbschema.project import SqlProject
from .domain.base32id import validate_base32_id
from .domain.build import Build

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
    "--org",
    required=True,
    help="Organization slug.",
)
@click.option(
    "--project",
    required=True,
    help="Project slug.",
)
@click.option(
    "--edition",
    required=True,
    help="Edition slug.",
)
@click.option(
    "--build-id",
    required=True,
    help="Base32-encoded public build ID.",
)
def publish_edition(
    *, org: str, project: str, edition: str, build_id: str
) -> None:
    """Publish an edition by writing a KV entry via the Cloudflare API."""
    logger = structlog.get_logger("docverse")

    cf_api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    cf_account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    cf_kv_namespace_id = os.environ.get("CLOUDFLARE_KV_NAMESPACE_ID")

    if not cf_api_token:
        msg = "CLOUDFLARE_API_TOKEN environment variable is required"
        raise click.ClickException(msg)
    if not cf_account_id:
        msg = "CLOUDFLARE_ACCOUNT_ID environment variable is required"
        raise click.ClickException(msg)
    if not cf_kv_namespace_id:
        msg = "CLOUDFLARE_KV_NAMESPACE_ID environment variable is required"
        raise click.ClickException(msg)

    asyncio.run(
        _publish_edition(
            org_slug=org,
            project_slug=project,
            edition_slug=edition,
            build_id=build_id,
            cf_api_token=cf_api_token,
            cf_account_id=cf_account_id,
            cf_kv_namespace_id=cf_kv_namespace_id,
            logger=logger,
        )
    )


async def _publish_edition(  # noqa: PLR0913
    *,
    org_slug: str,
    project_slug: str,
    edition_slug: str,
    build_id: str,
    cf_api_token: str,
    cf_account_id: str,
    cf_kv_namespace_id: str,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Look up the build and write the KV entry."""
    build = await _lookup_build(
        org_slug=org_slug,
        project_slug=project_slug,
        edition_slug=edition_slug,
        build_id=build_id,
    )

    kv_key = f"{project_slug}/{edition_slug}"
    kv_value = json.dumps(
        {"build_id": build_id, "r2_prefix": build.storage_prefix}
    )

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{cf_account_id}"
        f"/storage/kv/namespaces/{cf_kv_namespace_id}/values/{kv_key}"
    )

    try:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                url,
                content=kv_value,
                headers={
                    "Authorization": f"Bearer {cf_api_token}",
                    "Content-Type": "application/json",
                },
            )
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        msg = f"Cloudflare KV API error: {exc.response.status_code}"
        raise click.ClickException(msg) from exc

    logger.info(
        "Published edition",
        org=org_slug,
        project=project_slug,
        edition=edition_slug,
        build_id=build_id,
        kv_key=kv_key,
    )


async def _lookup_build(
    *, org_slug: str, project_slug: str, edition_slug: str, build_id: str
) -> Build:
    """Look up a build by org/project/build-id using a temporary engine."""
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    try:
        session_factory = async_scoped_session(
            async_sessionmaker(engine),
            scopefunc=asyncio.current_task,
        )
        session = session_factory()
        try:
            # Resolve org
            result = await session.execute(
                select(SqlOrganization).where(SqlOrganization.slug == org_slug)
            )
            org_row = result.scalar_one_or_none()
            if org_row is None:
                msg = f"Organization {org_slug!r} not found"
                raise click.ClickException(msg)

            # Resolve project
            result = await session.execute(
                select(SqlProject).where(
                    SqlProject.org_id == org_row.id,
                    SqlProject.slug == project_slug,
                )
            )
            project_row = result.scalar_one_or_none()
            if project_row is None:
                msg = f"Project {project_slug!r} not found"
                raise click.ClickException(msg)

            # Resolve edition
            result = await session.execute(
                select(SqlEdition).where(
                    SqlEdition.project_id == project_row.id,
                    SqlEdition.slug == edition_slug,
                    SqlEdition.date_deleted.is_(None),
                )
            )
            edition_row = result.scalar_one_or_none()
            if edition_row is None:
                msg = f"Edition {edition_slug!r} not found"
                raise click.ClickException(msg)

            # Resolve build
            try:
                public_id = validate_base32_id(build_id)
            except ValueError as exc:
                msg = f"Invalid build ID {build_id!r}: {exc}"
                raise click.ClickException(msg) from exc
            result = await session.execute(
                select(SqlBuild).where(
                    SqlBuild.project_id == project_row.id,
                    SqlBuild.public_id == public_id,
                    SqlBuild.date_deleted.is_(None),
                )
            )
            build_row = result.scalar_one_or_none()
            if build_row is None:
                msg = f"Build {build_id!r} not found"
                raise click.ClickException(msg)

            return Build.model_validate(build_row)
        finally:
            await session.close()
    finally:
        await engine.dispose()


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
