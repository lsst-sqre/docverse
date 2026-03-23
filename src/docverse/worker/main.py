"""arq worker configuration for Docverse.

Launch with: ``arq docverse.worker.main.WorkerSettings``
"""

from __future__ import annotations

from importlib.metadata import version
from typing import Any

import structlog
from safir.database import create_database_engine, is_database_current
from safir.dependencies.db_session import db_session_dependency
from safir.logging import configure_logging

from docverse.config import Configuration
from docverse.database import get_current_revision
from docverse.services.credential_encryptor import CredentialEncryptor

from .functions import build_processing, ping

config = Configuration()


async def startup(ctx: dict[str, Any]) -> None:
    """Initialize resources for the arq worker process."""
    configure_logging(
        profile=config.log_profile,
        log_level=config.log_level,
        name="docverse.worker",
    )
    logger = structlog.get_logger("docverse.worker")

    engine = create_database_engine(
        config.database_url, config.database_password
    )
    if not await is_database_current(
        engine, logger, config.alembic_config_path
    ):
        msg = "Database schema is not current."
        raise RuntimeError(msg)
    db_revision = await get_current_revision(engine)
    await engine.dispose()
    logger.info(
        "Docverse worker startup",
        app_version=version("docverse"),
        db_revision=db_revision,
    )

    await db_session_dependency.initialize(
        config.database_url,
        config.database_password,
    )

    retired_key = (
        config.credential_encryption_key_retired.get_secret_value()
        if config.credential_encryption_key_retired
        else None
    )
    ctx["encryptor"] = CredentialEncryptor(
        current_key=config.credential_encryption_key.get_secret_value(),
        retired_key=retired_key,
    )

    logger.info("Worker startup complete")


async def shutdown(ctx: dict[str, Any]) -> None:
    """Clean up resources for the arq worker process."""
    await db_session_dependency.aclose()
    logger = structlog.get_logger("docverse.worker")
    logger.info("Worker shutdown complete")


class WorkerSettings:
    """arq WorkerSettings for Docverse."""

    functions = [build_processing, ping]
    redis_settings = config.arq_redis_settings
    queue_name = config.arq_queue_name
    on_startup = startup
    on_shutdown = shutdown
