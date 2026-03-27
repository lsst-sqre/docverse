"""FastAPI application factory for Docverse."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import metadata, version

import structlog
from fastapi import FastAPI
from rubin.gafaelfawr import GafaelfawrClient
from safir.database import create_database_engine, is_database_current
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency
from safir.dependencies.http_client import http_client_dependency
from safir.fastapi import ClientRequestError, client_request_error_handler
from safir.logging import configure_logging, configure_uvicorn_logging
from safir.middleware.x_forwarded import XForwardedMiddleware
from safir.slack.webhook import SlackRouteErrorHandler

from .config import config
from .database import get_current_revision
from .dependencies.context import context_dependency
from .handlers.admin import admin_router
from .handlers.internal import internal_router
from .handlers.orgs import orgs_router
from .handlers.queue import queue_router
from .services.credential_encryptor import CredentialEncryptor
from .storage.user_info_store import GafaelfawrUserInfoStore

__all__ = ["app"]

configure_logging(
    profile=config.log_profile,
    log_level=config.log_level,
    name="docverse",
)
configure_uvicorn_logging(config.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:  # noqa: ARG001
    """Context manager for application startup and shutdown."""
    logger = structlog.get_logger("docverse")

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
        "Docverse startup",
        app_version=version("docverse"),
        db_revision=db_revision,
    )

    await db_session_dependency.initialize(
        config.database_url,
        config.database_password,
    )
    await arq_dependency.initialize(
        mode=config.arq_mode,
        redis_settings=config.arq_redis_settings,
    )

    retired_key = (
        config.credential_encryption_key_retired.get_secret_value()
        if config.credential_encryption_key_retired
        else None
    )
    encryptor = CredentialEncryptor(
        current_key=config.credential_encryption_key.get_secret_value(),
        retired_key=retired_key,
    )

    http_client = await http_client_dependency()
    gafaelfawr_client = GafaelfawrClient(http_client)
    user_info_store = GafaelfawrUserInfoStore(gafaelfawr_client)

    await context_dependency.initialize(
        credential_encryptor=encryptor,
        superadmin_usernames=config.superadmin_usernames,
        user_info_store=user_info_store,
        arq_queue_name=config.arq_queue_name,
    )
    yield
    await context_dependency.aclose()
    await http_client_dependency.aclose()
    await db_session_dependency.aclose()


_metadata = metadata("docverse")

app = FastAPI(
    title="Docverse",
    description=_metadata.get("Summary", ""),
    version=version("docverse"),
    openapi_url=f"{config.path_prefix}/openapi.json",
    docs_url=f"{config.path_prefix}/docs",
    redoc_url=f"{config.path_prefix}/redoc",
    openapi_tags=[
        {
            "name": "orgs",
            "description": "Organization and membership management.",
        },
        {
            "name": "projects",
            "description": "Projects, builds, and editions.",
        },
        {"name": "queue", "description": "Background job status."},
        {
            "name": "admin",
            "description": "Superuser organization administration.",
        },
    ],
    lifespan=lifespan,
)
"""The main FastAPI application."""

app.exception_handler(ClientRequestError)(client_request_error_handler)
app.include_router(internal_router)
app.include_router(admin_router, prefix=config.path_prefix)
app.include_router(orgs_router, prefix=config.path_prefix)
app.include_router(queue_router, prefix=config.path_prefix)
app.add_middleware(XForwardedMiddleware)

if config.slack_webhook:
    logger = structlog.get_logger("docverse")
    SlackRouteErrorHandler.initialize(config.slack_webhook, "docverse", logger)
    logger.debug("Initialized Slack webhook")
