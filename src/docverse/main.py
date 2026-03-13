"""FastAPI application factory for Docverse."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import metadata, version

import structlog
from fastapi import FastAPI
from safir.database import create_database_engine, is_database_current
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency
from safir.fastapi import ClientRequestError, client_request_error_handler
from safir.logging import configure_logging, configure_uvicorn_logging
from safir.middleware.x_forwarded import XForwardedMiddleware
from safir.slack.webhook import SlackRouteErrorHandler

from .config import config
from .dependencies.context import context_dependency
from .handlers.admin import admin_router
from .handlers.internal import internal_router
from .handlers.queue import queue_router

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
    await engine.dispose()

    await db_session_dependency.initialize(
        config.database_url,
        config.database_password,
    )
    await arq_dependency.initialize(
        mode=config.arq_mode,
        redis_settings=config.arq_redis_settings,
    )
    await context_dependency.initialize()
    yield
    await context_dependency.aclose()
    await db_session_dependency.aclose()


_metadata = metadata("docverse")

app = FastAPI(
    title="Docverse",
    description=_metadata.get("Summary", ""),
    version=version("docverse"),
    openapi_url=f"{config.path_prefix}/openapi.json",
    docs_url=f"{config.path_prefix}/docs",
    redoc_url=f"{config.path_prefix}/redoc",
    lifespan=lifespan,
)
"""The main FastAPI application."""

app.exception_handler(ClientRequestError)(client_request_error_handler)
app.include_router(internal_router)
app.include_router(admin_router, prefix=config.path_prefix)
app.include_router(queue_router, prefix=config.path_prefix)
app.add_middleware(XForwardedMiddleware)

if config.slack_webhook:
    logger = structlog.get_logger("docverse")
    SlackRouteErrorHandler.initialize(config.slack_webhook, "docverse", logger)
    logger.debug("Initialized Slack webhook")
