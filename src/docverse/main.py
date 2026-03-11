"""FastAPI application factory for Docverse."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import metadata, version

import structlog
from fastapi import FastAPI
from safir.logging import configure_logging, configure_uvicorn_logging
from safir.middleware.x_forwarded import XForwardedMiddleware
from safir.slack.webhook import SlackRouteErrorHandler

from .config import config
from .handlers.internal import internal_router

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
    yield


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

app.include_router(internal_router)
app.add_middleware(XForwardedMiddleware)

if config.slack_webhook:
    logger = structlog.get_logger("docverse")
    SlackRouteErrorHandler.initialize(config.slack_webhook, "docverse", logger)
    logger.debug("Initialized Slack webhook")
