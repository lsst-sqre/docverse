"""Test fixtures for docverse tests."""

from collections.abc import AsyncGenerator

import pytest_asyncio
import structlog
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from safir.database import (
    create_database_engine,
    initialize_database,
    stamp_database_async,
)

from docverse.config import config
from docverse.dbschema import Base
from docverse.main import app as docverse_app


@pytest_asyncio.fixture
async def app() -> AsyncGenerator[FastAPI]:
    """Return a configured test application.

    Wraps the application in a lifespan manager so that startup and shutdown
    events are sent during test execution.
    """
    logger = structlog.get_logger("docverse")
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    await initialize_database(engine, logger, schema=Base.metadata, reset=True)
    await stamp_database_async(engine)
    await engine.dispose()

    async with LifespanManager(docverse_app):
        yield docverse_app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncGenerator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(
        base_url="https://example.com/",
        transport=ASGITransport(app=app),
    ) as client:
        yield client
