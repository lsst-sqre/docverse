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
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import OrgMembershipCreate, OrgRole, PrincipalType
from docverse.config import config
from docverse.dbschema import Base
from docverse.main import app as docverse_app
from docverse.storage.membership_store import OrgMembershipStore
from docverse.storage.organization_store import OrganizationStore


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


@pytest_asyncio.fixture
async def db_session(
    app: FastAPI,  # noqa: ARG001
) -> AsyncGenerator[async_scoped_session[AsyncSession]]:
    """Provide a database session for direct store tests.

    The ``app`` parameter ensures the application lifespan (and therefore
    the database engine initialisation) runs before this fixture.
    """
    async for session in db_session_dependency():
        yield session


async def seed_org_with_admin(
    client: AsyncClient,
    org_slug: str,
    admin_username: str,
) -> None:
    """Create an org via admin API and seed an admin membership via DB.

    This solves the bootstrap problem: the membership API requires
    an existing admin, but we need to create the first admin.
    """
    # Create the org via the admin API (no auth required)
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": org_slug,
            "title": f"Test Org {org_slug}",
            "base_domain": f"{org_slug}.example.com",
        },
    )
    # Seed the admin membership directly via DB
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug(org_slug)
            assert org is not None
            membership_store = OrgMembershipStore(
                session=session, logger=logger
            )
            await membership_store.create(
                org_id=org.id,
                data=OrgMembershipCreate(
                    principal=admin_username,
                    principal_type=PrincipalType.user,
                    role=OrgRole.admin,
                ),
            )
            await session.commit()
