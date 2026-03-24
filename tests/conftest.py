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
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import OrgMembershipCreate, OrgRole, PrincipalType
from docverse.config import config
from docverse.dbschema import Base
from docverse.dependencies.context import context_dependency
from docverse.main import app as docverse_app
from docverse.storage.membership_store import OrgMembershipStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.user_info_store import StubUserInfoStore

__all__ = [
    "seed_group_member",
    "seed_member",
    "seed_org_with_admin",
]


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
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    await initialize_database(engine, logger, schema=Base.metadata, reset=True)
    await stamp_database_async(engine)
    await engine.dispose()

    async with LifespanManager(docverse_app):
        context_dependency._user_info_store = StubUserInfoStore()
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
    """Create an org via admin API with an initial admin member."""
    response = await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": org_slug,
            "title": f"Test Org {org_slug}",
            "base_domain": f"{org_slug}.example.com",
            "members": [
                {
                    "principal": admin_username,
                    "principal_type": "user",
                    "role": "admin",
                }
            ],
        },
    )
    assert response.status_code == 201


async def seed_member(
    org_slug: str,
    username: str,
    role: OrgRole,
) -> None:
    """Seed a membership with a given role directly via the DB."""
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
                    principal=username,
                    principal_type=PrincipalType.user,
                    role=role,
                ),
            )
            await session.commit()


async def seed_group_member(
    org_slug: str,
    group_name: str,
    role: OrgRole,
) -> None:
    """Seed a group membership with a given role directly via the DB."""
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
                    principal=group_name,
                    principal_type=PrincipalType.group,
                    role=role,
                ),
            )
            await session.commit()
