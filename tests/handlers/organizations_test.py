"""Tests for the GET /orgs/:org endpoint."""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency

from docverse.client.models import OrgMembershipCreate, OrgRole, PrincipalType
from docverse.storage.membership_store import OrgMembershipStore
from docverse.storage.organization_store import OrganizationStore
from tests.conftest import seed_org_with_admin


async def _seed_reader(org_slug: str, username: str) -> None:
    """Seed a reader membership directly via DB."""
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
                    role=OrgRole.reader,
                ),
            )
            await session.commit()


@pytest.mark.asyncio
async def test_get_organization(client: AsyncClient) -> None:
    await seed_org_with_admin(client, "test-org", "testuser")
    response = await client.get(
        "/docverse/orgs/test-org",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["slug"] == "test-org"
    assert data["title"] == "Test Org test-org"
    assert data["self_url"].endswith("/orgs/test-org")
    assert data["projects_url"].endswith("/orgs/test-org/projects")
    assert data["members_url"].endswith("/orgs/test-org/members")
    assert "date_created" in data
    assert "date_updated" in data


@pytest.mark.asyncio
async def test_get_organization_as_reader(client: AsyncClient) -> None:
    await seed_org_with_admin(client, "reader-org", "admin")
    await _seed_reader("reader-org", "readeruser")
    response = await client.get(
        "/docverse/orgs/reader-org",
        headers={"X-Auth-Request-User": "readeruser"},
    )
    assert response.status_code == 200
    assert response.json()["slug"] == "reader-org"


@pytest.mark.asyncio
async def test_get_organization_not_found(client: AsyncClient) -> None:
    await seed_org_with_admin(client, "exists-org", "testuser")
    response = await client.get(
        "/docverse/orgs/nonexistent",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_organization_unauthorized(client: AsyncClient) -> None:
    await seed_org_with_admin(client, "auth-org", "admin")
    response = await client.get(
        "/docverse/orgs/auth-org",
        headers={"X-Auth-Request-User": "stranger"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_organization_no_auth(client: AsyncClient) -> None:
    await seed_org_with_admin(client, "noauth-org", "admin")
    response = await client.get(
        "/docverse/orgs/noauth-org",
    )
    assert response.status_code == 403
