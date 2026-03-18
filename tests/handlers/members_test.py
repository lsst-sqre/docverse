"""Tests for organization membership endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    """Create an org and seed an admin membership."""
    await seed_org_with_admin(client, "mem-org", "admin-user")


@pytest.mark.asyncio
async def test_create_member(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "jdoe",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["principal"] == "jdoe"
    assert data["id"] == "user:jdoe"
    assert data["self_url"].endswith("/orgs/mem-org/members/user:jdoe")


@pytest.mark.asyncio
async def test_list_members(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/mem-org/members",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 200
    data = response.json()
    # At least the admin user seeded by setup
    assert len(data) >= 1


@pytest.mark.asyncio
async def test_get_member(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/mem-org/members/user:admin-user",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 200
    assert response.json()["principal"] == "admin-user"


@pytest.mark.asyncio
async def test_get_member_not_found(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/mem-org/members/user:nobody",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_member(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "removeme",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    response = await client.delete(
        "/docverse/orgs/mem-org/members/user:removeme",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 204

    response = await client.get(
        "/docverse/orgs/mem-org/members/user:removeme",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_duplicate_member(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "dup-user",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    response = await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "dup-user",
            "principal_type": "user",
            "role": "uploader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 409
