"""Tests for project endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    """Create an org and seed an admin membership."""
    await seed_org_with_admin(client, "proj-org", "testuser")


@pytest.mark.asyncio
async def test_create_project(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "my-docs",
            "title": "My Docs",
            "doc_repo": "https://github.com/example/docs",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["slug"] == "my-docs"
    assert data["title"] == "My Docs"
    assert "id" not in data
    assert data["self_url"].endswith("/orgs/proj-org/projects/my-docs")


@pytest.mark.asyncio
async def test_list_projects(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "proj-aa",
            "title": "A",
            "doc_repo": "https://github.com/example/a",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    slugs = [p["slug"] for p in data]
    assert "proj-aa" in slugs
    assert "Link" in response.headers
    assert "X-Total-Count" in response.headers


@pytest.mark.asyncio
async def test_get_project(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "get-proj",
            "title": "Get Proj",
            "doc_repo": "https://github.com/example/get",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects/get-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["slug"] == "get-proj"


@pytest.mark.asyncio
async def test_get_project_not_found(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/proj-org/projects/nonexistent",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_project(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "patch-proj",
            "title": "Original",
            "doc_repo": "https://github.com/example/patch",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/patch-proj",
        json={"title": "Updated"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["title"] == "Updated"


@pytest.mark.asyncio
async def test_delete_project(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "del-proj",
            "title": "Delete Me",
            "doc_repo": "https://github.com/example/del",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.delete(
        "/docverse/orgs/proj-org/projects/del-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204

    # Should not be found after soft delete
    response = await client.get(
        "/docverse/orgs/proj-org/projects/del-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_permission_denied_no_auth(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
    )
    assert response.status_code == 403
