"""Tests for build endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import seed_org_with_admin

CONTENT_HASH = (
    "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
)


async def _setup(client: AsyncClient) -> None:
    """Create org, membership, and project."""
    await seed_org_with_admin(client, "build-org", "testuser")
    await client.post(
        "/docverse/orgs/build-org/projects",
        json={
            "slug": "build-proj",
            "title": "Build Project",
            "doc_repo": "https://github.com/example/build",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )


@pytest.mark.asyncio
async def test_create_build(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        json={
            "git_ref": "main",
            "content_hash": CONTENT_HASH,
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["git_ref"] == "main"
    assert data["status"] == "pending"
    # No credential configured for this org, so upload_url is None
    assert data["upload_url"] is None
    assert data["uploader"] == "testuser"


@pytest.mark.asyncio
async def test_list_builds(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        json={"git_ref": "main", "content_hash": CONTENT_HASH},
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert len(response.json()) >= 1
    assert "Link" in response.headers
    assert "X-Total-Count" in response.headers


@pytest.mark.asyncio
async def test_get_build(client: AsyncClient) -> None:
    await _setup(client)
    create_resp = await client.post(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        json={"git_ref": "main", "content_hash": CONTENT_HASH},
        headers={"X-Auth-Request-User": "testuser"},
    )
    build_id = create_resp.json()["id"]
    response = await client.get(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["id"] == build_id


@pytest.mark.asyncio
async def test_patch_build_upload_complete(client: AsyncClient) -> None:
    await _setup(client)
    create_resp = await client.post(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        json={"git_ref": "main", "content_hash": CONTENT_HASH},
        headers={"X-Auth-Request-User": "testuser"},
    )
    build_id = create_resp.json()["id"]
    response = await client.patch(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        json={"status": "uploaded"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "processing"
    assert data["queue_url"] is not None


@pytest.mark.asyncio
async def test_delete_build(client: AsyncClient) -> None:
    await _setup(client)
    create_resp = await client.post(
        "/docverse/orgs/build-org/projects/build-proj/builds",
        json={"git_ref": "main", "content_hash": CONTENT_HASH},
        headers={"X-Auth-Request-User": "testuser"},
    )
    build_id = create_resp.json()["id"]
    response = await client.delete(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204
