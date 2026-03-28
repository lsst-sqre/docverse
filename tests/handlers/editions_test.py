"""Tests for edition endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    """Create org, membership, and project."""
    await seed_org_with_admin(client, "ed-org", "testuser")
    await client.post(
        "/docverse/orgs/ed-org/projects",
        json={
            "slug": "ed-proj",
            "title": "Ed Project",
            "doc_repo": "https://github.com/example/ed",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )


@pytest.mark.asyncio
async def test_create_edition(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "main",
            "title": "Latest",
            "kind": "main",
            "tracking_mode": "git_ref",
            "tracking_params": {"git_ref": "main"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["slug"] == "main"
    assert data["kind"] == "main"
    assert data["tracking_mode"] == "git_ref"
    assert data["build_url"] is None
    assert data["self_url"].endswith(
        "/orgs/ed-org/projects/ed-proj/editions/main"
    )


@pytest.mark.asyncio
async def test_list_editions(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "list-ed",
            "title": "List Ed",
            "kind": "draft",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert len(response.json()) >= 1
    assert "Link" in response.headers
    assert "X-Total-Count" in response.headers


@pytest.mark.asyncio
async def test_get_edition(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "get-ed",
            "title": "Get Ed",
            "kind": "release",
            "tracking_mode": "semver_release",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/get-ed",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["slug"] == "get-ed"


@pytest.mark.asyncio
async def test_update_edition(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "upd-ed",
            "title": "Original",
            "kind": "draft",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/upd-ed",
        json={"title": "Updated"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["title"] == "Updated"


@pytest.mark.asyncio
async def test_delete_edition(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "del-ed",
            "title": "Delete Me",
            "kind": "draft",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.delete(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/del-ed",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204

    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/del-ed",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_default_edition(client: AsyncClient) -> None:
    """The __main edition is accessible via GET."""
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["slug"] == "__main"
    assert data["kind"] == "main"


@pytest.mark.asyncio
async def test_delete_default_edition_blocked(client: AsyncClient) -> None:
    """DELETE __main returns 403."""
    await _setup(client)
    response = await client.delete(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_patch_default_edition_kind_blocked(
    client: AsyncClient,
) -> None:
    """PATCH __main with kind returns 403."""
    await _setup(client)
    response = await client.patch(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main",
        json={"kind": "draft"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_patch_default_edition_allowed_fields(
    client: AsyncClient,
) -> None:
    """PATCH __main with title/tracking_mode succeeds."""
    await _setup(client)
    response = await client.patch(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main",
        json={
            "title": "Updated Main",
            "tracking_mode": "lsst_doc",
            "lifecycle_exempt": False,
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["title"] == "Updated Main"
    assert data["tracking_mode"] == "lsst_doc"
    assert data["lifecycle_exempt"] is False


@pytest.mark.asyncio
async def test_user_cannot_create_dunder_edition(
    client: AsyncClient,
) -> None:
    """POST edition with __main slug returns 422 (Pydantic rejects it)."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "__main",
            "title": "Sneaky",
            "kind": "main",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422
