"""Tests for admin organization endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_create_organization(client: AsyncClient) -> None:
    response = await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "test-org",
            "title": "Test Organization",
            "base_domain": "test.example.com",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["slug"] == "test-org"
    assert data["title"] == "Test Organization"
    assert data["base_domain"] == "test.example.com"
    assert data["url_scheme"] == "subdomain"
    assert data["root_path_prefix"] == "/"
    assert data["purgatory_retention"] == 2592000
    assert data["id"] is not None
    assert data["date_created"] is not None
    assert data["date_updated"] is not None
    assert data["self_url"].endswith("/admin/orgs/test-org")


@pytest.mark.asyncio
async def test_create_duplicate_organization(
    client: AsyncClient,
) -> None:
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "dup-org",
            "title": "Dup Org",
            "base_domain": "dup.example.com",
        },
    )
    response = await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "dup-org",
            "title": "Dup Org 2",
            "base_domain": "dup2.example.com",
        },
    )
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_list_organizations(client: AsyncClient) -> None:
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "list-org-a",
            "title": "Org A",
            "base_domain": "a.example.com",
        },
    )
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "list-org-b",
            "title": "Org B",
            "base_domain": "b.example.com",
        },
    )
    response = await client.get("/docverse/admin/orgs")
    assert response.status_code == 200
    data = response.json()
    slugs = [o["slug"] for o in data]
    assert "list-org-a" in slugs
    assert "list-org-b" in slugs
    for org in data:
        assert org["self_url"].endswith(f"/admin/orgs/{org['slug']}")


@pytest.mark.asyncio
async def test_get_organization(client: AsyncClient) -> None:
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "get-org",
            "title": "Get Org",
            "base_domain": "get.example.com",
        },
    )
    response = await client.get("/docverse/admin/orgs/get-org")
    assert response.status_code == 200
    data = response.json()
    assert data["slug"] == "get-org"
    assert data["title"] == "Get Org"
    assert data["self_url"].endswith("/admin/orgs/get-org")


@pytest.mark.asyncio
async def test_get_organization_not_found(
    client: AsyncClient,
) -> None:
    response = await client.get("/docverse/admin/orgs/nonexistent")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_organization(
    client: AsyncClient,
) -> None:
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "patch-org",
            "title": "Patch Org",
            "base_domain": "patch.example.com",
        },
    )
    response = await client.patch(
        "/docverse/admin/orgs/patch-org",
        json={
            "title": "Updated Org",
            "purgatory_retention": 5184000,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["title"] == "Updated Org"
    assert data["purgatory_retention"] == 5184000
    # Unchanged fields should remain
    assert data["base_domain"] == "patch.example.com"
    assert data["slug"] == "patch-org"
    assert data["self_url"].endswith("/admin/orgs/patch-org")


@pytest.mark.asyncio
async def test_update_organization_not_found(
    client: AsyncClient,
) -> None:
    response = await client.patch(
        "/docverse/admin/orgs/nonexistent",
        json={"title": "Nope"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_organization(
    client: AsyncClient,
) -> None:
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "del-org",
            "title": "Del Org",
            "base_domain": "del.example.com",
        },
    )
    response = await client.delete("/docverse/admin/orgs/del-org")
    assert response.status_code == 204

    # Verify it's gone
    response = await client.get("/docverse/admin/orgs/del-org")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_organization_not_found(
    client: AsyncClient,
) -> None:
    response = await client.delete("/docverse/admin/orgs/nonexistent")
    assert response.status_code == 404
