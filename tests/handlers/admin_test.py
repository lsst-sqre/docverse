"""Tests for admin organization endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

SUPERADMIN_HEADERS = {"X-Auth-Request-User": "superadmin"}


@pytest.mark.asyncio
async def test_create_organization(client: AsyncClient) -> None:
    response = await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "test-org",
            "title": "Test Organization",
            "base_domain": "test.example.com",
        },
        headers=SUPERADMIN_HEADERS,
    )
    assert response.status_code == 201
    data = response.json()
    assert data["slug"] == "test-org"
    assert data["title"] == "Test Organization"
    assert data["base_domain"] == "test.example.com"
    assert data["url_scheme"] == "subdomain"
    assert data["root_path_prefix"] == "/"
    assert data["purgatory_retention"] == 2592000
    assert "id" not in data
    assert data["date_created"] is not None
    assert data["date_updated"] is not None
    assert "/admin/orgs/test-org" in data["self_url"]
    assert "/orgs/test-org" in data["org_url"]


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
        headers=SUPERADMIN_HEADERS,
    )
    response = await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "dup-org",
            "title": "Dup Org 2",
            "base_domain": "dup2.example.com",
        },
        headers=SUPERADMIN_HEADERS,
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
        headers=SUPERADMIN_HEADERS,
    )
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "list-org-b",
            "title": "Org B",
            "base_domain": "b.example.com",
        },
        headers=SUPERADMIN_HEADERS,
    )
    response = await client.get(
        "/docverse/admin/orgs",
        headers=SUPERADMIN_HEADERS,
    )
    assert response.status_code == 200
    data = response.json()
    slugs = [o["slug"] for o in data]
    assert "list-org-a" in slugs
    assert "list-org-b" in slugs
    for org in data:
        assert f"/admin/orgs/{org['slug']}" in org["self_url"]
        assert f"/orgs/{org['slug']}" in org["org_url"]


@pytest.mark.asyncio
async def test_get_organization(client: AsyncClient) -> None:
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "get-org",
            "title": "Get Org",
            "base_domain": "get.example.com",
        },
        headers=SUPERADMIN_HEADERS,
    )
    response = await client.get(
        "/docverse/admin/orgs/get-org",
        headers=SUPERADMIN_HEADERS,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["slug"] == "get-org"
    assert data["title"] == "Get Org"
    assert "/admin/orgs/get-org" in data["self_url"]
    assert "/orgs/get-org" in data["org_url"]


@pytest.mark.asyncio
async def test_get_organization_not_found(
    client: AsyncClient,
) -> None:
    response = await client.get(
        "/docverse/admin/orgs/nonexistent",
        headers=SUPERADMIN_HEADERS,
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
        headers=SUPERADMIN_HEADERS,
    )
    response = await client.delete(
        "/docverse/admin/orgs/del-org",
        headers=SUPERADMIN_HEADERS,
    )
    assert response.status_code == 204

    # Verify it's gone
    response = await client.get(
        "/docverse/admin/orgs/del-org",
        headers=SUPERADMIN_HEADERS,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_organization_not_found(
    client: AsyncClient,
) -> None:
    response = await client.delete(
        "/docverse/admin/orgs/nonexistent",
        headers=SUPERADMIN_HEADERS,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_create_organization_with_members(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "mem-org",
            "title": "Members Org",
            "base_domain": "mem.example.com",
            "members": [
                {
                    "principal": "alice",
                    "principal_type": "user",
                    "role": "admin",
                },
                {
                    "principal": "bob",
                    "principal_type": "user",
                    "role": "reader",
                },
            ],
        },
        headers=SUPERADMIN_HEADERS,
    )
    assert response.status_code == 201

    # Verify memberships exist by listing them as alice (admin)
    response = await client.get(
        "/docverse/orgs/mem-org/members",
        headers={"X-Auth-Request-User": "alice"},
    )
    assert response.status_code == 200
    members = response.json()
    principals = {m["principal"] for m in members}
    assert "alice" in principals
    assert "bob" in principals


@pytest.mark.asyncio
async def test_create_organization_without_members(
    client: AsyncClient,
) -> None:
    """Creating an org without members is backward compatible."""
    response = await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "no-mem-org",
            "title": "No Members Org",
            "base_domain": "nomem.example.com",
        },
        headers=SUPERADMIN_HEADERS,
    )
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_create_organization_with_duplicate_members(
    client: AsyncClient,
) -> None:
    """Duplicate members are silently deduplicated (first wins)."""
    response = await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "dup-mem-org",
            "title": "Dup Members Org",
            "base_domain": "dupmem.example.com",
            "members": [
                {
                    "principal": "alice",
                    "principal_type": "user",
                    "role": "admin",
                },
                {
                    "principal": "alice",
                    "principal_type": "user",
                    "role": "reader",
                },
            ],
        },
        headers=SUPERADMIN_HEADERS,
    )
    assert response.status_code == 201

    # Verify only one membership for alice (first one, admin)
    response = await client.get(
        "/docverse/orgs/dup-mem-org/members",
        headers={"X-Auth-Request-User": "alice"},
    )
    assert response.status_code == 200
    members = response.json()
    alice_members = [m for m in members if m["principal"] == "alice"]
    assert len(alice_members) == 1
    assert alice_members[0]["role"] == "admin"


@pytest.mark.asyncio
async def test_admin_requires_superadmin(client: AsyncClient) -> None:
    """Admin endpoints reject non-superadmin users with 403."""
    # No auth header
    response = await client.get("/docverse/admin/orgs")
    assert response.status_code == 403

    # Non-superadmin user
    response = await client.get(
        "/docverse/admin/orgs",
        headers={"X-Auth-Request-User": "regular-user"},
    )
    assert response.status_code == 403
