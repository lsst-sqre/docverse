"""Tests for the /orgs/:org endpoint."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from docverse.client.models import OrgRole
from tests.conftest import seed_member, seed_org_with_admin


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
    await seed_member("reader-org", "readeruser", OrgRole.reader)
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


# --- PATCH /orgs/{org} tests ---


@pytest.mark.asyncio
async def test_patch_organization(client: AsyncClient) -> None:
    await seed_org_with_admin(client, "patch-org", "admin")
    response = await client.patch(
        "/docverse/orgs/patch-org",
        json={
            "title": "Updated Title",
            "purgatory_retention": 5184000,
        },
        headers={"X-Auth-Request-User": "admin"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["title"] == "Updated Title"
    assert data["purgatory_retention"] == 5184000
    # Unchanged fields preserved
    assert data["slug"] == "patch-org"
    assert data["base_domain"] == "patch-org.example.com"
    assert data["self_url"].endswith("/orgs/patch-org")


@pytest.mark.asyncio
async def test_patch_organization_not_admin(client: AsyncClient) -> None:
    await seed_org_with_admin(client, "patch-reader-org", "admin")
    await seed_member("patch-reader-org", "reader", OrgRole.reader)
    response = await client.patch(
        "/docverse/orgs/patch-reader-org",
        json={"title": "Nope"},
        headers={"X-Auth-Request-User": "reader"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_patch_organization_unauthorized(client: AsyncClient) -> None:
    await seed_org_with_admin(client, "patch-unauth-org", "admin")
    response = await client.patch(
        "/docverse/orgs/patch-unauth-org",
        json={"title": "Nope"},
        headers={"X-Auth-Request-User": "stranger"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_patch_organization_slot_labels(client: AsyncClient) -> None:
    await seed_org_with_admin(client, "patch-svc-org", "admin")
    headers = {"X-Auth-Request-User": "admin"}
    # Create a credential and service
    await client.post(
        "/docverse/orgs/patch-svc-org/credentials",
        json={
            "label": "aws-cred",
            "credentials": {
                "provider": "aws",
                "access_key_id": "AKIAEXAMPLE",
                "secret_access_key": "secret",
            },
        },
        headers=headers,
    )
    await client.post(
        "/docverse/orgs/patch-svc-org/services",
        json={
            "label": "my-s3",
            "credential_label": "aws-cred",
            "config": {
                "provider": "aws_s3",
                "bucket": "my-bucket",
                "region": "us-east-1",
            },
        },
        headers=headers,
    )
    # PATCH the slot labels
    response = await client.patch(
        "/docverse/orgs/patch-svc-org",
        json={
            "publishing_store_label": "my-s3",
            "staging_store_label": "my-s3",
        },
        headers=headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["publishing_store"]["label"] == "my-s3"
    assert data["publishing_store"]["category"] == "object_storage"
    assert "self_url" in data["publishing_store"]
    assert data["staging_store"]["label"] == "my-s3"


@pytest.mark.asyncio
async def test_patch_organization_not_found(client: AsyncClient) -> None:
    await seed_org_with_admin(client, "patch-exists-org", "admin")
    response = await client.patch(
        "/docverse/orgs/nonexistent",
        json={"title": "Nope"},
        headers={"X-Auth-Request-User": "admin"},
    )
    assert response.status_code == 404
