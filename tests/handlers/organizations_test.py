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
    assert data["dashboard_template_url"].endswith(
        "/orgs/test-org/dashboard-template"
    )
    assert data["dashboard_template_url"].startswith("http")
    assert data["keeper_sync_url"].endswith("/orgs/test-org/keeper-sync")
    assert data["keeper_sync_url"].startswith("http")
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


@pytest.mark.asyncio
async def test_patch_organization_lifecycle_rules_valid(
    client: AsyncClient,
) -> None:
    """Valid lifecycle_rules PATCH persists the typed JSONB payload."""
    await seed_org_with_admin(client, "patch-lifecycle-org", "admin")
    rules = [
        {"type": "draft_inactivity", "max_days_inactive": 30},
        {
            "type": "build_history_orphan",
            "min_position": 5,
            "min_age_days": 30,
        },
        {"type": "ref_deleted"},
    ]
    response = await client.patch(
        "/docverse/orgs/patch-lifecycle-org",
        json={"lifecycle_rules": rules},
        headers={"X-Auth-Request-User": "admin"},
    )
    assert response.status_code == 200
    assert response.json()["lifecycle_rules"] == rules

    # Round-trips through GET as well.
    get_response = await client.get(
        "/docverse/orgs/patch-lifecycle-org",
        headers={"X-Auth-Request-User": "admin"},
    )
    assert get_response.status_code == 200
    assert get_response.json()["lifecycle_rules"] == rules


@pytest.mark.asyncio
async def test_patch_organization_lifecycle_rules_unknown_type(
    client: AsyncClient,
) -> None:
    """A 422 is returned when a rule names an unknown discriminator tag."""
    await seed_org_with_admin(client, "patch-bad-lifecycle-org", "admin")
    response = await client.patch(
        "/docverse/orgs/patch-bad-lifecycle-org",
        json={
            "lifecycle_rules": [
                {"type": "purgatory_eviction", "enabled": True},
            ],
        },
        headers={"X-Auth-Request-User": "admin"},
    )
    assert response.status_code == 422
    detail = response.json()["detail"]
    # The discriminator-aware error names both the field and the bad tag.
    assert any("lifecycle_rules" in str(err.get("loc", [])) for err in detail)


@pytest.mark.asyncio
async def test_patch_organization_lifecycle_rules_missing_field(
    client: AsyncClient,
) -> None:
    """A 422 is returned when a known rule omits a required field."""
    await seed_org_with_admin(
        client, "patch-missing-field-lifecycle-org", "admin"
    )
    response = await client.patch(
        "/docverse/orgs/patch-missing-field-lifecycle-org",
        json={
            "lifecycle_rules": [
                {"type": "draft_inactivity"},
            ],
        },
        headers={"X-Auth-Request-User": "admin"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_patch_organization_lifecycle_rules_duplicate_types(
    client: AsyncClient,
) -> None:
    """A 422 is returned when the same rule type appears twice."""
    await seed_org_with_admin(client, "patch-dup-lifecycle-org", "admin")
    response = await client.patch(
        "/docverse/orgs/patch-dup-lifecycle-org",
        json={
            "lifecycle_rules": [
                {"type": "draft_inactivity", "max_days_inactive": 30},
                {"type": "draft_inactivity", "max_days_inactive": 60},
            ],
        },
        headers={"X-Auth-Request-User": "admin"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_patch_organization_lifecycle_rules_missing_discriminator(
    client: AsyncClient,
) -> None:
    """A 422 is returned when a rule omits the ``type`` discriminator."""
    await seed_org_with_admin(
        client, "patch-no-discriminator-lifecycle-org", "admin"
    )
    response = await client.patch(
        "/docverse/orgs/patch-no-discriminator-lifecycle-org",
        json={
            "lifecycle_rules": [
                {"max_days_inactive": 30},
            ],
        },
        headers={"X-Auth-Request-User": "admin"},
    )
    assert response.status_code == 422
