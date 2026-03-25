"""Tests for organization service endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    """Create org, seed admin, and create a credential."""
    await seed_org_with_admin(client, "svc-org", "testuser")
    # Create a credential that services can reference
    await client.post(
        "/docverse/orgs/svc-org/credentials",
        json={
            "label": "primary-aws",
            "credentials": {
                "provider": "aws",
                "access_key_id": "AKIAEXAMPLE",
                "secret_access_key": "secret",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )


@pytest.mark.asyncio
async def test_create_service(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "my-s3",
            "credential_label": "primary-aws",
            "config": {
                "provider": "aws_s3",
                "bucket": "my-bucket",
                "region": "us-east-1",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["label"] == "my-s3"
    assert data["category"] == "object_storage"
    assert data["provider"] == "aws_s3"
    assert data["config"] == {"bucket": "my-bucket", "region": "us-east-1"}
    assert data["credential_label"] == "primary-aws"
    assert "self_url" in data
    assert "org_url" in data


@pytest.mark.asyncio
async def test_list_services(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    # Create two services
    await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "store-a",
            "credential_label": "primary-aws",
            "config": {
                "provider": "aws_s3",
                "bucket": "bucket-a",
                "region": "us-east-1",
            },
        },
        headers=headers,
    )
    await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "store-b",
            "credential_label": "primary-aws",
            "config": {
                "provider": "aws_s3",
                "bucket": "bucket-b",
                "region": "us-west-2",
            },
        },
        headers=headers,
    )
    response = await client.get(
        "/docverse/orgs/svc-org/services",
        headers=headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    labels = {s["label"] for s in data}
    assert "store-a" in labels
    assert "store-b" in labels


@pytest.mark.asyncio
async def test_get_service(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "get-me",
            "credential_label": "primary-aws",
            "config": {
                "provider": "aws_s3",
                "bucket": "my-bucket",
                "region": "us-east-1",
            },
        },
        headers=headers,
    )
    response = await client.get(
        "/docverse/orgs/svc-org/services/get-me",
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json()["label"] == "get-me"
    assert response.json()["provider"] == "aws_s3"


@pytest.mark.asyncio
async def test_delete_service(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "delete-me",
            "credential_label": "primary-aws",
            "config": {
                "provider": "aws_s3",
                "bucket": "b",
                "region": "us-east-1",
            },
        },
        headers=headers,
    )
    response = await client.delete(
        "/docverse/orgs/svc-org/services/delete-me",
        headers=headers,
    )
    assert response.status_code == 204

    # Verify it's gone
    response = await client.get(
        "/docverse/orgs/svc-org/services/delete-me",
        headers=headers,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_duplicate_service_label(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    svc_json = {
        "label": "duplicate-me",
        "credential_label": "primary-aws",
        "config": {
            "provider": "aws_s3",
            "bucket": "b",
            "region": "us-east-1",
        },
    }
    r1 = await client.post(
        "/docverse/orgs/svc-org/services",
        json=svc_json,
        headers=headers,
    )
    assert r1.status_code == 201

    r2 = await client.post(
        "/docverse/orgs/svc-org/services",
        json=svc_json,
        headers=headers,
    )
    assert r2.status_code == 409


@pytest.mark.asyncio
async def test_service_with_missing_credential(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "no-cred",
            "credential_label": "nonexistent",
            "config": {
                "provider": "aws_s3",
                "bucket": "b",
                "region": "us-east-1",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_service_with_incompatible_credential(
    client: AsyncClient,
) -> None:
    """Test creating a service with a credential of incompatible provider."""
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    # primary-aws is an "aws" credential; cloudflare_r2 requires "cloudflare"
    response = await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "bad-combo",
            "credential_label": "primary-aws",
            "config": {
                "provider": "cloudflare_r2",
                "account_id": "abc123",
                "bucket": "my-bucket",
            },
        },
        headers=headers,
    )
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_delete_service_referenced_by_slot(
    client: AsyncClient,
) -> None:
    """Test that deleting a service referenced by an org slot is blocked."""
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    # Create a service
    response = await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "slot-svc",
            "credential_label": "primary-aws",
            "config": {
                "provider": "aws_s3",
                "bucket": "b",
                "region": "us-east-1",
            },
        },
        headers=headers,
    )
    assert response.status_code == 201

    # Assign the service to an org slot via org PATCH
    response = await client.patch(
        "/docverse/orgs/svc-org",
        json={"publishing_store_label": "slot-svc"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    # Try to delete the service — should be blocked
    response = await client.delete(
        "/docverse/orgs/svc-org/services/slot-svc",
        headers=headers,
    )
    assert response.status_code == 409
