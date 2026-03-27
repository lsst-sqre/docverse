"""Tests for organization service endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    """Create org, seed admin, and create credentials."""
    await seed_org_with_admin(client, "svc-org", "testuser")
    headers = {"X-Auth-Request-User": "testuser"}
    # Create an AWS credential for AWS services
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
        headers=headers,
    )
    # Create an S3 credential for generic S3-compatible services (R2, minio)
    await client.post(
        "/docverse/orgs/svc-org/credentials",
        json={
            "label": "primary-s3",
            "credentials": {
                "provider": "s3",
                "access_key_id": "S3ACCESSKEY",
                "secret_access_key": "s3secret",
            },
        },
        headers=headers,
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
    # primary-aws is an "aws" credential; cloudflare_r2 requires "s3"
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


@pytest.mark.asyncio
async def test_create_cloudflare_r2_service(client: AsyncClient) -> None:
    """Test creating a Cloudflare R2 service with an s3 credential."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "my-r2",
            "credential_label": "primary-s3",
            "config": {
                "provider": "cloudflare_r2",
                "account_id": "abc123",
                "bucket": "my-bucket",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["label"] == "my-r2"
    assert data["provider"] == "cloudflare_r2"
    assert data["credential_label"] == "primary-s3"


@pytest.mark.asyncio
async def test_create_minio_service(client: AsyncClient) -> None:
    """Test creating a MinIO service with an s3 credential."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "my-minio",
            "credential_label": "primary-s3",
            "config": {
                "provider": "minio",
                "endpoint_url": "http://minio:9000",
                "bucket": "docs",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["label"] == "my-minio"
    assert data["provider"] == "minio"
    assert data["credential_label"] == "primary-s3"


@pytest.mark.asyncio
async def test_patch_service_credential(client: AsyncClient) -> None:
    """Test updating a service's credential_label via PATCH."""
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    # Create service with primary-aws
    await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "patch-me",
            "credential_label": "primary-aws",
            "config": {
                "provider": "aws_s3",
                "bucket": "b",
                "region": "us-east-1",
            },
        },
        headers=headers,
    )
    # Create a second AWS credential
    await client.post(
        "/docverse/orgs/svc-org/credentials",
        json={
            "label": "secondary-aws",
            "credentials": {
                "provider": "aws",
                "access_key_id": "AKIAEXAMPLE2",
                "secret_access_key": "secret2",
            },
        },
        headers=headers,
    )
    # PATCH to the new credential
    response = await client.patch(
        "/docverse/orgs/svc-org/services/patch-me",
        json={"credential_label": "secondary-aws"},
        headers=headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["credential_label"] == "secondary-aws"
    assert data["label"] == "patch-me"


@pytest.mark.asyncio
async def test_patch_service_nonexistent_credential(
    client: AsyncClient,
) -> None:
    """Test PATCH with a credential that doesn't exist returns 404."""
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "patch-404",
            "credential_label": "primary-aws",
            "config": {
                "provider": "aws_s3",
                "bucket": "b",
                "region": "us-east-1",
            },
        },
        headers=headers,
    )
    response = await client.patch(
        "/docverse/orgs/svc-org/services/patch-404",
        json={"credential_label": "nonexistent"},
        headers=headers,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_patch_service_incompatible_credential(
    client: AsyncClient,
) -> None:
    """Test PATCH with an incompatible credential returns 409."""
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    # Create a Cloudflare R2 service (requires s3 credential)
    await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "patch-409",
            "credential_label": "primary-s3",
            "config": {
                "provider": "cloudflare_r2",
                "account_id": "abc123",
                "bucket": "b",
            },
        },
        headers=headers,
    )
    # Try to PATCH to an aws credential (incompatible with cloudflare_r2)
    response = await client.patch(
        "/docverse/orgs/svc-org/services/patch-409",
        json={"credential_label": "primary-aws"},
        headers=headers,
    )
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_patch_service_noop(client: AsyncClient) -> None:
    """Test that an empty PATCH body returns the service unchanged."""
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    await client.post(
        "/docverse/orgs/svc-org/services",
        json={
            "label": "patch-noop",
            "credential_label": "primary-aws",
            "config": {
                "provider": "aws_s3",
                "bucket": "b",
                "region": "us-east-1",
            },
        },
        headers=headers,
    )
    response = await client.patch(
        "/docverse/orgs/svc-org/services/patch-noop",
        json={},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json()["credential_label"] == "primary-aws"


@pytest.mark.asyncio
async def test_patch_nonexistent_service(client: AsyncClient) -> None:
    """Test PATCH on a non-existent service returns 404."""
    await _setup(client)
    response = await client.patch(
        "/docverse/orgs/svc-org/services/no-such-service",
        json={"credential_label": "primary-aws"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404
