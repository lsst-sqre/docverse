"""Tests for credential endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    """Create org and seed admin membership."""
    await seed_org_with_admin(client, "cred-org", "testuser")


@pytest.mark.asyncio
async def test_create_credential(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/cred-org/credentials",
        json={
            "label": "primary-s3",
            "service_type": "s3",
            "credential": {
                "endpoint_url": "https://s3.example.com",
                "bucket": "my-bucket",
                "access_key_id": "AKIAEXAMPLE",
                "secret_access_key": "secret",
                "region": "us-east-1",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["label"] == "primary-s3"
    assert data["service_type"] == "s3"
    assert "self_url" in data
    assert "org_url" in data
    # Credential payload should NOT be in the response
    assert "credential" not in data
    assert "encrypted_credential" not in data


@pytest.mark.asyncio
async def test_list_credentials(client: AsyncClient) -> None:
    await _setup(client)
    # Create two credentials
    for label in ("cred-a1", "cred-b2"):
        await client.post(
            "/docverse/orgs/cred-org/credentials",
            json={
                "label": label,
                "service_type": "s3",
                "credential": {
                    "endpoint_url": "https://s3.example.com",
                    "bucket": "bucket",
                    "access_key_id": "AKIA",
                    "secret_access_key": "secret",
                },
            },
            headers={"X-Auth-Request-User": "testuser"},
        )
    response = await client.get(
        "/docverse/orgs/cred-org/credentials",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    labels = {c["label"] for c in data}
    assert "cred-a1" in labels
    assert "cred-b2" in labels


@pytest.mark.asyncio
async def test_get_credential(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/cred-org/credentials",
        json={
            "label": "get-me",
            "service_type": "r2",
            "credential": {
                "endpoint_url": "https://r2.example.com",
                "bucket": "b",
                "access_key_id": "A",
                "secret_access_key": "S",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/cred-org/credentials/get-me",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["label"] == "get-me"
    assert response.json()["service_type"] == "r2"


@pytest.mark.asyncio
async def test_delete_credential(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/cred-org/credentials",
        json={
            "label": "delete-me",
            "service_type": "s3",
            "credential": {
                "endpoint_url": "https://s3.example.com",
                "bucket": "b",
                "access_key_id": "A",
                "secret_access_key": "S",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.delete(
        "/docverse/orgs/cred-org/credentials/delete-me",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204

    # Verify it's gone
    response = await client.get(
        "/docverse/orgs/cred-org/credentials/delete-me",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_duplicate_credential_label(client: AsyncClient) -> None:
    await _setup(client)
    cred_json = {
        "label": "duplicate-me",
        "service_type": "s3",
        "credential": {
            "endpoint_url": "https://s3.example.com",
            "bucket": "b",
            "access_key_id": "A",
            "secret_access_key": "S",
        },
    }
    headers = {"X-Auth-Request-User": "testuser"}
    r1 = await client.post(
        "/docverse/orgs/cred-org/credentials",
        json=cred_json,
        headers=headers,
    )
    assert r1.status_code == 201

    r2 = await client.post(
        "/docverse/orgs/cred-org/credentials",
        json=cred_json,
        headers=headers,
    )
    assert r2.status_code == 409
