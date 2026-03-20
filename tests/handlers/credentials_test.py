"""Tests for credential endpoints."""

from __future__ import annotations

from typing import Any

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
            "label": "primary-aws",
            "credentials": {
                "provider": "aws",
                "access_key_id": "AKIAEXAMPLE",
                "secret_access_key": "secret",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["label"] == "primary-aws"
    assert data["provider"] == "aws"
    assert "self_url" in data
    assert "org_url" in data
    # Credential payload should NOT be in the response
    assert "credentials" not in data
    assert "encrypted_credentials" not in data


@pytest.mark.asyncio
async def test_list_credentials(client: AsyncClient) -> None:
    await _setup(client)
    # Create two credentials
    for label, provider in [("cred-a1", "aws"), ("cred-b2", "cloudflare")]:
        payload: dict[str, Any] = {
            "label": label,
            "credentials": {"provider": provider},
        }
        if provider == "aws":
            payload["credentials"]["access_key_id"] = "AKIA"
            payload["credentials"]["secret_access_key"] = "secret"  # noqa: S105
        else:
            payload["credentials"]["api_token"] = "cf-token"  # noqa: S105
        await client.post(
            "/docverse/orgs/cred-org/credentials",
            json=payload,
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
            "credentials": {
                "provider": "cloudflare",
                "api_token": "cf-token-123",
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
    assert response.json()["provider"] == "cloudflare"


@pytest.mark.asyncio
async def test_delete_credential(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/cred-org/credentials",
        json={
            "label": "delete-me",
            "credentials": {
                "provider": "aws",
                "access_key_id": "AKIA",
                "secret_access_key": "secret",
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
        "credentials": {
            "provider": "aws",
            "access_key_id": "AKIA",
            "secret_access_key": "secret",
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


@pytest.mark.asyncio
async def test_delete_credential_referenced_by_service(
    client: AsyncClient,
) -> None:
    """Deleting a credential that a service references should be blocked."""
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    # Create a credential
    await client.post(
        "/docverse/orgs/cred-org/credentials",
        json={
            "label": "in-use",
            "credentials": {
                "provider": "aws",
                "access_key_id": "AKIA",
                "secret_access_key": "secret",
            },
        },
        headers=headers,
    )
    # Create a service referencing that credential
    await client.post(
        "/docverse/orgs/cred-org/services",
        json={
            "label": "my-svc",
            "credential_label": "in-use",
            "config": {
                "provider": "aws_s3",
                "bucket": "b",
                "region": "us-east-1",
            },
        },
        headers=headers,
    )
    # Try to delete the credential — should be blocked
    response = await client.delete(
        "/docverse/orgs/cred-org/credentials/in-use",
        headers=headers,
    )
    assert response.status_code == 409
