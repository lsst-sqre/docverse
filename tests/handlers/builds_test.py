"""Tests for build endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient
from safir.metrics import MockEventPublisher

from docverse.client.models import BuildAnnotations
from docverse.dependencies.context import context_dependency
from tests.conftest import seed_build, seed_org_with_admin

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
            "source_url": "https://example.com/example/build",
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
    assert response.status_code == 422
    data = response.json()
    assert data["detail"][0]["type"] == "missing_configuration"
    assert "object store" in data["detail"][0]["msg"].lower()


@pytest.mark.asyncio
async def test_create_build_with_annotations(client: AsyncClient) -> None:
    """Annotations round-trip via DB seeding + GET (POST needs a store)."""
    await _setup(client)
    build_id = await seed_build(
        "build-org",
        "build-proj",
        annotations=BuildAnnotations.model_validate(
            {
                "commit_sha": "abc123",
                "ci_platform": "github-actions",
                "custom_key": "custom_value",
            }
        ),
    )
    response = await client.get(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["annotations"]["commit_sha"] == "abc123"
    assert data["annotations"]["ci_platform"] == "github-actions"
    assert data["annotations"]["custom_key"] == "custom_value"


@pytest.mark.asyncio
async def test_list_builds(client: AsyncClient) -> None:
    await _setup(client)
    await seed_build("build-org", "build-proj")
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
    build_id = await seed_build("build-org", "build-proj")
    response = await client.get(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["id"] == build_id


@pytest.mark.asyncio
async def test_get_build_malformed_id(client: AsyncClient) -> None:
    """GET with a malformed base32 build ID returns 422."""
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/build-org/projects/build-proj/builds/not-a-valid-id",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_patch_build_upload_complete(client: AsyncClient) -> None:
    await _setup(client)
    build_id = await seed_build("build-org", "build-proj")
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
async def test_patch_build_publishes_build_uploaded(
    client: AsyncClient,
) -> None:
    """PATCH status=uploaded emits one build_uploaded with provenance."""
    await _setup(client)
    build_id = await seed_build(
        "build-org",
        "build-proj",
        uploader="ci-bot",
        annotations=BuildAnnotations.model_validate(
            {
                "commit_sha": "abc123",
                "github_run_id": "42",
                "ci_platform": "github-actions",
            }
        ),
    )
    response = await client.patch(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        json={"status": "uploaded"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    events = context_dependency._events
    assert events is not None
    publisher = events.build_uploaded
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == "build-org"
    assert event.project == "build-proj"
    assert event.uploader == "ci-bot"
    assert event.commit_sha == "abc123"
    assert event.github_run_id == "42"
    assert event.ci_platform == "github-actions"
    # Provenance fields the uploader did not annotate are null.
    assert event.github_repository is None
    assert event.github_actor is None


@pytest.mark.asyncio
async def test_patch_build_noop_does_not_publish_build_uploaded(
    client: AsyncClient,
) -> None:
    """A non-uploaded PATCH must not emit a build_uploaded event."""
    await _setup(client)
    build_id = await seed_build("build-org", "build-proj")
    response = await client.patch(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        json={"status": "pending"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    events = context_dependency._events
    assert events is not None
    publisher = events.build_uploaded
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 0


@pytest.mark.asyncio
async def test_delete_build(client: AsyncClient) -> None:
    await _setup(client)
    build_id = await seed_build("build-org", "build-proj")
    response = await client.delete(
        f"/docverse/orgs/build-org/projects/build-proj/builds/{build_id}",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204
