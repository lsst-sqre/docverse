"""Handler-level authorization tests.

Verifies that each endpoint enforces the correct minimum role via the
auth dependency wiring.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from docverse.client.models import OrgRole
from tests.conftest import seed_member, seed_org_with_admin

CONTENT_HASH = (
    "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
)


async def _setup(client: AsyncClient) -> None:
    """Create org with admin, reader, uploader, and a project."""
    await seed_org_with_admin(client, "auth-org", "admin-user")
    await seed_member("auth-org", "read-user", OrgRole.reader)
    await seed_member("auth-org", "upload-user", OrgRole.uploader)

    # Create a project for build/edition tests
    await client.post(
        "/docverse/orgs/auth-org/projects",
        json={
            "slug": "auth-proj",
            "title": "Auth Project",
            "doc_repo": "https://github.com/example/auth",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )


async def _create_build(client: AsyncClient) -> str:
    """Create a build and return its ID."""
    resp = await client.post(
        "/docverse/orgs/auth-org/projects/auth-proj/builds",
        json={"git_ref": "main", "content_hash": CONTENT_HASH},
        headers={"X-Auth-Request-User": "admin-user"},
    )
    return str(resp.json()["id"])


async def _create_edition(client: AsyncClient) -> str:
    """Create an edition and return its slug."""
    slug = "auth-ed"
    await client.post(
        "/docverse/orgs/auth-org/projects/auth-proj/editions",
        json={
            "slug": slug,
            "title": "Auth Edition",
            "kind": "draft",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    return slug


# ------------------------------------------------------------------
# No auth / non-member
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_auth_header(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get("/docverse/orgs/auth-org/projects")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_non_member(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/auth-org/projects",
        headers={"X-Auth-Request-User": "stranger"},
    )
    assert response.status_code == 403


# ------------------------------------------------------------------
# Projects — admin-only for write, reader for read
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_projects_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/auth-org/projects",
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_create_project_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/auth-org/projects",
        json={
            "slug": "forbidden",
            "title": "No",
            "doc_repo": "https://github.com/example/no",
        },
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_project_as_uploader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/auth-org/projects",
        json={
            "slug": "forbidden",
            "title": "No",
            "doc_repo": "https://github.com/example/no",
        },
        headers={"X-Auth-Request-User": "upload-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_update_project_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.patch(
        "/docverse/orgs/auth-org/projects/auth-proj",
        json={"title": "Nope"},
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_delete_project_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.delete(
        "/docverse/orgs/auth-org/projects/auth-proj",
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 403


# ------------------------------------------------------------------
# Builds — uploader+ for create/patch, admin for delete, reader for read
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_builds_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/auth-org/projects/auth-proj/builds",
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_create_build_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/auth-org/projects/auth-proj/builds",
        json={"git_ref": "main", "content_hash": CONTENT_HASH},
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_build_as_uploader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/auth-org/projects/auth-proj/builds",
        json={"git_ref": "main", "content_hash": CONTENT_HASH},
        headers={"X-Auth-Request-User": "upload-user"},
    )
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_patch_build_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    build_id = await _create_build(client)
    response = await client.patch(
        f"/docverse/orgs/auth-org/projects/auth-proj/builds/{build_id}",
        json={"status": "uploaded"},
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_delete_build_as_uploader(client: AsyncClient) -> None:
    await _setup(client)
    build_id = await _create_build(client)
    response = await client.delete(
        f"/docverse/orgs/auth-org/projects/auth-proj/builds/{build_id}",
        headers={"X-Auth-Request-User": "upload-user"},
    )
    assert response.status_code == 403


# ------------------------------------------------------------------
# Editions — admin-only for write, reader for read
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_editions_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/auth-org/projects/auth-proj/editions",
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_create_edition_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/auth-org/projects/auth-proj/editions",
        json={
            "slug": "forbidden",
            "title": "No",
            "kind": "draft",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_edition_as_uploader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/auth-org/projects/auth-proj/editions",
        json={
            "slug": "forbidden",
            "title": "No",
            "kind": "draft",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": "upload-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_update_edition_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    slug = await _create_edition(client)
    response = await client.patch(
        f"/docverse/orgs/auth-org/projects/auth-proj/editions/{slug}",
        json={"title": "Nope"},
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_delete_edition_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    slug = await _create_edition(client)
    response = await client.delete(
        f"/docverse/orgs/auth-org/projects/auth-proj/editions/{slug}",
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 403


# ------------------------------------------------------------------
# Members — admin-only
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_members_as_reader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/auth-org/members",
        headers={"X-Auth-Request-User": "read-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_members_as_uploader(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/auth-org/members",
        headers={"X-Auth-Request-User": "upload-user"},
    )
    assert response.status_code == 403
