"""Tests for group-based authorization through the HTTP stack.

Verifies the full path: HTTP request, OrgRoleDependency,
UserInfoStore.get_groups(), and resolve_role(groups=...) for
group-type memberships.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from docverse.client.models import OrgRole
from docverse.dependencies.context import context_dependency
from docverse.storage.user_info_store import StubUserInfoStore
from tests.conftest import seed_group_member, seed_org_with_admin

CONTENT_HASH = (
    "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
)


@pytest.mark.asyncio
async def test_group_reader_can_list_projects(
    client: AsyncClient,
) -> None:
    """A user whose group has reader role can list projects."""
    await seed_org_with_admin(client, "grp-read-org", "admin-user")
    await seed_group_member("grp-read-org", "g_team", OrgRole.reader)

    context_dependency._user_info_store = StubUserInfoStore(groups=["g_team"])
    response = await client.get(
        "/docverse/orgs/grp-read-org/projects",
        headers={"X-Auth-Request-User": "group-reader"},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_user_without_group_denied(
    client: AsyncClient,
) -> None:
    """A user not in the required group is denied access."""
    await seed_org_with_admin(client, "grp-deny-org", "admin-user")
    await seed_group_member("grp-deny-org", "g_team", OrgRole.reader)

    context_dependency._user_info_store = StubUserInfoStore(groups=[])
    response = await client.get(
        "/docverse/orgs/grp-deny-org/projects",
        headers={"X-Auth-Request-User": "outsider"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_group_uploader_can_create_build(
    client: AsyncClient,
) -> None:
    """A user whose group has uploader role can create a build."""
    await seed_org_with_admin(client, "grp-up-org", "admin-user")
    await seed_group_member("grp-up-org", "g_uploaders", OrgRole.uploader)

    # Create a project as the admin
    await client.post(
        "/docverse/orgs/grp-up-org/projects",
        json={
            "slug": "grp-proj",
            "title": "Group Project",
            "doc_repo": "https://github.com/example/grp",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )

    context_dependency._user_info_store = StubUserInfoStore(
        groups=["g_uploaders"]
    )
    # Uploader has permission but org has no store → 422
    response = await client.post(
        "/docverse/orgs/grp-up-org/projects/grp-proj/builds",
        json={
            "git_ref": "main",
            "content_hash": CONTENT_HASH,
        },
        headers={"X-Auth-Request-User": "group-uploader"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_group_reader_cannot_create_build(
    client: AsyncClient,
) -> None:
    """A user whose group has only reader role cannot create a build."""
    await seed_org_with_admin(client, "grp-ro-org", "admin-user")
    await seed_group_member("grp-ro-org", "g_team", OrgRole.reader)

    # Create a project as the admin
    await client.post(
        "/docverse/orgs/grp-ro-org/projects",
        json={
            "slug": "grp-ro-proj",
            "title": "Group RO Project",
            "doc_repo": "https://github.com/example/grp-ro",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )

    context_dependency._user_info_store = StubUserInfoStore(groups=["g_team"])
    response = await client.post(
        "/docverse/orgs/grp-ro-org/projects/grp-ro-proj/builds",
        json={
            "git_ref": "main",
            "content_hash": CONTENT_HASH,
        },
        headers={"X-Auth-Request-User": "group-reader"},
    )
    assert response.status_code == 403
