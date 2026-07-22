"""Tests for the non-admin ``GET /orgs`` listing endpoint."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from docverse.client.models import OrgRole
from docverse.dependencies.context import context_dependency
from docverse.storage.user_info_store import StubUserInfoStore
from tests.conftest import seed_group_member, seed_member, seed_org_with_admin


@pytest.mark.asyncio
async def test_member_sees_own_orgs_with_roles(client: AsyncClient) -> None:
    """A member of two orgs sees exactly those two with correct roles."""
    await seed_org_with_admin(client, "list-org-a", "alice")
    await seed_org_with_admin(client, "list-org-b", "other-admin")
    await seed_member("list-org-b", "alice", OrgRole.reader)
    # An org alice is not a member of.
    await seed_org_with_admin(client, "list-org-c", "carol")

    response = await client.get(
        "/docverse/orgs",
        headers={"X-Auth-Request-User": "alice"},
    )
    assert response.status_code == 200
    data = response.json()
    by_slug = {entry["slug"]: entry for entry in data}
    assert set(by_slug) == {"list-org-a", "list-org-b"}
    assert by_slug["list-org-a"]["role"] == "admin"
    assert by_slug["list-org-b"]["role"] == "reader"
    assert by_slug["list-org-a"]["title"] == "Test Org list-org-a"
    assert by_slug["list-org-a"]["self_url"].endswith("/orgs/list-org-a")
    assert by_slug["list-org-a"]["self_url"].startswith("http")


@pytest.mark.asyncio
async def test_superadmin_sees_all_orgs(client: AsyncClient) -> None:
    """A superadmin sees every org, each with the admin effective role."""
    await seed_org_with_admin(client, "sa-org-a", "alice")
    await seed_org_with_admin(client, "sa-org-b", "bob")

    response = await client.get(
        "/docverse/orgs",
        headers={"X-Auth-Request-User": "superadmin"},
    )
    assert response.status_code == 200
    data = response.json()
    by_slug = {entry["slug"]: entry for entry in data}
    assert {"sa-org-a", "sa-org-b"} <= set(by_slug)
    assert by_slug["sa-org-a"]["role"] == "admin"
    assert by_slug["sa-org-b"]["role"] == "admin"


@pytest.mark.asyncio
async def test_non_member_sees_empty_list(client: AsyncClient) -> None:
    """A user with no memberships receives an empty list (200, not 403)."""
    await seed_org_with_admin(client, "empty-org", "owner")

    response = await client.get(
        "/docverse/orgs",
        headers={"X-Auth-Request-User": "nobody"},
    )
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_group_membership_grants_visibility(
    client: AsyncClient,
) -> None:
    """A caller sees an org where only their group holds a role."""
    await seed_org_with_admin(client, "grp-list-org", "owner")
    await seed_group_member("grp-list-org", "g_team", OrgRole.uploader)

    context_dependency._user_info_store = StubUserInfoStore(groups=["g_team"])
    response = await client.get(
        "/docverse/orgs",
        headers={"X-Auth-Request-User": "group-user"},
    )
    assert response.status_code == 200
    by_slug = {entry["slug"]: entry for entry in response.json()}
    assert "grp-list-org" in by_slug
    assert by_slug["grp-list-org"]["role"] == "uploader"


@pytest.mark.asyncio
async def test_highest_role_wins_across_memberships(
    client: AsyncClient,
) -> None:
    """When user and group memberships overlap, the higher role wins."""
    await seed_org_with_admin(client, "dual-org", "owner")
    await seed_member("dual-org", "dual-user", OrgRole.reader)
    await seed_group_member("dual-org", "g_admins", OrgRole.admin)

    context_dependency._user_info_store = StubUserInfoStore(
        groups=["g_admins"]
    )
    response = await client.get(
        "/docverse/orgs",
        headers={"X-Auth-Request-User": "dual-user"},
    )
    assert response.status_code == 200
    by_slug = {entry["slug"]: entry for entry in response.json()}
    assert by_slug["dual-org"]["role"] == "admin"
