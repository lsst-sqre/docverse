"""Tests for auth context binding to the structlog request logger.

Verifies that ``bind_username`` (admin endpoints) and
``OrgRoleDependency`` (org-scoped endpoints) call
``RequestContext.rebind_logger`` with the expected keys.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from docverse.client.models import OrgRole
from docverse.dependencies.context import context_dependency
from docverse.storage.user_info_store import StubUserInfoStore
from tests.conftest import seed_group_member, seed_org_with_admin

# ------------------------------------------------------------------
# Admin endpoints — bind_username
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_endpoint_binds_username(
    client: AsyncClient,
    rebind_spy: list[dict[str, Any]],
) -> None:
    """Admin endpoints bind username to the request logger."""
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "log-org",
            "title": "Log Org",
            "base_domain": "log.example.com",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert any(call.get("username") == "admin-user" for call in rebind_spy)


@pytest.mark.asyncio
async def test_admin_endpoint_no_auth_header(
    client: AsyncClient,
    rebind_spy: list[dict[str, Any]],
) -> None:
    """Admin endpoints without auth header do not bind username."""
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "no-auth-org",
            "title": "No Auth Org",
            "base_domain": "noauth.example.com",
        },
    )
    assert not any("username" in call for call in rebind_spy)


# ------------------------------------------------------------------
# Org-scoped endpoints — OrgRoleDependency
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_org_endpoint_user_membership_binds_auth_context(
    client: AsyncClient,
    rebind_spy: list[dict[str, Any]],
) -> None:
    """Org endpoint with user membership binds full auth context."""
    await seed_org_with_admin(client, "log-user-org", "alice")

    response = await client.get(
        "/docverse/orgs/log-user-org/projects",
        headers={"X-Auth-Request-User": "alice"},
    )
    assert response.status_code == 200

    # Find the auth-context rebind call (the one with auth_basis)
    auth_calls = [c for c in rebind_spy if "auth_basis" in c]
    assert len(auth_calls) == 1
    call = auth_calls[0]
    assert call["username"] == "alice"
    assert call["auth_basis"] == "user_membership"
    assert call["auth_role"] == "admin"
    assert "auth_group" not in call


@pytest.mark.asyncio
async def test_org_endpoint_group_membership_binds_auth_group(
    client: AsyncClient,
    rebind_spy: list[dict[str, Any]],
) -> None:
    """Org endpoint with group membership binds auth_group."""
    await seed_org_with_admin(client, "log-grp-org", "org-admin")
    await seed_group_member("log-grp-org", "g_devs", OrgRole.reader)

    context_dependency._user_info_store = StubUserInfoStore(groups=["g_devs"])
    response = await client.get(
        "/docverse/orgs/log-grp-org/projects",
        headers={"X-Auth-Request-User": "group-user"},
    )
    assert response.status_code == 200

    auth_calls = [c for c in rebind_spy if "auth_basis" in c]
    assert len(auth_calls) == 1
    call = auth_calls[0]
    assert call["username"] == "group-user"
    assert call["auth_basis"] == "group_membership"
    assert call["auth_role"] == "reader"
    assert call["auth_group"] == "g_devs"


@pytest.fixture(autouse=False)
def _enable_superadmin(app: FastAPI) -> None:  # noqa: ARG001
    """Configure a super admin username for the test."""
    context_dependency._superadmin_usernames = ["superadmin"]


@pytest.mark.asyncio
@pytest.mark.usefixtures("_enable_superadmin")
async def test_org_endpoint_superadmin_binds_super_admin_basis(
    client: AsyncClient,
    rebind_spy: list[dict[str, Any]],
) -> None:
    """Org endpoint with super admin binds super_admin basis and role."""
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "log-sa-org",
            "title": "Log SA Org",
            "base_domain": "logsa.example.com",
        },
    )

    response = await client.get(
        "/docverse/orgs/log-sa-org/projects",
        headers={"X-Auth-Request-User": "superadmin"},
    )
    assert response.status_code == 200

    auth_calls = [c for c in rebind_spy if "auth_basis" in c]
    assert len(auth_calls) == 1
    call = auth_calls[0]
    assert call["username"] == "superadmin"
    assert call["auth_basis"] == "super_admin"
    assert call["auth_role"] == "super_admin"
    assert "auth_group" not in call
