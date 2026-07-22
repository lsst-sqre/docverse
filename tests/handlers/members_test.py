"""Tests for organization membership endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient
from safir.metrics import MockEventPublisher

from docverse.dependencies.context import context_dependency
from docverse.metrics import (
    MembershipChangeAction,
    MetricsOrgRole,
    MetricsPrincipalType,
)
from tests.conftest import seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    """Create an org and seed an admin membership."""
    await seed_org_with_admin(client, "mem-org", "admin-user")


@pytest.mark.asyncio
async def test_create_member(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "jdoe",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["principal"] == "jdoe"
    assert data["id"] == "user:jdoe"
    assert data["self_url"].endswith("/orgs/mem-org/members/user:jdoe")
    assert response.headers["Location"] == data["self_url"]


@pytest.mark.asyncio
async def test_list_members(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/mem-org/members",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 200
    data = response.json()
    # At least the admin user seeded by setup
    assert len(data) >= 1


@pytest.mark.asyncio
async def test_get_member(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/mem-org/members/user:admin-user",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 200
    assert response.json()["principal"] == "admin-user"


@pytest.mark.asyncio
async def test_get_member_not_found(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/mem-org/members/user:nobody",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_member(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "removeme",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    response = await client.delete(
        "/docverse/orgs/mem-org/members/user:removeme",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 204

    response = await client.get(
        "/docverse/orgs/mem-org/members/user:removeme",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_post_member_publishes_membership_changed(
    client: AsyncClient,
) -> None:
    """POST member emits one membership_changed with action=add."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "jdoe",
            "principal_type": "user",
            "role": "uploader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 201

    events = context_dependency._events
    assert events is not None
    publisher = events.membership_changed
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == "mem-org"
    assert event.project is None
    assert event.action == MembershipChangeAction.add
    assert event.role == MetricsOrgRole.uploader
    assert event.principal_type == MetricsPrincipalType.user
    assert event.principal == "jdoe"


@pytest.mark.asyncio
async def test_delete_member_publishes_membership_changed(
    client: AsyncClient,
) -> None:
    """DELETE member emits one membership_changed with action=remove."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "g_spherex",
            "principal_type": "group",
            "role": "admin",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    response = await client.delete(
        "/docverse/orgs/mem-org/members/group:g_spherex",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 204

    events = context_dependency._events
    assert events is not None
    publisher = events.membership_changed
    assert isinstance(publisher, MockEventPublisher)
    remove_events = [
        e
        for e in publisher.published
        if e.action == MembershipChangeAction.remove
    ]
    assert len(remove_events) == 1
    event = remove_events[0]
    assert event.organization == "mem-org"
    assert event.project is None
    assert event.role == MetricsOrgRole.admin
    assert event.principal_type == MetricsPrincipalType.group
    assert event.principal == "g_spherex"


@pytest.mark.asyncio
async def test_patch_member_role(client: AsyncClient) -> None:
    """PATCH with a new role updates and returns the membership."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "promote-me",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    response = await client.patch(
        "/docverse/orgs/mem-org/members/user:promote-me",
        json={"role": "admin"},
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["role"] == "admin"
    assert data["principal"] == "promote-me"
    assert data["id"] == "user:promote-me"

    # The change is durable.
    response = await client.get(
        "/docverse/orgs/mem-org/members/user:promote-me",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.json()["role"] == "admin"


@pytest.mark.asyncio
async def test_patch_member_rejects_principal_change(
    client: AsyncClient,
) -> None:
    """Attempts to change identity fields are rejected by extra=forbid."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "immutable-user",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    for body in ({"principal": "someone-else"}, {"principal_type": "group"}):
        response = await client.patch(
            "/docverse/orgs/mem-org/members/user:immutable-user",
            json=body,
            headers={"X-Auth-Request-User": "admin-user"},
        )
        assert response.status_code == 422


@pytest.mark.asyncio
async def test_patch_member_not_found(client: AsyncClient) -> None:
    """PATCH on a missing member returns 404."""
    await _setup(client)
    response = await client.patch(
        "/docverse/orgs/mem-org/members/user:nobody",
        json={"role": "admin"},
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_patch_member_requires_admin(client: AsyncClient) -> None:
    """A non-admin member cannot change roles."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "reader-user",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    response = await client.patch(
        "/docverse/orgs/mem-org/members/user:reader-user",
        json={"role": "admin"},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_duplicate_member(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "dup-user",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    response = await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "dup-user",
            "principal_type": "user",
            "role": "uploader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 409
