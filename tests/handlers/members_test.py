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
async def test_patch_member_rejects_null_role(client: AsyncClient) -> None:
    """An explicit ``{"role": null}`` is a 422, not a silent no-op or 500."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "null-role-user",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    response = await client.patch(
        "/docverse/orgs/mem-org/members/user:null-role-user",
        json={"role": None},
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 422

    # The role is unchanged (the rejected patch never touched storage).
    response = await client.get(
        "/docverse/orgs/mem-org/members/user:null-role-user",
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.json()["role"] == "reader"


@pytest.mark.asyncio
async def test_patch_member_role_change_publishes_remove_and_add(
    client: AsyncClient,
) -> None:
    """An in-place role change emits remove(old role) + add(new role)."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "role-churn",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    events = context_dependency._events
    assert events is not None
    publisher = events.membership_changed
    assert isinstance(publisher, MockEventPublisher)
    baseline = len(publisher.published)

    response = await client.patch(
        "/docverse/orgs/mem-org/members/user:role-churn",
        json={"role": "admin"},
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert response.status_code == 200

    new_events = publisher.published[baseline:]
    assert len(new_events) == 2
    remove_event, add_event = new_events
    assert remove_event.action == MembershipChangeAction.remove
    assert remove_event.role == MetricsOrgRole.reader
    assert remove_event.principal == "role-churn"
    assert remove_event.principal_type == MetricsPrincipalType.user
    assert add_event.action == MembershipChangeAction.add
    assert add_event.role == MetricsOrgRole.admin
    assert add_event.principal == "role-churn"
    assert add_event.principal_type == MetricsPrincipalType.user


@pytest.mark.asyncio
async def test_patch_member_no_op_publishes_nothing(
    client: AsyncClient,
) -> None:
    """Patching to the same role (or an empty body) emits no event."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/mem-org/members",
        json={
            "principal": "steady",
            "principal_type": "user",
            "role": "reader",
        },
        headers={"X-Auth-Request-User": "admin-user"},
    )
    events = context_dependency._events
    assert events is not None
    publisher = events.membership_changed
    assert isinstance(publisher, MockEventPublisher)
    baseline = len(publisher.published)

    same_role = await client.patch(
        "/docverse/orgs/mem-org/members/user:steady",
        json={"role": "reader"},
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert same_role.status_code == 200
    empty = await client.patch(
        "/docverse/orgs/mem-org/members/user:steady",
        json={},
        headers={"X-Auth-Request-User": "admin-user"},
    )
    assert empty.status_code == 200

    assert len(publisher.published) == baseline


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
