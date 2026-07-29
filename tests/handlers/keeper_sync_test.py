"""Tests for the org-scoped LTD Keeper sync configuration handlers."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from docverse.client.models import OrgRole
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"
_ORG = "ks-org"


async def _setup(client: AsyncClient) -> None:
    await seed_org_with_admin(client, _ORG, _ADMIN)


@pytest.mark.asyncio
async def test_get_returns_default_disabled_config(
    client: AsyncClient,
) -> None:
    """A never-set org returns ``enabled=False`` and the default URL."""
    await _setup(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is False
    assert body["ltd_base_url"] == "https://keeper.lsst.codes/"
    assert body["project_slugs"] == []


@pytest.mark.asyncio
async def test_put_round_trips_explicit_allowlist(
    client: AsyncClient,
) -> None:
    await _setup(client)
    payload = {
        "enabled": True,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": ["dmtn-001", "sqr-112"],
    }
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=payload,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is True
    assert body["ltd_base_url"] == "https://keeper.lsst.codes/"
    assert body["project_slugs"] == ["dmtn-001", "sqr-112"]

    fetched = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert fetched.status_code == 200
    assert fetched.json() == body


@pytest.mark.asyncio
async def test_put_round_trips_wildcard(
    client: AsyncClient,
) -> None:
    """``project_slugs="*"`` round-trips intact through put-then-get."""
    await _setup(client)
    payload = {
        "enabled": True,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": "*",
    }
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=payload,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    assert response.json()["project_slugs"] == "*"

    fetched = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert fetched.status_code == 200
    assert fetched.json()["project_slugs"] == "*"


@pytest.mark.asyncio
async def test_put_is_idempotent(client: AsyncClient) -> None:
    """Re-PUTing the same payload yields the same response body."""
    await _setup(client)
    payload = {
        "enabled": True,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": ["alpha"],
    }
    first = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=payload,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    second = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=payload,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()


@pytest.mark.asyncio
async def test_put_can_disable_without_clearing_other_fields(
    client: AsyncClient,
) -> None:
    """Operators can disable sync without clearing imported state."""
    await _setup(client)
    enable = {
        "enabled": True,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": ["foo"],
    }
    await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=enable,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    disable = {**enable, "enabled": False}
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=disable,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is False
    assert body["project_slugs"] == ["foo"]


@pytest.mark.asyncio
async def test_patch_only_enabled_leaves_other_fields(
    client: AsyncClient,
) -> None:
    """A merge patch of only ``enabled`` leaves the other fields untouched."""
    await _setup(client)
    await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": True,
            "ltd_base_url": "https://keeper.example.com/",
            "project_slugs": ["dmtn-001", "sqr-112"],
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    response = await client.patch(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={"enabled": False},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is False
    assert body["ltd_base_url"] == "https://keeper.example.com/"
    assert body["project_slugs"] == ["dmtn-001", "sqr-112"]


@pytest.mark.asyncio
async def test_patch_project_slugs_replaces_wholesale(
    client: AsyncClient,
) -> None:
    """Providing ``project_slugs`` replaces the whole list (no append)."""
    await _setup(client)
    await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": True,
            "ltd_base_url": "https://keeper.lsst.codes/",
            "project_slugs": ["alpha", "beta"],
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    response = await client.patch(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={"project_slugs": ["gamma"]},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["project_slugs"] == ["gamma"]
    # Untouched fields survive the merge.
    assert body["enabled"] is True


@pytest.mark.asyncio
async def test_patch_empty_body_leaves_config_unchanged(
    client: AsyncClient,
) -> None:
    """An empty merge patch is a no-op that returns the current config."""
    await _setup(client)
    payload = {
        "enabled": True,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": ["only"],
    }
    await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=payload,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    response = await client.patch(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    assert response.json() == {**payload}


@pytest.mark.asyncio
async def test_patch_on_never_set_org_merges_onto_defaults(
    client: AsyncClient,
) -> None:
    """Patching a never-configured org merges onto the default config."""
    await _setup(client)
    response = await client.patch(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={"enabled": True},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is True
    assert body["ltd_base_url"] == "https://keeper.lsst.codes/"
    assert body["project_slugs"] == []


@pytest.mark.asyncio
async def test_patch_rejects_unknown_field(client: AsyncClient) -> None:
    """An unknown field is rejected with a 422 (``extra=forbid``)."""
    await _setup(client)
    response = await client.patch(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={"unknown": True},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ["enabled", "ltd_base_url", "project_slugs"],
)
async def test_patch_rejects_explicit_null(
    client: AsyncClient, field: str
) -> None:
    """An explicit ``null`` for any field is a 422, not a 500 or no-op."""
    await _setup(client)
    response = await client.patch(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={field: None},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_patch_403_for_non_admin(client: AsyncClient) -> None:
    await _setup(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.patch(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={"enabled": False},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_patch_404_for_unknown_org(client: AsyncClient) -> None:
    response = await client.patch(
        "/docverse/orgs/missing-org/keeper-sync",
        json={"enabled": False},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_403_for_non_admin(client: AsyncClient) -> None:
    await _setup(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_put_403_for_non_admin(client: AsyncClient) -> None:
    await _setup(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": True,
            "ltd_base_url": "https://keeper.lsst.codes/",
            "project_slugs": "*",
        },
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_403_without_auth_header(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(f"/docverse/orgs/{_ORG}/keeper-sync")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_404_for_unknown_org(client: AsyncClient) -> None:
    response = await client.get(
        "/docverse/orgs/missing-org/keeper-sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_put_rejects_invalid_url(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": True,
            "ltd_base_url": "not-a-url",
            "project_slugs": "*",
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_put_rejects_unknown_wildcard_token(
    client: AsyncClient,
) -> None:
    """Only the literal ``"*"`` is accepted; other strings are rejected."""
    await _setup(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": True,
            "ltd_base_url": "https://keeper.lsst.codes/",
            "project_slugs": "ALL",
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 422
