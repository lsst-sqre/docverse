"""Tests for the org-scoped LTD Keeper sync configuration handlers."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from docverse.models import OrgRole
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
    assert body["project_slug_patterns"] == []
    assert body["exclude_project_slugs"] == []
    assert body["exclude_project_slug_patterns"] == []


@pytest.mark.asyncio
async def test_put_round_trips_scope_fields(client: AsyncClient) -> None:
    """``PUT`` persists the three scope fields, and ``GET`` returns them."""
    await _setup(client)
    payload = {
        "enabled": True,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": ["sqr-112"],
        "project_slug_patterns": [r"sqr-\d+", r"dmtn-\d+"],
        "exclude_project_slugs": ["www"],
        "exclude_project_slug_patterns": [r"test-.*"],
    }
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=payload,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    assert response.json() == payload

    fetched = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert fetched.status_code == 200
    assert fetched.json() == payload


@pytest.mark.asyncio
async def test_patch_round_trips_scope_fields(client: AsyncClient) -> None:
    """``PATCH`` persists the scope fields, and ``GET`` returns them."""
    await _setup(client)
    response = await client.patch(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": True,
            "project_slug_patterns": [r"sqr-\d+"],
            "exclude_project_slugs": ["www"],
            "exclude_project_slug_patterns": [r"test-.*"],
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200

    fetched = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert fetched.status_code == 200
    body = fetched.json()
    assert body["project_slug_patterns"] == [r"sqr-\d+"]
    assert body["exclude_project_slugs"] == ["www"]
    assert body["exclude_project_slug_patterns"] == [r"test-.*"]


@pytest.mark.asyncio
async def test_patch_one_scope_field_leaves_the_others(
    client: AsyncClient,
) -> None:
    """Patching one scope field does not disturb the other three."""
    await _setup(client)
    stored = {
        "enabled": True,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": ["sqr-112"],
        "project_slug_patterns": [r"sqr-\d+"],
        "exclude_project_slugs": ["www"],
        "exclude_project_slug_patterns": [r"test-.*"],
    }
    await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=stored,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    response = await client.patch(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={"exclude_project_slugs": ["www", "legacy"]},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    assert response.json() == {
        **stored,
        "exclude_project_slugs": ["www", "legacy"],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method",
    ["put", "patch"],
)
@pytest.mark.parametrize(
    "field",
    ["project_slug_patterns", "exclude_project_slug_patterns"],
)
async def test_write_rejects_uncompilable_pattern(
    client: AsyncClient, method: str, field: str
) -> None:
    """A bad regex is a 422 naming the field and pattern; nothing is stored."""
    await _setup(client)
    stored = {
        "enabled": True,
        "ltd_base_url": "https://keeper.lsst.codes/",
        "project_slugs": ["sqr-112"],
        "project_slug_patterns": [],
        "exclude_project_slugs": [],
        "exclude_project_slug_patterns": [],
    }
    await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=stored,
        headers={"X-Auth-Request-User": _ADMIN},
    )

    body = (
        {**stored, field: ["sqr-("]} if method == "put" else {field: ["sqr-("]}
    )
    response = await client.request(
        method.upper(),
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json=body,
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 422
    detail = response.text
    assert field in detail
    assert "sqr-(" in detail

    fetched = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert fetched.json() == stored


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
    assert response.json() == {
        **payload,
        "project_slug_patterns": [],
        "exclude_project_slugs": [],
        "exclude_project_slug_patterns": [],
    }


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
async def test_put_rejects_unknown_field(client: AsyncClient) -> None:
    """``PUT`` still 422s on an unknown field, and stores nothing.

    ``KeeperSyncConfig`` itself now ignores unknown keys so an older
    reader can load a row a newer server wrote, but the ``PUT`` request
    body is ``KeeperSyncConfigWrite``, which forbids them — a caller's
    typo must not be silently dropped on the way into the database.
    """
    await _setup(client)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": True,
            "ltd_base_url": "https://keeper.lsst.codes/",
            "project_slugs": ["sqr-112"],
            "unknown": True,
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 422

    fetched = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert fetched.status_code == 200
    assert fetched.json()["enabled"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "enabled",
        "ltd_base_url",
        "project_slugs",
        "project_slug_patterns",
        "exclude_project_slugs",
        "exclude_project_slug_patterns",
    ],
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
