"""Tests for the ``{ltd_slug}`` path parameter's constraints.

The three per-project keeper-sync routes take an LTD product slug
straight out of the URL and hand it to the org's keeper-sync scope,
where it is matched against org-admin-supplied regular expressions. A
pathological pattern paired with an arbitrarily long path segment is
enough to stall the event loop, so the segment is constrained to what
LTD Keeper itself accepts for a product slug and rejected by FastAPI
before any of that runs.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from docverse.models import KeeperSyncConfig
from docverse_server.services.keeper_sync_project import (
    KeeperSyncProjectService,
)
from docverse_server.services.keeper_sync_run import KeeperSyncRunService
from tests.conftest import seed_org_with_admin

_ADMIN = "admin-user"
_ORG = "ks-org"

_ROUTES = [
    ("GET", "/docverse/orgs/{org}/keeper-sync/projects/{slug}"),
    ("GET", "/docverse/orgs/{org}/keeper-sync/projects/{slug}/editions"),
    ("POST", "/docverse/orgs/{org}/keeper-sync/projects/{slug}/refresh"),
]
"""The three routes that address one LTD project by slug."""

_OVER_LONG_SLUG = "a" * 300
"""Longer than any slug LTD can store, and long enough to matter.

The cost of a backtracking-prone pattern grows with the length of the
subject string, so the length cap is the mitigation PRD #667 chose over
a match timeout or an alternative regex engine.
"""


async def _setup_org(client: AsyncClient) -> None:
    """Seed an org syncing everything, so only the slug can be at fault."""
    await seed_org_with_admin(client, _ORG, _ADMIN)
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": True,
            "ltd_base_url": "https://keeper.lsst.codes/",
            "project_slugs": "*",
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200


async def _request(
    client: AsyncClient, method: str, template: str, slug: str
) -> int:
    response = await client.request(
        method,
        template.format(org=_ORG, slug=slug),
        headers={"X-Auth-Request-User": _ADMIN},
    )
    return response.status_code


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "template"), _ROUTES)
async def test_over_long_ltd_slug_is_422(
    client: AsyncClient, method: str, template: str
) -> None:
    """A path segment longer than an LTD slug can be is rejected."""
    await _setup_org(client)
    assert await _request(client, method, template, _OVER_LONG_SLUG) == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "template"), _ROUTES)
@pytest.mark.parametrize(
    "slug",
    ["Pipelines", "pipe_lines", "-pipelines", "pipelines-", "p"],
    ids=["uppercase", "underscore", "leading-dash", "trailing-dash", "single"],
)
async def test_ltd_slug_outside_the_slug_class_is_422(
    client: AsyncClient, method: str, template: str, slug: str
) -> None:
    """Only what LTD Keeper accepts for a product slug gets through.

    LTD validates product slugs against ``^[a-z]+[-a-z0-9]*[a-z0-9]+$``,
    so a slug that LTD could not have issued names no product Docverse
    could ever sync — and there is no reason to let it reach the scope
    rule to find that out.
    """
    await _setup_org(client)
    assert await _request(client, method, template, slug) == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "template"), _ROUTES)
async def test_rejected_ltd_slug_never_reaches_the_scope_gate(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    template: str,
) -> None:
    """A malformed slug is refused before any keeper-sync work happens.

    The point of the constraint is that the regular expressions in the
    org's scope never see the segment, so this spies on both ends of
    that path: the scope rule itself, and the three service methods
    whose first act is the org lookup that feeds it. The organization
    *is* still read once, by the role dependency that resolves
    ``{org}`` — that lookup is the auth check, and it runs on every
    request to these routes whatever the slug looks like.
    """
    await _setup_org(client)

    def _unreachable(*args: object, **kwargs: object) -> object:
        msg = "the keeper-sync scope gate must not run for a rejected slug"
        raise AssertionError(msg)

    monkeypatch.setattr(KeeperSyncConfig, "is_in_scope", _unreachable)
    monkeypatch.setattr(
        KeeperSyncProjectService, "get_project_status", _unreachable
    )
    monkeypatch.setattr(
        KeeperSyncProjectService, "list_project_editions", _unreachable
    )
    monkeypatch.setattr(KeeperSyncRunService, "refresh_project", _unreachable)

    assert await _request(client, method, template, _OVER_LONG_SLUG) == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "template"), _ROUTES)
async def test_well_formed_ltd_slug_still_reaches_the_scope_gate(
    client: AsyncClient, method: str, template: str
) -> None:
    """The constraint admits every slug LTD can actually issue.

    The guard against over-tightening: a hyphenated, digit-bearing slug
    of the shape the lsst.io migration is full of still resolves.
    """
    await _setup_org(client)
    assert await _request(client, method, template, "sqr-112") < 400
