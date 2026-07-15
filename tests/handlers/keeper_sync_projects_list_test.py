"""Tests for ``GET /orgs/{org}/keeper-sync/projects``.

The paginated org-scoped listing complements the per-project status
endpoint: operators can enumerate every project that has a
``keeper_sync_state`` row at the org level, without first having to
know an LTD slug.
"""

from __future__ import annotations

from typing import Any, Literal

import pytest
import structlog
from docverse.client.models import OrgRole
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency
from safir.http import PaginationLinkData
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"
_ORG = "ks-projects-list-org"
_LTD_BASE = "https://keeper.lsst.codes"


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


async def _setup_org(client: AsyncClient) -> None:
    await seed_org_with_admin(client, _ORG, _ADMIN)


async def _enable_sync(
    client: AsyncClient,
    *,
    project_slugs: list[str] | Literal["*"] = "*",
) -> None:
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": True,
            "ltd_base_url": f"{_LTD_BASE}/",
            "project_slugs": project_slugs,
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200


async def _create_project(client: AsyncClient, *, slug: str) -> int:
    response = await client.post(
        f"/docverse/orgs/{_ORG}/projects",
        json={
            "slug": slug,
            "title": slug.title(),
            "source_url": f"https://example.com/example/{slug}",
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201
    async for session in db_session_dependency():
        org_store = OrganizationStore(session=session, logger=_logger())
        org = await org_store.get_by_slug(_ORG)
        assert org is not None
        proj_store = ProjectStore(session=session, logger=_logger())
        project = await proj_store.get_by_slug(org_id=org.id, slug=slug)
        assert project is not None
        return project.id
    msg = "no session"
    raise AssertionError(msg)


async def _get_org_id() -> int:
    async for session in db_session_dependency():
        store = OrganizationStore(session=session, logger=_logger())
        org = await store.get_by_slug(_ORG)
        assert org is not None
        return org.id
    msg = "no session"
    raise AssertionError(msg)


async def _seed_state(
    *,
    org_id: int,
    resource_type: ResourceType,
    ltd_slug: str,
    ltd_id: int | None = None,
    docverse_id: int | None = None,
    annotations: dict[str, Any] | None = None,
) -> None:
    async for session in db_session_dependency():
        async with session.begin():
            store = KeeperSyncStateStore(session=session, logger=_logger())
            await store.upsert(
                org_id=org_id,
                resource_type=resource_type,
                ltd_id=ltd_id,
                ltd_slug=ltd_slug,
                docverse_id=docverse_id,
                annotations=annotations,
            )
            await session.commit()
        return
    msg = "no session"
    raise AssertionError(msg)


# ---------------------------------------------------------------------------
# Auth + config gating
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_projects_returns_404_when_sync_disabled(
    client: AsyncClient,
) -> None:
    """No PUT to enable sync — endpoint 404s, matching project-status shape."""
    await _setup_org(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_projects_returns_404_when_org_missing(
    client: AsyncClient,
) -> None:
    """An org that does not exist returns 404 from the listing too."""
    response = await client.get(
        "/docverse/orgs/no-such-org/keeper-sync/projects",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_projects_403_for_non_admin(client: AsyncClient) -> None:
    """A reader-role user gets 403."""
    await _setup_org(client)
    await _enable_sync(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_projects_403_when_no_auth_header(
    client: AsyncClient,
) -> None:
    """No ``X-Auth-Request-User`` header → 403 from ``require_admin``."""
    await _setup_org(client)
    await _enable_sync(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects",
    )
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# Empty + scope behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_projects_empty_when_no_state_rows(
    client: AsyncClient,
) -> None:
    """Sync enabled but no state rows yet — 200 with an empty page."""
    await _setup_org(client)
    await _enable_sync(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers["X-Total-Count"] == "0"


@pytest.mark.asyncio
async def test_list_projects_only_returns_projects_with_state_rows(
    client: AsyncClient,
) -> None:
    """Allowlisted-but-never-synced slugs are excluded from the listing.

    Acceptance criterion: only projects with a ``KeeperSyncState`` row
    of ``resource_type=project`` for this org appear. Operators can
    still inspect never-seen-but-allowlisted slugs via the per-project
    GET; they intentionally do not show up in the org-wide collection.
    """
    await _setup_org(client)
    # Allowlist three slugs but only seed state rows for two of them.
    await _enable_sync(
        client, project_slugs=["pipelines", "obstac", "unobserved"]
    )
    org_id = await _get_org_id()
    await _seed_state(
        org_id=org_id,
        resource_type=ResourceType.project,
        ltd_slug="pipelines",
    )
    await _seed_state(
        org_id=org_id,
        resource_type=ResourceType.project,
        ltd_slug="obstac",
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    entries = response.json()
    slugs = {entry["ltd_slug"] for entry in entries}
    assert slugs == {"pipelines", "obstac"}
    assert response.headers["X-Total-Count"] == "2"


# ---------------------------------------------------------------------------
# HATEOAS + body shape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_projects_carries_full_hateoas_set(
    client: AsyncClient,
) -> None:
    """Each entry carries the full HATEOAS set plus tier_status + main_edition.

    Mirrors the per-project status response so a client paginating
    through the listing has the same self-describing handles as a
    direct per-project GET — no second round-trip required to drill
    into a project.
    """
    await _setup_org(client)
    await _enable_sync(client)
    ltd_slug = "pipelines"
    project_id = await _create_project(client, slug=ltd_slug)
    org_id = await _get_org_id()
    await _seed_state(
        org_id=org_id,
        resource_type=ResourceType.project,
        ltd_slug=ltd_slug,
        docverse_id=project_id,
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    entries = response.json()
    assert len(entries) == 1
    entry = entries[0]
    assert entry["ltd_slug"] == ltd_slug
    assert entry["self_url"] == str(
        client.base_url.join(
            f"/docverse/orgs/{_ORG}/keeper-sync/projects/{ltd_slug}"
        )
    )
    assert entry["org_url"] == str(
        client.base_url.join(f"/docverse/orgs/{_ORG}")
    )
    assert entry["project_url"] == str(
        client.base_url.join(f"/docverse/orgs/{_ORG}/projects/{ltd_slug}")
    )
    assert entry["sync_refresh_url"] == str(
        client.base_url.join(
            f"/docverse/orgs/{_ORG}/keeper-sync/projects/{ltd_slug}/refresh"
        )
    )
    assert entry["editions_sync_url"] == str(
        client.base_url.join(
            f"/docverse/orgs/{_ORG}/keeper-sync/projects/{ltd_slug}/editions"
        )
    )
    assert {tier["tier"] for tier in entry["tier_status"]} == {
        "main",
        "discovery",
        "other",
    }
    # Auto-created __main edition is reflected even without an edition
    # state row.
    main_edition = entry["main_edition"]
    assert main_edition is not None
    assert main_edition["slug"] == "__main"
    assert main_edition["kind"] == "main"
    # Edition-diff is never populated on the list endpoint.
    assert entry.get("edition_diff") is None


@pytest.mark.asyncio
async def test_list_projects_self_url_resolves_to_per_project_get(
    client: AsyncClient,
) -> None:
    """The ``self_url`` of each entry is a working per-project status URL.

    Acceptance criterion: ``self_url`` matches ``GET /orgs/{org}/
    keeper-sync/projects/{ltd_slug}``. The strongest way to lock that
    is to follow the URL and confirm the per-project endpoint answers
    with the same ``ltd_slug``.
    """
    await _setup_org(client)
    await _enable_sync(client)
    ltd_slug = "pipelines"
    await _seed_state(
        org_id=await _get_org_id(),
        resource_type=ResourceType.project,
        ltd_slug=ltd_slug,
    )

    listing = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert listing.status_code == 200
    self_url = listing.json()[0]["self_url"]

    detail = await client.get(
        self_url, headers={"X-Auth-Request-User": _ADMIN}
    )
    assert detail.status_code == 200
    assert detail.json()["ltd_slug"] == ltd_slug


@pytest.mark.asyncio
async def test_list_projects_project_url_null_when_no_docverse_project(
    client: AsyncClient,
) -> None:
    """``project_url`` is null when the state row has no ``docverse_id``.

    Mirrors the per-project GET's null-handling; a state row may exist
    for an allowlisted slug discovered by tier_main before the project
    sync has actually imported it.
    """
    await _setup_org(client)
    await _enable_sync(client)
    await _seed_state(
        org_id=await _get_org_id(),
        resource_type=ResourceType.project,
        ltd_slug="pipelines",
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    entry = response.json()[0]
    assert entry["project_url"] is None
    assert entry["main_edition"] is None


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_projects_paginates_with_cursor(
    client: AsyncClient,
) -> None:
    """``limit`` + ``cursor`` paginate through the project listing."""
    await _setup_org(client)
    await _enable_sync(client)
    org_id = await _get_org_id()
    for slug in ("alpha", "beta", "gamma", "delta"):
        await _seed_state(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug=slug,
        )

    first = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects?limit=2",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert first.status_code == 200
    page_one = first.json()
    assert len(page_one) == 2
    assert first.headers["X-Total-Count"] == "4"
    links = PaginationLinkData.from_header(first.headers.get("link"))
    assert links.next_url is not None

    second = await client.get(
        links.next_url, headers={"X-Auth-Request-User": _ADMIN}
    )
    assert second.status_code == 200
    page_two = second.json()
    assert len(page_two) == 2

    page_one_slugs = {e["ltd_slug"] for e in page_one}
    page_two_slugs = {e["ltd_slug"] for e in page_two}
    assert page_one_slugs.isdisjoint(page_two_slugs)
    assert page_one_slugs | page_two_slugs == {
        "alpha",
        "beta",
        "gamma",
        "delta",
    }


# ---------------------------------------------------------------------------
# N+1 prevention — query count independent of page size
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_projects_query_count_independent_of_page_size(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """DB query count over a page is independent of page size.

    Acceptance criterion: counts SQL emissions and asserts a constant
    against the same backend listing over two different page sizes.
    The store / service should batch project + main-edition lookups so
    extending the page does not add round-trips.
    """
    await _setup_org(client)
    await _enable_sync(client)
    org_id = await _get_org_id()
    # Six fully imported projects with main editions auto-created. The
    # auto-creation path runs on POST /projects so each project gets a
    # ``__main`` edition row without further plumbing.
    project_slugs = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"]
    for slug in project_slugs:
        project_id = await _create_project(client, slug=slug)
        await _seed_state(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug=slug,
            docverse_id=project_id,
        )

    bind = db_session.bind
    assert bind is not None
    sync_engine = bind.sync_engine
    tally: list[int] = [0]

    def _bump(_a: Any, _b: Any, _c: Any, _d: Any, _e: Any, _f: Any) -> None:
        tally[0] += 1

    counts: dict[int, int] = {}
    for limit in (2, 6):
        tally[0] = 0
        event.listen(sync_engine, "before_cursor_execute", _bump)
        try:
            response = await client.get(
                f"/docverse/orgs/{_ORG}/keeper-sync/projects?limit={limit}",
                headers={"X-Auth-Request-User": _ADMIN},
            )
        finally:
            event.remove(sync_engine, "before_cursor_execute", _bump)
        assert response.status_code == 200
        assert len(response.json()) == limit
        counts[limit] = tally[0]

    # Both page sizes should emit the same number of queries. If the
    # main-edition or project lookup were per-row, this would break with
    # a 4-query delta between limit=2 and limit=6.
    assert counts[2] == counts[6], counts
