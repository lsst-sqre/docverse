"""End-to-end pagination tests through the HTTP API."""

from __future__ import annotations

import pytest
from httpx import AsyncClient
from safir.database import PaginationLinkData

from tests.conftest import seed_org_with_admin

CONTENT_HASH = (
    "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
)
AUTH = {"X-Auth-Request-User": "testuser"}


async def _setup(client: AsyncClient) -> None:
    """Create org + admin."""
    await seed_org_with_admin(client, "pag-org", "testuser")


async def _create_project(client: AsyncClient, slug: str) -> None:
    resp = await client.post(
        "/docverse/orgs/pag-org/projects",
        json={
            "slug": slug,
            "title": f"Project {slug}",
            "doc_repo": f"https://github.com/example/{slug}",
        },
        headers=AUTH,
    )
    assert resp.status_code == 201


# ---------------------------------------------------------------------------
# Project pagination
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_project_pagination_forward(
    client: AsyncClient,
) -> None:
    """Create 5 projects, paginate with limit=2, collect all."""
    await _setup(client)
    slugs = [f"pag-proj-{c}" for c in "abcde"]
    for slug in slugs:
        await _create_project(client, slug)

    collected: list[str] = []
    url: str | None = "/docverse/orgs/pag-org/projects?limit=2"
    while url:
        resp = await client.get(url, headers=AUTH)
        assert resp.status_code == 200
        data = resp.json()
        collected.extend(p["slug"] for p in data)
        assert "X-Total-Count" in resp.headers
        assert resp.headers["X-Total-Count"] == "5"
        links = PaginationLinkData.from_header(resp.headers.get("link"))
        url = links.next_url

    assert collected == sorted(slugs)


@pytest.mark.asyncio
async def test_project_order_date_created(
    client: AsyncClient,
) -> None:
    """order=date_created returns newest first."""
    await _setup(client)
    for slug in ["ord-a", "ord-b", "ord-c"]:
        await _create_project(client, slug)

    resp = await client.get(
        "/docverse/orgs/pag-org/projects?order=date_created",
        headers=AUTH,
    )
    assert resp.status_code == 200
    data = resp.json()
    slugs = [p["slug"] for p in data]
    # date_created DESC means last created first
    assert slugs == ["ord-c", "ord-b", "ord-a"]


@pytest.mark.asyncio
async def test_project_default_headers(
    client: AsyncClient,
) -> None:
    """Verify Link and X-Total-Count headers are present."""
    await _setup(client)
    await _create_project(client, "hdr-proj")
    resp = await client.get("/docverse/orgs/pag-org/projects", headers=AUTH)
    assert resp.status_code == 200
    assert "Link" in resp.headers
    assert "X-Total-Count" in resp.headers
    assert resp.headers["X-Total-Count"] == "1"


# ---------------------------------------------------------------------------
# Edition pagination + kind filter
# ---------------------------------------------------------------------------


async def _create_edition(
    client: AsyncClient,
    project_slug: str,
    edition_slug: str,
    kind: str = "draft",
) -> None:
    resp = await client.post(
        f"/docverse/orgs/pag-org/projects/{project_slug}/editions",
        json={
            "slug": edition_slug,
            "title": f"Ed {edition_slug}",
            "kind": kind,
            "tracking_mode": "git_ref",
        },
        headers=AUTH,
    )
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_edition_kind_filter(
    client: AsyncClient,
) -> None:
    """Filter editions by kind; X-Total-Count reflects filtered set."""
    await _setup(client)
    await _create_project(client, "ed-filt-proj")
    await _create_edition(client, "ed-filt-proj", "ed-draft-1", "draft")
    await _create_edition(client, "ed-filt-proj", "ed-draft-2", "draft")
    await _create_edition(client, "ed-filt-proj", "ed-main", "main")

    # Unfiltered (3 explicit + 1 auto-created __main)
    resp = await client.get(
        "/docverse/orgs/pag-org/projects/ed-filt-proj/editions",
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.headers["X-Total-Count"] == "4"

    # Filtered to drafts
    resp = await client.get(
        "/docverse/orgs/pag-org/projects/ed-filt-proj/editions?kind=draft",
        headers=AUTH,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert resp.headers["X-Total-Count"] == "2"
    assert all(e["kind"] == "draft" for e in data)


@pytest.mark.asyncio
async def test_edition_pagination_forward(
    client: AsyncClient,
) -> None:
    """Paginate editions with limit=2."""
    await _setup(client)
    await _create_project(client, "ed-pag-proj")
    for slug in ["ed-a", "ed-b", "ed-c", "ed-d"]:
        await _create_edition(client, "ed-pag-proj", slug)

    collected: list[str] = []
    url: str | None = (
        "/docverse/orgs/pag-org/projects/ed-pag-proj/editions?limit=2"
    )
    while url:
        resp = await client.get(url, headers=AUTH)
        assert resp.status_code == 200
        collected.extend(e["slug"] for e in resp.json())
        links = PaginationLinkData.from_header(resp.headers.get("link"))
        url = links.next_url

    assert collected == ["ed-a", "ed-b", "ed-c", "ed-d", "__main"]


# ---------------------------------------------------------------------------
# Build pagination + status filter
# ---------------------------------------------------------------------------


async def _create_build(
    client: AsyncClient,
    project_slug: str,
) -> str:
    """Create a build and return its ID."""
    resp = await client.post(
        f"/docverse/orgs/pag-org/projects/{project_slug}/builds",
        json={"git_ref": "main", "content_hash": CONTENT_HASH},
        headers=AUTH,
    )
    assert resp.status_code == 201
    build_id: str = resp.json()["id"]
    return build_id


@pytest.mark.asyncio
async def test_build_status_filter(
    client: AsyncClient,
) -> None:
    """Filter builds by status."""
    await _setup(client)
    await _create_project(client, "bld-filt-proj")
    await _create_build(client, "bld-filt-proj")
    build_id = await _create_build(client, "bld-filt-proj")

    # Signal upload on second build → transitions to processing
    await client.patch(
        f"/docverse/orgs/pag-org/projects/bld-filt-proj/builds/{build_id}",
        json={"status": "uploaded"},
        headers=AUTH,
    )

    # Filter to pending only
    resp = await client.get(
        "/docverse/orgs/pag-org/projects/bld-filt-proj/builds?status=pending",
        headers=AUTH,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert resp.headers["X-Total-Count"] == "1"
    assert data[0]["status"] == "pending"

    # Filter to processing
    resp = await client.get(
        "/docverse/orgs/pag-org/projects/bld-filt-proj/builds"
        "?status=processing",
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert len(resp.json()) == 1
    assert resp.headers["X-Total-Count"] == "1"


@pytest.mark.asyncio
async def test_build_pagination_forward(
    client: AsyncClient,
) -> None:
    """Paginate builds: first page respects limit, headers present."""
    await _setup(client)
    await _create_project(client, "bld-pag-proj")
    for _ in range(4):
        await _create_build(client, "bld-pag-proj")

    # Verify first page returns exactly limit items and has next link
    resp = await client.get(
        "/docverse/orgs/pag-org/projects/bld-pag-proj/builds?limit=2",
        headers=AUTH,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert resp.headers["X-Total-Count"] == "4"
    links = PaginationLinkData.from_header(resp.headers.get("link"))
    assert links.next_url is not None


# ---------------------------------------------------------------------------
# Validation / error cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_cursor(client: AsyncClient) -> None:
    """Invalid cursor returns 422."""
    await _setup(client)
    resp = await client.get(
        "/docverse/orgs/pag-org/projects?cursor=not_valid_cursor"
        "&order=date_created",
        headers=AUTH,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_limit_too_high(client: AsyncClient) -> None:
    """Limit > 100 is rejected."""
    await _setup(client)
    resp = await client.get(
        "/docverse/orgs/pag-org/projects?limit=101",
        headers=AUTH,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_limit_too_low(client: AsyncClient) -> None:
    """Limit < 1 is rejected."""
    await _setup(client)
    resp = await client.get(
        "/docverse/orgs/pag-org/projects?limit=0",
        headers=AUTH,
    )
    assert resp.status_code == 422
