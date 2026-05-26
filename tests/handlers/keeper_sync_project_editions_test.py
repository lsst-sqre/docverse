"""Tests for ``GET /orgs/{org}/keeper-sync/projects/{ltd_slug}/editions``.

The paginated collection endpoint that complements the project-status
``main_edition`` summary: an operator who has confirmed the ``__main``
edition state via the status endpoint can drill into the project's
full edition list here without paying that cost on every status poll.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

import pytest
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency
from safir.http import PaginationLinkData

from docverse.client.models import (
    EditionCreate,
    EditionKind,
    OrgRole,
    TrackingMode,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"
_ORG = "ks-editions-org"
_LTD_SLUG = "pipelines"
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


async def _create_project(
    client: AsyncClient, *, slug: str = _LTD_SLUG
) -> int:
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
        proj_store = ProjectStore(session=session, logger=_logger())
        org_store = OrganizationStore(session=session, logger=_logger())
        org = await org_store.get_by_slug(_ORG)
        assert org is not None
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


async def _seed_extra_editions(
    *, project_id: int, slugs: list[str]
) -> dict[str, int]:
    """Create extra non-main editions for the project; return slug→id."""
    ids: dict[str, int] = {}
    async for session in db_session_dependency():
        async with session.begin():
            store = EditionStore(session=session, logger=_logger())
            for slug in slugs:
                edition = await store.create(
                    project_id=project_id,
                    data=EditionCreate(
                        slug=slug,
                        title=slug.title(),
                        kind=EditionKind.draft,
                        tracking_mode=TrackingMode.git_ref,
                        tracking_params={"git_ref": "main"},
                    ),
                )
                ids[slug] = edition.id
            await session.commit()
            return ids
    msg = "no session"
    raise AssertionError(msg)


async def _seed_state(
    *,
    org_id: int,
    resource_type: ResourceType,
    ltd_slug: str,
    ltd_id: int | None = None,
    docverse_id: int | None = None,
    date_last_synced: datetime | None = None,
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
                date_last_synced=date_last_synced,
                annotations=annotations,
            )
            await session.commit()
        return
    msg = "no session"
    raise AssertionError(msg)


# ---------------------------------------------------------------------------
# Auth + gating
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_editions_returns_404_when_sync_disabled(
    client: AsyncClient,
) -> None:
    """No PUT to enable sync — endpoint 404s the same way as project-status."""
    await _setup_org(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/editions",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_editions_returns_404_when_slug_not_in_allowlist(
    client: AsyncClient,
) -> None:
    """A slug outside the configured allowlist 404s."""
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/not-allowed/editions",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_editions_403_for_non_admin(client: AsyncClient) -> None:
    """A reader-role user gets 403."""
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/editions",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_editions_403_when_no_auth_header(
    client: AsyncClient,
) -> None:
    """No ``X-Auth-Request-User`` header → 403 from ``require_admin``."""
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/editions",
    )
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# Empty-page behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_editions_empty_when_no_project(
    client: AsyncClient,
) -> None:
    """Allowlisted slug with no Docverse project — 200 + empty page.

    The slug is sync-eligible, so 404 is wrong; the project just has
    no editions yet (the auto-creation path runs the first time
    keeper-sync imports the LTD product). Operators see an empty
    list with ``X-Total-Count: 0``.
    """
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/editions",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers["X-Total-Count"] == "0"


# ---------------------------------------------------------------------------
# Body shape + state-row left-join
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_editions_returns_main_plus_drafts_with_state(
    client: AsyncClient,
) -> None:
    """Editions list includes ``__main`` and any drafts, with state attached.

    Each entry shares the shape of ``KeeperSyncEditionStatus``: an
    ``edition_url`` (HATEOAS), ``slug``, ``kind``, and the LTD-side
    join columns populated from the matching ``keeper_sync_state``
    row when one exists.
    """
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    project_id = await _create_project(client, slug=_LTD_SLUG)
    org_id = await _get_org_id()
    # Trigger the project-state row so editions are discoverable.
    await _seed_state(
        org_id=org_id,
        resource_type=ResourceType.project,
        ltd_slug=_LTD_SLUG,
        docverse_id=project_id,
    )
    edition_ids = await _seed_extra_editions(
        project_id=project_id, slugs=["branch-a", "branch-b"]
    )
    sync_time = datetime(2026, 5, 7, 10, tzinfo=UTC)
    await _seed_state(
        org_id=org_id,
        resource_type=ResourceType.edition,
        ltd_id=99,
        ltd_slug="branch-a",
        docverse_id=edition_ids["branch-a"],
        date_last_synced=sync_time,
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/editions",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    entries = response.json()
    # Order follows the DB's default collation (en_US.UTF-8): letters
    # come before underscore-prefixed tokens, so ``__main`` lands last.
    assert [e["slug"] for e in entries] == [
        "branch-a",
        "branch-b",
        "__main",
    ]
    assert response.headers["X-Total-Count"] == "3"

    branch_a = next(e for e in entries if e["slug"] == "branch-a")
    assert branch_a["kind"] == "draft"
    assert branch_a["ltd_id"] == 99
    assert branch_a["ltd_slug"] == "branch-a"
    assert datetime.fromisoformat(branch_a["date_last_synced"]) == sync_time
    expected_url = str(
        client.base_url.join(
            f"/docverse/orgs/{_ORG}/projects/{_LTD_SLUG}/editions/branch-a"
        )
    )
    assert branch_a["edition_url"] == expected_url

    branch_b = next(e for e in entries if e["slug"] == "branch-b")
    assert branch_b["ltd_id"] is None
    assert branch_b["ltd_slug"] is None


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_editions_paginates_with_cursor(
    client: AsyncClient,
) -> None:
    """``limit`` + ``cursor`` paginate through the project's editions."""
    await _setup_org(client)
    await _enable_sync(client, project_slugs=[_LTD_SLUG])
    project_id = await _create_project(client, slug=_LTD_SLUG)
    org_id = await _get_org_id()
    await _seed_state(
        org_id=org_id,
        resource_type=ResourceType.project,
        ltd_slug=_LTD_SLUG,
        docverse_id=project_id,
    )
    # Three drafts plus auto-created __main = 4 editions total.
    await _seed_extra_editions(
        project_id=project_id, slugs=["alpha", "beta", "gamma"]
    )

    first = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/projects/{_LTD_SLUG}/editions"
        "?limit=2",
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

    page_one_slugs = [e["slug"] for e in page_one]
    page_two_slugs = [e["slug"] for e in page_two]
    assert set(page_one_slugs).isdisjoint(set(page_two_slugs))
    # The combined ordering is slug ASC under the DB's default
    # collation (en_US.UTF-8): letters before underscore-prefixed
    # tokens, so ``__main`` lands at the end.
    assert page_one_slugs + page_two_slugs == [
        "alpha",
        "beta",
        "gamma",
        "__main",
    ]
