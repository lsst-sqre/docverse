"""Tests for project endpoints."""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import pytest
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy import select, update

from docverse.dbschema.organization import SqlOrganization
from docverse.dbschema.project import SqlProject
from docverse.dependencies.context import context_dependency
from docverse.factory import Factory
from docverse.metrics import LifecycleAction
from docverse.storage.editionpublisher import (
    EditionPublisher,
    MockEditionPublisher,
)
from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from tests.conftest import seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    """Create an org and seed an admin membership."""
    await seed_org_with_admin(client, "proj-org", "testuser")


@pytest.mark.asyncio
async def test_create_project(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "my-docs",
            "title": "My Docs",
            "source_url": "https://example.com/example/docs",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["slug"] == "my-docs"
    assert data["title"] == "My Docs"
    assert "id" not in data
    assert data["self_url"].endswith("/orgs/proj-org/projects/my-docs")


@pytest.mark.asyncio
async def test_create_project_with_lifecycle_rules(
    client: AsyncClient,
) -> None:
    """POST persists typed lifecycle_rules; GET round-trips them."""
    await _setup(client)
    rules = [
        {"type": "draft_inactivity", "max_days_inactive": 14},
        {"type": "ref_deleted"},
    ]
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "lifecycle-create",
            "title": "Lifecycle Create",
            "source_url": "https://example.com/example/lifecycle-create",
            "lifecycle_rules": rules,
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    assert response.json()["lifecycle_rules"] == rules

    get_response = await client.get(
        "/docverse/orgs/proj-org/projects/lifecycle-create",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert get_response.status_code == 200
    assert get_response.json()["lifecycle_rules"] == rules


@pytest.mark.asyncio
async def test_list_projects(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "proj-aa",
            "title": "A",
            "source_url": "https://example.com/example/a",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    slugs = [p["slug"] for p in data]
    assert "proj-aa" in slugs
    assert "Link" in response.headers
    assert "X-Total-Count" in response.headers
    proj = next(p for p in data if p["slug"] == "proj-aa")
    assert proj["dashboard_template_url"].endswith(
        "/orgs/proj-org/projects/proj-aa/dashboard-template"
    )
    assert proj["dashboard_template_url"].startswith("http")


@pytest.mark.asyncio
async def test_get_project(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "get-proj",
            "title": "Get Proj",
            "source_url": "https://example.com/example/get",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects/get-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["slug"] == "get-proj"
    assert data["dashboard_template_url"].endswith(
        "/orgs/proj-org/projects/get-proj/dashboard-template"
    )
    assert data["dashboard_template_url"].startswith("http")


@pytest.mark.asyncio
async def test_get_project_not_found(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/proj-org/projects/nonexistent",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_project(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "patch-proj",
            "title": "Original",
            "source_url": "https://example.com/example/patch",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/patch-proj",
        json={"title": "Updated"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["title"] == "Updated"


@pytest.mark.asyncio
async def test_patch_project_lifecycle_rules_valid(
    client: AsyncClient,
) -> None:
    """Valid lifecycle_rules PATCH persists the typed JSONB payload."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "lifecycle-proj",
            "title": "Lifecycle",
            "source_url": "https://example.com/example/lifecycle",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    rules = [
        {"type": "draft_inactivity", "max_days_inactive": 14},
        {
            "type": "build_history_orphan",
            "min_position": 3,
            "min_age_days": 15,
        },
    ]
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/lifecycle-proj",
        json={"lifecycle_rules": rules},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["lifecycle_rules"] == rules

    get_response = await client.get(
        "/docverse/orgs/proj-org/projects/lifecycle-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert get_response.status_code == 200
    assert get_response.json()["lifecycle_rules"] == rules


@pytest.mark.asyncio
async def test_patch_project_lifecycle_rules_unknown_type(
    client: AsyncClient,
) -> None:
    """A 422 is returned when a rule names an unknown discriminator tag."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "bad-lifecycle-proj",
            "title": "Bad",
            "source_url": "https://example.com/example/bad",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/bad-lifecycle-proj",
        json={
            "lifecycle_rules": [
                {"type": "purgatory_eviction", "enabled": True},
            ],
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_patch_project_lifecycle_rules_missing_field(
    client: AsyncClient,
) -> None:
    """A 422 is returned when a known rule omits a required field."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "missing-field-proj",
            "title": "Missing",
            "source_url": "https://example.com/example/missing",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/missing-field-proj",
        json={
            "lifecycle_rules": [
                {"type": "build_history_orphan", "min_position": 5},
            ],
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_patch_project_lifecycle_rules_duplicate_types(
    client: AsyncClient,
) -> None:
    """A 422 is returned when the same rule type appears twice."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "dup-lifecycle-proj",
            "title": "Dup",
            "source_url": "https://example.com/example/dup",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/dup-lifecycle-proj",
        json={
            "lifecycle_rules": [
                {"type": "ref_deleted"},
                {"type": "ref_deleted"},
            ],
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_delete_project(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "del-proj",
            "title": "Delete Me",
            "source_url": "https://example.com/example/del",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.delete(
        "/docverse/orgs/proj-org/projects/del-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204

    # Should not be found after soft delete
    response = await client.get(
        "/docverse/orgs/proj-org/projects/del-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_project_writes_manual_delete_tombstone(
    client: AsyncClient,
) -> None:
    """DELETE handler stamps a ``manual_delete`` tombstone on the row."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "tomb-proj",
            "title": "Tomb Project",
            "source_url": "https://example.com/example/tomb",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )

    logger = structlog.get_logger("test")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            project_store = ProjectStore(session=session, logger=logger)
            org = await org_store.get_by_slug("proj-org")
            assert org is not None
            project = await project_store.get_by_slug(
                org_id=org.id, slug="tomb-proj"
            )
            assert project is not None
            state_store = KeeperSyncStateStore(session=session, logger=logger)
            await state_store.upsert(
                org_id=org.id,
                resource_type=ResourceType.project,
                ltd_slug="tomb-proj",
                docverse_id=project.id,
            )
            await session.commit()

    response = await client.delete(
        "/docverse/orgs/proj-org/projects/tomb-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204

    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug("proj-org")
            assert org is not None
            state_store = KeeperSyncStateStore(session=session, logger=logger)
            state = await state_store.get(
                org_id=org.id,
                resource_type=ResourceType.project,
                ltd_slug="tomb-proj",
                include_tombstoned=True,
            )
    assert state is not None
    assert state.date_tombstoned is not None
    assert state.tombstone_reason == "manual_delete"


async def _enable_cdn(org_slug: str) -> None:
    """Set ``cdn_service_label`` on the seeded org so unpublish runs."""
    async for session in db_session_dependency():
        async with session.begin():
            await session.execute(
                update(SqlOrganization)
                .where(SqlOrganization.slug == org_slug)
                .values(cdn_service_label="cdn-prod")
            )
            await session.commit()


@pytest.mark.asyncio
async def test_delete_project_unpublishes_each_edition(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DELETE project unpublishes the CDN pointer of every edition.

    Seeds an org with a ``cdn_service_label`` and a project with two
    extra editions on top of the auto-created ``__main`` edition. After
    the project is soft-deleted, asserts the publisher recorded one
    ``unpublish`` call per edition (``__main`` plus the two created),
    each keyed on the deleted project's slug.
    """
    await _setup(client)
    await _enable_cdn("proj-org")
    headers = {"X-Auth-Request-User": "testuser"}
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "cdn-del-proj",
            "title": "CDN Delete Project",
            "source_url": "https://example.com/example/cdn-del",
        },
        headers=headers,
    )
    for slug in ("draft-a", "draft-b"):
        await client.post(
            "/docverse/orgs/proj-org/projects/cdn-del-proj/editions",
            json={
                "slug": slug,
                "title": slug,
                "kind": "draft",
                "tracking_mode": "git_ref",
            },
            headers=headers,
        )

    mock_publisher = MockEditionPublisher()

    async def _create(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
    ) -> EditionPublisher:
        _ = (self, org_id, service_label)
        return mock_publisher

    monkeypatch.setattr(Factory, "create_edition_publisher_for_org", _create)

    response = await client.delete(
        "/docverse/orgs/proj-org/projects/cdn-del-proj",
        headers=headers,
    )
    assert response.status_code == 204

    unpublished = {
        (call.project_slug, call.edition_slug)
        for call in mock_publisher.unpublish_calls
    }
    assert unpublished == {
        ("cdn-del-proj", "__main"),
        ("cdn-del-proj", "draft-a"),
        ("cdn-del-proj", "draft-b"),
    }


@pytest.mark.asyncio
async def test_delete_project_no_cdn_is_noop(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DELETE project on a no-CDN org does not invoke the publisher.

    The seeded org has ``cdn_service_label=NULL``, so
    ``EditionPublishingService.unpublish`` short-circuits before
    resolving a publisher. The factory's publisher resolver is patched
    to raise to prove no resolution attempt happens.
    """
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "no-cdn-proj",
            "title": "No CDN Project",
            "source_url": "https://example.com/example/no-cdn",
        },
        headers=headers,
    )

    async def _boom(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
    ) -> EditionPublisher:
        _ = (self, org_id, service_label)
        msg = "Publisher must not be resolved when cdn_service_label is NULL"
        raise AssertionError(msg)

    monkeypatch.setattr(Factory, "create_edition_publisher_for_org", _boom)

    response = await client.delete(
        "/docverse/orgs/proj-org/projects/no-cdn-proj",
        headers=headers,
    )
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_delete_project_idempotent_re_run(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-deleting a project is a 404; the publisher is not invoked again.

    First delete unpublishes the editions; a second DELETE finds nothing
    to delete (project is already soft-deleted) and returns 404 without
    queuing extra unpublish calls.
    """
    await _setup(client)
    await _enable_cdn("proj-org")
    headers = {"X-Auth-Request-User": "testuser"}
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "redel-proj",
            "title": "Re-Delete Project",
            "source_url": "https://example.com/example/redel",
        },
        headers=headers,
    )

    mock_publisher = MockEditionPublisher()

    async def _create(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
    ) -> EditionPublisher:
        _ = (self, org_id, service_label)
        return mock_publisher

    monkeypatch.setattr(Factory, "create_edition_publisher_for_org", _create)

    response = await client.delete(
        "/docverse/orgs/proj-org/projects/redel-proj",
        headers=headers,
    )
    assert response.status_code == 204
    first_pass = len(mock_publisher.unpublish_calls)
    assert first_pass == 1  # __main only

    response = await client.delete(
        "/docverse/orgs/proj-org/projects/redel-proj",
        headers=headers,
    )
    assert response.status_code == 404
    assert len(mock_publisher.unpublish_calls) == first_pass


@pytest.mark.asyncio
async def test_search_by_slug(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    for slug, title in [
        ("pipelines-guide", "Pipelines Guide"),
        ("pipeline-tutorial", "Pipeline Tutorial"),
        ("admin-manual", "Admin Manual"),
    ]:
        await client.post(
            "/docverse/orgs/proj-org/projects",
            json={
                "slug": slug,
                "title": title,
                "source_url": f"https://example.com/example/{slug}",
            },
            headers=headers,
        )
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "pipeline"},
        headers=headers,
    )
    assert response.status_code == 200
    data = response.json()
    slugs = [p["slug"] for p in data]
    assert "pipelines-guide" in slugs
    assert "pipeline-tutorial" in slugs
    assert "admin-manual" not in slugs
    assert int(response.headers["X-Total-Count"]) == len(data)
    assert 'rel="next"' not in response.headers.get("Link", "")


@pytest.mark.asyncio
async def test_search_by_title(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    for slug, title in [
        ("proj-a", "Deployment Guide"),
        ("proj-b", "Developer Handbook"),
        ("proj-c", "API Reference"),
    ]:
        await client.post(
            "/docverse/orgs/proj-org/projects",
            json={
                "slug": slug,
                "title": title,
                "source_url": f"https://example.com/example/{slug}",
            },
            headers=headers,
        )
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "guide"},
        headers=headers,
    )
    assert response.status_code == 200
    data = response.json()
    slugs = [p["slug"] for p in data]
    assert "proj-a" in slugs
    assert "proj-c" not in slugs


@pytest.mark.asyncio
async def test_search_no_results(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "zzzznonexistent"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers["X-Total-Count"] == "0"


@pytest.mark.asyncio
async def test_search_pagination(client: AsyncClient) -> None:
    """Search results can be paginated via cursor."""
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    # Create 4 projects that all match "pipeline" to exceed a limit of 2
    for i in range(4):
        await client.post(
            "/docverse/orgs/proj-org/projects",
            json={
                "slug": f"pipeline-{i}",
                "title": f"Pipeline Project {i}",
                "source_url": f"https://example.com/example/pipeline-{i}",
            },
            headers=headers,
        )

    # First page
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "pipeline", "limit": 2},
        headers=headers,
    )
    assert response.status_code == 200
    first_page = response.json()
    assert len(first_page) == 2
    total = int(response.headers["X-Total-Count"])
    assert total == 4
    assert "Link" in response.headers
    link_header = response.headers["Link"]
    assert 'rel="next"' in link_header

    # Extract next cursor from Link header
    next_cursor = None
    for link_part in link_header.split(","):
        stripped = link_part.strip()
        if 'rel="next"' in stripped:
            url_part = stripped.split(";")[0].strip().strip("<>")
            parsed = urlparse(url_part)
            qs = parse_qs(parsed.query)
            next_cursor = qs["cursor"][0]
            break
    assert next_cursor is not None

    # Second page
    response2 = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "pipeline", "limit": 2, "cursor": next_cursor},
        headers=headers,
    )
    assert response2.status_code == 200
    second_page = response2.json()
    assert len(second_page) == 2
    assert int(response2.headers["X-Total-Count"]) == total

    # No duplicates across pages
    first_slugs = {p["slug"] for p in first_page}
    second_slugs = {p["slug"] for p in second_page}
    assert first_slugs.isdisjoint(second_slugs)


@pytest.mark.asyncio
async def test_search_org_scoping(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    # Create a project in proj-org
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "scoped-proj",
            "title": "Scoped Project",
            "source_url": "https://example.com/example/scoped",
        },
        headers=headers,
    )
    # Create a second org with a similarly-named project
    await seed_org_with_admin(client, "other-org", "testuser")
    await client.post(
        "/docverse/orgs/other-org/projects",
        json={
            "slug": "scoped-proj",
            "title": "Scoped Project Other",
            "source_url": "https://example.com/example/scoped-other",
        },
        headers=headers,
    )
    # Search in proj-org should not return other-org projects
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "scoped"},
        headers=headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["slug"] == "scoped-proj"
    assert data[0]["self_url"].endswith("/orgs/proj-org/projects/scoped-proj")


@pytest.mark.asyncio
async def test_search_excludes_soft_deleted(client: AsyncClient) -> None:
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "deleted-proj",
            "title": "Deleted Project",
            "source_url": "https://example.com/example/deleted",
        },
        headers=headers,
    )
    await client.delete(
        "/docverse/orgs/proj-org/projects/deleted-proj",
        headers=headers,
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "deleted"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_create_project_duplicate_slug(client: AsyncClient) -> None:
    await _setup(client)
    payload = {
        "slug": "dup-proj",
        "title": "First",
        "source_url": "https://example.com/example/dup",
    }
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json=payload,
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json=payload,
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_create_project_has_default_edition(
    client: AsyncClient,
) -> None:
    """POST project creates a __main edition with default tracking."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "default-ed",
            "title": "Default Ed",
            "source_url": "https://example.com/example/default-ed",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    edition = data["default_edition"]
    assert edition is not None
    assert edition["slug"] == "__main"
    assert edition["kind"] == "main"
    assert edition["tracking_mode"] == "git_ref"
    assert edition["tracking_params"] == {"git_ref": "main"}
    assert edition["title"] == "Main"
    assert edition["lifecycle_exempt"] is True
    assert edition["self_url"].endswith(
        "/orgs/proj-org/projects/default-ed/editions/__main"
    )
    # default_edition is the __main edition so its published_url is the
    # project publishing root (no v/{slug}/ suffix).
    assert edition["published_url"] == (
        "https://default-ed.proj-org.example.com/"
    )


@pytest.mark.asyncio
async def test_create_project_custom_default_edition(
    client: AsyncClient,
) -> None:
    """POST project with custom default_edition config."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "custom-ed",
            "title": "Custom Ed",
            "source_url": "https://example.com/example/custom-ed",
            "default_edition": {
                "tracking_mode": "lsst_doc",
                "title": "Custom Main",
                "lifecycle_exempt": False,
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    edition = response.json()["default_edition"]
    assert edition["tracking_mode"] == "lsst_doc"
    assert edition["title"] == "Custom Main"
    assert edition["lifecycle_exempt"] is False


@pytest.mark.asyncio
async def test_get_project_includes_default_edition(
    client: AsyncClient,
) -> None:
    """GET single project includes the default edition."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "get-ed-proj",
            "title": "Get Ed Proj",
            "source_url": "https://example.com/example/get-ed",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects/get-ed-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    edition = response.json()["default_edition"]
    assert edition is not None
    assert edition["slug"] == "__main"
    assert edition["published_url"] == (
        "https://get-ed-proj.proj-org.example.com/"
    )


@pytest.mark.asyncio
async def test_list_projects_no_default_edition(
    client: AsyncClient,
) -> None:
    """GET project list omits default_edition (None)."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "list-ed-proj",
            "title": "List Ed Proj",
            "source_url": "https://example.com/example/list-ed",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    for project in response.json():
        assert project["default_edition"] is None


@pytest.mark.asyncio
async def test_patch_project_includes_default_edition(
    client: AsyncClient,
) -> None:
    """PATCH project response includes the default edition."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "patch-ed-proj",
            "title": "Patch Ed Proj",
            "source_url": "https://example.com/example/patch-ed",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/patch-ed-proj",
        json={"title": "Patched"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    edition = response.json()["default_edition"]
    assert edition is not None
    assert edition["slug"] == "__main"


@pytest.mark.asyncio
async def test_create_project_org_default_edition_config(
    client: AsyncClient,
) -> None:
    """Org-level default_edition_config is used when project omits it."""
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "org-dec",
            "title": "Org With Default Config",
            "base_domain": "example.io",
            "default_edition_config": {
                "tracking_mode": "git_ref",
                "tracking_params": {"git_ref": "develop"},
                "title": "Org Default",
            },
            "members": [
                {
                    "principal": "testuser",
                    "principal_type": "user",
                    "role": "admin",
                }
            ],
        },
        headers={"X-Auth-Request-User": "superadmin"},
    )
    response = await client.post(
        "/docverse/orgs/org-dec/projects",
        json={
            "slug": "org-proj",
            "title": "Org Proj",
            "source_url": "https://example.com/example/org-proj",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    edition = response.json()["default_edition"]
    assert edition["tracking_params"] == {"git_ref": "develop"}
    assert edition["title"] == "Org Default"


@pytest.mark.asyncio
async def test_create_project_request_overrides_org_config(
    client: AsyncClient,
) -> None:
    """Explicit default_edition in request overrides org config."""
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "org-override",
            "title": "Org Override",
            "base_domain": "example.io",
            "default_edition_config": {
                "tracking_mode": "git_ref",
                "tracking_params": {"git_ref": "develop"},
                "title": "Org Default",
            },
            "members": [
                {
                    "principal": "testuser",
                    "principal_type": "user",
                    "role": "admin",
                }
            ],
        },
        headers={"X-Auth-Request-User": "superadmin"},
    )
    response = await client.post(
        "/docverse/orgs/org-override/projects",
        json={
            "slug": "override-proj",
            "title": "Override Proj",
            "source_url": "https://example.com/example/override",
            "default_edition": {
                "tracking_mode": "git_ref",
                "tracking_params": {"git_ref": "master"},
                "title": "Request Override",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    edition = response.json()["default_edition"]
    assert edition["tracking_params"] == {"git_ref": "master"}
    assert edition["title"] == "Request Override"


@pytest.mark.asyncio
async def test_permission_denied_no_auth(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/proj-org/projects",
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_project_with_github_binding_only(
    client: AsyncClient,
) -> None:
    """POST with ``github`` derives the source_url from the binding."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "gh-only",
            "title": "GitHub Only",
            "github": {"owner": "lsst", "repo": "docverse"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["github"] == {
        "owner": "lsst",
        "repo": "docverse",
        "installation_id": None,
        # installation_id is NULL -> derived status is not_installed;
        # the test app leaves the GitHub App feature unconfigured so
        # app_url is absent.
        "installation_status": "not_installed",
        "app_url": None,
    }
    # source_url is derived from the binding, not stored separately.
    assert data["source_url"] == "https://github.com/lsst/docverse"


@pytest.mark.asyncio
async def test_create_project_rejects_github_source_url(
    client: AsyncClient,
) -> None:
    """POST with a github.com ``source_url`` fails with 422 (Rule A).

    The breaking change from PRD #346: a github.com URL must be supplied
    through the structured ``github`` field, not ``source_url``.
    """
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "gh-url",
            "title": "GH URL",
            "source_url": "https://github.com/lsst/gh-url",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_project_rejects_source_url_and_github_together(
    client: AsyncClient,
) -> None:
    """POST with both ``source_url`` and ``github`` fails with 422 (Rule B)."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "both-proj",
            "title": "Both",
            "source_url": "https://gitlab.com/lsst/both",
            "github": {"owner": "lsst", "repo": "both"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_project_non_github_source_url_leaves_github_null(
    client: AsyncClient,
) -> None:
    """POST with a non-GitHub source URL leaves ``github`` NULL."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "gitlab-proj",
            "title": "GitLab",
            "source_url": "https://gitlab.com/lsst/gitlab-proj",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["source_url"] == "https://gitlab.com/lsst/gitlab-proj"
    assert data["github"] is None


@pytest.mark.asyncio
async def test_create_project_without_source_or_github(
    client: AsyncClient,
) -> None:
    """POST without source_url or github creates a project with both NULL."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "bare-proj",
            "title": "Bare",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["source_url"] is None
    assert data["github"] is None


@pytest.mark.asyncio
async def test_patch_project_rejects_source_url_and_github_together(
    client: AsyncClient,
) -> None:
    """PATCH with both source_url and github fails with 422 (Rule B)."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "patch-both",
            "title": "Patch Both",
            "github": {"owner": "lsst", "repo": "patch-both"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/patch-both",
        json={
            "source_url": "https://gitlab.com/lsst/patch-both",
            "github": {"owner": "other", "repo": "two"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_patch_project_flips_github_to_non_github(
    client: AsyncClient,
) -> None:
    """PATCH that clears ``github`` and sets a GitLab source URL succeeds."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "flip-proj",
            "title": "Flip",
            "github": {"owner": "lsst", "repo": "flip-proj"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/flip-proj",
        json={
            "github": None,
            "source_url": "https://gitlab.com/lsst/flip-proj",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["github"] is None
    assert data["source_url"] == "https://gitlab.com/lsst/flip-proj"


@pytest.mark.asyncio
async def test_patch_project_sets_github_binding_drops_source_url(
    client: AsyncClient,
) -> None:
    """PATCH adding ``github`` writes the binding and drops the stored URL.

    The project starts non-GitHub (a GitLab ``source_url``); adding the
    binding nulls that column, so the response source_url is now derived
    from the binding rather than echoing the dropped GitLab URL.
    """
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "add-gh",
            "title": "Add GH",
            "source_url": "https://gitlab.com/lsst/add-gh",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/add-gh",
        json={"github": {"owner": "lsst", "repo": "add-gh"}},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["github"] == {
        "owner": "lsst",
        "repo": "add-gh",
        "installation_id": None,
        "installation_status": "not_installed",
        "app_url": None,
    }
    # The GitLab URL is dropped; source_url is derived from the binding.
    assert data["source_url"] == "https://github.com/lsst/add-gh"


@pytest.mark.asyncio
async def test_patch_project_source_url_null_leaves_github_intact(
    client: AsyncClient,
) -> None:
    """PATCH ``source_url: null`` is a no-op for a GitHub-bound project."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "keep-gh",
            "title": "Keep GH",
            "github": {"owner": "lsst", "repo": "keep-gh"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/keep-gh",
        json={"source_url": None},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["github"] == {
        "owner": "lsst",
        "repo": "keep-gh",
        "installation_id": None,
        "installation_status": "not_installed",
        "app_url": None,
    }
    assert data["source_url"] == "https://github.com/lsst/keep-gh"


@pytest.mark.asyncio
async def test_project_github_installed_status_and_app_url(
    client: AsyncClient,
) -> None:
    """``installation_status`` flips to installed once the id is set.

    A NULL ``github_installation_id`` derives ``not_installed`` (covered
    by the create/patch tests above). Here we persist an installation id
    out-of-band — as the resolve worker or ``installation`` webhook would
    — and stamp the captured GitHub App install-page URL on the shared
    context, then assert both surface on the GET response.
    """
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "gh-installed",
            "title": "GH Installed",
            "github": {"owner": "lsst", "repo": "gh-installed"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )

    # Persist an installation id the way the resolve worker would.
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            result = await session.execute(
                select(SqlProject.id).where(SqlProject.slug == "gh-installed")
            )
            project_id = result.scalar_one()
            store = ProjectStore(session=session, logger=logger)
            updated = await store.update_github_metadata(
                project_id=project_id,
                expected_owner="lsst",
                expected_repo="gh-installed",
                installation_id=42,
                owner_id=111,
                repo_id=222,
            )
            await session.commit()
        assert updated
        break

    # Stand in for the startup ``GET /app`` html_url capture.
    context_dependency.set_github_app_html_url(
        "https://github.com/apps/docverse"
    )

    response = await client.get(
        "/docverse/orgs/proj-org/projects/gh-installed",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    binding = response.json()["github"]
    assert binding["installation_id"] == 42
    assert binding["installation_status"] == "installed"
    assert binding["app_url"] == "https://github.com/apps/docverse"


@pytest.mark.asyncio
async def test_post_project_publishes_project_lifecycle(
    client: AsyncClient,
) -> None:
    """POST project emits one project_lifecycle with action=create."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "lifecycle-create-proj",
            "title": "Lifecycle Create",
            "source_url": "https://example.com/example/lc-create",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201

    events = context_dependency._events
    assert events is not None
    publisher = events.project_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == "proj-org"
    assert event.project == "lifecycle-create-proj"
    assert event.action == LifecycleAction.create


@pytest.mark.asyncio
async def test_patch_project_publishes_project_lifecycle(
    client: AsyncClient,
) -> None:
    """PATCH project emits one project_lifecycle with action=update."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "lifecycle-update-proj",
            "title": "Original",
            "source_url": "https://example.com/example/lc-update",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/lifecycle-update-proj",
        json={"title": "Updated"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    events = context_dependency._events
    assert events is not None
    publisher = events.project_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    update_events = [
        e for e in publisher.published if e.action == LifecycleAction.update
    ]
    assert len(update_events) == 1
    event = update_events[0]
    assert event.organization == "proj-org"
    assert event.project == "lifecycle-update-proj"


@pytest.mark.asyncio
async def test_delete_project_publishes_project_lifecycle(
    client: AsyncClient,
) -> None:
    """DELETE project emits one project_lifecycle with action=delete."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "lifecycle-delete-proj",
            "title": "Delete Me",
            "source_url": "https://example.com/example/lc-delete",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.delete(
        "/docverse/orgs/proj-org/projects/lifecycle-delete-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204

    events = context_dependency._events
    assert events is not None
    publisher = events.project_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    delete_events = [
        e for e in publisher.published if e.action == LifecycleAction.delete
    ]
    assert len(delete_events) == 1
    event = delete_events[0]
    assert event.organization == "proj-org"
    assert event.project == "lifecycle-delete-proj"
