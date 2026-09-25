"""Tests for project endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

import pytest
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency
from safir.http import PaginationLinkData
from safir.metrics import MockEventPublisher
from sqlalchemy import select, update

from docverse.models import BuildStatus, EditionKind
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.project import SqlProject
from docverse_server.dependencies.context import context_dependency
from docverse_server.domain.base32id import (
    serialize_base32_id,
    validate_base32_id,
)
from docverse_server.domain.slug import VersionRule, parse_slug_rewrite_rules
from docverse_server.factory import Factory
from docverse_server.metrics import (
    ConditionalGetEndpoint,
    ConditionalGetOutcome,
    ConditionalGetPrecondition,
    LifecycleAction,
)
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.editionpublisher import (
    EditionPublisher,
    MockEditionPublisher,
)
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from tests.conftest import seed_build, seed_org_with_admin


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
    # ``id`` is the project's Base32 public ID, never its integer row id.
    assert isinstance(data["id"], str)
    assert data["self_url"].endswith("/orgs/proj-org/projects/my-docs")
    assert response.headers["Location"] == data["self_url"]


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


@pytest.mark.asyncio
async def test_delete_project_cascades_to_editions_and_builds(
    client: AsyncClient,
) -> None:
    """DELETE project stamps its editions and builds with one timestamp.

    Three builds, one still ``pending``, and three live editions
    (``__main`` plus two drafts). After the DELETE every one of those
    rows carries the project's own ``date_deleted``, the ``pending``
    build reads ``cancelled``, and each edition's ``keeper_sync_state``
    row is tombstoned — so a deleted project's storage ages into
    purgatory instead of being pinned by its own editions.
    """
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "cascade-proj",
            "title": "Cascade Project",
            "source_url": "https://example.com/example/cascade",
        },
        headers=headers,
    )
    for slug in ("draft-a", "draft-b"):
        await client.post(
            "/docverse/orgs/proj-org/projects/cascade-proj/editions",
            json={
                "slug": slug,
                "title": slug,
                "kind": "draft",
                "tracking_mode": "git_ref",
            },
            headers=headers,
        )
    finished_ids = [
        await seed_build("proj-org", "cascade-proj", git_ref=f"v{index}")
        for index in (1, 2)
    ]
    pending_id = await seed_build(
        "proj-org", "cascade-proj", git_ref="pending-ref"
    )

    logger = structlog.get_logger("test")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug("proj-org")
            assert org is not None
            project_store = ProjectStore(session=session, logger=logger)
            project = await project_store.get_by_slug(
                org_id=org.id, slug="cascade-proj"
            )
            assert project is not None
            project_id = project.id
            build_store = BuildStore(session=session, logger=logger)
            state_store = KeeperSyncStateStore(session=session, logger=logger)
            editions = (
                (
                    await session.execute(
                        select(SqlEdition).where(
                            SqlEdition.project_id == project.id
                        )
                    )
                )
                .scalars()
                .all()
            )
            for index, edition in enumerate(editions):
                await state_store.upsert(
                    org_id=org.id,
                    resource_type=ResourceType.edition,
                    ltd_id=7100 + index,
                    ltd_slug=edition.slug,
                    docverse_id=edition.id,
                )
            edition_ids = [edition.id for edition in editions]
            for build_id in finished_ids:
                build = await build_store.get_by_public_id(
                    project_id=project.id,
                    public_id=validate_base32_id(build_id),
                )
                assert build is not None
                await build_store.transition_status(
                    build_id=build.id, new_status=BuildStatus.processing
                )
                await build_store.transition_status(
                    build_id=build.id, new_status=BuildStatus.completed
                )
            await session.commit()

    response = await client.delete(
        "/docverse/orgs/proj-org/projects/cascade-proj",
        headers=headers,
    )
    assert response.status_code == 204

    async for session in db_session_dependency():
        async with session.begin():
            project_deleted = (
                await session.execute(
                    select(SqlProject.date_deleted).where(
                        SqlProject.slug == "cascade-proj"
                    )
                )
            ).scalar_one()
            edition_stamps = (
                (
                    await session.execute(
                        select(SqlEdition.date_deleted).where(
                            SqlEdition.project_id == project_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            builds = (
                (
                    await session.execute(
                        select(SqlBuild).where(
                            SqlBuild.project_id == project_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug("proj-org")
            assert org is not None
            state_store = KeeperSyncStateStore(session=session, logger=logger)
            states = await state_store.list_for_org(
                org_id=org.id,
                resource_type=ResourceType.edition,
                docverse_ids=edition_ids,
                include_tombstoned=True,
            )
            await session.commit()

    assert project_deleted is not None
    assert len(edition_stamps) == 3
    assert set(edition_stamps) == {project_deleted}
    assert len(builds) == 3
    assert {build.date_deleted for build in builds} == {project_deleted}
    by_public_id = {
        serialize_base32_id(build.public_id): build for build in builds
    }
    assert by_public_id[pending_id].status == BuildStatus.cancelled
    for build_id in finished_ids:
        assert by_public_id[build_id].status == BuildStatus.completed
    assert len(states) == 3
    assert all(state.date_tombstoned is not None for state in states)
    assert {state.tombstone_reason for state in states} == {"manual_delete"}


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
async def test_list_projects_includes_default_edition(
    client: AsyncClient,
) -> None:
    """Every listing row embeds the same default edition the GET does.

    Ook polls the listing to learn which projects changed; embedding
    the ``__main`` edition here is what lets it read the new
    ``published_url`` and ``build_url`` without a second request per
    project (task #660).
    """
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    for slug in ("list-ed-a", "list-ed-b"):
        response = await client.post(
            "/docverse/orgs/proj-org/projects",
            json={
                "slug": slug,
                "title": f"List Ed {slug}",
                "source_url": f"https://example.com/example/{slug}",
            },
            headers=headers,
        )
        assert response.status_code == 201

    response = await client.get(
        "/docverse/orgs/proj-org/projects", headers=headers
    )
    assert response.status_code == 200
    rows = {p["slug"]: p for p in response.json()}
    assert set(rows) == {"list-ed-a", "list-ed-b"}
    for slug, row in rows.items():
        single = await client.get(
            f"/docverse/orgs/proj-org/projects/{slug}", headers=headers
        )
        assert single.status_code == 200
        assert row["default_edition"] == single.json()["default_edition"]
        assert row["default_edition"]["slug"] == "__main"


@pytest.mark.asyncio
async def test_search_projects_includes_default_edition(
    client: AsyncClient,
) -> None:
    """The ``q`` search path embeds the default edition too."""
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={"slug": "search-ed", "title": "Search Ed"},
        headers=headers,
    )
    assert response.status_code == 201

    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "search-ed"},
        headers=headers,
    )
    assert response.status_code == 200
    rows = response.json()
    assert [p["slug"] for p in rows] == ["search-ed"]
    single = await client.get(
        "/docverse/orgs/proj-org/projects/search-ed", headers=headers
    )
    assert rows[0]["default_edition"] == single.json()["default_edition"]


@pytest.mark.asyncio
async def test_list_projects_deleted_row_has_no_default_edition(
    client: AsyncClient,
) -> None:
    """A soft-deleted row keeps ``default_edition: null``.

    Deleting a project soft-deletes its editions with it, and the
    single GET with ``include_deleted`` shows no edition for it; the
    listing agrees rather than resurrecting a deleted edition.
    """
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}
    for slug in ("del-ed-live", "del-ed-dead"):
        response = await client.post(
            "/docverse/orgs/proj-org/projects",
            json={"slug": slug, "title": slug},
            headers=headers,
        )
        assert response.status_code == 201
    deleted = await client.delete(
        "/docverse/orgs/proj-org/projects/del-ed-dead", headers=headers
    )
    assert deleted.status_code == 204

    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"include_deleted": "true"},
        headers=headers,
    )
    assert response.status_code == 200
    rows = {p["slug"]: p for p in response.json()}
    assert rows["del-ed-live"]["default_edition"]["slug"] == "__main"
    assert rows["del-ed-dead"]["date_deleted"] is not None
    assert rows["del-ed-dead"]["default_edition"] is None


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
        # Not learned until the resolve worker reads it from GitHub.
        "default_branch": None,
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
        "default_branch": None,
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
        "default_branch": None,
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
async def test_get_project_shows_github_default_branch(
    client: AsyncClient,
) -> None:
    """``github.default_branch`` reports the stored column (PRD #721).

    An existing bound project reads ``null`` until the resolve worker
    learns its default branch, then reports what GitHub said.
    """
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "gh-branch",
            "title": "GH Branch",
            "github": {"owner": "lsst", "repo": "gh-branch"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/proj-org/projects/gh-branch",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["github"]["default_branch"] is None

    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            result = await session.execute(
                select(SqlProject.id).where(SqlProject.slug == "gh-branch")
            )
            project_id = result.scalar_one()
            store = ProjectStore(session=session, logger=logger)
            await store.set_github_default_branch(
                project_id=project_id, value="master"
            )
            await session.commit()
        break

    response = await client.get(
        "/docverse/orgs/proj-org/projects/gh-branch",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["github"]["default_branch"] == "master"


@pytest.mark.asyncio
async def test_create_project_rejects_github_default_branch(
    client: AsyncClient,
) -> None:
    """POST cannot set ``github.default_branch``: GitHub owns it."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "gh-set-branch",
            "title": "GH Set Branch",
            "github": {
                "owner": "lsst",
                "repo": "gh-set-branch",
                "default_branch": "main",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_patch_project_rejects_github_default_branch(
    client: AsyncClient,
) -> None:
    """PATCH cannot set ``github.default_branch``: GitHub owns it."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "gh-patch-branch",
            "title": "GH Patch Branch",
            "github": {"owner": "lsst", "repo": "gh-patch-branch"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/gh-patch-branch",
        json={
            "github": {
                "owner": "lsst",
                "repo": "gh-patch-branch",
                "default_branch": "main",
            },
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


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


@pytest.mark.asyncio
async def test_patch_project_version_slug_rules(
    client: AsyncClient,
) -> None:
    """Version rule types round-trip through project PATCH and GET."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "slugrules-proj",
            "title": "Slug Rules",
            "source_url": "https://gitlab.com/lsst/slugrules-proj",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    rules = [
        {"type": "semver"},
        {"type": "lsst_doc"},
        {"type": "eups_major"},
        {"type": "eups_weekly"},
    ]
    response = await client.patch(
        "/docverse/orgs/proj-org/projects/slugrules-proj",
        json={"slug_rewrite_rules": rules},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["slug_rewrite_rules"] == rules

    get_response = await client.get(
        "/docverse/orgs/proj-org/projects/slugrules-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert get_response.status_code == 200
    stored = get_response.json()["slug_rewrite_rules"]
    assert stored == rules
    parsed = parse_slug_rewrite_rules(stored)
    assert all(isinstance(rule, VersionRule) for rule in parsed)
    assert all(
        rule.edition_kind == EditionKind.release
        for rule in parsed
        if isinstance(rule, VersionRule)
    )


@pytest.mark.asyncio
async def test_patch_project_edition_autocreation(
    client: AsyncClient,
) -> None:
    """``edition_autocreation`` round-trips through project PATCH/GET."""
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "autocreate-proj",
            "title": "Autocreate",
            "source_url": "https://gitlab.com/lsst/autocreate-proj",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )

    response = await client.patch(
        "/docverse/orgs/proj-org/projects/autocreate-proj",
        json={"edition_autocreation": {"semver_aggregates": False}},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["edition_autocreation"] == {
        "semver_aggregates": False
    }

    get_response = await client.get(
        "/docverse/orgs/proj-org/projects/autocreate-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert get_response.status_code == 200
    assert get_response.json()["edition_autocreation"] == {
        "semver_aggregates": False
    }


@pytest.mark.asyncio
async def test_project_responses_carry_base32_public_id(
    client: AsyncClient,
) -> None:
    """Single and listing project responses expose ``id`` as Base32.

    The value is the project's ``public_id``, never its integer row id,
    per the "no database IDs on the wire" convention: it must be a
    12+2-character hyphenated Crockford Base32 string that decodes back
    to a positive integer.
    """
    await _setup(client)
    await client.post(
        "/docverse/orgs/proj-org/projects",
        json={"slug": "pid-proj", "title": "PID Proj"},
        headers={"X-Auth-Request-User": "testuser"},
    )

    response = await client.get(
        "/docverse/orgs/proj-org/projects/pid-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    single = response.json()
    project_id = single["id"]
    assert isinstance(project_id, str)
    assert len(project_id) == 17
    assert project_id.count("-") == 3
    assert validate_base32_id(project_id) > 0
    assert serialize_base32_id(validate_base32_id(project_id)) == project_id

    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    entry = next(p for p in response.json() if p["slug"] == "pid-proj")
    assert entry["id"] == project_id


# ---------------------------------------------------------------------------
# ``updated_since`` filter and ``order=date_updated``
# ---------------------------------------------------------------------------

# Fixed instants for the clock-filter tests, oldest to newest. Stamped
# directly onto the rows because ``projects.date_updated`` is written by
# the transaction clock: projects created through the API in one test
# would otherwise be indistinguishable to a ``>=`` filter.
H_OLD = datetime(2026, 1, 1, tzinfo=UTC)
H_MID = datetime(2026, 2, 1, tzinfo=UTC)
H_NEW = datetime(2026, 3, 1, tzinfo=UTC)


async def _stamp_date_updated(*stamps: tuple[str, datetime]) -> None:
    """Pin ``date_updated`` on each named project to a fixed instant.

    An explicit value in ``.values()`` beats the column's ``onupdate``,
    so this is how a test names a clock the transaction clock would
    otherwise choose for it.
    """
    async for session in db_session_dependency():
        async with session.begin():
            for slug, stamp in stamps:
                await session.execute(
                    update(SqlProject)
                    .where(SqlProject.slug == slug)
                    .values(date_updated=stamp)
                    .execution_options(synchronize_session=False)
                )
            await session.commit()
        break


async def _stamp_org_date_updated(slug: str, stamp: datetime) -> None:
    """Pin an organization's ``date_updated`` to a fixed instant."""
    async for session in db_session_dependency():
        async with session.begin():
            await session.execute(
                update(SqlOrganization)
                .where(SqlOrganization.slug == slug)
                .values(date_updated=stamp)
                .execution_options(synchronize_session=False)
            )
            await session.commit()
        break


async def _stamp_edition_date_updated(
    project_slug: str, edition_slug: str, stamp: datetime
) -> None:
    """Pin one edition's ``date_updated`` to a fixed instant."""
    async for session in db_session_dependency():
        async with session.begin():
            project_id = (
                await session.execute(
                    select(SqlProject.id).where(
                        SqlProject.slug == project_slug
                    )
                )
            ).scalar_one()
            await session.execute(
                update(SqlEdition)
                .where(
                    SqlEdition.project_id == project_id,
                    SqlEdition.slug == edition_slug,
                )
                .values(date_updated=stamp)
                .execution_options(synchronize_session=False)
            )
            await session.commit()
        break


async def _resolve_github_binding(
    *, slug: str, owner: str, repo: str, installation_id: int
) -> None:
    """Land a GitHub binding the way the resolve worker does.

    Stands in for ``worker.functions.project_github_resolve``, which
    calls :meth:`~docverse_server.storage.project_store.ProjectStore
    .update_github_metadata` after its GitHub round-trip.
    """
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            project_id = (
                await session.execute(
                    select(SqlProject.id).where(SqlProject.slug == slug)
                )
            ).scalar_one()
            store = ProjectStore(session=session, logger=logger)
            updated = await store.update_github_metadata(
                project_id=project_id,
                expected_owner=owner,
                expected_repo=repo,
                installation_id=installation_id,
                owner_id=111,
                repo_id=222,
            )
            await session.commit()
        assert updated
        break


async def _seed_clocked_projects(client: AsyncClient) -> None:
    """Create three ``tick-*`` projects with pinned ``date_updated``."""
    headers = {"X-Auth-Request-User": "testuser"}
    for slug in ("tick-old", "tick-mid", "tick-new"):
        response = await client.post(
            "/docverse/orgs/proj-org/projects",
            json={"slug": slug, "title": f"Tick {slug}"},
            headers=headers,
        )
        assert response.status_code == 201

    await _stamp_date_updated(
        ("tick-old", H_OLD), ("tick-mid", H_MID), ("tick-new", H_NEW)
    )


@pytest.mark.asyncio
async def test_list_projects_updated_since(client: AsyncClient) -> None:
    """``updated_since`` returns exactly the projects at or after it."""
    await _setup(client)
    await _seed_clocked_projects(client)

    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"updated_since": H_MID.isoformat()},
        headers={"X-Auth-Request-User": "testuser"},
    )

    assert response.status_code == 200
    assert sorted(p["slug"] for p in response.json()) == [
        "tick-mid",
        "tick-new",
    ]
    assert response.headers["X-Total-Count"] == "2"


@pytest.mark.asyncio
async def test_list_projects_updated_since_with_query(
    client: AsyncClient,
) -> None:
    """The filter also narrows the ``q`` fuzzy-search path."""
    await _setup(client)
    await _seed_clocked_projects(client)

    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "tick", "updated_since": H_NEW.isoformat()},
        headers={"X-Auth-Request-User": "testuser"},
    )

    assert response.status_code == 200
    assert [p["slug"] for p in response.json()] == ["tick-new"]
    assert response.headers["X-Total-Count"] == "1"


@pytest.mark.asyncio
async def test_list_projects_updated_since_after_github_resolve(
    client: AsyncClient,
) -> None:
    """A GitHub-binding write brings a project back into the window.

    Task #644: the resolve worker's ``update_github_metadata`` used to
    pin ``date_updated``, so a project whose ``installation_status``
    had just flipped stayed invisible to a poller's ``updated_since``
    pass. The clock now advances, so the next poll picks it up.
    """
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "tick-gh",
            "title": "Tick GH",
            "github": {"owner": "lsst", "repo": "tick-gh"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    await _stamp_date_updated(("tick-gh", H_OLD))
    params = {"updated_since": H_MID.isoformat()}
    headers = {"X-Auth-Request-User": "testuser"}

    before = await client.get(
        "/docverse/orgs/proj-org/projects", params=params, headers=headers
    )
    assert before.status_code == 200
    assert [p["slug"] for p in before.json()] == []

    await _resolve_github_binding(
        slug="tick-gh", owner="lsst", repo="tick-gh", installation_id=7
    )

    after = await client.get(
        "/docverse/orgs/proj-org/projects", params=params, headers=headers
    )

    assert after.status_code == 200
    assert [p["slug"] for p in after.json()] == ["tick-gh"]
    assert after.headers["X-Total-Count"] == "1"


@pytest.mark.asyncio
async def test_list_projects_updated_since_rejects_naive(
    client: AsyncClient,
) -> None:
    """A naive timestamp is a 422: the instant it names is ambiguous."""
    await _setup(client)

    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"updated_since": "2026-02-01T00:00:00"},
        headers={"X-Auth-Request-User": "testuser"},
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_list_projects_order_date_updated_paginates(
    client: AsyncClient,
) -> None:
    """``order=date_updated`` is newest-first and pages both ways."""
    await _setup(client)
    await _seed_clocked_projects(client)
    headers = {"X-Auth-Request-User": "testuser"}

    first = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"order": "date_updated", "limit": 2},
        headers=headers,
    )
    assert first.status_code == 200
    assert [p["slug"] for p in first.json()] == ["tick-new", "tick-mid"]

    next_url = PaginationLinkData.from_header(
        first.headers.get("link")
    ).next_url
    assert next_url is not None
    second = await client.get(next_url, headers=headers)
    assert second.status_code == 200
    assert [p["slug"] for p in second.json()] == ["tick-old"]

    prev_url = PaginationLinkData.from_header(
        second.headers.get("link")
    ).prev_url
    assert prev_url is not None
    back = await client.get(prev_url, headers=headers)
    assert back.status_code == 200
    assert [p["slug"] for p in back.json()] == ["tick-new", "tick-mid"]


# ---------------------------------------------------------------------------
# ``include_deleted`` and the ``date_deleted`` response field
# ---------------------------------------------------------------------------


async def _seed_live_and_deleted(client: AsyncClient) -> None:
    """Create ``sunk-live`` and soft-delete ``sunk-dead``."""
    headers = {"X-Auth-Request-User": "testuser"}
    for slug in ("sunk-live", "sunk-dead"):
        response = await client.post(
            "/docverse/orgs/proj-org/projects",
            json={"slug": slug, "title": f"Sunk {slug}"},
            headers=headers,
        )
        assert response.status_code == 201
    response = await client.delete(
        "/docverse/orgs/proj-org/projects/sunk-dead",
        headers=headers,
    )
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_live_project_carries_null_date_deleted(
    client: AsyncClient,
) -> None:
    """``date_deleted`` is present and null on a live project."""
    await _setup(client)
    await _seed_live_and_deleted(client)
    headers = {"X-Auth-Request-User": "testuser"}

    single = await client.get(
        "/docverse/orgs/proj-org/projects/sunk-live", headers=headers
    )
    listing = await client.get(
        "/docverse/orgs/proj-org/projects", headers=headers
    )

    assert single.status_code == 200
    assert "date_deleted" in single.json()
    assert single.json()["date_deleted"] is None
    assert listing.status_code == 200
    entry = next(p for p in listing.json() if p["slug"] == "sunk-live")
    assert entry["date_deleted"] is None


@pytest.mark.asyncio
async def test_get_project_include_deleted(client: AsyncClient) -> None:
    """The single GET returns a deleted project only behind the flag."""
    await _setup(client)
    await _seed_live_and_deleted(client)
    headers = {"X-Auth-Request-User": "testuser"}

    without_flag = await client.get(
        "/docverse/orgs/proj-org/projects/sunk-dead", headers=headers
    )
    with_flag = await client.get(
        "/docverse/orgs/proj-org/projects/sunk-dead",
        params={"include_deleted": "true"},
        headers=headers,
    )

    assert without_flag.status_code == 404
    assert with_flag.status_code == 200
    assert with_flag.json()["slug"] == "sunk-dead"
    assert with_flag.json()["date_deleted"] is not None


@pytest.mark.asyncio
async def test_write_endpoints_ignore_include_deleted(
    client: AsyncClient,
) -> None:
    """PATCH and DELETE still 404 on a deleted project, flag or not.

    ``include_deleted`` is a read-only affordance: a deleted project is
    something a consumer may still want to *see*, never something an
    operator may keep editing.
    """
    await _setup(client)
    await _seed_live_and_deleted(client)
    headers = {"X-Auth-Request-User": "testuser"}
    params = {"include_deleted": "true"}

    patched = await client.patch(
        "/docverse/orgs/proj-org/projects/sunk-dead",
        params=params,
        json={"title": "Risen"},
        headers=headers,
    )
    deleted = await client.delete(
        "/docverse/orgs/proj-org/projects/sunk-dead",
        params=params,
        headers=headers,
    )

    assert patched.status_code == 404
    assert deleted.status_code == 404


@pytest.mark.asyncio
async def test_list_projects_include_deleted(client: AsyncClient) -> None:
    """The listing shows and counts deleted projects behind the flag."""
    await _setup(client)
    await _seed_live_and_deleted(client)
    headers = {"X-Auth-Request-User": "testuser"}

    without_flag = await client.get(
        "/docverse/orgs/proj-org/projects", headers=headers
    )
    with_flag = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"include_deleted": "true"},
        headers=headers,
    )

    assert without_flag.status_code == 200
    assert "sunk-dead" not in [p["slug"] for p in without_flag.json()]
    assert without_flag.headers["X-Total-Count"] == "1"
    assert with_flag.status_code == 200
    entry = next(p for p in with_flag.json() if p["slug"] == "sunk-dead")
    assert entry["date_deleted"] is not None
    assert with_flag.headers["X-Total-Count"] == "2"


@pytest.mark.asyncio
async def test_list_projects_include_deleted_with_query(
    client: AsyncClient,
) -> None:
    """The ``q`` search path honours the flag the same way."""
    await _setup(client)
    await _seed_live_and_deleted(client)
    headers = {"X-Auth-Request-User": "testuser"}

    without_flag = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "sunk"},
        headers=headers,
    )
    with_flag = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"q": "sunk", "include_deleted": "true"},
        headers=headers,
    )

    assert [p["slug"] for p in without_flag.json()] == ["sunk-live"]
    assert without_flag.headers["X-Total-Count"] == "1"
    assert sorted(p["slug"] for p in with_flag.json()) == [
        "sunk-dead",
        "sunk-live",
    ]
    assert with_flag.headers["X-Total-Count"] == "2"


# ---------------------------------------------------------------------------
# Conditional GET on the project listing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_projects_sends_validators(client: AsyncClient) -> None:
    """The listing carries a weak ``ETag`` and no date validator."""
    await _setup(client)
    await _seed_clocked_projects(client)

    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={"X-Auth-Request-User": "testuser"},
    )

    assert response.status_code == 200
    assert response.headers["ETag"].startswith('W/"')
    assert "Last-Modified" not in response.headers


@pytest.mark.asyncio
async def test_list_projects_if_none_match_is_empty_304(
    client: AsyncClient,
) -> None:
    """Echoing the tag back earns a bodyless 304 repeating the tag."""
    await _setup(client)
    await _seed_clocked_projects(client)
    headers = {"X-Auth-Request-User": "testuser"}

    first = await client.get(
        "/docverse/orgs/proj-org/projects", headers=headers
    )
    assert first.status_code == 200

    second = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )

    assert second.status_code == 304
    assert second.content == b""
    assert second.headers["ETag"] == first.headers["ETag"]
    assert "Last-Modified" not in second.headers


@pytest.mark.asyncio
async def test_list_projects_etag_changes_after_a_project_changes(
    client: AsyncClient,
) -> None:
    """A moved watermark retires the tag the caller was holding."""
    await _setup(client)
    await _seed_clocked_projects(client)
    headers = {"X-Auth-Request-User": "testuser"}

    first = await client.get(
        "/docverse/orgs/proj-org/projects", headers=headers
    )
    assert first.status_code == 200

    patched = await client.patch(
        "/docverse/orgs/proj-org/projects/tick-old",
        json={"title": "Retitled"},
        headers=headers,
    )
    assert patched.status_code == 200

    second = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )

    assert second.status_code == 200
    assert second.headers["ETag"] != first.headers["ETag"]


@pytest.mark.asyncio
async def test_list_projects_etag_changes_after_a_clock_below_the_max(
    client: AsyncClient,
) -> None:
    """A late commit under the maximum retires the caller's tag.

    ``date_updated`` is stamped with the transaction's *start* clock
    and commit order is not start order, so a slow writer can land a
    clock below the maximum a poller already holds. A tag built on that
    maximum would not move, and the poller would keep being told 304
    about a change it has never seen; the sum of every row's clock
    moves whichever direction the write landed in.
    """
    await _setup(client)
    await _seed_clocked_projects(client)
    headers = {"X-Auth-Request-User": "testuser"}

    first = await client.get(
        "/docverse/orgs/proj-org/projects", headers=headers
    )
    assert first.status_code == 200

    # tick-old moves within January, staying under tick-new's March.
    await _stamp_date_updated(("tick-old", datetime(2026, 1, 15, tzinfo=UTC)))

    second = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )

    assert second.status_code == 200
    assert second.headers["ETag"] != first.headers["ETag"]


@pytest.mark.asyncio
async def test_list_projects_etag_survives_an_edition_config_patch(
    client: AsyncClient,
) -> None:
    """An edition-configuration edit does not retire the listing tag.

    The listing embeds each project's default edition, but its
    watermark is over project clocks alone, and an edition PATCH that
    only touches configuration leaves the project clock where it was.
    The tag is weak precisely so it can stand for a body that differs
    in ways a poller does not track; a consumer that needs the
    edition's configuration reads the single project, whose tag hashes
    the edition clock separately (task #660).
    """
    await _setup(client)
    await _seed_clocked_projects(client)
    headers = {"X-Auth-Request-User": "testuser"}

    first = await client.get(
        "/docverse/orgs/proj-org/projects", headers=headers
    )
    assert first.status_code == 200

    patched = await client.patch(
        "/docverse/orgs/proj-org/projects/tick-old/editions/__main",
        json={"title": "Retitled edition"},
        headers=headers,
    )
    assert patched.status_code == 200

    second = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )
    assert second.status_code == 304

    unconditional = await client.get(
        "/docverse/orgs/proj-org/projects", headers=headers
    )
    assert unconditional.headers["ETag"] == first.headers["ETag"]
    rows = {p["slug"]: p for p in unconditional.json()}
    assert rows["tick-old"]["default_edition"]["title"] == "Retitled edition"


@pytest.mark.asyncio
async def test_list_projects_pages_have_distinct_etags(
    client: AsyncClient,
) -> None:
    """Each page of one listing validates as its own representation."""
    await _setup(client)
    await _seed_clocked_projects(client)
    headers = {"X-Auth-Request-User": "testuser"}

    first = await client.get(
        "/docverse/orgs/proj-org/projects",
        params={"limit": 1},
        headers=headers,
    )
    assert first.status_code == 200
    next_url = PaginationLinkData.from_header(first.headers["Link"]).next_url
    assert next_url is not None

    second = await client.get(next_url, headers=headers)

    assert second.status_code == 200
    assert second.headers["ETag"] != first.headers["ETag"]


@pytest.mark.asyncio
async def test_list_projects_publishes_conditional_get_event(
    client: AsyncClient,
) -> None:
    """One event per precondition-bearing request, and none otherwise."""
    await _setup(client)
    await _seed_clocked_projects(client)
    headers = {"X-Auth-Request-User": "testuser"}

    events = context_dependency._events
    assert events is not None
    publisher = events.conditional_get
    assert isinstance(publisher, MockEventPublisher)

    first = await client.get(
        "/docverse/orgs/proj-org/projects", headers=headers
    )
    assert first.status_code == 200
    # An unconditional request is not conditional traffic, so it is not
    # counted as a cache miss either.
    assert publisher.published == []

    second = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )
    assert second.status_code == 304

    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == "proj-org"
    assert event.project is None
    assert event.endpoint == ConditionalGetEndpoint.projects_list
    assert event.outcome == ConditionalGetOutcome.not_modified
    assert event.precondition == ConditionalGetPrecondition.etag


# ---------------------------------------------------------------------------
# Conditional GET on the single project
# ---------------------------------------------------------------------------


async def _seed_one_project(client: AsyncClient) -> None:
    """Create a single ``solo`` project with its ``__main`` edition."""
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={"slug": "solo", "title": "Solo"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_get_project_sends_validators(client: AsyncClient) -> None:
    """The single project carries a weak ``ETag`` and nothing else."""
    await _setup(client)
    await _seed_one_project(client)

    response = await client.get(
        "/docverse/orgs/proj-org/projects/solo",
        headers={"X-Auth-Request-User": "testuser"},
    )

    assert response.status_code == 200
    assert response.headers["ETag"].startswith('W/"')
    assert "Last-Modified" not in response.headers


@pytest.mark.asyncio
async def test_get_project_if_none_match_is_empty_304(
    client: AsyncClient,
) -> None:
    """Echoing the tag back earns a bodyless 304 repeating the tag."""
    await _setup(client)
    await _seed_one_project(client)
    headers = {"X-Auth-Request-User": "testuser"}

    first = await client.get(
        "/docverse/orgs/proj-org/projects/solo", headers=headers
    )
    assert first.status_code == 200

    second = await client.get(
        "/docverse/orgs/proj-org/projects/solo",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )

    assert second.status_code == 304
    assert second.content == b""
    assert second.headers["ETag"] == first.headers["ETag"]
    assert "Last-Modified" not in second.headers


@pytest.mark.asyncio
async def test_get_project_etag_changes_after_project_patch(
    client: AsyncClient,
) -> None:
    """A metadata edit retires the tag the caller was holding."""
    await _setup(client)
    await _seed_one_project(client)
    headers = {"X-Auth-Request-User": "testuser"}

    first = await client.get(
        "/docverse/orgs/proj-org/projects/solo", headers=headers
    )
    assert first.status_code == 200

    patched = await client.patch(
        "/docverse/orgs/proj-org/projects/solo",
        json={"title": "Retitled"},
        headers=headers,
    )
    assert patched.status_code == 200

    second = await client.get(
        "/docverse/orgs/proj-org/projects/solo",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )

    assert second.status_code == 200
    assert second.headers["ETag"] != first.headers["ETag"]


@pytest.mark.asyncio
async def test_get_project_etag_changes_after_default_edition_patch(
    client: AsyncClient,
) -> None:
    """The embedded default edition is part of what the tag covers.

    A ``__main`` metadata edit leaves ``projects.date_updated`` alone —
    only a repoint touches that — so this is the test that fails if the
    edition's clock stops reaching the tag.
    """
    await _setup(client)
    await _seed_one_project(client)
    headers = {"X-Auth-Request-User": "testuser"}

    first = await client.get(
        "/docverse/orgs/proj-org/projects/solo", headers=headers
    )
    assert first.status_code == 200
    project_clock = first.json()["date_updated"]

    patched = await client.patch(
        "/docverse/orgs/proj-org/projects/solo/editions/__main",
        json={"title": "Current"},
        headers=headers,
    )
    assert patched.status_code == 200

    second = await client.get(
        "/docverse/orgs/proj-org/projects/solo",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )

    assert second.status_code == 200
    assert second.headers["ETag"] != first.headers["ETag"]
    # The project row itself did not move; the edition's clock did.
    assert second.json()["date_updated"] == project_clock


@pytest.mark.asyncio
async def test_get_project_etag_changes_after_org_patch(
    client: AsyncClient,
) -> None:
    """The org's URL settings are part of what the tag covers.

    ``default_edition.published_url`` is derived from the org's
    ``base_domain``, ``url_scheme``, and ``root_path_prefix``, none of
    which touch the project or edition rows. Without the org's clock in
    the watermark a ``PATCH /orgs/{org}`` rewrites the body while the
    validators stand still, and a poller is told 304 about a URL it has
    never seen.
    """
    await _setup(client)
    await _seed_one_project(client)
    headers = {"X-Auth-Request-User": "testuser"}

    first = await client.get(
        "/docverse/orgs/proj-org/projects/solo", headers=headers
    )
    assert first.status_code == 200
    assert (
        first.json()["default_edition"]["published_url"]
        == "https://solo.proj-org.example.com/"
    )

    patched = await client.patch(
        "/docverse/orgs/proj-org",
        json={"base_domain": "docs.example.net"},
        headers=headers,
    )
    assert patched.status_code == 200

    second = await client.get(
        "/docverse/orgs/proj-org/projects/solo",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )

    assert second.status_code == 200
    assert second.headers["ETag"] != first.headers["ETag"]
    assert (
        second.json()["default_edition"]["published_url"]
        == "https://solo.docs.example.net/"
    )


@pytest.mark.asyncio
async def test_get_project_etag_changes_after_github_resolve(
    client: AsyncClient,
) -> None:
    """The resolve worker's binding write invalidates a cached project.

    Task #644: the four GitHub-binding writes used to pin
    ``date_updated``, which froze the project's validators even though
    ``installation_status`` had flipped on the wire. The clock now
    advances, so a poller holding the old tag is told to refetch.
    """
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/proj-org/projects",
        json={
            "slug": "resolved",
            "title": "Resolved",
            "github": {"owner": "lsst", "repo": "resolved"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    headers = {"X-Auth-Request-User": "testuser"}

    first = await client.get(
        "/docverse/orgs/proj-org/projects/resolved", headers=headers
    )
    assert first.status_code == 200
    assert first.json()["github"]["installation_status"] == "not_installed"

    await _resolve_github_binding(
        slug="resolved", owner="lsst", repo="resolved", installation_id=42
    )

    second = await client.get(
        "/docverse/orgs/proj-org/projects/resolved",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )

    assert second.status_code == 200
    assert second.headers["ETag"] != first.headers["ETag"]
    assert second.json()["github"]["installation_status"] == "installed"


@pytest.mark.asyncio
async def test_get_project_publishes_conditional_get_event(
    client: AsyncClient,
) -> None:
    """The event names the single-project endpoint and its project."""
    await _setup(client)
    await _seed_one_project(client)
    headers = {"X-Auth-Request-User": "testuser"}

    events = context_dependency._events
    assert events is not None
    publisher = events.conditional_get
    assert isinstance(publisher, MockEventPublisher)

    first = await client.get(
        "/docverse/orgs/proj-org/projects/solo", headers=headers
    )
    assert first.status_code == 200
    assert publisher.published == []

    second = await client.get(
        "/docverse/orgs/proj-org/projects/solo",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )
    assert second.status_code == 304

    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == "proj-org"
    assert event.project == "solo"
    assert event.endpoint == ConditionalGetEndpoint.project
    assert event.outcome == ConditionalGetOutcome.not_modified
    assert event.precondition == ConditionalGetPrecondition.etag


@pytest.mark.asyncio
async def test_get_project_etag_changes_when_project_clock_drops(
    client: AsyncClient,
) -> None:
    """A project clock landing below the org clock retires the tag.

    ``date_updated`` is PostgreSQL's transaction *start* time and
    commit order is not start order, so a slow writer can stamp the
    project row with an instant that is already behind the org's clock.
    Folding the three clocks together with ``max()`` would hide such a
    write — the maximum stays put — so the tag hashes each clock as its
    own part instead (task #650, finding 3).
    """
    await _setup(client)
    await _seed_one_project(client)
    headers = {"X-Auth-Request-User": "testuser"}
    # Push the org's clock far ahead of everything else, so a project
    # write landing "late" is necessarily below it.
    await _stamp_org_date_updated("proj-org", datetime(2027, 3, 4, tzinfo=UTC))

    first = await client.get(
        "/docverse/orgs/proj-org/projects/solo", headers=headers
    )
    assert first.status_code == 200

    await _stamp_date_updated(("solo", datetime(2026, 1, 15, tzinfo=UTC)))

    second = await client.get(
        "/docverse/orgs/proj-org/projects/solo",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )

    assert second.status_code == 200
    assert second.headers["ETag"] != first.headers["ETag"]


@pytest.mark.asyncio
async def test_get_project_etag_changes_when_edition_clock_drops(
    client: AsyncClient,
) -> None:
    """A default-edition clock below the org clock retires the tag too.

    The embedded ``__main`` edition is the third row the tag covers,
    and it is stamped by the same transaction-start clock, so it can
    land below the org's the same way the project row can.
    """
    await _setup(client)
    await _seed_one_project(client)
    headers = {"X-Auth-Request-User": "testuser"}
    await _stamp_org_date_updated("proj-org", datetime(2027, 3, 4, tzinfo=UTC))

    first = await client.get(
        "/docverse/orgs/proj-org/projects/solo", headers=headers
    )
    assert first.status_code == 200

    await _stamp_edition_date_updated(
        "solo", "__main", datetime(2026, 1, 15, tzinfo=UTC)
    )

    second = await client.get(
        "/docverse/orgs/proj-org/projects/solo",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )

    assert second.status_code == 200
    assert second.headers["ETag"] != first.headers["ETag"]


@pytest.mark.asyncio
async def test_list_projects_ignores_if_modified_since(
    client: AsyncClient,
) -> None:
    """The listing has no date validator to be polled with.

    ``Last-Modified`` can only name the newest clock in the listing,
    and that maximum is not monotonic under commit-order skew, so the
    date validator was dropped entirely (task #650). An
    ``If-Modified-Since`` is ignored the way an unparsable one always
    was: the listing is answered in full.
    """
    await _setup(client)
    await _seed_clocked_projects(client)
    headers = {"X-Auth-Request-User": "testuser"}

    response = await client.get(
        "/docverse/orgs/proj-org/projects",
        headers={
            **headers,
            "If-Modified-Since": "Wed, 01 Jan 2031 00:00:00 GMT",
        },
    )

    assert response.status_code == 200
    assert len(response.json()) == 3
    assert "Last-Modified" not in response.headers


@pytest.mark.asyncio
async def test_get_project_ignores_if_modified_since(
    client: AsyncClient,
) -> None:
    """The single project has no date validator either."""
    await _setup(client)
    await _seed_one_project(client)
    headers = {"X-Auth-Request-User": "testuser"}

    response = await client.get(
        "/docverse/orgs/proj-org/projects/solo",
        headers={
            **headers,
            "If-Modified-Since": "Wed, 01 Jan 2031 00:00:00 GMT",
        },
    )

    assert response.status_code == 200
    assert response.json()["slug"] == "solo"
    assert "Last-Modified" not in response.headers
