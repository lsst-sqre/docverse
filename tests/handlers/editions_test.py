"""Tests for edition endpoints."""

from __future__ import annotations

import re

import pytest
import structlog
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import BuildCreate
from docverse.client.models.builds import BuildAnnotations
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from tests.conftest import seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    """Create org, membership, and project."""
    await seed_org_with_admin(client, "ed-org", "testuser")
    await client.post(
        "/docverse/orgs/ed-org/projects",
        json={
            "slug": "ed-proj",
            "title": "Ed Project",
            "doc_repo": "https://github.com/example/ed",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )


@pytest.mark.asyncio
async def test_create_edition(client: AsyncClient) -> None:
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "main",
            "title": "Latest",
            "kind": "main",
            "tracking_mode": "git_ref",
            "tracking_params": {"git_ref": "main"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["slug"] == "main"
    assert data["kind"] == "main"
    assert data["tracking_mode"] == "git_ref"
    assert data["build_url"] is None
    assert data["self_url"].endswith(
        "/orgs/ed-org/projects/ed-proj/editions/main"
    )
    assert data["history_url"].endswith(
        "/orgs/ed-org/projects/ed-proj/editions/main/history"
    )
    assert data["rollback_url"].endswith(
        "/orgs/ed-org/projects/ed-proj/editions/main/rollback"
    )


@pytest.mark.asyncio
async def test_list_editions(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "list-ed",
            "title": "List Ed",
            "kind": "draft",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert len(response.json()) >= 1
    assert "Link" in response.headers
    assert "X-Total-Count" in response.headers


@pytest.mark.asyncio
async def test_get_edition(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "get-ed",
            "title": "Get Ed",
            "kind": "release",
            "tracking_mode": "semver_release",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/get-ed",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["slug"] == "get-ed"
    assert data["history_url"].endswith(
        "/orgs/ed-org/projects/ed-proj/editions/get-ed/history"
    )
    assert data["rollback_url"].endswith(
        "/orgs/ed-org/projects/ed-proj/editions/get-ed/rollback"
    )


@pytest.mark.asyncio
async def test_update_edition(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "upd-ed",
            "title": "Original",
            "kind": "draft",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.patch(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/upd-ed",
        json={"title": "Updated"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["title"] == "Updated"


@pytest.mark.asyncio
async def test_delete_edition(client: AsyncClient) -> None:
    await _setup(client)
    await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "del-ed",
            "title": "Delete Me",
            "kind": "draft",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    response = await client.delete(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/del-ed",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204

    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/del-ed",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_default_edition(client: AsyncClient) -> None:
    """The __main edition is accessible via GET."""
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["slug"] == "__main"
    assert data["kind"] == "main"


@pytest.mark.asyncio
async def test_delete_default_edition_blocked(client: AsyncClient) -> None:
    """DELETE __main returns 403."""
    await _setup(client)
    response = await client.delete(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_patch_default_edition_kind_blocked(
    client: AsyncClient,
) -> None:
    """PATCH __main with kind returns 403."""
    await _setup(client)
    response = await client.patch(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main",
        json={"kind": "draft"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_patch_default_edition_allowed_fields(
    client: AsyncClient,
) -> None:
    """PATCH __main with title/tracking_mode succeeds."""
    await _setup(client)
    response = await client.patch(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main",
        json={
            "title": "Updated Main",
            "tracking_mode": "lsst_doc",
            "lifecycle_exempt": False,
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["title"] == "Updated Main"
    assert data["tracking_mode"] == "lsst_doc"
    assert data["lifecycle_exempt"] is False


@pytest.mark.asyncio
async def test_user_cannot_create_dunder_edition(
    client: AsyncClient,
) -> None:
    """POST edition with __main slug returns 422 (Pydantic rejects it)."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/ed-org/projects/ed-proj/editions",
        json={
            "slug": "__main",
            "title": "Sneaky",
            "kind": "main",
            "tracking_mode": "git_ref",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


async def _record_builds_in_history(
    db_session: async_scoped_session[AsyncSession],
    org_slug: str,
    project_slug: str,
    edition_slug: str,
    n_builds: int,
) -> list[int]:
    """Create builds and record them in the edition's history.

    Returns the list of build internal IDs (oldest first).
    """
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    history_store = EditionBuildHistoryStore(session=db_session, logger=logger)

    org = await org_store.get_by_slug(org_slug)
    assert org is not None
    project = await proj_store.get_by_slug(org_id=org.id, slug=project_slug)
    assert project is not None
    edition = await edition_store.get_by_slug(
        project_id=project.id, slug=edition_slug
    )
    assert edition is not None

    build_ids: list[int] = []
    for i in range(n_builds):
        build = await build_store.create(
            project_id=project.id,
            data=BuildCreate(
                git_ref=f"refs/tags/v{i}",
                content_hash=f"sha256:{i:064x}",
            ),
            uploader="testuser",
            project_slug=project_slug,
        )
        build_ids.append(build.id)
        await history_store.record(edition_id=edition.id, build_id=build.id)
    return build_ids


@pytest.mark.asyncio
async def test_get_edition_history(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """GET history returns entries in position order with build info."""
    await _setup(client)
    async with db_session.begin():
        await _record_builds_in_history(
            db_session, "ed-org", "ed-proj", "__main", n_builds=3
        )
        await db_session.commit()

    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main/history",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    # Position 1 = most recent (last build recorded)
    assert data[0]["position"] == 1
    assert data[0]["git_ref"] == "refs/tags/v2"
    assert data[1]["position"] == 2
    assert data[1]["git_ref"] == "refs/tags/v1"
    assert data[2]["position"] == 3
    assert data[2]["git_ref"] == "refs/tags/v0"
    # Each entry has expected fields
    for entry in data:
        assert "build_url" in entry
        assert "build_id" in entry
        assert "date_created" in entry
        assert entry["build_status"] == "pending"
        assert entry["build_deleted"] is False
        assert entry["annotations"] is None
    # Pagination headers present
    assert "Link" in response.headers
    assert "X-Total-Count" in response.headers
    assert response.headers["X-Total-Count"] == "3"


@pytest.mark.asyncio
async def test_get_edition_history_with_annotations(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """GET history returns annotations when builds have them."""
    await _setup(client)
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        org_store = OrganizationStore(session=db_session, logger=logger)
        proj_store = ProjectStore(session=db_session, logger=logger)
        edition_store = EditionStore(session=db_session, logger=logger)
        build_store = BuildStore(session=db_session, logger=logger)
        history_store = EditionBuildHistoryStore(
            session=db_session, logger=logger
        )

        org = await org_store.get_by_slug("ed-org")
        assert org is not None
        project = await proj_store.get_by_slug(org_id=org.id, slug="ed-proj")
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition is not None

        build = await build_store.create(
            project_id=project.id,
            data=BuildCreate(
                git_ref="main",
                content_hash=f"sha256:{'a' * 64}",
                annotations=BuildAnnotations.model_validate(
                    {
                        "commit_sha": "deadbeef",
                        "ci_platform": "github-actions",
                        "custom": "value",
                    }
                ),
            ),
            uploader="testuser",
            project_slug="ed-proj",
        )
        await history_store.record(edition_id=edition.id, build_id=build.id)
        await db_session.commit()

    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main/history",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    annotations = data[0]["annotations"]
    assert annotations["commit_sha"] == "deadbeef"
    assert annotations["ci_platform"] == "github-actions"
    assert annotations["custom"] == "value"


@pytest.mark.asyncio
async def test_get_edition_history_empty(client: AsyncClient) -> None:
    """GET history for edition with no builds returns empty list."""
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main/history",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers["X-Total-Count"] == "0"


@pytest.mark.asyncio
async def test_get_edition_history_pagination(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """GET history supports multi-page cursor navigation."""
    await _setup(client)
    async with db_session.begin():
        await _record_builds_in_history(
            db_session, "ed-org", "ed-proj", "__main", n_builds=5
        )
        await db_session.commit()

    # First page (limit=2)
    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main/history",
        params={"limit": 2},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert response.headers["X-Total-Count"] == "5"
    # There should be a next link
    link = response.headers["Link"]
    assert 'rel="next"' in link

    # Extract the cursor from the next link
    match = re.search(r"cursor=([^&>]+)", link)
    assert match is not None
    next_cursor = match.group(1)

    # Second page
    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main/history",
        params={"limit": 2, "cursor": next_cursor},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data2 = response.json()
    assert len(data2) == 2
    assert response.headers["X-Total-Count"] == "5"


@pytest.mark.asyncio
async def test_get_edition_history_single_page(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """When all results fit in one page, no next link is present."""
    await _setup(client)
    async with db_session.begin():
        await _record_builds_in_history(
            db_session, "ed-org", "ed-proj", "__main", n_builds=3
        )
        await db_session.commit()

    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main/history",
        params={"limit": 10},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    assert response.headers["X-Total-Count"] == "3"
    link = response.headers.get("Link", "")
    assert 'rel="next"' not in link


@pytest.mark.asyncio
async def test_get_edition_history_not_found(client: AsyncClient) -> None:
    """GET history for non-existent edition returns 404."""
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/no-such/history",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_edition_history_invalid_cursor(
    client: AsyncClient,
) -> None:
    """GET history with invalid cursor returns 422."""
    await _setup(client)
    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main/history",
        params={"cursor": "abc"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_edition_history_excludes_deleted_by_default(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Soft-deleted builds are excluded from history by default."""
    await _setup(client)
    async with db_session.begin():
        build_ids = await _record_builds_in_history(
            db_session, "ed-org", "ed-proj", "__main", n_builds=3
        )
        # Soft-delete the middle build (v1, position 2)
        build_store = BuildStore(
            session=db_session, logger=structlog.get_logger("docverse")
        )
        await build_store.soft_delete(build_id=build_ids[1])
        await db_session.commit()

    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main/history",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert response.headers["X-Total-Count"] == "2"
    git_refs = [entry["git_ref"] for entry in data]
    assert "refs/tags/v1" not in git_refs
    for entry in data:
        assert entry["build_deleted"] is False


@pytest.mark.asyncio
async def test_get_edition_history_include_deleted(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """include_deleted=true shows soft-deleted builds."""
    await _setup(client)
    async with db_session.begin():
        build_ids = await _record_builds_in_history(
            db_session, "ed-org", "ed-proj", "__main", n_builds=3
        )
        build_store = BuildStore(
            session=db_session, logger=structlog.get_logger("docverse")
        )
        await build_store.soft_delete(build_id=build_ids[1])
        await db_session.commit()

    response = await client.get(
        "/docverse/orgs/ed-org/projects/ed-proj/editions/__main/history",
        params={"include_deleted": "true"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    assert response.headers["X-Total-Count"] == "3"
    # The deleted build (v1) should have build_deleted=True
    deleted_entries = [e for e in data if e["build_deleted"] is True]
    assert len(deleted_entries) == 1
    assert deleted_entries[0]["git_ref"] == "refs/tags/v1"
