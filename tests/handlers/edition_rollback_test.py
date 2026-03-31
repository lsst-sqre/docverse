"""Tests for the edition rollback endpoint."""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import BuildCreate
from docverse.domain.base32id import serialize_base32_id
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
    await seed_org_with_admin(client, "rb-org", "testuser")
    await client.post(
        "/docverse/orgs/rb-org/projects",
        json={
            "slug": "rb-proj",
            "title": "Rollback Project",
            "doc_repo": "https://github.com/example/rb",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )


async def _create_builds_with_history(
    db_session: async_scoped_session[AsyncSession],
    n_builds: int,
) -> list[tuple[int, int]]:
    """Create builds and record them in __main edition history.

    Returns list of (build_internal_id, build_public_id) tuples,
    oldest first.
    """
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    history_store = EditionBuildHistoryStore(session=db_session, logger=logger)

    org = await org_store.get_by_slug("rb-org")
    assert org is not None
    project = await proj_store.get_by_slug(org_id=org.id, slug="rb-proj")
    assert project is not None
    edition = await edition_store.get_by_slug(
        project_id=project.id, slug="__main"
    )
    assert edition is not None

    builds: list[tuple[int, int]] = []
    for i in range(n_builds):
        build = await build_store.create(
            project_id=project.id,
            data=BuildCreate(
                git_ref=f"refs/tags/v{i}",
                content_hash=f"sha256:{i:064x}",
            ),
            uploader="testuser",
        )
        builds.append((build.id, build.public_id))
        await history_store.record(edition_id=edition.id, build_id=build.id)
    return builds


@pytest.mark.asyncio
async def test_rollback_success(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """POST rollback with valid build in history returns 200."""
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    # Roll back to the first build (v0)
    target_public_id = serialize_base32_id(builds[0][1])
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["slug"] == "__main"
    assert data["build_url"] is not None
    assert target_public_id.replace("-", "") in data["build_url"].replace(
        "-", ""
    )


@pytest.mark.asyncio
async def test_rollback_unauthorized(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Non-admin gets 403 on rollback."""
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=1)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "unknownuser"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_rollback_build_not_in_history(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Valid build not in this edition's history returns 404."""
    await _setup(client)
    async with db_session.begin():
        # Create a build but do NOT record it in the edition's history
        logger = structlog.get_logger("docverse")
        org_store = OrganizationStore(session=db_session, logger=logger)
        proj_store = ProjectStore(session=db_session, logger=logger)
        org = await org_store.get_by_slug("rb-org")
        assert org is not None
        project = await proj_store.get_by_slug(org_id=org.id, slug="rb-proj")
        assert project is not None
        build_store = BuildStore(session=db_session, logger=logger)
        build = await build_store.create(
            project_id=project.id,
            data=BuildCreate(
                git_ref="refs/tags/orphan",
                content_hash="sha256:" + "a" * 64,
            ),
            uploader="testuser",
        )
        orphan_public_id = serialize_base32_id(build.public_id)
        await db_session.commit()

    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": orphan_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_rollback_build_not_found(client: AsyncClient) -> None:
    """Nonexistent build public ID returns 404."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": "0000-0000-0000-00"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_rollback_records_in_history(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """After rollback, GET history shows the rollback target at position 1."""
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    # Roll back to build v0
    target_public_id = serialize_base32_id(builds[0][1])
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    # Check history — position 1 should be the rollback target
    history_response = await client.get(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/history",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert history_response.status_code == 200
    history = history_response.json()
    assert len(history) == 4  # 3 original + 1 rollback entry
    assert history[0]["position"] == 1
    assert history[0]["git_ref"] == "refs/tags/v0"


@pytest.mark.asyncio
async def test_rollback_malformed_build_id(client: AsyncClient) -> None:
    """Malformed base32 build ID returns 404."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": "totally-invalid"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_rollback_edition_not_found(client: AsyncClient) -> None:
    """Rollback on a nonexistent edition slug returns 404."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/no-such-edition/rollback",
        json={"build": "0000-0000-0000-00"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_rollback_missing_build_field(client: AsyncClient) -> None:
    """Missing 'build' field in request body returns 422."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422
