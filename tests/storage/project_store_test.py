"""Tests for ProjectStore."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import (
    OrganizationCreate,
    ProjectCreate,
    ProjectUpdate,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.pagination import ProjectSlugCursor
from docverse.storage.project_store import ProjectStore


@pytest.fixture
def store(
    db_session: async_scoped_session[AsyncSession],
) -> ProjectStore:
    logger = structlog.get_logger("docverse")
    return ProjectStore(session=db_session, logger=logger)


@pytest.fixture
def org_store(
    db_session: async_scoped_session[AsyncSession],
) -> OrganizationStore:
    logger = structlog.get_logger("docverse")
    return OrganizationStore(session=db_session, logger=logger)


async def _create_org(
    org_store: OrganizationStore,
    slug: str = "test-org",
) -> int:
    org = await org_store.create(
        OrganizationCreate(
            slug=slug, title="Test Org", base_domain="test.example.com"
        )
    )
    return org.id


@pytest.mark.asyncio
async def test_create_project(
    db_session: async_scoped_session[AsyncSession],
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(org_store)
        project = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="my-project",
                title="My Project",
                doc_repo="https://github.com/example/repo",
            ),
        )
        await db_session.commit()
    assert project.slug == "my-project"
    assert project.title == "My Project"
    assert project.org_id == org_id
    assert project.date_created is not None
    assert project.date_deleted is None


@pytest.mark.asyncio
async def test_get_by_slug(
    db_session: async_scoped_session[AsyncSession],
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(org_store)
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="find-me",
                title="Find Me",
                doc_repo="https://github.com/example/repo",
            ),
        )
        found = await store.get_by_slug(org_id=org_id, slug="find-me")
        await db_session.commit()
    assert found is not None
    assert found.slug == "find-me"


@pytest.mark.asyncio
async def test_get_by_slug_not_found(
    db_session: async_scoped_session[AsyncSession],
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(org_store)
        found = await store.get_by_slug(org_id=org_id, slug="nope")
        await db_session.commit()
    assert found is None


@pytest.mark.asyncio
async def test_list_by_org(
    db_session: async_scoped_session[AsyncSession],
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(org_store)
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="proj-aa",
                title="A",
                doc_repo="https://github.com/example/a",
            ),
        )
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="proj-bb",
                title="B",
                doc_repo="https://github.com/example/b",
            ),
        )
        result = await store.list_by_org(
            org_id,
            cursor_type=ProjectSlugCursor,
            limit=25,
        )
        await db_session.commit()
    assert len(result.entries) == 2
    assert result.entries[0].slug == "proj-aa"
    assert result.entries[1].slug == "proj-bb"


@pytest.mark.asyncio
async def test_update_project(
    db_session: async_scoped_session[AsyncSession],
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(org_store)
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="upd-proj",
                title="Original",
                doc_repo="https://github.com/example/repo",
            ),
        )
        updated = await store.update(
            org_id=org_id,
            slug="upd-proj",
            data=ProjectUpdate(title="Updated"),
        )
        await db_session.commit()
    assert updated is not None
    assert updated.title == "Updated"
    assert updated.slug == "upd-proj"


@pytest.mark.asyncio
async def test_soft_delete(
    db_session: async_scoped_session[AsyncSession],
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    async with db_session.begin():
        org_id = await _create_org(org_store)
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="del-proj",
                title="Delete Me",
                doc_repo="https://github.com/example/repo",
            ),
        )
        deleted = await store.soft_delete(org_id=org_id, slug="del-proj")
        assert deleted is True
        # Should not be found after soft delete
        found = await store.get_by_slug(org_id=org_id, slug="del-proj")
        await db_session.commit()
    assert found is None
