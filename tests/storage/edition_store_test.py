"""Tests for EditionStore."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import (
    BuildCreate,
    EditionCreate,
    EditionKind,
    EditionUpdate,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore


@pytest.fixture
def edition_store(
    db_session: async_scoped_session[AsyncSession],
) -> EditionStore:
    logger = structlog.get_logger("docverse")
    return EditionStore(session=db_session, logger=logger)


async def _create_project(
    db_session: async_scoped_session[AsyncSession],
) -> int:
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug="ed-org",
            title="Ed Org",
            base_domain="ed.example.com",
        )
    )
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="ed-proj",
            title="Ed Project",
            doc_repo="https://github.com/example/repo",
        ),
    )
    return project.id


@pytest.mark.asyncio
async def test_create_edition(
    db_session: async_scoped_session[AsyncSession],
    edition_store: EditionStore,
) -> None:
    async with db_session.begin():
        project_id = await _create_project(db_session)
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="main",
                title="Latest",
                kind=EditionKind.main,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )
        await db_session.commit()
    assert edition.slug == "main"
    assert edition.kind == EditionKind.main
    assert edition.tracking_mode == TrackingMode.git_ref
    assert edition.current_build_id is None
    assert edition.current_build_public_id is None


@pytest.mark.asyncio
async def test_get_by_slug(
    db_session: async_scoped_session[AsyncSession],
    edition_store: EditionStore,
) -> None:
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="find-ed",
                title="Find Ed",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        found = await edition_store.get_by_slug(
            project_id=project_id, slug="find-ed"
        )
        await db_session.commit()
    assert found is not None
    assert found.slug == "find-ed"


@pytest.mark.asyncio
async def test_list_by_project(
    db_session: async_scoped_session[AsyncSession],
    edition_store: EditionStore,
) -> None:
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="ed-aa",
                title="A",
                kind=EditionKind.main,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="ed-bb",
                title="B",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.semver_release,
            ),
        )
        editions = await edition_store.list_by_project(project_id)
        await db_session.commit()
    assert len(editions) == 2


@pytest.mark.asyncio
async def test_update_edition(
    db_session: async_scoped_session[AsyncSession],
    edition_store: EditionStore,
) -> None:
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="upd-ed",
                title="Original",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        updated = await edition_store.update(
            project_id=project_id,
            slug="upd-ed",
            data=EditionUpdate(title="Updated"),
        )
        await db_session.commit()
    assert updated is not None
    assert updated.title == "Updated"


@pytest.mark.asyncio
async def test_set_current_build(
    db_session: async_scoped_session[AsyncSession],
    edition_store: EditionStore,
) -> None:
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        project_id = await _create_project(db_session)
        build_store = BuildStore(session=db_session, logger=logger)
        build = await build_store.create(
            project_id=project_id,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            ),
            uploader="testuser",
        )
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="with-build",
                title="With Build",
                kind=EditionKind.main,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        updated = await edition_store.set_current_build(
            edition_id=edition.id, build_id=build.id
        )
        await db_session.commit()
    assert updated.current_build_id == build.id
    assert updated.current_build_public_id == build.public_id


@pytest.mark.asyncio
async def test_soft_delete_edition(
    db_session: async_scoped_session[AsyncSession],
    edition_store: EditionStore,
) -> None:
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="del-ed",
                title="Delete Me",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        deleted = await edition_store.soft_delete(
            project_id=project_id, slug="del-ed"
        )
        assert deleted is True
        found = await edition_store.get_by_slug(
            project_id=project_id, slug="del-ed"
        )
        await db_session.commit()
    assert found is None
