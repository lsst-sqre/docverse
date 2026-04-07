"""Tests for BuildStore."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import (
    BuildCreate,
    BuildStatus,
    OrganizationCreate,
    ProjectCreate,
)
from docverse.domain.base32id import serialize_base32_id
from docverse.exceptions import InvalidBuildStateError
from docverse.storage.build_store import BuildStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore


@pytest.fixture
def build_store(
    db_session: async_scoped_session[AsyncSession],
) -> BuildStore:
    logger = structlog.get_logger("docverse")
    return BuildStore(session=db_session, logger=logger)


async def _create_org_and_project(
    db_session: async_scoped_session[AsyncSession],
) -> tuple[int, int]:
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug="build-org",
            title="Build Org",
            base_domain="build.example.com",
        )
    )
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="build-proj",
            title="Build Project",
            doc_repo="https://github.com/example/repo",
        ),
    )
    return org.id, project.id


def _build_data() -> BuildCreate:
    return BuildCreate(
        git_ref="main",
        content_hash="sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    )


@pytest.mark.asyncio
async def test_create_build(
    db_session: async_scoped_session[AsyncSession],
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await db_session.commit()
    assert build.status == BuildStatus.pending
    assert build.public_id > 0
    assert build.staging_key.startswith("__staging/")
    assert build.uploader == "testuser"
    assert build.git_ref == "main"


@pytest.mark.asyncio
async def test_create_build_sets_storage_prefix(
    db_session: async_scoped_session[AsyncSession],
    build_store: BuildStore,
) -> None:
    """storage_prefix is computed as {project_slug}/__builds/{base32_id}/."""
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await db_session.commit()

    base32_id = serialize_base32_id(build.public_id)
    expected = f"build-proj/__builds/{base32_id}/"
    assert build.storage_prefix == expected


@pytest.mark.asyncio
async def test_transition_pending_to_processing(
    db_session: async_scoped_session[AsyncSession],
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        processing = await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        await db_session.commit()
    assert processing.status == BuildStatus.processing
    assert processing.date_uploaded is not None


@pytest.mark.asyncio
async def test_transition_processing_to_completed(
    db_session: async_scoped_session[AsyncSession],
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        completed = await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.completed
        )
        await db_session.commit()
    assert completed.status == BuildStatus.completed
    assert completed.date_completed is not None


@pytest.mark.asyncio
async def test_transition_processing_to_failed(
    db_session: async_scoped_session[AsyncSession],
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        failed = await build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.failed
        )
        await db_session.commit()
    assert failed.status == BuildStatus.failed
    assert failed.date_completed is not None


@pytest.mark.asyncio
async def test_invalid_transition_raises(
    db_session: async_scoped_session[AsyncSession],
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        # Cannot go directly from pending to completed
        with pytest.raises(InvalidBuildStateError):
            await build_store.transition_status(
                build_id=build.id, new_status=BuildStatus.completed
            )
        await db_session.commit()


@pytest.mark.asyncio
async def test_list_by_project(
    db_session: async_scoped_session[AsyncSession],
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="user1",
        )
        await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=BuildCreate(
                git_ref="v1.0",
                content_hash="sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            ),
            uploader="user2",
        )
        result = await build_store.list_by_project(project_id, limit=25)
        await db_session.commit()
    assert len(result.entries) == 2


@pytest.mark.asyncio
async def test_get_by_public_id(
    db_session: async_scoped_session[AsyncSession],
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        found = await build_store.get_by_public_id(
            project_id=project_id, public_id=build.public_id
        )
        await db_session.commit()
    assert found is not None
    assert found.id == build.id


@pytest.mark.asyncio
async def test_soft_delete_build(
    db_session: async_scoped_session[AsyncSession],
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        deleted = await build_store.soft_delete(build_id=build.id)
        assert deleted is True
        found = await build_store.get_by_public_id(
            project_id=project_id, public_id=build.public_id
        )
        await db_session.commit()
    assert found is None


@pytest.mark.asyncio
async def test_update_inventory(
    db_session: async_scoped_session[AsyncSession],
    build_store: BuildStore,
) -> None:
    async with db_session.begin():
        _, project_id = await _create_org_and_project(db_session)
        build = await build_store.create(
            project_id=project_id,
            project_slug="build-proj",
            data=_build_data(),
            uploader="testuser",
        )
        updated = await build_store.update_inventory(
            build_id=build.id, object_count=42, total_size_bytes=1024000
        )
        await db_session.commit()
    assert updated.object_count == 42
    assert updated.total_size_bytes == 1024000
