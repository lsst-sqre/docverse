"""Tests for ProjectStore."""

from __future__ import annotations

import asyncio

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

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
    db_session: AsyncSession,
) -> ProjectStore:
    logger = structlog.get_logger("docverse")
    return ProjectStore(session=db_session, logger=logger)


@pytest.fixture
def org_store(
    db_session: AsyncSession,
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
    db_session: AsyncSession,
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
                source_url="https://github.com/example/repo",
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
    db_session: AsyncSession,
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
                source_url="https://github.com/example/repo",
            ),
        )
        found = await store.get_by_slug(org_id=org_id, slug="find-me")
        await db_session.commit()
    assert found is not None
    assert found.slug == "find-me"


@pytest.mark.asyncio
async def test_get_by_slug_not_found(
    db_session: AsyncSession,
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
    db_session: AsyncSession,
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
                source_url="https://github.com/example/a",
            ),
        )
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="proj-bb",
                title="B",
                source_url="https://github.com/example/b",
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
    db_session: AsyncSession,
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
                source_url="https://github.com/example/repo",
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
async def test_rename_repo_by_repo_id_preserves_date_updated(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A GitHub-side repo rename must not bump ``date_updated``.

    ``date_updated`` is the operator-visible "last source-coordinate
    edit" signal; those edits arrive through PUT/PATCH, not through a
    GitHub-side metadata sync. The rename still rewrites ``github_repo``
    and the matching ``source_url``.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store)
        created = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="rename-me",
                title="Rename Me",
                source_url="https://github.com/acme/old-repo",
            ),
            github_owner="acme",
            github_repo="old-repo",
        )
        # Set github_repo_id without bumping date_updated so the baseline
        # below is the create timestamp.
        await store.apply_installation_scope(
            installation_id=111,
            owner="acme",
            owner_id=222,
            repo="old-repo",
            repo_id=333,
        )
        await db_session.commit()

    async with db_session.begin():
        before = await store.get_by_id(created.id)
    assert before is not None
    baseline = before.date_updated

    # Run the rename in a later transaction so a re-fired
    # ``onupdate=func.now()`` would yield a strictly greater timestamp
    # than the create transaction's ``now()``.
    await asyncio.sleep(0.05)
    async with db_session.begin():
        updated_ids = await store.rename_repo_by_repo_id(
            github_repo_id=333,
            new_repo="new-repo",
        )
        await db_session.commit()
    assert updated_ids == [created.id]

    async with db_session.begin():
        after = await store.get_by_id(created.id)
    assert after is not None
    assert after.github_repo == "new-repo"
    assert after.source_url == "https://github.com/acme/new-repo"
    assert after.date_updated == baseline


@pytest.mark.asyncio
async def test_transfer_repo_by_repo_id_preserves_date_updated(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A GitHub-side repo transfer must not bump ``date_updated``.

    The transfer still flips ``github_owner`` / ``github_owner_id`` /
    ``github_repo`` and rewrites the matching ``source_url``.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store)
        created = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="transfer-me",
                title="Transfer Me",
                source_url="https://github.com/acme/repo",
            ),
            github_owner="acme",
            github_repo="repo",
        )
        await store.apply_installation_scope(
            installation_id=111,
            owner="acme",
            owner_id=222,
            repo="repo",
            repo_id=333,
        )
        await db_session.commit()

    async with db_session.begin():
        before = await store.get_by_id(created.id)
    assert before is not None
    baseline = before.date_updated

    await asyncio.sleep(0.05)
    async with db_session.begin():
        updated_ids = await store.transfer_repo_by_repo_id(
            github_repo_id=333,
            new_owner="beta",
            new_owner_id=444,
            new_repo="repo",
        )
        await db_session.commit()
    assert updated_ids == [created.id]

    async with db_session.begin():
        after = await store.get_by_id(created.id)
    assert after is not None
    assert after.github_owner == "beta"
    assert after.github_owner_id == 444
    assert after.source_url == "https://github.com/beta/repo"
    assert after.date_updated == baseline


@pytest.mark.asyncio
async def test_update_github_metadata_preserves_date_updated(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Capturing the github_*_id columns preserves ``date_updated``.

    The three numeric ids are sync-bookkeeping, not an operator-visible
    source-coordinate edit.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store)
        created = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="meta-me",
                title="Meta Me",
                source_url="https://github.com/acme/repo",
            ),
            github_owner="acme",
            github_repo="repo",
        )
        await db_session.commit()

    async with db_session.begin():
        before = await store.get_by_id(created.id)
    assert before is not None
    baseline = before.date_updated

    await asyncio.sleep(0.05)
    async with db_session.begin():
        updated = await store.update_github_metadata(
            project_id=created.id,
            expected_owner="acme",
            expected_repo="repo",
            installation_id=10,
            owner_id=20,
            repo_id=30,
        )
        await db_session.commit()
    assert updated is True

    async with db_session.begin():
        after = await store.get_by_id(created.id)
    assert after is not None
    assert after.github_installation_id == 10
    assert after.github_owner_id == 20
    assert after.github_repo_id == 30
    assert after.date_updated == baseline


@pytest.mark.asyncio
async def test_update_github_metadata_skips_on_binding_change(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Returns ``False`` and writes nothing when the binding flipped.

    The ``expected_owner``/``expected_repo`` guard protects against a
    PATCH that rewrote ``github`` between enqueue and the worker run.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store)
        created = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="stale-me",
                title="Stale Me",
                source_url="https://github.com/acme/repo",
            ),
            github_owner="acme",
            github_repo="repo",
        )
        await db_session.commit()

    async with db_session.begin():
        updated = await store.update_github_metadata(
            project_id=created.id,
            expected_owner="acme",
            expected_repo="different-repo",
            installation_id=10,
            owner_id=20,
            repo_id=30,
        )
        await db_session.commit()
    assert updated is False

    async with db_session.begin():
        after = await store.get_by_id(created.id)
    assert after is not None
    assert after.github_repo_id is None
    assert after.github_owner_id is None
    assert after.github_installation_id is None


@pytest.mark.asyncio
async def test_soft_delete(
    db_session: AsyncSession,
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
                source_url="https://github.com/example/repo",
            ),
        )
        deleted = await store.soft_delete(org_id=org_id, slug="del-proj")
        assert deleted is True
        # Should not be found after soft delete
        found = await store.get_by_slug(org_id=org_id, slug="del-proj")
        await db_session.commit()
    assert found is None
