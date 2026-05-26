"""Tests for ProjectStore."""

from __future__ import annotations

import asyncio

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    OrganizationCreate,
    ProjectCreate,
    ProjectGitHubBindingCreate,
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
                source_url="https://example.com/example/repo",
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
                source_url="https://example.com/example/repo",
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
                source_url="https://example.com/example/a",
            ),
        )
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="proj-bb",
                title="B",
                source_url="https://example.com/example/b",
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
                source_url="https://example.com/example/repo",
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
    GitHub-side metadata sync. The rename still flips ``github_repo``;
    the effective source URL is derived from the binding.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store)
        created = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="rename-me",
                title="Rename Me",
                github=ProjectGitHubBindingCreate(
                    owner="acme", repo="old-repo"
                ),
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
    assert after.source_url is None
    assert after.effective_source_url == "https://github.com/acme/new-repo"
    assert after.date_updated == baseline


@pytest.mark.asyncio
async def test_transfer_repo_by_repo_id_preserves_date_updated(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A GitHub-side repo transfer must not bump ``date_updated``.

    The transfer still flips ``github_owner`` / ``github_owner_id`` /
    ``github_repo``; the effective source URL is derived from the
    binding.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store)
        created = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="transfer-me",
                title="Transfer Me",
                github=ProjectGitHubBindingCreate(owner="acme", repo="repo"),
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
    assert after.source_url is None
    assert after.effective_source_url == "https://github.com/beta/repo"
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
                github=ProjectGitHubBindingCreate(owner="acme", repo="repo"),
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
                github=ProjectGitHubBindingCreate(owner="acme", repo="repo"),
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
                source_url="https://example.com/example/repo",
            ),
        )
        deleted = await store.soft_delete(org_id=org_id, slug="del-proj")
        assert deleted is True
        # Should not be found after soft delete
        found = await store.get_by_slug(org_id=org_id, slug="del-proj")
        await db_session.commit()
    assert found is None


# ── list_by_github_repo ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_by_github_repo_matches_by_repo_id(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A project resolved against ``github_repo_id`` is returned.

    The ``id`` path is the rename-robust primary key: the numeric repo
    id outlives display-name renames and transfers, so a webhook
    delivered for ``new-name`` still matches a project whose
    ``github_repo`` column has not yet been updated by the rename
    webhook (or never will be, if that webhook is missed).
    """
    async with db_session.begin():
        org_id = await _create_org(org_store)
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="docs",
                title="Docs",
                github=ProjectGitHubBindingCreate(
                    owner="acme", repo="templates"
                ),
            ),
            github_owner="acme",
            github_repo="templates",
        )
        await store.apply_installation_scope(
            installation_id=99,
            owner="acme",
            owner_id=999,
            repo="templates",
            repo_id=12345,
        )
        result = await store.list_by_github_repo(
            repo_id=12345, owner="acme", repo="templates"
        )
        await db_session.commit()
    assert [p.slug for p in result] == ["docs"]


@pytest.mark.asyncio
async def test_list_by_github_repo_matches_pre_resolve_by_owner_repo(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A project with NULL ``github_repo_id`` still matches by name pair.

    A freshly-created project has structured owner/repo but no numeric
    ids yet (those are filled opportunistically by the resolve worker
    or the installation webhook). The webhook must still route to it.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store)
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="docs",
                title="Docs",
                github=ProjectGitHubBindingCreate(
                    owner="acme", repo="templates"
                ),
            ),
            github_owner="acme",
            github_repo="templates",
        )
        result = await store.list_by_github_repo(
            repo_id=None, owner="acme", repo="templates"
        )
        await db_session.commit()
    assert [p.slug for p in result] == ["docs"]


@pytest.mark.asyncio
async def test_list_by_github_repo_matches_case_insensitively(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Owner/repo matching is case-insensitive.

    GitHub canonical casing (``Acme/Templates``) and the webhook's
    delivered casing (``acme/templates``) must both resolve to the
    same project row.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store)
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="docs",
                title="Docs",
                github=ProjectGitHubBindingCreate(
                    owner="Acme", repo="Templates"
                ),
            ),
            github_owner="Acme",
            github_repo="Templates",
        )
        result = await store.list_by_github_repo(
            repo_id=None, owner="acme", repo="templates"
        )
        await db_session.commit()
    assert [p.slug for p in result] == ["docs"]


@pytest.mark.asyncio
async def test_list_by_github_repo_returns_multiple_matches(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Multiple project slugs may share one upstream GitHub repo."""
    async with db_session.begin():
        org_id = await _create_org(org_store)
        for slug in ("docs-a", "docs-b"):
            await store.create(
                org_id=org_id,
                data=ProjectCreate(
                    slug=slug,
                    title=slug,
                    github=ProjectGitHubBindingCreate(
                        owner="acme", repo="templates"
                    ),
                ),
                github_owner="acme",
                github_repo="templates",
            )
        await store.apply_installation_scope(
            installation_id=99,
            owner="acme",
            owner_id=999,
            repo="templates",
            repo_id=12345,
        )
        result = await store.list_by_github_repo(
            repo_id=12345, owner="acme", repo="templates"
        )
        await db_session.commit()
    assert {p.slug for p in result} == {"docs-a", "docs-b"}


@pytest.mark.asyncio
async def test_list_by_github_repo_no_match_returns_empty(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A repo with no bound projects returns an empty list, no error."""
    async with db_session.begin():
        await _create_org(org_store)
        result = await store.list_by_github_repo(
            repo_id=42, owner="ghost", repo="repo"
        )
        await db_session.commit()
    assert result == []


@pytest.mark.asyncio
async def test_list_by_github_repo_excludes_non_github_projects(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A project with NULL github_owner/repo never matches."""
    async with db_session.begin():
        org_id = await _create_org(org_store)
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="gitlab-proj",
                title="GitLab Proj",
                source_url="https://gitlab.com/acme/templates",
            ),
        )
        result = await store.list_by_github_repo(
            repo_id=None, owner="acme", repo="templates"
        )
        await db_session.commit()
    assert result == []


@pytest.mark.asyncio
async def test_list_by_github_repo_excludes_soft_deleted(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Soft-deleted projects are not returned by webhook lookups."""
    async with db_session.begin():
        org_id = await _create_org(org_store)
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="docs",
                title="Docs",
                github=ProjectGitHubBindingCreate(
                    owner="acme", repo="templates"
                ),
            ),
            github_owner="acme",
            github_repo="templates",
        )
        await store.soft_delete(org_id=org_id, slug="docs")
        result = await store.list_by_github_repo(
            repo_id=None, owner="acme", repo="templates"
        )
        await db_session.commit()
    assert result == []


@pytest.mark.asyncio
async def test_list_by_github_repo_dedupes_id_and_name_matches(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A project matched by both repo_id and owner/repo appears once.

    The two-query union exists so pre-resolve projects (no repo_id) and
    rename-survivors (repo_id stable, name flipped) both surface; for
    a project that satisfies both predicates, the result must dedup on
    the project id rather than return a duplicate.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store)
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="docs",
                title="Docs",
                github=ProjectGitHubBindingCreate(
                    owner="acme", repo="templates"
                ),
            ),
            github_owner="acme",
            github_repo="templates",
        )
        await store.apply_installation_scope(
            installation_id=99,
            owner="acme",
            owner_id=999,
            repo="templates",
            repo_id=12345,
        )
        result = await store.list_by_github_repo(
            repo_id=12345, owner="acme", repo="templates"
        )
        await db_session.commit()
    assert [p.slug for p in result] == ["docs"]
