"""Tests for ProjectStore."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    ProjectGitHubBindingCreate,
    ProjectUpdate,
    TrackingMode,
)
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.dbschema.project import SqlProject
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.pagination import (
    ProjectDateUpdatedCursor,
    ProjectSlugCursor,
)
from docverse_server.storage.project_store import ProjectStore


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
async def test_rename_repo_by_repo_id_advances_date_updated(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A GitHub-side repo rename advances ``date_updated``.

    PRD #634 turned ``date_updated`` into a change signal for pollers
    such as Ook rather than a "last operator edit" marker. A rename
    flips ``github_repo`` and therefore the project's
    ``source_url`` on the wire, so the clock — and with it the
    listing's ETag, ``Last-Modified``, and ``updated_since`` filter —
    has to move with it.
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

    # Run the rename in a later transaction: ``func.now()`` is
    # transaction-stable in PostgreSQL, so only a separate transaction
    # yields a strictly greater timestamp than the create's.
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
    assert after.date_updated > baseline


@pytest.mark.asyncio
async def test_transfer_repo_by_repo_id_advances_date_updated(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A GitHub-side repo transfer advances ``date_updated``.

    The transfer moves the repo to a new owner namespace, so the
    project's ``source_url`` changes on the wire and the clock a poller
    reads has to follow it.
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
    assert after.date_updated > baseline


@pytest.mark.asyncio
async def test_apply_installation_scope_advances_date_updated(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Capturing the installation scope advances ``date_updated``.

    ``github_installation_id`` surfaces on the wire as the binding's
    ``installation_status`` (and its ``app_url``), so an
    ``installation`` webhook bringing ``owner/repo`` into scope is a
    change a poller must be able to see.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store)
        created = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="scope-me",
                title="Scope Me",
                github=ProjectGitHubBindingCreate(owner="Acme", repo="Docs"),
            ),
            github_owner="Acme",
            github_repo="Docs",
        )
        await db_session.commit()

    async with db_session.begin():
        before = await store.get_by_id(created.id)
    assert before is not None
    baseline = before.date_updated

    await asyncio.sleep(0.05)
    async with db_session.begin():
        # Lower-cased payload, to keep the case-insensitive match
        # covered alongside the clock.
        updated_ids = await store.apply_installation_scope(
            installation_id=11,
            owner="acme",
            owner_id=22,
            repo="docs",
            repo_id=33,
        )
        await db_session.commit()
    assert updated_ids == [created.id]

    async with db_session.begin():
        after = await store.get_by_id(created.id)
    assert after is not None
    assert after.github_installation_id == 11
    assert after.date_updated > baseline


@pytest.mark.asyncio
async def test_update_github_metadata_advances_date_updated(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """The resolve worker's write advances ``date_updated``.

    ``github_installation_id`` drives the binding's
    ``installation_status`` on the wire, so the resolve worker landing
    it is a change to what the project GET returns — and therefore a
    change the project clock has to report.
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
    assert after.date_updated > baseline


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
        deleted = await store.soft_delete(
            org_id=org_id,
            slug="del-proj",
            reason=TombstoneReason.manual_delete,
        )
        assert deleted is True
        # Should not be found after soft delete
        found = await store.get_by_slug(org_id=org_id, slug="del-proj")
        await db_session.commit()
    assert found is None


@pytest.mark.asyncio
async def test_soft_delete_project_stamps_tombstone_when_state_row_exists(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A project state row is stamped in the same flush as ``date_deleted``."""
    logger = structlog.get_logger("docverse")
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        org_id = await _create_org(org_store, slug="proj-tomb-org")
        project = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="del-tomb-proj",
                title="Tomb Me",
                source_url="https://example.com/example/repo",
            ),
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="del-tomb-proj",
            docverse_id=project.id,
        )
        deleted = await store.soft_delete(
            org_id=org_id,
            slug="del-tomb-proj",
            reason=TombstoneReason.manual_delete,
        )
        assert deleted is True
        await db_session.commit()

    async with db_session.begin():
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="del-tomb-proj",
            include_tombstoned=True,
        )
    assert state is not None
    assert state.date_tombstoned is not None
    assert state.tombstone_reason == "manual_delete"


@pytest.mark.asyncio
async def test_soft_delete_project_no_state_row_is_tombstone_noop(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """The soft-delete succeeds without creating a tombstone row."""
    logger = structlog.get_logger("docverse")
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        org_id = await _create_org(org_store, slug="proj-no-state-org")
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="del-no-state-proj",
                title="No State",
                source_url="https://example.com/example/repo",
            ),
        )
        deleted = await store.soft_delete(
            org_id=org_id,
            slug="del-no-state-proj",
            reason=TombstoneReason.manual_delete,
        )
        assert deleted is True
        await db_session.commit()

    async with db_session.begin():
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug="del-no-state-proj",
            include_tombstoned=True,
        )
    assert state is None


_CASCADE_HASH = "sha256:" + "c" * 64


async def _seed_cascade_project(
    db_session: AsyncSession,
    store: ProjectStore,
    *,
    org_id: int,
    slug: str,
) -> tuple[int, list[int], list[int]]:
    """Create a project with two editions and two builds.

    Returns the project id plus the edition and build ids, so a test
    can read the rows back and compare timestamps.
    """
    logger = structlog.get_logger("docverse")
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    project = await store.create(
        org_id=org_id,
        data=ProjectCreate(
            slug=slug,
            title=slug,
            source_url="https://example.com/example/repo",
        ),
    )
    edition_ids = []
    for edition_slug in (f"{slug}-a", f"{slug}-b"):
        edition = await edition_store.create(
            project_id=project.id,
            data=EditionCreate(
                slug=edition_slug,
                title=edition_slug,
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        edition_ids.append(edition.id)
    build_ids = []
    for git_ref in ("main", "feature"):
        build = await build_store.create(
            project_id=project.id,
            project_slug=slug,
            data=BuildCreate(git_ref=git_ref, content_hash=_CASCADE_HASH),
            uploader="testuser",
        )
        build_ids.append(build.id)
    return project.id, edition_ids, build_ids


@pytest.mark.asyncio
async def test_soft_delete_cascades_with_one_shared_timestamp(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Project, editions and builds are stamped in one flush.

    ``func.now()`` is transaction-stable in PostgreSQL, so a single
    shared timestamp across all three tables is the observable proof
    that the cascade never opened a second transaction — and it is what
    the tombstone revive keys on to tell cascade siblings apart from
    rows deleted individually.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store, slug="cascade-org")
        project_id, edition_ids, build_ids = await _seed_cascade_project(
            db_session, store, org_id=org_id, slug="cascade-proj"
        )
        deleted = await store.soft_delete(
            org_id=org_id,
            slug="cascade-proj",
            reason=TombstoneReason.manual_delete,
        )
        assert deleted is True
        await db_session.commit()

    async with db_session.begin():
        project_deleted = (
            await db_session.execute(
                select(SqlProject.date_deleted).where(
                    SqlProject.id == project_id
                )
            )
        ).scalar_one()
        edition_stamps = (
            (
                await db_session.execute(
                    select(SqlEdition.date_deleted).where(
                        SqlEdition.id.in_(edition_ids)
                    )
                )
            )
            .scalars()
            .all()
        )
        build_stamps = (
            (
                await db_session.execute(
                    select(SqlBuild.date_deleted).where(
                        SqlBuild.id.in_(build_ids)
                    )
                )
            )
            .scalars()
            .all()
        )
        await db_session.commit()
    assert project_deleted is not None
    assert list(edition_stamps) == [project_deleted, project_deleted]
    assert list(build_stamps) == [project_deleted, project_deleted]


@pytest.mark.asyncio
async def test_soft_delete_leaves_other_projects_untouched(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """The cascade is scoped to the deleted project's own rows."""
    async with db_session.begin():
        org_id = await _create_org(org_store, slug="cascade-scope-org")
        _, kept_editions, kept_builds = await _seed_cascade_project(
            db_session, store, org_id=org_id, slug="kept-proj"
        )
        await _seed_cascade_project(
            db_session, store, org_id=org_id, slug="gone-proj"
        )
        await store.soft_delete(
            org_id=org_id,
            slug="gone-proj",
            reason=TombstoneReason.manual_delete,
        )
        await db_session.commit()

    async with db_session.begin():
        edition_stamps = (
            (
                await db_session.execute(
                    select(SqlEdition.date_deleted).where(
                        SqlEdition.id.in_(kept_editions)
                    )
                )
            )
            .scalars()
            .all()
        )
        build_rows = (
            (
                await db_session.execute(
                    select(SqlBuild).where(SqlBuild.id.in_(kept_builds))
                )
            )
            .scalars()
            .all()
        )
        await db_session.commit()
    assert list(edition_stamps) == [None, None]
    assert [row.date_deleted for row in build_rows] == [None, None]
    assert {row.status for row in build_rows} == {BuildStatus.pending}


@pytest.mark.asyncio
async def test_soft_delete_keeps_an_earlier_deletion_timestamp(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Rows deleted before the project keep their own timestamps.

    The cascade's ``date_deleted IS NULL`` predicate is what makes an
    individually deleted edition or build distinguishable from a
    cascade sibling later on.
    """
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        org_id = await _create_org(org_store, slug="cascade-earlier-org")
        project_id, edition_ids, build_ids = await _seed_cascade_project(
            db_session, store, org_id=org_id, slug="earlier-proj"
        )
        edition_store = EditionStore(session=db_session, logger=logger)
        build_store = BuildStore(session=db_session, logger=logger)
        assert await edition_store.soft_delete(
            org_id=org_id,
            project_id=project_id,
            slug="earlier-proj-a",
            reason=TombstoneReason.lifecycle_delete,
        )
        assert await build_store.soft_delete(build_id=build_ids[0])
        await db_session.commit()

    async with db_session.begin():
        early_edition = (
            await db_session.execute(
                select(SqlEdition.date_deleted).where(
                    SqlEdition.id == edition_ids[0]
                )
            )
        ).scalar_one()
        early_build = (
            await db_session.execute(
                select(SqlBuild.date_deleted).where(
                    SqlBuild.id == build_ids[0]
                )
            )
        ).scalar_one()
        await db_session.commit()

    async with db_session.begin():
        await store.soft_delete(
            org_id=org_id,
            slug="earlier-proj",
            reason=TombstoneReason.manual_delete,
        )
        await db_session.commit()

    async with db_session.begin():
        project_deleted = (
            await db_session.execute(
                select(SqlProject.date_deleted).where(
                    SqlProject.id == project_id
                )
            )
        ).scalar_one()
        edition_after = (
            await db_session.execute(
                select(SqlEdition.date_deleted).where(
                    SqlEdition.id == edition_ids[0]
                )
            )
        ).scalar_one()
        build_after = (
            await db_session.execute(
                select(SqlBuild.date_deleted).where(
                    SqlBuild.id == build_ids[0]
                )
            )
        ).scalar_one()
        sibling_build = (
            await db_session.execute(
                select(SqlBuild.date_deleted).where(
                    SqlBuild.id == build_ids[1]
                )
            )
        ).scalar_one()
        await db_session.commit()
    assert edition_after == early_edition
    assert build_after == early_build
    assert edition_after != project_deleted
    assert sibling_build == project_deleted


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
        await store.soft_delete(
            org_id=org_id,
            slug="docs",
            reason=TombstoneReason.manual_delete,
        )
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


# -- GitHub-bound project listing for git_ref_audit -----


@pytest.mark.asyncio
async def test_list_org_ids_with_github_bound_projects_only_includes_bound(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Returns the set of org_ids that own at least one GitHub-bound project.

    Three orgs are seeded:

    * ``gh-only`` — every project is GitHub-bound.
    * ``mixed`` — one GitHub-bound, one non-GitHub project.
    * ``non-gh-only`` — no GitHub-bound projects.

    The dispatcher uses this to skip orgs that have nothing for the
    audit to do.
    """
    async with db_session.begin():
        gh_only = await _create_org(org_store, slug="gh-only")
        mixed = await _create_org(org_store, slug="mixed")
        non_gh_only = await _create_org(org_store, slug="non-gh-only")
        await store.create(
            org_id=gh_only,
            data=ProjectCreate(
                slug="proj-a",
                title="A",
                github=ProjectGitHubBindingCreate(owner="acme", repo="proj-a"),
            ),
            github_owner="acme",
            github_repo="proj-a",
        )
        await store.create(
            org_id=mixed,
            data=ProjectCreate(
                slug="b-gh",
                title="B",
                github=ProjectGitHubBindingCreate(owner="acme", repo="b"),
            ),
            github_owner="acme",
            github_repo="b",
        )
        await store.create(
            org_id=mixed,
            data=ProjectCreate(
                slug="b-non-gh",
                title="B (Gitlab)",
                source_url="https://gitlab.example.com/b",
            ),
        )
        await store.create(
            org_id=non_gh_only,
            data=ProjectCreate(
                slug="c-non-gh",
                title="C",
                source_url="https://gitlab.example.com/c",
            ),
        )
        result = await store.list_org_ids_with_github_bound_projects()
        await db_session.commit()
    assert result == {gh_only, mixed}
    assert non_gh_only not in result


@pytest.mark.asyncio
async def test_list_org_ids_with_github_bound_projects_excludes_deleted(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A soft-deleted GitHub-bound project does not keep its org in scope.

    If the org's only GitHub-bound project is soft-deleted, the org
    no longer has any work for the audit and the dispatcher must
    skip it on the next tick.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store, slug="del-only-gh")
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="deletable",
                title="Deletable",
                github=ProjectGitHubBindingCreate(
                    owner="acme", repo="deletable"
                ),
            ),
            github_owner="acme",
            github_repo="deletable",
        )
        await store.soft_delete(
            org_id=org_id,
            slug="deletable",
            reason=TombstoneReason.manual_delete,
        )
        result = await store.list_org_ids_with_github_bound_projects()
        await db_session.commit()
    assert org_id not in result


@pytest.mark.asyncio
async def test_list_github_bound_by_org_returns_only_bound(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Per-org listing filters to projects with github_owner+repo set."""
    async with db_session.begin():
        org_id = await _create_org(org_store, slug="per-org-list")
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="gh-1",
                title="GH 1",
                github=ProjectGitHubBindingCreate(owner="acme", repo="one"),
            ),
            github_owner="acme",
            github_repo="one",
        )
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="gh-2",
                title="GH 2",
                github=ProjectGitHubBindingCreate(owner="acme", repo="two"),
            ),
            github_owner="acme",
            github_repo="two",
        )
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="non-gh",
                title="Non GH",
                source_url="https://gitlab.example.com/x",
            ),
        )
        result = await store.list_github_bound_by_org(org_id)
        await db_session.commit()
    assert [p.slug for p in result] == ["gh-1", "gh-2"]
    for project in result:
        assert project.github_owner is not None
        assert project.github_repo is not None


@pytest.mark.asyncio
async def test_list_github_bound_by_org_excludes_deleted(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Soft-deleted GitHub-bound projects are not surfaced to the audit."""
    async with db_session.begin():
        org_id = await _create_org(org_store, slug="per-org-del")
        kept = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="kept",
                title="Kept",
                github=ProjectGitHubBindingCreate(owner="acme", repo="kept"),
            ),
            github_owner="acme",
            github_repo="kept",
        )
        await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="deleted",
                title="Deleted",
                github=ProjectGitHubBindingCreate(
                    owner="acme", repo="deleted"
                ),
            ),
            github_owner="acme",
            github_repo="deleted",
        )
        await store.soft_delete(
            org_id=org_id,
            slug="deleted",
            reason=TombstoneReason.manual_delete,
        )
        result = await store.list_github_bound_by_org(org_id)
        await db_session.commit()
    assert [p.slug for p in result] == [kept.slug]


@pytest.mark.asyncio
async def test_list_slugs_by_ids_includes_a_deleted_project(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """A slug lookup for metrics must survive its project's deletion.

    The ``purgatory_cleanup`` sweep labels each reap with the build's
    project slug, and the builds it reaches most often belong to a
    project that was itself deleted — the cascade is what puts them in
    purgatory. A lookup that filtered on ``date_deleted`` would drop the
    slug precisely on the reaps that matter, so this one deliberately
    does not.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store, slug="slug-lookup-org")
        live = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="still-here",
                title="Still Here",
                source_url="https://example.com/example/live",
            ),
        )
        gone = await store.create(
            org_id=org_id,
            data=ProjectCreate(
                slug="deleted-project",
                title="Deleted Project",
                source_url="https://example.com/example/gone",
            ),
        )
        await store.soft_delete(
            org_id=org_id,
            slug="deleted-project",
            reason=TombstoneReason.manual_delete,
        )
        slugs = await store.list_slugs_by_ids([live.id, gone.id])
        await db_session.commit()

    assert slugs == {live.id: "still-here", gone.id: "deleted-project"}


@pytest.mark.asyncio
async def test_list_slugs_by_ids_is_empty_for_no_ids(
    db_session: AsyncSession,
    store: ProjectStore,
) -> None:
    """No ids means no query: the sweep often purges nothing at all."""
    async with db_session.begin():
        assert await store.list_slugs_by_ids([]) == {}


@pytest.mark.asyncio
async def test_create_mints_time_ordered_public_id(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """Projects created in succession sort by ``public_id`` in that order."""
    async with db_session.begin():
        org_id = await _create_org(org_store, slug="pid-order-org")
        first = await store.create(
            org_id=org_id,
            data=ProjectCreate(slug="first", title="First"),
        )
        second = await store.create(
            org_id=org_id,
            data=ProjectCreate(slug="second", title="Second"),
        )
        await db_session.commit()

    assert first.public_id > 0
    assert second.public_id > first.public_id


# ---------------------------------------------------------------------------
# ``date_updated`` ordering and the ``updated_since`` filter
# ---------------------------------------------------------------------------

# Three fixed instants, oldest to newest. Pinned rather than derived from
# ``now()`` because ``projects.date_updated`` is stamped by the
# transaction timestamp: rows written in one transaction would otherwise
# all share a value and there would be nothing to order or filter on.
T_OLD = datetime(2026, 1, 1, tzinfo=UTC)
T_MID = datetime(2026, 2, 1, tzinfo=UTC)
T_NEW = datetime(2026, 3, 1, tzinfo=UTC)


async def _seed_clocked_projects(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
    *,
    org_slug: str,
    title: str = "Clocked",
) -> int:
    """Create three projects stamped with `T_OLD`, `T_MID`, and `T_NEW`.

    The timestamps are written with a Core ``UPDATE`` because
    ``SqlProject.date_updated`` carries an ORM ``onupdate``, which would
    overwrite any value an ORM flush tried to set. ``expire_all`` then
    drops the identity map so the listing under test reads the new
    timestamps from the database rather than the stale loaded rows.

    Returns the org id.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store, slug=org_slug)
        for slug in ("clock-old", "clock-mid", "clock-new"):
            await store.create(
                org_id=org_id,
                data=ProjectCreate(slug=slug, title=title),
            )
        await db_session.commit()

    async with db_session.begin():
        for slug, stamp in (
            ("clock-old", T_OLD),
            ("clock-mid", T_MID),
            ("clock-new", T_NEW),
        ):
            await db_session.execute(
                update(SqlProject)
                .where(
                    SqlProject.org_id == org_id,
                    SqlProject.slug == slug,
                )
                .values(date_updated=stamp)
                .execution_options(synchronize_session=False)
            )
        await db_session.commit()
    db_session.expire_all()
    return org_id


@pytest.mark.asyncio
async def test_list_by_org_date_updated_orders_newest_first(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """``ProjectDateUpdatedCursor`` sorts most-recently-touched first."""
    org_id = await _seed_clocked_projects(
        db_session, store, org_store, org_slug="clock-order-org"
    )

    async with db_session.begin():
        result = await store.list_by_org(
            org_id, cursor_type=ProjectDateUpdatedCursor, limit=25
        )

    assert [p.slug for p in result.entries] == [
        "clock-new",
        "clock-mid",
        "clock-old",
    ]


@pytest.mark.asyncio
async def test_list_by_org_date_updated_pages_forward(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """The ``date_updated`` cursor walks forward without repeating rows."""
    org_id = await _seed_clocked_projects(
        db_session, store, org_store, org_slug="clock-fwd-org"
    )

    async with db_session.begin():
        first = await store.list_by_org(
            org_id, cursor_type=ProjectDateUpdatedCursor, limit=2
        )
        assert first.next_cursor is not None
        second = await store.list_by_org(
            org_id,
            cursor_type=ProjectDateUpdatedCursor,
            cursor=first.next_cursor,
            limit=2,
        )

    assert [p.slug for p in first.entries] == ["clock-new", "clock-mid"]
    assert [p.slug for p in second.entries] == ["clock-old"]
    assert second.next_cursor is None


@pytest.mark.asyncio
async def test_list_by_org_date_updated_pages_backward(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """The second page's ``prev`` cursor returns the first page."""
    org_id = await _seed_clocked_projects(
        db_session, store, org_store, org_slug="clock-back-org"
    )

    async with db_session.begin():
        first = await store.list_by_org(
            org_id, cursor_type=ProjectDateUpdatedCursor, limit=2
        )
        assert first.next_cursor is not None
        second = await store.list_by_org(
            org_id,
            cursor_type=ProjectDateUpdatedCursor,
            cursor=first.next_cursor,
            limit=2,
        )
        assert second.prev_cursor is not None
        back = await store.list_by_org(
            org_id,
            cursor_type=ProjectDateUpdatedCursor,
            cursor=second.prev_cursor,
            limit=2,
        )

    assert [p.slug for p in back.entries] == ["clock-new", "clock-mid"]


@pytest.mark.asyncio
async def test_list_by_org_updated_since_is_inclusive(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """``updated_since`` keeps rows whose clock equals the boundary.

    The boundary is inclusive so a poller can pass back the newest
    ``date_updated`` it saw without a "did I already have this?"
    round-trip; re-seeing one row is cheaper than the risk of skipping
    one written in the same microsecond.
    """
    org_id = await _seed_clocked_projects(
        db_session, store, org_store, org_slug="clock-since-org"
    )

    async with db_session.begin():
        result = await store.list_by_org(
            org_id,
            cursor_type=ProjectSlugCursor,
            limit=25,
            updated_since=T_MID,
        )

    assert [p.slug for p in result.entries] == ["clock-mid", "clock-new"]
    assert result.count == 2


@pytest.mark.asyncio
async def test_search_by_org_honours_updated_since(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """The fuzzy-search path applies the same inclusive clock filter."""
    org_id = await _seed_clocked_projects(
        db_session, store, org_store, org_slug="clock-search-org"
    )

    async with db_session.begin():
        result = await store.search_by_org(
            org_id, query="clock", limit=25, updated_since=T_MID
        )

    assert sorted(p.slug for p in result.entries) == [
        "clock-mid",
        "clock-new",
    ]
    assert result.count == 2


# ---------------------------------------------------------------------------
# ``include_deleted``
# ---------------------------------------------------------------------------


async def _seed_live_and_deleted(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
    *,
    org_slug: str,
) -> int:
    """Create a live ``gone-live`` project and a deleted ``gone-dead`` one.

    Both slugs share the ``gone-`` prefix so a single trigram query
    matches the pair, which is what the search-path test needs.

    Returns the org id.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store, slug=org_slug)
        for slug in ("gone-live", "gone-dead"):
            await store.create(
                org_id=org_id,
                data=ProjectCreate(slug=slug, title=f"Gone {slug}"),
            )
        await db_session.commit()

    async with db_session.begin():
        await store.soft_delete(
            org_id=org_id,
            slug="gone-dead",
            reason=TombstoneReason.manual_delete,
        )
        await db_session.commit()
    db_session.expire_all()
    return org_id


@pytest.mark.asyncio
async def test_get_by_slug_include_deleted_returns_deleted_project(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """``include_deleted`` resolves a soft-deleted slug to its row.

    ``uq_projects_org_slug`` ignores ``date_deleted``, so a slug is
    never reused after a delete and the widened lookup still names
    exactly one row.
    """
    org_id = await _seed_live_and_deleted(
        db_session, store, org_store, org_slug="gone-get-org"
    )

    async with db_session.begin():
        without_flag = await store.get_by_slug(org_id=org_id, slug="gone-dead")
        with_flag = await store.get_by_slug(
            org_id=org_id, slug="gone-dead", include_deleted=True
        )

    assert without_flag is None
    assert with_flag is not None
    assert with_flag.date_deleted is not None


@pytest.mark.asyncio
async def test_list_by_org_include_deleted_lists_and_counts_deleted(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """The ordered listing widens to deleted rows and counts them."""
    org_id = await _seed_live_and_deleted(
        db_session, store, org_store, org_slug="gone-list-org"
    )

    async with db_session.begin():
        without_flag = await store.list_by_org(
            org_id, cursor_type=ProjectSlugCursor, limit=25
        )
        with_flag = await store.list_by_org(
            org_id,
            cursor_type=ProjectSlugCursor,
            limit=25,
            include_deleted=True,
        )

    assert [p.slug for p in without_flag.entries] == ["gone-live"]
    assert without_flag.count == 1
    assert [p.slug for p in with_flag.entries] == ["gone-dead", "gone-live"]
    assert with_flag.count == 2


@pytest.mark.asyncio
async def test_search_by_org_include_deleted_matches_deleted(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """The fuzzy-search path widens the same way the listing does."""
    org_id = await _seed_live_and_deleted(
        db_session, store, org_store, org_slug="gone-search-org"
    )

    async with db_session.begin():
        without_flag = await store.search_by_org(
            org_id, query="gone", limit=25
        )
        with_flag = await store.search_by_org(
            org_id, query="gone", limit=25, include_deleted=True
        )

    assert [p.slug for p in without_flag.entries] == ["gone-live"]
    assert without_flag.count == 1
    assert sorted(p.slug for p in with_flag.entries) == [
        "gone-dead",
        "gone-live",
    ]
    assert with_flag.count == 2


@pytest.mark.asyncio
async def test_get_org_watermark_includes_deleted_projects(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """The watermark is the newest clock in the org, deleted rows too.

    A soft delete is the one mutation whose row drops out of the
    default listing, so a watermark that filtered deleted rows would
    sit still through exactly the change a poller most needs to see.
    """
    async with db_session.begin():
        org_id = await _create_org(org_store, slug="watermark-org")
        for slug in ("wm-live", "wm-dead"):
            await store.create(
                org_id=org_id,
                data=ProjectCreate(slug=slug, title=f"Watermark {slug}"),
            )
        await db_session.commit()

    async with db_session.begin():
        await store.soft_delete(
            org_id=org_id,
            slug="wm-dead",
            reason=TombstoneReason.manual_delete,
        )
        await db_session.commit()

    # Stamp the deleted row as the newest clock in the org. ``now()``
    # is transaction-stable in PostgreSQL, so rows written in one
    # transaction are otherwise indistinguishable.
    newest = datetime(2026, 5, 1, tzinfo=UTC)
    async with db_session.begin():
        await db_session.execute(
            update(SqlProject)
            .where(SqlProject.slug == "wm-live")
            .values(date_updated=datetime(2026, 4, 1, tzinfo=UTC))
            .execution_options(synchronize_session=False)
        )
        await db_session.execute(
            update(SqlProject)
            .where(SqlProject.slug == "wm-dead")
            .values(date_updated=newest)
            .execution_options(synchronize_session=False)
        )
        await db_session.commit()
    db_session.expire_all()

    async with db_session.begin():
        watermark = await store.get_org_watermark(org_id)

    assert watermark == newest


@pytest.mark.asyncio
async def test_get_org_watermark_falls_back_to_org_date_created(
    db_session: AsyncSession,
    store: ProjectStore,
    org_store: OrganizationStore,
) -> None:
    """An org with no projects is stamped with its own creation date.

    A shared sentinel would give every empty org the same validator, so
    a client could not tell one empty listing from another's.
    """
    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="watermark-empty-org",
                title="Test Org",
                base_domain="test.example.com",
            )
        )
        await db_session.commit()

    async with db_session.begin():
        watermark = await store.get_org_watermark(org.id)

    assert watermark == org.date_created
