"""Tests for EditionStore."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
import structlog
from fastapi import FastAPI
from safir.database import create_database_engine
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from docverse.client.models import (
    BuildCreate,
    EditionCreate,
    EditionKind,
    EditionUpdate,
    OrganizationCreate,
    ProjectCreate,
    PublishStatus,
    TrackingMode,
)
from docverse.config import config
from docverse.dbschema.build import SqlBuild
from docverse.dbschema.edition import SqlEdition
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.pagination import EditionSlugCursor
from docverse.storage.project_store import ProjectStore

_HASH = "sha256:" + "a" * 64


@pytest.fixture
def edition_store(
    db_session: AsyncSession,
) -> EditionStore:
    logger = structlog.get_logger("docverse")
    return EditionStore(session=db_session, logger=logger)


async def _create_project_with_org(
    db_session: AsyncSession,
) -> tuple[int, int]:
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
            source_url="https://example.com/example/repo",
        ),
    )
    return org.id, project.id


async def _create_project(
    db_session: AsyncSession,
) -> int:
    _, project_id = await _create_project_with_org(db_session)
    return project_id


@pytest.mark.asyncio
async def test_create_edition(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    async with db_session.begin():
        project_id = await _create_project(db_session)
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="main",
                title="Latest",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )
        await db_session.commit()
    assert edition.slug == "main"
    assert edition.kind == EditionKind.release
    assert edition.tracking_mode == TrackingMode.git_ref
    assert edition.current_build_id is None
    assert edition.current_build_public_id is None
    assert edition.publish_status is None


@pytest.mark.asyncio
async def test_edition_publish_status_persists(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """Setting ``publish_status`` via the ORM roundtrips through the DB."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="pub-ed",
                title="Pub",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        row = await db_session.get(SqlEdition, edition.id)
        assert row is not None
        row.publish_status = PublishStatus.published.value
        await db_session.commit()

    async with db_session.begin():
        refetched = await db_session.get(SqlEdition, edition.id)
        assert refetched is not None
        assert refetched.publish_status == PublishStatus.published.value


@pytest.mark.asyncio
async def test_get_by_slug(
    db_session: AsyncSession,
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
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="ed-aa",
                title="A",
                kind=EditionKind.release,
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
        result = await edition_store.list_by_project(
            project_id,
            cursor_type=EditionSlugCursor,
            limit=25,
        )
        await db_session.commit()
    assert len(result.entries) == 2


@pytest.mark.asyncio
async def test_update_edition(
    db_session: AsyncSession,
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
async def test_update_tracking(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """``update_tracking`` rewrites the tracking columns on an edition."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="track-ed",
                title="Track",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )
        await edition_store.update_tracking(
            edition_id=edition.id,
            tracking_mode=TrackingMode.semver_major,
            tracking_params={"major_version": 2},
        )
        await db_session.commit()

    async with db_session.begin():
        refetched = await edition_store.get_by_slug(
            project_id=project_id, slug="track-ed"
        )
    assert refetched is not None
    assert refetched.tracking_mode == TrackingMode.semver_major
    assert refetched.tracking_params == {"major_version": 2}


@pytest.mark.asyncio
async def test_update_tracking_missing_raises(
    edition_store: EditionStore,
) -> None:
    """``update_tracking`` raises when the edition id is unknown."""
    with pytest.raises(RuntimeError, match="Edition id=999999 not found"):
        await edition_store.update_tracking(
            edition_id=999999,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        )


@pytest.mark.asyncio
async def test_set_current_build(
    db_session: AsyncSession,
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
            project_slug="ed-proj",
        )
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="with-build",
                title="With Build",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        updated = await edition_store.set_current_build(
            edition_id=edition.id, build_id=build.id
        )
        await db_session.commit()
    assert updated is not None
    assert updated.current_build_id == build.id
    assert updated.current_build_public_id == build.public_id


@pytest.mark.asyncio
async def test_set_current_build_skips_stale(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """Skip when the edition already has a newer build."""
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        project_id = await _create_project(db_session)
        build_store = BuildStore(session=db_session, logger=logger)
        newer_build = await build_store.create(
            project_id=project_id,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:aaaa" + "0" * 60,
            ),
            uploader="testuser",
            project_slug="ed-proj",
        )
        older_build = await build_store.create(
            project_id=project_id,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:bbbb" + "0" * 60,
            ),
            uploader="testuser",
            project_slug="ed-proj",
        )
        # Set controlled timestamps
        for bid, ts in [
            (newer_build.id, datetime(2025, 6, 1, tzinfo=UTC)),
            (older_build.id, datetime(2025, 1, 1, tzinfo=UTC)),
        ]:
            row = (
                await db_session.execute(
                    select(SqlBuild).where(SqlBuild.id == bid)
                )
            ).scalar_one()
            row.date_created = ts
        await db_session.flush()

        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="guard-stale",
                title="Guard Stale",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        # Point edition to the newer build first
        applied = await edition_store.set_current_build(
            edition_id=edition.id, build_id=newer_build.id
        )
        assert applied is not None

        # Try to set to the older build — should be skipped
        skipped = await edition_store.set_current_build(
            edition_id=edition.id, build_id=older_build.id
        )
        await db_session.commit()
    assert skipped is None


@pytest.mark.asyncio
async def test_set_current_build_skips_equal(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """Skip when the incoming build has the same date_created."""
    logger = structlog.get_logger("docverse")
    same_time = datetime(2025, 3, 15, tzinfo=UTC)
    async with db_session.begin():
        project_id = await _create_project(db_session)
        build_store = BuildStore(session=db_session, logger=logger)
        build_a = await build_store.create(
            project_id=project_id,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:cccc" + "0" * 60,
            ),
            uploader="testuser",
            project_slug="ed-proj",
        )
        build_b = await build_store.create(
            project_id=project_id,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:dddd" + "0" * 60,
            ),
            uploader="testuser",
            project_slug="ed-proj",
        )
        # Give both builds the same date_created
        for bid in [build_a.id, build_b.id]:
            row = (
                await db_session.execute(
                    select(SqlBuild).where(SqlBuild.id == bid)
                )
            ).scalar_one()
            row.date_created = same_time
        await db_session.flush()

        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="guard-equal",
                title="Guard Equal",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        applied = await edition_store.set_current_build(
            edition_id=edition.id, build_id=build_a.id
        )
        assert applied is not None

        skipped = await edition_store.set_current_build(
            edition_id=edition.id, build_id=build_b.id
        )
        await db_session.commit()
    assert skipped is None


@pytest.mark.asyncio
async def test_set_current_build_applies_when_newer(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """set_current_build applies when the incoming build is newer."""
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        project_id = await _create_project(db_session)
        build_store = BuildStore(session=db_session, logger=logger)
        older_build = await build_store.create(
            project_id=project_id,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:eeee" + "0" * 60,
            ),
            uploader="testuser",
            project_slug="ed-proj",
        )
        newer_build = await build_store.create(
            project_id=project_id,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:ffff" + "0" * 60,
            ),
            uploader="testuser",
            project_slug="ed-proj",
        )
        for bid, ts in [
            (older_build.id, datetime(2025, 1, 1, tzinfo=UTC)),
            (newer_build.id, datetime(2025, 6, 1, tzinfo=UTC)),
        ]:
            row = (
                await db_session.execute(
                    select(SqlBuild).where(SqlBuild.id == bid)
                )
            ).scalar_one()
            row.date_created = ts
        await db_session.flush()

        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="guard-newer",
                title="Guard Newer",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        # Set to the older build first
        await edition_store.set_current_build(
            edition_id=edition.id, build_id=older_build.id
        )
        # Update to the newer build — should succeed
        updated = await edition_store.set_current_build(
            edition_id=edition.id, build_id=newer_build.id
        )
        await db_session.commit()
    assert updated is not None
    assert updated.current_build_id == newer_build.id
    assert updated.current_build_public_id == newer_build.public_id


@pytest.mark.asyncio
async def test_soft_delete_edition(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    async with db_session.begin():
        org_id, project_id = await _create_project_with_org(db_session)
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
            org_id=org_id,
            project_id=project_id,
            slug="del-ed",
            reason=TombstoneReason.manual_delete,
        )
        assert deleted is True
        found = await edition_store.get_by_slug(
            project_id=project_id, slug="del-ed"
        )
        await db_session.commit()
    assert found is None


@pytest.mark.asyncio
async def test_soft_delete_edition_stamps_tombstone_when_state_row_exists(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A state row is stamped in the same flush as ``date_deleted``."""
    logger = structlog.get_logger("docverse")
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        org_id, project_id = await _create_project_with_org(db_session)
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="del-tomb",
                title="Tombstone Me",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=4242,
            ltd_slug="del-tomb",
            docverse_id=edition.id,
        )
        deleted = await edition_store.soft_delete(
            org_id=org_id,
            project_id=project_id,
            slug="del-tomb",
            reason=TombstoneReason.lifecycle_delete,
        )
        assert deleted is True
        await db_session.commit()

    async with db_session.begin():
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=4242,
            include_tombstoned=True,
        )
    assert state is not None
    assert state.date_tombstoned is not None
    assert state.tombstone_reason == "lifecycle_delete"


@pytest.mark.asyncio
async def test_soft_delete_edition_no_state_row_is_tombstone_noop(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """The soft-delete succeeds without creating a tombstone row."""
    logger = structlog.get_logger("docverse")
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        org_id, project_id = await _create_project_with_org(db_session)
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="noimport",
                title="No Import",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        deleted = await edition_store.soft_delete(
            org_id=org_id,
            project_id=project_id,
            slug="noimport",
            reason=TombstoneReason.manual_delete,
        )
        assert deleted is True
        await db_session.commit()

    async with db_session.begin():
        rows = await state_store.list_for_org(
            org_id=org_id,
            resource_type=ResourceType.edition,
            docverse_ids=[edition.id],
            include_tombstoned=True,
        )
    assert rows == []


@pytest.mark.asyncio
async def test_soft_delete_edition_records_reason_as_passed(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """``reason=manual_delete`` is recorded on the state row as-is."""
    logger = structlog.get_logger("docverse")
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        org_id, project_id = await _create_project_with_org(db_session)
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="manual",
                title="Manual",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=7777,
            ltd_slug="manual",
            docverse_id=edition.id,
        )
        await edition_store.soft_delete(
            org_id=org_id,
            project_id=project_id,
            slug="manual",
            reason=TombstoneReason.manual_delete,
        )
        await db_session.commit()

    async with db_session.begin():
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=7777,
            include_tombstoned=True,
        )
    assert state is not None
    assert state.tombstone_reason == "manual_delete"


@pytest.mark.asyncio
async def test_soft_delete_edition_rollback_unwinds_both(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A rollback after ``soft_delete`` leaves both rows untouched."""
    logger = structlog.get_logger("docverse")
    state_store = KeeperSyncStateStore(session=db_session, logger=logger)
    async with db_session.begin():
        org_id, project_id = await _create_project_with_org(db_session)
        edition = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="rollback-me",
                title="Rollback",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=8888,
            ltd_slug="rollback-me",
            docverse_id=edition.id,
        )
        await db_session.commit()

    # Start a write transaction, perform the soft-delete + tombstone
    # write, then rollback by raising out of the ``begin()`` block.
    async def _run() -> None:
        async with db_session.begin():
            await edition_store.soft_delete(
                org_id=org_id,
                project_id=project_id,
                slug="rollback-me",
                reason=TombstoneReason.lifecycle_delete,
            )
            msg = "trigger rollback"
            raise RuntimeError(msg)

    with pytest.raises(RuntimeError, match="trigger rollback"):
        await _run()

    async with db_session.begin():
        found = await edition_store.get_by_slug(
            project_id=project_id, slug="rollback-me"
        )
        state = await state_store.get(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=8888,
            include_tombstoned=True,
        )
    assert found is not None
    assert state is not None
    assert state.date_tombstoned is None
    assert state.tombstone_reason is None


@pytest.mark.asyncio
async def test_find_matching_editions_git_ref(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A build without alternate_name matches a git_ref edition."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="main",
                title="Latest",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )
        matched = await edition_store.find_matching_editions(
            project_id=project_id,
            git_ref="main",
        )
        await db_session.commit()
    assert len(matched) == 1
    assert matched[0].slug == "main"


@pytest.mark.asyncio
async def test_find_matching_editions_git_ref_excludes_alternate(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A build with alternate_name must NOT match a git_ref edition."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="main",
                title="Latest",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )
        matched = await edition_store.find_matching_editions(
            project_id=project_id,
            git_ref="main",
            alternate_name="usdf-dev",
        )
        await db_session.commit()
    assert len(matched) == 0


@pytest.mark.asyncio
async def test_find_matching_editions_alternate_git_ref(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A build with matching git_ref AND alternate_name matches."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="usdf-dev",
                title="USDF Dev",
                kind=EditionKind.alternate,
                tracking_mode=TrackingMode.alternate_git_ref,
                tracking_params={
                    "git_ref": "main",
                    "alternate_name": "usdf-dev",
                },
            ),
        )
        matched = await edition_store.find_matching_editions(
            project_id=project_id,
            git_ref="main",
            alternate_name="usdf-dev",
        )
        await db_session.commit()
    assert len(matched) == 1
    assert matched[0].slug == "usdf-dev"


@pytest.mark.asyncio
async def test_find_matching_editions_alternate_git_ref_wrong_ref(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """Build with matching alternate_name but wrong git_ref: no match."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="usdf-dev",
                title="USDF Dev",
                kind=EditionKind.alternate,
                tracking_mode=TrackingMode.alternate_git_ref,
                tracking_params={
                    "git_ref": "main",
                    "alternate_name": "usdf-dev",
                },
            ),
        )
        matched = await edition_store.find_matching_editions(
            project_id=project_id,
            git_ref="develop",
            alternate_name="usdf-dev",
        )
        await db_session.commit()
    assert len(matched) == 0


@pytest.mark.asyncio
async def test_find_matching_editions_no_alternate_vs_alternate_edition(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A build without alternate_name must NOT match alternate_git_ref."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="usdf-dev",
                title="USDF Dev",
                kind=EditionKind.alternate,
                tracking_mode=TrackingMode.alternate_git_ref,
                tracking_params={
                    "git_ref": "main",
                    "alternate_name": "usdf-dev",
                },
            ),
        )
        matched = await edition_store.find_matching_editions(
            project_id=project_id,
            git_ref="main",
        )
        await db_session.commit()
    assert len(matched) == 0


# ── Version-based matching ─────────────────────────────────────────────────


async def _create_edition_internal(
    edition_store: EditionStore,
    project_id: int,
    *,
    slug: str,
    kind: EditionKind,
    tracking_mode: TrackingMode,
    tracking_params: dict[str, Any] | None = None,
    build_id: int | None = None,
) -> int:
    """Create an edition via create_internal, optionally setting a build."""
    edition = await edition_store.create_internal(
        project_id=project_id,
        slug=slug,
        title=slug,
        kind=kind,
        tracking_mode=tracking_mode,
        tracking_params=tracking_params,
    )
    if build_id is not None:
        await edition_store.set_current_build(
            edition_id=edition.id, build_id=build_id
        )
    return edition.id


@pytest.mark.asyncio
async def test_find_matching_semver_release(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """semver_release matches stable semver tags, not prereleases."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await _create_edition_internal(
            edition_store,
            project_id,
            slug="latest",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.semver_release,
        )

        # Stable tag matches
        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="v1.0.0"
        )
        assert len(matched) == 1

        # Prerelease does NOT match
        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="v1.0.0-rc.1"
        )
        assert len(matched) == 0

        # Non-semver does NOT match
        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="main"
        )
        assert len(matched) == 0
        await db_session.commit()


@pytest.mark.asyncio
async def test_find_matching_semver_major(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """semver_major matches stable tags with the correct major version."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await _create_edition_internal(
            edition_store,
            project_id,
            slug="2",
            kind=EditionKind.major,
            tracking_mode=TrackingMode.semver_major,
            tracking_params={"major_version": 2},
        )

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="v2.1.0"
        )
        assert len(matched) == 1

        # Wrong major
        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="v3.0.0"
        )
        assert len(matched) == 0

        # Prerelease
        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="v2.0.0-rc.1"
        )
        assert len(matched) == 0
        await db_session.commit()


@pytest.mark.asyncio
async def test_find_matching_semver_minor(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """semver_minor matches stable tags with correct major+minor."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await _create_edition_internal(
            edition_store,
            project_id,
            slug="2.1",
            kind=EditionKind.minor,
            tracking_mode=TrackingMode.semver_minor,
            tracking_params={"major_version": 2, "minor_version": 1},
        )

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="v2.1.5"
        )
        assert len(matched) == 1

        # Wrong minor
        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="v2.2.0"
        )
        assert len(matched) == 0
        await db_session.commit()


@pytest.mark.asyncio
async def test_find_matching_eups_major(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """eups_major_release matches EUPS major version tags."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await _create_edition_internal(
            edition_store,
            project_id,
            slug="eups-latest",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.eups_major_release,
        )

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="v12_0"
        )
        assert len(matched) == 1

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="main"
        )
        assert len(matched) == 0
        await db_session.commit()


@pytest.mark.asyncio
async def test_find_matching_eups_weekly(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """eups_weekly_release matches EUPS weekly tags."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await _create_edition_internal(
            edition_store,
            project_id,
            slug="weekly",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.eups_weekly_release,
        )

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="w_2024_05"
        )
        assert len(matched) == 1

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="v12_0"
        )
        assert len(matched) == 0
        await db_session.commit()


@pytest.mark.asyncio
async def test_find_matching_eups_daily(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """eups_daily_release matches EUPS daily tags."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await _create_edition_internal(
            edition_store,
            project_id,
            slug="daily",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.eups_daily_release,
        )

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="d_2024_01_15"
        )
        assert len(matched) == 1

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="w_2024_05"
        )
        assert len(matched) == 0
        await db_session.commit()


@pytest.mark.asyncio
async def test_find_matching_lsst_doc_version(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """lsst_doc matches document version tags."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await _create_edition_internal(
            edition_store,
            project_id,
            slug="current",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.lsst_doc,
        )

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="v1.0"
        )
        assert len(matched) == 1
        await db_session.commit()


@pytest.mark.asyncio
async def test_find_matching_lsst_doc_main_unpublished(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """lsst_doc accepts main when edition is unpublished."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await _create_edition_internal(
            edition_store,
            project_id,
            slug="current",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.lsst_doc,
        )

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="main"
        )
        assert len(matched) == 1
        await db_session.commit()


@pytest.mark.asyncio
async def test_find_matching_lsst_doc_main_when_showing_main(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """lsst_doc accepts main when currently showing main."""
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        project_id = await _create_project(db_session)
        build_store = BuildStore(session=db_session, logger=logger)
        main_build = await build_store.create(
            project_id=project_id,
            data=BuildCreate(git_ref="main", content_hash=_HASH),
            uploader="testuser",
            project_slug="ed-proj",
        )
        edition = await edition_store.create_internal(
            project_id=project_id,
            slug="current",
            title="current",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.lsst_doc,
        )
        await edition_store.set_current_build(
            edition_id=edition.id, build_id=main_build.id
        )

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="main"
        )
        assert len(matched) == 1
        await db_session.commit()


@pytest.mark.asyncio
async def test_find_matching_lsst_doc_main_rejected_when_showing_version(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """lsst_doc rejects main when currently showing a version tag."""
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        project_id = await _create_project(db_session)
        build_store = BuildStore(session=db_session, logger=logger)
        version_build = await build_store.create(
            project_id=project_id,
            data=BuildCreate(git_ref="v1.0", content_hash=_HASH),
            uploader="testuser",
            project_slug="ed-proj",
        )
        edition = await edition_store.create_internal(
            project_id=project_id,
            slug="current",
            title="current",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.lsst_doc,
        )
        await edition_store.set_current_build(
            edition_id=edition.id, build_id=version_build.id
        )

        matched = await edition_store.find_matching_editions(
            project_id=project_id, git_ref="main"
        )
        assert len(matched) == 0
        await db_session.commit()


# ── Main-slug/kind invariant (ck_editions_main_slug_kind) ─────────────────


@pytest.mark.asyncio
async def test_main_kind_requires_main_slug(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """kind=main with a non-__main slug must violate the CHECK."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await db_session.commit()
    with pytest.raises(IntegrityError):
        async with db_session.begin():
            await edition_store.create_internal(
                project_id=project_id,
                slug="not-main",
                title="Not Main",
                kind=EditionKind.main,
                tracking_mode=TrackingMode.git_ref,
            )


@pytest.mark.asyncio
async def test_main_slug_requires_main_kind(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """slug=__main with a non-main kind must violate the CHECK."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await db_session.commit()
    with pytest.raises(IntegrityError):
        async with db_session.begin():
            await edition_store.create_internal(
                project_id=project_id,
                slug="__main",
                title="Squatter",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
            )


@pytest.mark.asyncio
async def test_second_main_edition_returns_existing(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A second ``__main`` insert resolves to the existing row, no raise.

    ``create_internal`` is race-tolerant via ``ON CONFLICT DO NOTHING``
    on ``uq_editions_project_lower_slug``: the unique-slug invariant is
    still preserved (only one row exists), but the contract is "return
    the existing edition" rather than "raise IntegrityError".
    """
    async with db_session.begin():
        project_id = await _create_project(db_session)
        first = await edition_store.create_internal(
            project_id=project_id,
            slug="__main",
            title="Main",
            kind=EditionKind.main,
            tracking_mode=TrackingMode.git_ref,
        )
        await db_session.commit()
    async with db_session.begin():
        second = await edition_store.create_internal(
            project_id=project_id,
            slug="__main",
            title="Main duplicate",
            kind=EditionKind.main,
            tracking_mode=TrackingMode.git_ref,
        )
        await db_session.commit()
    assert second.id == first.id
    assert second.title == "Main"  # original title preserved on conflict


# ── Case-insensitive slug uniqueness ───────────────────────────────────────


@pytest.mark.asyncio
async def test_case_only_different_slugs_rejected_by_db(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """Slugs differing only by case in one project violate the unique index."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="DM-54112",
                title="Ticket",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        await db_session.commit()
    with pytest.raises(IntegrityError):
        async with db_session.begin():
            await edition_store.create(
                project_id=project_id,
                data=EditionCreate(
                    slug="dm-54112",
                    title="Lowercase duplicate",
                    kind=EditionKind.draft,
                    tracking_mode=TrackingMode.git_ref,
                ),
            )


@pytest.mark.asyncio
async def test_get_by_slug_case_insensitive(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """get_by_slug matches across case and skips soft-deleted rows."""
    async with db_session.begin():
        org_id, project_id = await _create_project_with_org(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="DM-54112",
                title="Ticket",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
            ),
        )
        # Lowercase form resolves; canonical stored casing is returned.
        edition = await edition_store.get_by_slug(
            project_id=project_id, slug="dm-54112"
        )
        assert edition is not None
        assert edition.slug == "DM-54112"
        # Original case also resolves.
        edition = await edition_store.get_by_slug(
            project_id=project_id, slug="DM-54112"
        )
        assert edition is not None
        assert edition.slug == "DM-54112"
        # Different slug returns None.
        assert (
            await edition_store.get_by_slug(
                project_id=project_id, slug="other-slug"
            )
            is None
        )
        await db_session.commit()

    # Soft-deleting the row releases the slug for reuse.
    async with db_session.begin():
        await edition_store.soft_delete(
            org_id=org_id,
            project_id=project_id,
            slug="DM-54112",
            reason=TombstoneReason.manual_delete,
        )
        assert (
            await edition_store.get_by_slug(
                project_id=project_id, slug="dm-54112"
            )
            is None
        )
        await db_session.commit()


# ── Race-tolerant create_internal (uq_editions_project_lower_slug) ─────────


@pytest.mark.asyncio
async def test_create_internal_returns_existing_when_row_exists(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A second ``create_internal`` resolves to the existing row.

    Maps the storage-layer guarantee that backs ``KeeperSyncService.
    _ensure_edition`` called twice back-to-back for an already-existing
    edition: the second call must return the existing row without
    raising ``IntegrityError`` from ``uq_editions_project_lower_slug``.
    """
    async with db_session.begin():
        project_id = await _create_project(db_session)
        first = await edition_store.create_internal(
            project_id=project_id,
            slug="DM-28900",
            title="DM-28900",
            kind=EditionKind.draft,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "tickets/DM-28900"},
        )
        await db_session.commit()

    async with db_session.begin():
        second = await edition_store.create_internal(
            project_id=project_id,
            slug="DM-28900",
            title="DM-28900 (retry)",
            kind=EditionKind.draft,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "tickets/DM-28900"},
        )
        await db_session.commit()

    assert second.id == first.id

    async with db_session.begin():
        result = await db_session.execute(
            select(SqlEdition).where(SqlEdition.project_id == project_id)
        )
        rows = result.scalars().all()
        await db_session.commit()
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_create_internal_concurrent_same_slug(
    app: FastAPI,
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """Two concurrent ``create_internal`` calls converge on one row.

    Reproduces the QA failure addressed by this issue: two
    ``keeper_sync_project`` workers racing through ``_ensure_edition``
    for the same ``(project_id, slug)``. Without ``ON CONFLICT DO
    NOTHING`` on ``uq_editions_project_lower_slug``, one INSERT loses
    the race and crashes with ``IntegrityError``. With it, both
    transactions succeed: the loser silently re-fetches the winning
    row.
    """
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await db_session.commit()

    engine = create_database_engine(
        config.database_url, config.database_password
    )
    try:
        maker = async_sessionmaker(engine, expire_on_commit=False)

        async def insert() -> int:
            async with maker() as session:
                store = EditionStore(session=session, logger=logger)
                async with session.begin():
                    edition = await store.create_internal(
                        project_id=project_id,
                        slug="DM-28900",
                        title="DM-28900",
                        kind=EditionKind.draft,
                        tracking_mode=TrackingMode.git_ref,
                        tracking_params={"git_ref": "tickets/DM-28900"},
                    )
                    await session.commit()
                return edition.id

        edition_id_a, edition_id_b = await asyncio.gather(insert(), insert())
    finally:
        await engine.dispose()

    assert edition_id_a == edition_id_b

    async with db_session.begin():
        result = await db_session.execute(
            select(SqlEdition).where(
                SqlEdition.project_id == project_id,
                func.lower(SqlEdition.slug) == "dm-28900",
            )
        )
        rows = result.scalars().all()
        await db_session.commit()
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_create_internal_concurrent_mixed_case_slug(
    app: FastAPI,
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """Concurrent inserts with mixed-case slugs converge on one row.

    ``get_by_slug`` and ``uq_editions_project_lower_slug`` are both
    case-insensitive, so two callers passing ``DM-28900`` and
    ``dm-28900`` for the same project must collapse onto one edition
    row regardless of insert order.
    """
    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await db_session.commit()

    engine = create_database_engine(
        config.database_url, config.database_password
    )
    try:
        maker = async_sessionmaker(engine, expire_on_commit=False)

        async def insert(slug: str) -> int:
            async with maker() as session:
                store = EditionStore(session=session, logger=logger)
                async with session.begin():
                    edition = await store.create_internal(
                        project_id=project_id,
                        slug=slug,
                        title=slug,
                        kind=EditionKind.draft,
                        tracking_mode=TrackingMode.git_ref,
                        tracking_params={"git_ref": f"tickets/{slug}"},
                    )
                    await session.commit()
                return edition.id

        edition_id_upper, edition_id_lower = await asyncio.gather(
            insert("DM-28900"), insert("dm-28900")
        )
    finally:
        await engine.dispose()

    assert edition_id_upper == edition_id_lower

    async with db_session.begin():
        result = await db_session.execute(
            select(SqlEdition).where(
                SqlEdition.project_id == project_id,
                func.lower(SqlEdition.slug) == "dm-28900",
            )
        )
        rows = result.scalars().all()
        await db_session.commit()
    assert len(rows) == 1


# ── list_draft_editions_by_git_ref ────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_draft_editions_by_git_ref_selects_matching_draft(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A draft edition tracking the deleted ref is selected.

    Pins the happy-path filter used by
    :class:`docverse.services.ref_deleted_processor
    .RefDeletedWebhookProcessor`: server-side filter on
    ``kind='draft' AND tracking_mode IN ('git_ref',
    'alternate_git_ref') AND lifecycle_exempt=False AND
    date_deleted IS NULL AND tracking_params->>'git_ref' = :ref``.
    """
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="dm-1",
                title="DM-1",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "tickets/DM-1"},
            ),
        )
        result = await edition_store.list_draft_editions_by_git_ref(
            project_id=project_id, git_ref="tickets/DM-1"
        )
        await db_session.commit()
    assert [e.slug for e in result] == ["dm-1"]


@pytest.mark.asyncio
async def test_list_draft_editions_by_git_ref_excludes_release(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A release-kind edition pinned to the same ref is excluded.

    The webhook fast path is strictly for draft branch-tracking
    editions; a release edition pinned to a tag survives an upstream
    force-recreate of that tag.
    """
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="v1",
                title="v1",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "v1"},
            ),
        )
        result = await edition_store.list_draft_editions_by_git_ref(
            project_id=project_id, git_ref="v1"
        )
        await db_session.commit()
    assert result == []


@pytest.mark.asyncio
async def test_list_draft_editions_by_git_ref_excludes_lifecycle_exempt(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A draft edition with ``lifecycle_exempt=True`` is excluded."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="demo",
                title="Demo",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "demo"},
                lifecycle_exempt=True,
            ),
        )
        result = await edition_store.list_draft_editions_by_git_ref(
            project_id=project_id, git_ref="demo"
        )
        await db_session.commit()
    assert result == []


@pytest.mark.asyncio
async def test_list_draft_editions_by_git_ref_excludes_soft_deleted(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """Already-soft-deleted draft editions are filtered out."""
    async with db_session.begin():
        org_id, project_id = await _create_project_with_org(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="dm-2",
                title="DM-2",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "tickets/DM-2"},
            ),
        )
        await edition_store.soft_delete(
            org_id=org_id,
            project_id=project_id,
            slug="dm-2",
            reason=TombstoneReason.manual_delete,
        )
        result = await edition_store.list_draft_editions_by_git_ref(
            project_id=project_id, git_ref="tickets/DM-2"
        )
        await db_session.commit()
    assert result == []


@pytest.mark.asyncio
async def test_list_draft_editions_by_git_ref_excludes_wrong_tracking_mode(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A draft edition with a non-literal-ref tracking mode is excluded."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="v2",
                title="v2",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.semver_release,
                tracking_params={"git_ref": "v2"},
            ),
        )
        result = await edition_store.list_draft_editions_by_git_ref(
            project_id=project_id, git_ref="v2"
        )
        await db_session.commit()
    assert result == []


@pytest.mark.asyncio
async def test_list_draft_editions_by_git_ref_includes_alternate_git_ref(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """An ``alternate_git_ref`` draft on the same ref is included.

    Mirrors the lifecycle evaluator's literal-ref candidate set: both
    ``git_ref`` and ``alternate_git_ref`` modes are branch-tracking, so
    a deletion of the underlying ref applies to both.
    """
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="dm-3-usdf",
                title="DM-3 (usdf)",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.alternate_git_ref,
                tracking_params={
                    "git_ref": "tickets/DM-3",
                    "alternate_name": "usdf",
                },
            ),
        )
        result = await edition_store.list_draft_editions_by_git_ref(
            project_id=project_id, git_ref="tickets/DM-3"
        )
        await db_session.commit()
    assert [e.slug for e in result] == ["dm-3-usdf"]


@pytest.mark.asyncio
async def test_list_draft_editions_by_git_ref_excludes_other_ref(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """A draft edition tracking a different ref is not returned.

    The server-side ``tracking_params->>'git_ref' = :ref`` filter is
    what keeps a delete event for ``feature-x`` from sweeping up an
    edition on ``feature-y`` in the same project.
    """
    async with db_session.begin():
        project_id = await _create_project(db_session)
        await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="feature-y",
                title="Feature Y",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "feature-y"},
            ),
        )
        result = await edition_store.list_draft_editions_by_git_ref(
            project_id=project_id, git_ref="feature-x"
        )
        await db_session.commit()
    assert result == []


@pytest.mark.asyncio
async def test_list_draft_editions_by_git_ref_scoped_to_project(
    db_session: AsyncSession,
    edition_store: EditionStore,
) -> None:
    """Editions in a sibling project on the same ref are not returned."""
    async with db_session.begin():
        project_id = await _create_project(db_session)
        logger = structlog.get_logger("docverse")
        proj_store = ProjectStore(session=db_session, logger=logger)
        org_store = OrganizationStore(session=db_session, logger=logger)
        other_org = await org_store.create(
            OrganizationCreate(
                slug="ed-org-other",
                title="Other Org",
                base_domain="ed-other.example.com",
            )
        )
        other = await proj_store.create(
            org_id=other_org.id,
            data=ProjectCreate(
                slug="ed-proj-other",
                title="Other Proj",
                source_url="https://example.com/other/repo",
            ),
        )
        await edition_store.create(
            project_id=other.id,
            data=EditionCreate(
                slug="feature-z",
                title="Feature Z",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "feature-z"},
            ),
        )
        result = await edition_store.list_draft_editions_by_git_ref(
            project_id=project_id, git_ref="feature-z"
        )
        await db_session.commit()
    assert result == []
