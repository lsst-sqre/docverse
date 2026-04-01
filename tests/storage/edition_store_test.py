"""Tests for EditionStore."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
import structlog
from sqlalchemy import select
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
from docverse.dbschema.build import SqlBuild
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.pagination import EditionSlugCursor
from docverse.storage.project_store import ProjectStore

_HASH = "sha256:" + "a" * 64


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
        result = await edition_store.list_by_project(
            project_id,
            cursor_type=EditionSlugCursor,
            limit=25,
        )
        await db_session.commit()
    assert len(result.entries) == 2


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
    assert updated is not None
    assert updated.current_build_id == build.id
    assert updated.current_build_public_id == build.public_id


@pytest.mark.asyncio
async def test_set_current_build_skips_stale(
    db_session: async_scoped_session[AsyncSession],
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
        )
        older_build = await build_store.create(
            project_id=project_id,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:bbbb" + "0" * 60,
            ),
            uploader="testuser",
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
                kind=EditionKind.main,
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
    db_session: async_scoped_session[AsyncSession],
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
        )
        build_b = await build_store.create(
            project_id=project_id,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:dddd" + "0" * 60,
            ),
            uploader="testuser",
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
                kind=EditionKind.main,
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
    db_session: async_scoped_session[AsyncSession],
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
        )
        newer_build = await build_store.create(
            project_id=project_id,
            data=BuildCreate(
                git_ref="main",
                content_hash="sha256:ffff" + "0" * 60,
            ),
            uploader="testuser",
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
                kind=EditionKind.main,
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


@pytest.mark.asyncio
async def test_find_matching_editions_git_ref(
    db_session: async_scoped_session[AsyncSession],
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
                kind=EditionKind.main,
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
    db_session: async_scoped_session[AsyncSession],
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
                kind=EditionKind.main,
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
    db_session: async_scoped_session[AsyncSession],
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
