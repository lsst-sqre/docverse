"""Tests for EditionBuildHistoryStore."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.client.models import (
    BuildCreate,
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore


@pytest.fixture
def history_store(
    db_session: async_scoped_session[AsyncSession],
) -> EditionBuildHistoryStore:
    logger = structlog.get_logger("docverse")
    return EditionBuildHistoryStore(session=db_session, logger=logger)


async def _create_edition_and_builds(
    db_session: async_scoped_session[AsyncSession],
    *,
    n_builds: int = 1,
    edition_slug: str = "main",
    org_slug: str = "hist-org",
    project_slug: str = "hist-proj",
) -> tuple[int, int, list[int]]:
    """Create an org, project, edition, and n builds.

    Returns (edition_id, project_id, [build_ids]).
    """
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)

    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title=f"Org {org_slug}",
            base_domain=f"{org_slug}.example.com",
        )
    )
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug=project_slug,
            title=f"Project {project_slug}",
            doc_repo="https://github.com/example/repo",
        ),
    )
    edition = await edition_store.create(
        project_id=project.id,
        data=EditionCreate(
            slug=edition_slug,
            title=f"Edition {edition_slug}",
            kind=EditionKind.main,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        ),
    )
    build_ids: list[int] = []
    for i in range(n_builds):
        build = await build_store.create(
            project_id=project.id,
            data=BuildCreate(
                git_ref="main",
                content_hash=(f"sha256:{i:064x}"),
            ),
            uploader="testuser",
        )
        build_ids.append(build.id)
    return edition.id, project.id, build_ids


@pytest.mark.asyncio
async def test_first_record(
    db_session: async_scoped_session[AsyncSession],
    history_store: EditionBuildHistoryStore,
) -> None:
    """First history entry gets position 1."""
    async with db_session.begin():
        edition_id, _, build_ids = await _create_edition_and_builds(
            db_session, n_builds=1
        )
        entry = await history_store.record(
            edition_id=edition_id, build_id=build_ids[0]
        )
        await db_session.commit()
    assert entry.position == 1
    assert entry.build_id == build_ids[0]
    assert entry.edition_id == edition_id


@pytest.mark.asyncio
async def test_subsequent_records_shift_positions(
    db_session: async_scoped_session[AsyncSession],
    history_store: EditionBuildHistoryStore,
) -> None:
    """Recording a second build shifts the first to position 2."""
    async with db_session.begin():
        edition_id, _, build_ids = await _create_edition_and_builds(
            db_session, n_builds=2
        )
        await history_store.record(
            edition_id=edition_id, build_id=build_ids[0]
        )
        entry_b = await history_store.record(
            edition_id=edition_id, build_id=build_ids[1]
        )
        history = await history_store.list_by_edition(edition_id)
        await db_session.commit()

    assert entry_b.position == 1
    assert len(history) == 2
    assert history[0].build_id == build_ids[1]
    assert history[0].position == 1
    assert history[1].build_id == build_ids[0]
    assert history[1].position == 2


@pytest.mark.asyncio
async def test_editions_independent(
    db_session: async_scoped_session[AsyncSession],
    history_store: EditionBuildHistoryStore,
) -> None:
    """Shifting positions for one edition does not affect another."""
    async with db_session.begin():
        ed1_id, project_id, builds1 = await _create_edition_and_builds(
            db_session,
            n_builds=2,
            edition_slug="ed-one",
            org_slug="ind-org",
            project_slug="ind-proj",
        )
        # Create a second edition in the same project
        logger = structlog.get_logger("docverse")
        edition_store = EditionStore(session=db_session, logger=logger)
        ed2 = await edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug="ed-two",
                title="Edition Two",
                kind=EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "v1.0"},
            ),
        )

        # Record a build for each edition
        await history_store.record(edition_id=ed1_id, build_id=builds1[0])
        await history_store.record(edition_id=ed2.id, build_id=builds1[0])

        # Record a second build for edition 1 only
        await history_store.record(edition_id=ed1_id, build_id=builds1[1])

        history_ed1 = await history_store.list_by_edition(ed1_id)
        history_ed2 = await history_store.list_by_edition(ed2.id)
        await db_session.commit()

    # Edition 1 has two entries, shifted
    assert len(history_ed1) == 2
    assert history_ed1[0].position == 1
    assert history_ed1[1].position == 2

    # Edition 2 still has one entry at position 1
    assert len(history_ed2) == 1
    assert history_ed2[0].position == 1


@pytest.mark.asyncio
async def test_list_with_build_info_includes_status(
    db_session: async_scoped_session[AsyncSession],
    history_store: EditionBuildHistoryStore,
) -> None:
    """list_by_edition_with_build_info returns status fields."""
    async with db_session.begin():
        edition_id, _, build_ids = await _create_edition_and_builds(
            db_session, n_builds=1, org_slug="status-org"
        )
        await history_store.record(
            edition_id=edition_id, build_id=build_ids[0]
        )
        result = await history_store.list_by_edition_with_build_info(
            edition_id, limit=10, include_deleted=True
        )
        await db_session.commit()

    assert len(result.entries) == 1
    entry = result.entries[0]
    assert entry.build_status == "pending"
    assert entry.build_date_deleted is None


@pytest.mark.asyncio
async def test_list_with_build_info_filters_deleted(
    db_session: async_scoped_session[AsyncSession],
    history_store: EditionBuildHistoryStore,
) -> None:
    """Soft-deleted builds are excluded by default."""
    async with db_session.begin():
        edition_id, _, build_ids = await _create_edition_and_builds(
            db_session, n_builds=2, org_slug="del-org"
        )
        await history_store.record(
            edition_id=edition_id, build_id=build_ids[0]
        )
        await history_store.record(
            edition_id=edition_id, build_id=build_ids[1]
        )

        # Soft-delete the first build
        logger = structlog.get_logger("docverse")
        build_store = BuildStore(session=db_session, logger=logger)
        await build_store.soft_delete(build_id=build_ids[0])

        # Default: exclude deleted
        result_default = await history_store.list_by_edition_with_build_info(
            edition_id, limit=10
        )
        # Explicit include
        result_all = await history_store.list_by_edition_with_build_info(
            edition_id, limit=10, include_deleted=True
        )
        await db_session.commit()

    assert result_default.count == 1
    assert result_all.count == 2
    deleted_entries = [
        e for e in result_all.entries if e.build_date_deleted is not None
    ]
    assert len(deleted_entries) == 1
