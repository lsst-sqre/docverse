"""Tests for :class:`InventoryCensusStore`.

Seeds orgs, projects, editions, and builds — including soft-deleted rows
and a soft-deleted project — and asserts the read-only grouped aggregate
excludes everything deleted, rolls projects up to their org, and emits a
row for an org with no projects.
"""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from docverse.client.models import (
    BuildCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.dbschema.build import SqlBuild
from docverse.dbschema.edition import SqlEdition
from docverse.dbschema.project import SqlProject
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.inventory_census_store import InventoryCensusStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

_CONTENT_HASH = (
    "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _make_build(
    db_session: AsyncSession,
    *,
    project_id: int,
    project_slug: str,
    git_ref: str,
    total_size_bytes: int | None,
    deleted: bool = False,
) -> None:
    """Create one build with an explicit size, optionally soft-deleted."""
    build_store = BuildStore(session=db_session, logger=_logger())
    build = await build_store.create(
        project_id=project_id,
        project_slug=project_slug,
        data=BuildCreate(git_ref=git_ref, content_hash=_CONTENT_HASH),
        uploader="testuser",
    )
    row = await db_session.get(SqlBuild, build.id)
    assert row is not None
    row.total_size_bytes = total_size_bytes
    if deleted:
        row.date_deleted = func.now()
    await db_session.flush()


@pytest.mark.asyncio
async def test_aggregate_inventory_excludes_deleted_and_rolls_up(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Census counts active rows only, rolled up per org and per project."""
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)

    async with db_session.begin():
        alpha = await org_store.create(
            OrganizationCreate(
                slug="alpha",
                title="Alpha",
                base_domain="alpha.example.com",
            )
        )
        # ``beta`` deliberately has no projects: it must still yield one
        # org census row with project_count == 0.
        await org_store.create(
            OrganizationCreate(
                slug="beta",
                title="Beta",
                base_domain="beta.example.com",
            )
        )

        p_one = await proj_store.create(
            org_id=alpha.id,
            data=ProjectCreate(
                slug="p-one",
                title="Project One",
                source_url="https://example.com/example/one",
            ),
        )
        await proj_store.create(
            org_id=alpha.id,
            data=ProjectCreate(
                slug="p-two",
                title="Project Two",
                source_url="https://example.com/example/two",
            ),
        )
        p_gone = await proj_store.create(
            org_id=alpha.id,
            data=ProjectCreate(
                slug="p-gone",
                title="Project Gone",
                source_url="https://example.com/example/gone",
            ),
        )

        # p-one: 2 active editions + 1 soft-deleted edition.
        for slug in ("__main", "v1"):
            await edition_store.create_internal(
                project_id=p_one.id,
                slug=slug,
                title=slug,
                kind=EditionKind.main
                if slug == "__main"
                else EditionKind.release,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            )
        dead_edition = await edition_store.create_internal(
            project_id=p_one.id,
            slug="v0",
            title="v0",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "v0"},
        )
        dead_edition_row = await db_session.get(SqlEdition, dead_edition.id)
        assert dead_edition_row is not None
        dead_edition_row.date_deleted = func.now()

        # p-one: 2 sized active builds + 1 NULL-sized active build + 1
        # soft-deleted build. total_build_bytes == 100 + 200 == 300,
        # build_count == 3 (the NULL-sized build still counts).
        await _make_build(
            db_session,
            project_id=p_one.id,
            project_slug=p_one.slug,
            git_ref="a",
            total_size_bytes=100,
        )
        await _make_build(
            db_session,
            project_id=p_one.id,
            project_slug=p_one.slug,
            git_ref="b",
            total_size_bytes=200,
        )
        await _make_build(
            db_session,
            project_id=p_one.id,
            project_slug=p_one.slug,
            git_ref="c",
            total_size_bytes=None,
        )
        await _make_build(
            db_session,
            project_id=p_one.id,
            project_slug=p_one.slug,
            git_ref="d",
            total_size_bytes=999,
            deleted=True,
        )

        # p-gone is soft-deleted but still carries an active edition and
        # an active build — both must be excluded from the census.
        await edition_store.create_internal(
            project_id=p_gone.id,
            slug="__main",
            title="main",
            kind=EditionKind.main,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        )
        await _make_build(
            db_session,
            project_id=p_gone.id,
            project_slug=p_gone.slug,
            git_ref="x",
            total_size_bytes=500,
        )
        gone_row = await db_session.get(SqlProject, p_gone.id)
        assert gone_row is not None
        gone_row.date_deleted = func.now()

    store = InventoryCensusStore(session=db_session, logger=logger)
    async with db_session.begin():
        census = await store.aggregate_inventory()

    orgs = {o.org_slug: o for o in census.orgs}
    assert set(orgs) == {"alpha", "beta"}
    assert orgs["alpha"].project_count == 2
    assert orgs["alpha"].edition_count == 2
    assert orgs["alpha"].build_count == 3
    assert orgs["alpha"].total_build_bytes == 300
    assert orgs["beta"].project_count == 0
    assert orgs["beta"].edition_count == 0
    assert orgs["beta"].build_count == 0
    assert orgs["beta"].total_build_bytes == 0

    projects = {p.project_slug: p for p in census.projects}
    assert set(projects) == {"p-one", "p-two"}
    assert projects["p-one"].org_slug == "alpha"
    assert projects["p-one"].edition_count == 2
    assert projects["p-one"].build_count == 3
    assert projects["p-one"].total_build_bytes == 300
    assert projects["p-two"].edition_count == 0
    assert projects["p-two"].build_count == 0
    assert projects["p-two"].total_build_bytes == 0
