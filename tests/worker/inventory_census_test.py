"""Integration tests for the ``inventory_census`` worker function.

Seeds orgs, projects, editions, and builds — including soft-deleted rows
and a soft-deleted project — and asserts the worker publishes one
org-scoped ``resource_inventory`` row per org plus one project-scoped row
per non-deleted project, excluding everything deleted.
"""

from __future__ import annotations

import httpx
import pytest
import structlog
from safir.metrics import MockEventPublisher
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from docverse.client.models import (
    BuildCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.config import Configuration
from docverse.dbschema.build import SqlBuild
from docverse.dbschema.edition import SqlEdition
from docverse.dbschema.project import SqlProject
from docverse.metrics import build_event_manager
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.worker.functions.inventory_census import inventory_census
from tests.worker.conftest import make_worker_ctx

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
    total_size_bytes: int,
    deleted: bool = False,
) -> None:
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
async def test_inventory_census_publishes_org_and_project_rows(
    app: None,
    db_session: AsyncSession,
) -> None:
    """The census emits one org row + one row per live project, no deleted."""
    logger = _logger()
    _manager, events = await build_event_manager(Configuration())

    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="census-org",
                title="Census Org",
                base_domain="census.example.com",
            )
        )
        kept = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="kept",
                title="Kept",
                source_url="https://example.com/example/kept",
            ),
        )
        removed = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="removed",
                title="Removed",
                source_url="https://example.com/example/removed",
            ),
        )

        # kept: 2 active editions + 1 soft-deleted edition.
        for slug, kind in (
            ("__main", EditionKind.main),
            ("v1", EditionKind.release),
        ):
            await edition_store.create_internal(
                project_id=kept.id,
                slug=slug,
                title=slug,
                kind=kind,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            )
        dead_edition = await edition_store.create_internal(
            project_id=kept.id,
            slug="v0",
            title="v0",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "v0"},
        )
        dead_edition_row = await db_session.get(SqlEdition, dead_edition.id)
        assert dead_edition_row is not None
        dead_edition_row.date_deleted = func.now()

        # kept: 2 active builds (10 + 20) + 1 soft-deleted build (99).
        await _make_build(
            db_session,
            project_id=kept.id,
            project_slug=kept.slug,
            git_ref="a",
            total_size_bytes=10,
        )
        await _make_build(
            db_session,
            project_id=kept.id,
            project_slug=kept.slug,
            git_ref="b",
            total_size_bytes=20,
        )
        await _make_build(
            db_session,
            project_id=kept.id,
            project_slug=kept.slug,
            git_ref="c",
            total_size_bytes=99,
            deleted=True,
        )

        # removed is soft-deleted but carries an active edition + build,
        # both of which must be excluded from the census.
        await edition_store.create_internal(
            project_id=removed.id,
            slug="__main",
            title="main",
            kind=EditionKind.main,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        )
        await _make_build(
            db_session,
            project_id=removed.id,
            project_slug=removed.slug,
            git_ref="x",
            total_size_bytes=500,
        )
        removed_row = await db_session.get(SqlProject, removed.id)
        assert removed_row is not None
        removed_row.date_deleted = func.now()

    http_client = httpx.AsyncClient()
    ctx = make_worker_ctx(http_client=http_client, events=events)

    result = await inventory_census(ctx)
    await ctx["http_client"].aclose()
    assert result == "completed"

    publisher = events.resource_inventory
    assert isinstance(publisher, MockEventPublisher)

    org_rows = [e for e in publisher.published if e.project is None]
    project_rows = [e for e in publisher.published if e.project is not None]

    assert len(org_rows) == 1
    org_event = org_rows[0]
    assert org_event.organization == "census-org"
    assert org_event.project_count == 1
    assert org_event.edition_count == 2
    assert org_event.build_count == 2
    assert org_event.total_build_bytes == 30

    assert len(project_rows) == 1
    project_event = project_rows[0]
    assert project_event.organization == "census-org"
    assert project_event.project == "kept"
    assert project_event.project_count is None
    assert project_event.edition_count == 2
    assert project_event.build_count == 2
    assert project_event.total_build_bytes == 30
