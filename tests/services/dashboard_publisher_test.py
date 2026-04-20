"""Integration tests for DashboardPublisher."""

from __future__ import annotations

import json

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    BuildCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.config import Configuration
from docverse.services.dashboard_publisher import DashboardPublisher
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.objectstore import MockObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

_HASH = "sha256:" + "a" * 64


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _make_publisher(session: AsyncSession) -> DashboardPublisher:
    logger = _logger()
    return DashboardPublisher(
        org_store=OrganizationStore(session=session, logger=logger),
        project_store=ProjectStore(session=session, logger=logger),
        edition_store=EditionStore(session=session, logger=logger),
        build_store=BuildStore(session=session, logger=logger),
        config=Configuration(),
        logger=logger,
    )


@pytest.mark.asyncio
async def test_publisher_uploads_dashboard_and_switcher(
    db_session: AsyncSession,
) -> None:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="pub-org",
                title="Pub Org",
                base_domain="pub.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="pub-proj",
                title="Pub Project",
                doc_repo="https://github.com/example/pub",
            ),
        )
        await edition_store.create_internal(
            project_id=project.id,
            slug="__main",
            title="Latest",
            kind=EditionKind.main,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        )
        build = await build_store.create(
            project_id=project.id,
            data=BuildCreate(git_ref="v1.0.0", content_hash=_HASH),
            uploader="testuser",
            project_slug="pub-proj",
        )
        release = await edition_store.create_internal(
            project_id=project.id,
            slug="v1.0.0",
            title="v1.0.0",
            kind=EditionKind.release,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "v1.0.0"},
        )
        await edition_store.set_current_build(
            edition_id=release.id,
            build_id=build.id,
            skip_date_guard=True,
        )
        await db_session.commit()

    publisher = _make_publisher(db_session)
    mock_store = MockObjectStore()

    async def _provider() -> MockObjectStore:
        return mock_store

    async with db_session.begin():
        context, progress = await publisher.publish(
            org_id=org.id,
            project_id=project.id,
            object_store_provider=_provider,
        )

    assert progress.object_count == 3
    assert progress.total_size_bytes > 0

    html_obj = mock_store.objects["pub-proj/__dashboard.html"]
    assert html_obj.content_type == "text/html; charset=utf-8"
    html_text = html_obj.data.decode("utf-8")
    assert "v1.0.0" in html_text
    # Assets from template.toml must be inlined into the rendered HTML:
    # CSS in a single <style>, JS in a single <script>, SVG raw, and the
    # PNG favicon as a base64 data URI.
    assert "<style>" in html_text
    assert "<script>" in html_text
    assert "<svg" in html_text
    assert "data:image/png;base64," in html_text

    switcher_obj = mock_store.objects["pub-proj/__switcher.json"]
    assert switcher_obj.content_type == "application/json; charset=utf-8"
    payload = json.loads(switcher_obj.data.decode("utf-8"))
    versions = [entry["version"] for entry in payload]
    assert versions == ["__main", "v1.0.0"]

    error_obj = mock_store.objects["pub-proj/__404.html"]
    assert error_obj.content_type == "text/html; charset=utf-8"
    error_text = error_obj.data.decode("utf-8")
    assert "404" in error_text
    assert "Pub Project" in error_text

    # rendered_at is shared across artifacts (single context)
    assert context.rendered_at.isoformat() in html_obj.data.decode("utf-8")
    assert context.rendered_at.isoformat() in error_obj.data.decode("utf-8")


@pytest.mark.asyncio
async def test_publisher_handles_empty_project(
    db_session: AsyncSession,
) -> None:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)

    async with db_session.begin():
        org = await org_store.create(
            OrganizationCreate(
                slug="empty-pub-org",
                title="Empty",
                base_domain="empty.example.com",
            )
        )
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="empty-pub-proj",
                title="Empty Project",
                doc_repo="https://github.com/example/empty",
            ),
        )
        await db_session.commit()

    publisher = _make_publisher(db_session)
    mock_store = MockObjectStore()

    async def _provider() -> MockObjectStore:
        return mock_store

    async with db_session.begin():
        await publisher.publish(
            org_id=org.id,
            project_id=project.id,
            object_store_provider=_provider,
        )

    assert "empty-pub-proj/__dashboard.html" in mock_store.objects
    switcher = mock_store.objects["empty-pub-proj/__switcher.json"]
    assert json.loads(switcher.data.decode("utf-8")) == []

    error = mock_store.objects["empty-pub-proj/__404.html"]
    assert error.content_type == "text/html; charset=utf-8"
    assert "404" in error.data.decode("utf-8")
