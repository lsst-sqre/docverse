"""Tests for the PATCH edition emergency build-override flow."""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import BuildCreate
from docverse.client.models.queue_enums import JobKind, PublishStatus
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import serialize_base32_id
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from tests.conftest import seed_org_with_admin
from tests.support.arq_testing import get_jobs_by_name


async def _setup(client: AsyncClient) -> None:
    """Create org, membership, and project."""
    await seed_org_with_admin(client, "pov-org", "testuser")
    await client.post(
        "/docverse/orgs/pov-org/projects",
        json={
            "slug": "pov-proj",
            "title": "Override Project",
            "source_url": "https://example.com/example/pov",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )


async def _create_orphan_build(
    db_session: AsyncSession,
) -> tuple[int, int]:
    """Create a build NOT recorded in any edition's history.

    Returns ``(build_internal_id, build_public_id)``.
    """
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)

    org = await org_store.get_by_slug("pov-org")
    assert org is not None
    project = await proj_store.get_by_slug(org_id=org.id, slug="pov-proj")
    assert project is not None
    build = await build_store.create(
        project_id=project.id,
        data=BuildCreate(
            git_ref="refs/tags/orphan",
            content_hash="sha256:" + "a" * 64,
        ),
        uploader="testuser",
        project_slug="pov-proj",
    )
    return build.id, build.public_id


@pytest.mark.asyncio
async def test_patch_override_success(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """PATCH with orphan build override returns 200 and pending status."""
    await _setup(client)
    async with db_session.begin():
        _, build_public_id = await _create_orphan_build(db_session)
        await db_session.commit()

    target_public_id = serialize_base32_id(build_public_id)
    response = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["slug"] == "__main"
    assert data["publish_status"] == PublishStatus.pending.value
    assert data["build_url"] is not None
    assert target_public_id.replace("-", "") in data["build_url"].replace(
        "-", ""
    )


@pytest.mark.asyncio
async def test_patch_override_persists_publish_status(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Override marks edition and new history entry publish_status=pending."""
    await _setup(client)
    async with db_session.begin():
        build_id, build_public_id = await _create_orphan_build(db_session)
        await db_session.commit()

    target_public_id = serialize_base32_id(build_public_id)
    response = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    async with db_session.begin():
        logger = structlog.get_logger("docverse")
        org_store = OrganizationStore(session=db_session, logger=logger)
        proj_store = ProjectStore(session=db_session, logger=logger)
        edition_store = EditionStore(session=db_session, logger=logger)
        history_store = EditionBuildHistoryStore(
            session=db_session, logger=logger
        )
        org = await org_store.get_by_slug("pov-org")
        assert org is not None
        project = await proj_store.get_by_slug(org_id=org.id, slug="pov-proj")
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition is not None
        assert edition.publish_status == PublishStatus.pending
        history_entries = await history_store.list_by_edition(edition.id)
        newest = history_entries[0]
        assert newest.position == 1
        assert newest.build_id == build_id
        assert newest.publish_status == PublishStatus.pending


@pytest.mark.asyncio
async def test_patch_override_creates_queue_job(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Override creates a publish_edition QueueJob row."""
    await _setup(client)
    async with db_session.begin():
        build_id, build_public_id = await _create_orphan_build(db_session)
        await db_session.commit()

    target_public_id = serialize_base32_id(build_public_id)
    response = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        org_store = OrganizationStore(session=db_session, logger=logger)
        proj_store = ProjectStore(session=db_session, logger=logger)
        edition_store = EditionStore(session=db_session, logger=logger)
        org = await org_store.get_by_slug("pov-org")
        assert org is not None
        project = await proj_store.get_by_slug(org_id=org.id, slug="pov-proj")
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition is not None

        result = await db_session.execute(
            select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.publish_edition.value
            )
        )
        rows = result.scalars().all()
        assert len(rows) == 1
        child = rows[0]
        assert child.edition_id == edition.id
        assert child.build_id == build_id
        assert child.org_id == org.id
        assert child.project_id == project.id


@pytest.mark.asyncio
async def test_patch_override_enqueues_arq_job(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Override enqueues a ``publish_edition`` arq job with correct payload."""
    await _setup(client)
    async with db_session.begin():
        build_id, build_public_id = await _create_orphan_build(db_session)
        await db_session.commit()

    target_public_id = serialize_base32_id(build_public_id)
    response = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    publish_jobs = get_jobs_by_name(mock_arq, "publish_edition")
    assert len(publish_jobs) == 1
    payload = publish_jobs[0].kwargs["payload"]
    assert payload["project_slug"] == "pov-proj"
    assert payload["edition_slug"] == "__main"
    assert payload["build_id"] == build_id
    assert payload["build_public_id"] == target_public_id
    assert "org_id" in payload
    assert "edition_id" in payload
    assert "queue_job_id" in payload

    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        qjs = QueueJobStore(session=db_session, logger=logger)
        child = await qjs.get(payload["queue_job_id"])
        assert child is not None
        assert child.kind == JobKind.publish_edition


@pytest.mark.asyncio
async def test_patch_override_build_not_found(client: AsyncClient) -> None:
    """PATCH with a nonexistent build public ID returns 404."""
    await _setup(client)
    response = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": "1000-0000-0000-05"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_patch_override_malformed_build_id(
    client: AsyncClient,
) -> None:
    """PATCH with a malformed base32 build ID returns 422."""
    await _setup(client)
    response = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": "totally-invalid"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422
