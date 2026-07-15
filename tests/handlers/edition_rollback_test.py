"""Tests for the edition rollback endpoint."""

from __future__ import annotations

import pytest
import structlog
from docverse.client.models import BuildCreate
from docverse.client.models.queue_enums import JobKind, PublishStatus
from httpx import AsyncClient
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.queue_job import SqlQueueJob
from docverse.dependencies.context import context_dependency
from docverse.domain.base32id import serialize_base32_id
from docverse.metrics import LifecycleAction, MetricsEditionKind
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
    await seed_org_with_admin(client, "rb-org", "testuser")
    await client.post(
        "/docverse/orgs/rb-org/projects",
        json={
            "slug": "rb-proj",
            "title": "Rollback Project",
            "source_url": "https://example.com/example/rb",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )


async def _create_builds_with_history(
    db_session: AsyncSession,
    n_builds: int,
) -> list[tuple[int, int]]:
    """Create builds and record them in __main edition history.

    Returns list of (build_internal_id, build_public_id) tuples,
    oldest first.
    """
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    build_store = BuildStore(session=db_session, logger=logger)
    history_store = EditionBuildHistoryStore(session=db_session, logger=logger)

    org = await org_store.get_by_slug("rb-org")
    assert org is not None
    project = await proj_store.get_by_slug(org_id=org.id, slug="rb-proj")
    assert project is not None
    edition = await edition_store.get_by_slug(
        project_id=project.id, slug="__main"
    )
    assert edition is not None

    builds: list[tuple[int, int]] = []
    for i in range(n_builds):
        build = await build_store.create(
            project_id=project.id,
            data=BuildCreate(
                git_ref=f"refs/tags/v{i}",
                content_hash=f"sha256:{i:064x}",
            ),
            uploader="testuser",
            project_slug="rb-proj",
        )
        builds.append((build.id, build.public_id))
        await history_store.record(edition_id=edition.id, build_id=build.id)
    return builds


@pytest.mark.asyncio
async def test_rollback_success(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """POST rollback with valid build in history returns 200."""
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    # Roll back to the first build (v0)
    target_public_id = serialize_base32_id(builds[0][1])
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["slug"] == "__main"
    assert data["build_url"] is not None
    assert target_public_id.replace("-", "") in data["build_url"].replace(
        "-", ""
    )
    assert data["published_url"] == "https://rb-proj.rb-org.example.com/"


@pytest.mark.asyncio
async def test_rollback_unauthorized(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Non-admin gets 403 on rollback."""
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=1)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "unknownuser"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_rollback_build_not_in_history(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Valid build not in this edition's history returns 404."""
    await _setup(client)
    async with db_session.begin():
        # Create a build but do NOT record it in the edition's history
        logger = structlog.get_logger("docverse")
        org_store = OrganizationStore(session=db_session, logger=logger)
        proj_store = ProjectStore(session=db_session, logger=logger)
        org = await org_store.get_by_slug("rb-org")
        assert org is not None
        project = await proj_store.get_by_slug(org_id=org.id, slug="rb-proj")
        assert project is not None
        build_store = BuildStore(session=db_session, logger=logger)
        build = await build_store.create(
            project_id=project.id,
            data=BuildCreate(
                git_ref="refs/tags/orphan",
                content_hash="sha256:" + "a" * 64,
            ),
            uploader="testuser",
            project_slug="rb-proj",
        )
        orphan_public_id = serialize_base32_id(build.public_id)
        await db_session.commit()

    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": orphan_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_rollback_build_not_found(client: AsyncClient) -> None:
    """Nonexistent build public ID returns 404."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": "1000-0000-0000-05"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_rollback_records_in_history(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """After rollback, GET history shows the rollback target at position 1."""
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    # Roll back to build v0
    target_public_id = serialize_base32_id(builds[0][1])
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    # Check history — position 1 should be the rollback target
    history_response = await client.get(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/history",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert history_response.status_code == 200
    history = history_response.json()
    assert len(history) == 4  # 3 original + 1 rollback entry
    assert history[0]["position"] == 1
    assert history[0]["git_ref"] == "refs/tags/v0"


@pytest.mark.asyncio
async def test_rollback_malformed_build_id(client: AsyncClient) -> None:
    """Malformed base32 build ID returns 422."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": "totally-invalid"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_rollback_edition_not_found(client: AsyncClient) -> None:
    """Rollback on a nonexistent edition slug returns 404."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/no-such-edition/rollback",
        json={"build": "1000-0000-0000-05"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_rollback_marks_publish_status_pending(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Rollback marks edition and new history entry as publish_status=pending.

    The response body should report ``publish_status`` pending, and both the
    persisted edition row and the newly inserted history entry should also
    carry ``publish_status`` pending.
    """
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["publish_status"] == PublishStatus.pending.value

    async with db_session.begin():
        logger = structlog.get_logger("docverse")
        org_store = OrganizationStore(session=db_session, logger=logger)
        proj_store = ProjectStore(session=db_session, logger=logger)
        edition_store = EditionStore(session=db_session, logger=logger)
        history_store = EditionBuildHistoryStore(
            session=db_session, logger=logger
        )
        org = await org_store.get_by_slug("rb-org")
        assert org is not None
        project = await proj_store.get_by_slug(org_id=org.id, slug="rb-proj")
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition is not None
        assert edition.publish_status == PublishStatus.pending
        history_entries = await history_store.list_by_edition(edition.id)
        newest = history_entries[0]
        assert newest.position == 1
        assert newest.build_id == builds[0][0]
        assert newest.publish_status == PublishStatus.pending


@pytest.mark.asyncio
async def test_rollback_creates_publish_edition_queue_job(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Rollback creates a publish_edition QueueJob row.

    The row should be linked to the edition and the rollback target build.
    """
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=2)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        org_store = OrganizationStore(session=db_session, logger=logger)
        proj_store = ProjectStore(session=db_session, logger=logger)
        edition_store = EditionStore(session=db_session, logger=logger)
        org = await org_store.get_by_slug("rb-org")
        assert org is not None
        project = await proj_store.get_by_slug(org_id=org.id, slug="rb-proj")
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
        assert child.build_id == builds[0][0]
        assert child.org_id == org.id
        assert child.project_id == project.id


@pytest.mark.asyncio
async def test_rollback_enqueues_publish_edition_arq_job(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Rollback enqueues a ``publish_edition`` arq job.

    Asserts payload shape and that the referenced ``queue_job_id`` exists.
    """
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=2)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    publish_jobs = get_jobs_by_name(mock_arq, "publish_edition")
    assert len(publish_jobs) == 1
    payload = publish_jobs[0].kwargs["payload"]
    assert payload["project_slug"] == "rb-proj"
    assert payload["edition_slug"] == "__main"
    assert payload["build_id"] == builds[0][0]
    assert payload["build_public_id"] == target_public_id
    assert "org_id" in payload
    assert "edition_id" in payload
    assert "queue_job_id" in payload
    # The rollback path tags its publish so the edition_published metric
    # reports trigger=rollback rather than the default build fan-out.
    assert payload["trigger"] == "rollback"

    logger = structlog.get_logger("docverse")
    async with db_session.begin():
        qjs = QueueJobStore(session=db_session, logger=logger)
        child = await qjs.get(payload["queue_job_id"])
        assert child is not None
        assert child.kind == JobKind.publish_edition


@pytest.mark.asyncio
async def test_rollback_publishes_edition_lifecycle(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Rollback emits one edition_lifecycle (rollback) with edition_kind."""
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=2)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    events = context_dependency._events
    assert events is not None
    publisher = events.edition_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    rollback_events = [
        e for e in publisher.published if e.action == LifecycleAction.rollback
    ]
    assert len(rollback_events) == 1
    event = rollback_events[0]
    assert event.organization == "rb-org"
    assert event.project == "rb-proj"
    # __main is the project's default edition (kind=main).
    assert event.edition_kind == MetricsEditionKind.main


@pytest.mark.asyncio
async def test_rollback_missing_build_field(client: AsyncClient) -> None:
    """Missing 'build' field in request body returns 422."""
    await _setup(client)
    response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 422
