"""Tests for the PATCH edition emergency build-override flow."""

from __future__ import annotations

from datetime import datetime

import pytest
import structlog
from httpx import AsyncClient
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import BuildCreate
from docverse.models.queue_enums import JobKind, PublishStatus
from docverse_server.dbschema.project import SqlProject
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.dependencies.context import context_dependency
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.metrics import LifecycleAction
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
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


async def _read_project_date_updated(db_session: AsyncSession) -> datetime:
    """Read the test project's ``date_updated`` straight from the database.

    A column-level SELECT rather than an ORM entity load, so the
    identity map cannot hand back a value that predates the Core
    ``UPDATE`` ``EditionStore.set_current_build`` issues.
    """
    return (
        await db_session.execute(
            select(SqlProject.date_updated).where(
                SqlProject.slug == "pov-proj"
            )
        )
    ).scalar_one()


@pytest.mark.asyncio
async def test_patch_override_noop_leaves_project_clock(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Re-PATCHing the already-current build leaves the project clock.

    The project's ``date_updated`` is a poller's change signal (PRD
    #634), so an override that repoints ``__main`` at the build it
    already serves must not retire every cached ``ETag`` and re-emit an
    identical row into every ``updated_since`` window.
    """
    await _setup(client)
    async with db_session.begin():
        _, build_public_id = await _create_orphan_build(db_session)
        await db_session.commit()

    target_public_id = serialize_base32_id(build_public_id)
    first = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert first.status_code == 200

    async with db_session.begin():
        before = await _read_project_date_updated(db_session)

    second = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert second.status_code == 200

    async with db_session.begin():
        after = await _read_project_date_updated(db_session)
    assert after == before


@pytest.mark.asyncio
async def test_patch_override_noop_records_no_history_or_job(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """A no-op override records no history row and enqueues no publish.

    The history row, the ``publish_status`` flip, and the
    ``publish_edition`` job all announce a repoint. None of them should
    be emitted for a repoint that did not happen, and the response
    carries the edition's real ``publish_status`` rather than a
    ``pending`` that nothing will ever clear.
    """
    await _setup(client)
    async with db_session.begin():
        build_id, build_public_id = await _create_orphan_build(db_session)
        await db_session.commit()

    target_public_id = serialize_base32_id(build_public_id)
    for _ in range(2):
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
        assert edition.current_build_id == build_id

        history_entries = await history_store.list_by_edition(edition.id)
        assert len(history_entries) == 1

        result = await db_session.execute(
            select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.publish_edition.value
            )
        )
        assert len(result.scalars().all()) == 1

    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    assert len(get_jobs_by_name(mock_arq, "publish_edition")) == 1


async def _fail_current_publish(db_session: AsyncSession) -> None:
    """Record the ``__main`` edition's current publish as failed.

    What a ``publish_edition`` job leaves behind when it gives up: the
    edition and the history row for the pair both carry ``failed``, and
    nothing in the tree picks that pair up again — the reconcile worker
    classifies it ``failed_left_alone`` by design, keeper-sync's
    self-heal skips it, and there is no republish endpoint.
    """
    logger = structlog.get_logger("docverse")
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)
    history_store = EditionBuildHistoryStore(session=db_session, logger=logger)

    org = await org_store.get_by_slug("pov-org")
    assert org is not None
    project = await proj_store.get_by_slug(org_id=org.id, slug="pov-proj")
    assert project is not None
    edition = await edition_store.get_by_slug(
        project_id=project.id, slug="__main"
    )
    assert edition is not None
    assert edition.current_build_id is not None
    await edition_store.set_publish_status(
        edition_id=edition.id, status=PublishStatus.failed
    )
    entry = await history_store.get_by_edition_and_build(
        edition_id=edition.id, build_id=edition.current_build_id
    )
    assert entry is not None
    await history_store.set_publish_status(
        history_id=entry.id, status=PublishStatus.failed
    )


@pytest.mark.asyncio
async def test_patch_override_redrives_a_failed_publish_of_same_build(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Re-PATCHing a failed publish's own build re-drives it.

    The override that names the build the edition already serves is,
    like rollback, the only operator-reachable retry for a
    ``publish_edition`` job that failed. It is inert only while the
    publish is settled; with the pair ``failed`` it records a fresh
    history row, returns to ``pending``, and enqueues another job.
    """
    await _setup(client)
    async with db_session.begin():
        _, build_public_id = await _create_orphan_build(db_session)
        await db_session.commit()

    target_public_id = serialize_base32_id(build_public_id)
    headers = {"X-Auth-Request-User": "testuser"}
    first = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers=headers,
    )
    assert first.status_code == 200

    async with db_session.begin():
        await _fail_current_publish(db_session)
        await db_session.commit()

    second = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers=headers,
    )
    assert second.status_code == 200
    assert second.json()["publish_status"] == PublishStatus.pending.value

    logger = structlog.get_logger("docverse")
    async with db_session.begin():
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
        history_entries = await history_store.list_by_edition(edition.id)
        assert len(history_entries) == 2

        result = await db_session.execute(
            select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.publish_edition.value
            )
        )
        assert len(result.scalars().all()) == 2

    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    assert len(get_jobs_by_name(mock_arq, "publish_edition")) == 2


@pytest.mark.asyncio
async def test_patch_override_redriving_a_failed_publish_leaves_clock(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Re-driving a failed publish is not a change to the project.

    The retry republishes the build the edition already serves, so the
    project's content has not moved and a poller holding its ``ETag``
    has nothing to refetch — even though this request is not the inert
    no-op a settled publish gets.
    """
    await _setup(client)
    async with db_session.begin():
        _, build_public_id = await _create_orphan_build(db_session)
        await db_session.commit()

    target_public_id = serialize_base32_id(build_public_id)
    headers = {"X-Auth-Request-User": "testuser"}
    first = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers=headers,
    )
    assert first.status_code == 200

    async with db_session.begin():
        await _fail_current_publish(db_session)
        await db_session.commit()

    async with db_session.begin():
        before = await _read_project_date_updated(db_session)

    second = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers=headers,
    )
    assert second.status_code == 200

    async with db_session.begin():
        after = await _read_project_date_updated(db_session)
    assert after == before


async def _count_dashboard_build_jobs(db_session: AsyncSession) -> int:
    """Count ``dashboard_build`` rows in ``queue_jobs``."""
    result = await db_session.execute(
        select(SqlQueueJob).where(
            SqlQueueJob.kind == JobKind.dashboard_build.value
        )
    )
    return len(list(result.scalars().all()))


async def _settle_dashboard_build_jobs(db_session: AsyncSession) -> None:
    """Drive every ``dashboard_build`` row to ``completed``.

    The enqueuer dedupes only against rows that are ``queued`` or
    ``in_progress``, so leaving the previous request's row in flight
    would suppress the next enqueue on its own and hide whichever
    behavior the test is actually after.
    """
    logger = structlog.get_logger("docverse")
    store = QueueJobStore(session=db_session, logger=logger)
    result = await db_session.execute(
        select(SqlQueueJob).where(
            SqlQueueJob.kind == JobKind.dashboard_build.value
        )
    )
    for row in result.scalars().all():
        await store.start_if_queued(row.id)
        await store.complete(row.id)


def _update_event_count() -> int:
    """Count published ``edition_lifecycle`` events with ``update``."""
    events = context_dependency._events
    assert events is not None
    publisher = events.edition_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    return len(
        [e for e in publisher.published if e.action == LifecycleAction.update]
    )


@pytest.mark.asyncio
async def test_patch_override_noop_announces_nothing(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """An inert build-only PATCH publishes no event, rebuilds nothing.

    The handler's ``200``-on-retry contract invites the client that lost
    its connection to send the request again. That retry must cost what
    it claims to cost: no ``EditionLifecycleEvent`` (the metric would
    report an update with no history row behind it) and no
    ``dashboard_build`` job (the worker renders and re-uploads the whole
    dashboard with no content-hash short-circuit).
    """
    await _setup(client)
    async with db_session.begin():
        _, build_public_id = await _create_orphan_build(db_session)
        await db_session.commit()

    target_public_id = serialize_base32_id(build_public_id)
    headers = {"X-Auth-Request-User": "testuser"}
    first = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers=headers,
    )
    assert first.status_code == 200

    async with db_session.begin():
        await _settle_dashboard_build_jobs(db_session)
        dashboard_rows_before = await _count_dashboard_build_jobs(db_session)
        await db_session.commit()
    assert dashboard_rows_before == 1
    assert _update_event_count() == 1

    second = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers=headers,
    )
    assert second.status_code == 200

    async with db_session.begin():
        assert await _count_dashboard_build_jobs(db_session) == 1
    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    assert len(get_jobs_by_name(mock_arq, "dashboard_build")) == 1
    assert _update_event_count() == 1


@pytest.mark.asyncio
async def test_patch_override_announces_a_real_repoint(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """A build override that moves the edition still announces itself.

    The inert-request short-circuit must not swallow the ordinary case:
    an override onto a build the edition was not serving changes what
    the project publishes, so it owes both the ``edition_lifecycle``
    event and the dashboard rebuild.
    """
    await _setup(client)
    async with db_session.begin():
        _, build_public_id = await _create_orphan_build(db_session)
        await db_session.commit()

    response = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": serialize_base32_id(build_public_id)},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    async with db_session.begin():
        assert await _count_dashboard_build_jobs(db_session) == 1
    assert _update_event_count() == 1


@pytest.mark.asyncio
async def test_patch_override_redriving_a_failed_publish_announces(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Re-driving a failed publish is a change, so it announces one.

    Naming the served build with the pair ``failed`` records a history
    row, returns the edition to ``pending``, and enqueues the publish —
    a real change, even though the build did not move — so it keeps the
    ``edition_lifecycle`` event and the dashboard rebuild that report
    it. Only a *settled* publish makes the request inert.
    """
    await _setup(client)
    async with db_session.begin():
        _, build_public_id = await _create_orphan_build(db_session)
        await db_session.commit()

    target_public_id = serialize_base32_id(build_public_id)
    headers = {"X-Auth-Request-User": "testuser"}
    first = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers=headers,
    )
    assert first.status_code == 200

    async with db_session.begin():
        await _fail_current_publish(db_session)
        await _settle_dashboard_build_jobs(db_session)
        await db_session.commit()

    second = await client.patch(
        "/docverse/orgs/pov-org/projects/pov-proj/editions/__main",
        json={"build": target_public_id},
        headers=headers,
    )
    assert second.status_code == 200

    async with db_session.begin():
        assert await _count_dashboard_build_jobs(db_session) == 2
    assert _update_event_count() == 2
