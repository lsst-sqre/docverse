"""Tests for the edition rollback endpoint."""

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
from docverse_server.metrics import LifecycleAction, MetricsEditionKind
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

        # Rollback is what puts two rows on one ``(edition, build)``
        # pair, so its payload has to name the row it just recorded:
        # a job that resolved the pair instead could pick up whichever
        # row a later rollback added (task #630).
        history_store = EditionBuildHistoryStore(
            session=db_session, logger=logger
        )
        recorded = await history_store.get_by_edition_and_build(
            edition_id=payload["edition_id"], build_id=builds[0][0]
        )
        assert recorded is not None
        assert recorded.position == 1
        assert payload["history_id"] == recorded.id


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
async def test_rollback_advances_project_date_updated(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """A ``__main`` rollback shows up on the project resource.

    PRD #634: Ook polls ``GET /orgs/{org}/projects`` with
    ``updated_since``, so a content change to the default edition has
    to move the project's ``date_updated`` — this is the end-to-end
    proof that ``EditionStore.set_current_build``'s project touch
    reaches the wire.
    """
    await _setup(client)

    before_response = await client.get(
        "/docverse/orgs/rb-org/projects/rb-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert before_response.status_code == 200
    before = datetime.fromisoformat(before_response.json()["date_updated"])

    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=2)
        await db_session.commit()

    rollback_response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": serialize_base32_id(builds[0][1])},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert rollback_response.status_code == 200

    after_response = await client.get(
        "/docverse/orgs/rb-org/projects/rb-proj",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert after_response.status_code == 200
    after = datetime.fromisoformat(after_response.json()["date_updated"])
    assert after > before


@pytest.mark.asyncio
async def test_rollback_retires_project_etag(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """A ``__main`` rollback invalidates the project's conditional GET.

    The companion to
    :func:`test_rollback_advances_project_date_updated`: a poller
    holding the project's ``ETag`` has to be told the content moved,
    not handed a 304 (PRD #634 §5).
    """
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}

    before_response = await client.get(
        "/docverse/orgs/rb-org/projects/rb-proj", headers=headers
    )
    assert before_response.status_code == 200
    etag = before_response.headers["ETag"]

    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=2)
        await db_session.commit()

    rollback_response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": serialize_base32_id(builds[0][1])},
        headers=headers,
    )
    assert rollback_response.status_code == 200

    after_response = await client.get(
        "/docverse/orgs/rb-org/projects/rb-proj",
        headers={**headers, "If-None-Match": etag},
    )

    assert after_response.status_code == 200
    assert after_response.headers["ETag"] != etag


@pytest.mark.asyncio
async def test_rollback_retires_listing_etag(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """A ``__main`` rollback invalidates the project listing's tag.

    The listing embeds the default edition (task #660) and hashes only
    project clocks, so this is the proof that the one edition change a
    poller must see — the current build moving — reaches the listing
    tag through the project touch in ``set_current_build``.
    """
    await _setup(client)
    headers = {"X-Auth-Request-User": "testuser"}

    before_response = await client.get(
        "/docverse/orgs/rb-org/projects", headers=headers
    )
    assert before_response.status_code == 200
    etag = before_response.headers["ETag"]
    before_edition = before_response.json()[0]["default_edition"]

    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=2)
        await db_session.commit()

    rollback_response = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": serialize_base32_id(builds[0][1])},
        headers=headers,
    )
    assert rollback_response.status_code == 200

    after_response = await client.get(
        "/docverse/orgs/rb-org/projects",
        headers={**headers, "If-None-Match": etag},
    )

    assert after_response.status_code == 200
    assert after_response.headers["ETag"] != etag
    after_edition = after_response.json()[0]["default_edition"]
    assert after_edition["build_url"] != before_edition["build_url"]
    assert after_edition["build_url"].endswith(
        serialize_base32_id(builds[0][1])
    )


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


async def _read_project_date_updated(db_session: AsyncSession) -> datetime:
    """Read the test project's ``date_updated`` straight from the database.

    A column-level SELECT rather than an ORM entity load, so the
    identity map cannot hand back a value that predates the Core
    ``UPDATE`` ``EditionStore.set_current_build`` issues.
    """
    return (
        await db_session.execute(
            select(SqlProject.date_updated).where(SqlProject.slug == "rb-proj")
        )
    ).scalar_one()


@pytest.mark.asyncio
async def test_rollback_noop_leaves_project_clock(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Rolling back to the served build leaves the project clock alone.

    The project's ``date_updated`` is a poller's change signal (PRD
    #634), so a rollback that lands ``__main`` on the build it already
    serves must not retire every cached ``ETag`` and re-emit an
    identical row into every ``updated_since`` window.
    """
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    first = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert first.status_code == 200

    async with db_session.begin():
        before = await _read_project_date_updated(db_session)

    second = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert second.status_code == 200

    async with db_session.begin():
        after = await _read_project_date_updated(db_session)
    assert after == before


@pytest.mark.asyncio
async def test_rollback_noop_records_no_history_or_job(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """A no-op rollback records no history row and enqueues no publish.

    The rollback stays a 200 carrying the unchanged edition rather than
    a 409: asking for the build already being served is a request whose
    postcondition already holds, and an operator retrying a rollback
    after a dropped connection should not have to tell a conflict from
    a success.
    """
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    for _ in range(2):
        response = await client.post(
            "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
            json={"build": target_public_id},
            headers={"X-Auth-Request-User": "testuser"},
        )
        assert response.status_code == 200

    history_response = await client.get(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/history",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert history_response.status_code == 200
    # 3 seeded entries plus the one the first rollback recorded.
    assert len(history_response.json()) == 4

    async with db_session.begin():
        result = await db_session.execute(
            select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.publish_edition.value
            )
        )
        assert len(result.scalars().all()) == 1

    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    assert len(get_jobs_by_name(mock_arq, "publish_edition")) == 1


@pytest.mark.asyncio
async def test_rollback_different_build_advances_project_clock(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """A rollback that moves ``__main`` still advances the project clock.

    The no-op short-circuit keys on ``current_build_id``, so a second
    rollback naming a *different* build is a real repoint and has to
    stay a change signal.
    """
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    first = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": serialize_base32_id(builds[0][1])},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert first.status_code == 200

    async with db_session.begin():
        before = await _read_project_date_updated(db_session)

    second = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": serialize_base32_id(builds[1][1])},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert second.status_code == 200

    async with db_session.begin():
        after = await _read_project_date_updated(db_session)
    assert after > before


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

    org = await org_store.get_by_slug("rb-org")
    assert org is not None
    project = await proj_store.get_by_slug(org_id=org.id, slug="rb-proj")
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
async def test_rollback_redrives_a_failed_publish_of_the_same_build(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Rolling back onto a failed publish re-drives it.

    A rollback naming the build the edition already serves is the only
    operator-reachable way to retry a ``publish_edition`` job that
    failed, so it is a no-op only while the publish is settled. With
    the pair ``failed``, the full path runs: a fresh history row, back
    to ``pending``, and another job.
    """
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    headers = {"X-Auth-Request-User": "testuser"}
    first = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers=headers,
    )
    assert first.status_code == 200

    async with db_session.begin():
        await _fail_current_publish(db_session)
        await db_session.commit()

    second = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers=headers,
    )
    assert second.status_code == 200
    assert second.json()["publish_status"] == PublishStatus.pending.value

    history_response = await client.get(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/history",
        headers=headers,
    )
    assert history_response.status_code == 200
    # 3 seeded entries plus one for each of the two rollbacks.
    assert len(history_response.json()) == 5

    async with db_session.begin():
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
async def test_rollback_redriving_a_failed_publish_leaves_the_clock(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Re-driving a failed publish is not a change to the project.

    The retry re-runs the publish of the build the edition is already
    serving, so no content moves and a poller holding the project's
    ``ETag`` has nothing to refetch — even though this request is not
    the inert no-op a settled publish gets.
    """
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    headers = {"X-Auth-Request-User": "testuser"}
    first = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers=headers,
    )
    assert first.status_code == 200

    async with db_session.begin():
        await _fail_current_publish(db_session)
        await db_session.commit()

    async with db_session.begin():
        before = await _read_project_date_updated(db_session)

    second = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
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


def _rollback_event_count() -> int:
    """Count published ``edition_lifecycle`` events with ``rollback``."""
    events = context_dependency._events
    assert events is not None
    publisher = events.edition_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    return len(
        [
            e
            for e in publisher.published
            if e.action == LifecycleAction.rollback
        ]
    )


@pytest.mark.asyncio
async def test_rollback_noop_announces_nothing(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """An inert rollback publishes no event and rebuilds no dashboard.

    The handler's ``200``-on-retry contract invites the client that lost
    its connection to send the request again. That retry must cost what
    it claims to cost: no ``EditionLifecycleEvent`` (the metric would
    report a rollback with no history row behind it) and no
    ``dashboard_build`` job (the worker renders and re-uploads the whole
    dashboard with no content-hash short-circuit).
    """
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    headers = {"X-Auth-Request-User": "testuser"}
    first = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers=headers,
    )
    assert first.status_code == 200

    async with db_session.begin():
        await _settle_dashboard_build_jobs(db_session)
        dashboard_rows_before = await _count_dashboard_build_jobs(db_session)
        await db_session.commit()
    events_before = _rollback_event_count()
    assert dashboard_rows_before == 1
    assert events_before == 1

    second = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers=headers,
    )
    assert second.status_code == 200

    async with db_session.begin():
        assert await _count_dashboard_build_jobs(db_session) == 1
    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    assert len(get_jobs_by_name(mock_arq, "dashboard_build")) == 1
    assert _rollback_event_count() == 1


@pytest.mark.asyncio
async def test_rollback_redriving_a_failed_publish_announces(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Re-driving a failed publish is a change, so it announces one.

    Rolling back onto the served build with the pair ``failed`` records
    a history row, returns the edition to ``pending``, and enqueues the
    publish — a real change, even though the build did not move — so it
    keeps the ``edition_lifecycle`` event and the dashboard rebuild that
    report it. Only a *settled* publish makes the request inert.
    """
    await _setup(client)
    async with db_session.begin():
        builds = await _create_builds_with_history(db_session, n_builds=3)
        await db_session.commit()

    target_public_id = serialize_base32_id(builds[0][1])
    headers = {"X-Auth-Request-User": "testuser"}
    first = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers=headers,
    )
    assert first.status_code == 200

    async with db_session.begin():
        await _fail_current_publish(db_session)
        await _settle_dashboard_build_jobs(db_session)
        await db_session.commit()

    second = await client.post(
        "/docverse/orgs/rb-org/projects/rb-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers=headers,
    )
    assert second.status_code == 200

    async with db_session.begin():
        assert await _count_dashboard_build_jobs(db_session) == 2
    assert _rollback_event_count() == 2
