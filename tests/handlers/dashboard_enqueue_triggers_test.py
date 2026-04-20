"""Integration tests for the dashboard_build enqueue hooks.

These tests verify that every lifecycle event described in the parent
PRD (#182) enqueues exactly one ``dashboard_build`` QueueJob:

- Edition POST (create)
- Edition PATCH (update)
- Edition DELETE (soft-delete)
- Edition rollback
- Project PATCH

Each test drives the real handler via the HTTP client and then asserts
on the ``SqlQueueJob`` table — this reuses the same MockArqQueue-backed
test infrastructure already in place for other enqueue-hook tests.
"""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import BuildCreate
from docverse.client.models.queue_enums import JobKind
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import serialize_base32_id
from docverse.services.dashboard import enqueue as dashboard_enqueue
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from tests.conftest import seed_org_with_admin


async def _setup(client: AsyncClient) -> None:
    """Create org, admin membership, and a single project."""
    await seed_org_with_admin(client, "dbt-org", "testuser")
    response = await client.post(
        "/docverse/orgs/dbt-org/projects",
        json={
            "slug": "dbt-proj",
            "title": "Dashboard Trigger Project",
            "doc_repo": "https://github.com/example/dbt",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201


async def _count_dashboard_jobs(db_session: AsyncSession) -> int:
    result = await db_session.execute(
        select(SqlQueueJob).where(
            SqlQueueJob.kind == JobKind.dashboard_build.value
        )
    )
    return len(list(result.scalars().all()))


def _dashboard_arq_jobs() -> list[str]:
    """Return enqueued arq job names across every queue."""
    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    queues = list(mock_arq._job_metadata.values())
    return [j.name for queue in queues for j in queue.values()]


@pytest.mark.asyncio
async def test_edition_create_enqueues_dashboard_build(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """POST /editions enqueues exactly one dashboard_build QueueJob."""
    await _setup(client)

    response = await client.post(
        "/docverse/orgs/dbt-org/projects/dbt-proj/editions",
        json={
            "slug": "new-ed",
            "title": "New",
            "kind": "draft",
            "tracking_mode": "git_ref",
            "tracking_params": {"git_ref": "main"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 201

    async with db_session.begin():
        assert await _count_dashboard_jobs(db_session) == 1
    assert _dashboard_arq_jobs().count("dashboard_build") == 1


@pytest.mark.asyncio
async def test_edition_patch_enqueues_dashboard_build(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """PATCH /editions/{slug} enqueues exactly one dashboard_build job."""
    await _setup(client)

    response = await client.patch(
        "/docverse/orgs/dbt-org/projects/dbt-proj/editions/__main",
        json={"title": "Renamed Main"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    async with db_session.begin():
        assert await _count_dashboard_jobs(db_session) == 1
    assert _dashboard_arq_jobs().count("dashboard_build") == 1


@pytest.mark.asyncio
async def test_edition_delete_enqueues_dashboard_build(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """DELETE /editions/{slug} enqueues exactly one dashboard_build job."""
    await _setup(client)

    # Create a non-__main edition to delete (the default __main cannot
    # be deleted).
    create_response = await client.post(
        "/docverse/orgs/dbt-org/projects/dbt-proj/editions",
        json={
            "slug": "to-delete",
            "title": "Ephemeral",
            "kind": "draft",
            "tracking_mode": "git_ref",
            "tracking_params": {"git_ref": "main"},
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert create_response.status_code == 201

    # The create itself enqueued one dashboard_build job.
    async with db_session.begin():
        before = await _count_dashboard_jobs(db_session)
    assert before == 1

    response = await client.delete(
        "/docverse/orgs/dbt-org/projects/dbt-proj/editions/to-delete",
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 204

    async with db_session.begin():
        after = await _count_dashboard_jobs(db_session)
    assert after - before == 1


@pytest.mark.asyncio
async def test_edition_rollback_enqueues_dashboard_build(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """POST /editions/{slug}/rollback enqueues a dashboard_build."""
    await _setup(client)
    logger = structlog.get_logger("docverse")

    async with db_session.begin():
        org_store = OrganizationStore(session=db_session, logger=logger)
        proj_store = ProjectStore(session=db_session, logger=logger)
        edition_store = EditionStore(session=db_session, logger=logger)
        build_store = BuildStore(session=db_session, logger=logger)
        history_store = EditionBuildHistoryStore(
            session=db_session, logger=logger
        )
        org = await org_store.get_by_slug("dbt-org")
        assert org is not None
        project = await proj_store.get_by_slug(org_id=org.id, slug="dbt-proj")
        assert project is not None
        edition = await edition_store.get_by_slug(
            project_id=project.id, slug="__main"
        )
        assert edition is not None
        build = await build_store.create(
            project_id=project.id,
            data=BuildCreate(
                git_ref="refs/tags/v0",
                content_hash="sha256:" + "a" * 64,
            ),
            uploader="testuser",
            project_slug="dbt-proj",
        )
        await history_store.record(edition_id=edition.id, build_id=build.id)
        target_public_id = serialize_base32_id(build.public_id)
        await db_session.commit()

    response = await client.post(
        "/docverse/orgs/dbt-org/projects/dbt-proj/editions/__main/rollback",
        json={"build": target_public_id},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    async with db_session.begin():
        assert await _count_dashboard_jobs(db_session) == 1


@pytest.mark.asyncio
async def test_project_patch_enqueues_dashboard_build(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """PATCH /projects/{slug} enqueues exactly one dashboard_build job."""
    await _setup(client)

    response = await client.patch(
        "/docverse/orgs/dbt-org/projects/dbt-proj",
        json={"title": "Renamed Project"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200

    async with db_session.begin():
        assert await _count_dashboard_jobs(db_session) == 1
    assert _dashboard_arq_jobs().count("dashboard_build") == 1


@pytest.mark.asyncio
async def test_enqueue_hook_failure_does_not_break_flow(
    client: AsyncClient,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the enqueue hook raises, the triggering request still succeeds."""
    await _setup(client)

    async def _boom(
        self: dashboard_enqueue.DashboardBuildEnqueuer,
        *,
        org_slug: str,
        project_slug: str,
    ) -> None:
        _ = (self, org_slug, project_slug)
        msg = "simulated enqueue failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        dashboard_enqueue.DashboardBuildEnqueuer,
        "enqueue_for_project_slug",
        _boom,
    )

    response = await client.patch(
        "/docverse/orgs/dbt-org/projects/dbt-proj",
        json={"title": "Still Works"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert response.json()["title"] == "Still Works"

    async with db_session.begin():
        assert await _count_dashboard_jobs(db_session) == 0
