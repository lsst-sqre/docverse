"""Tests for the org-scoped GET /orgs/:org/jobs/:job endpoint."""

from __future__ import annotations

import re

import pytest
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency

from docverse.client.models import OrgRole, ProjectCreate
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.domain.base32id import (
    generate_base32_id,
    serialize_base32_id,
    validate_base32_id,
)
from docverse.domain.queue import JobKind
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"


async def _org_id(org_slug: str) -> int:
    """Resolve an org's integer primary key from its slug."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug(org_slug)
            assert org is not None
            return org.id
    msg = "no session"
    raise AssertionError(msg)


async def _seed_job(
    org_id: int,
    *,
    kind: JobKind = JobKind.build_processing,
    project_id: int | None = None,
    keeper_sync_run_id: int | None = None,
) -> str:
    """Seed a queue job for an org; return its Base32 public id."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            store = QueueJobStore(session=session, logger=logger)
            job = await store.create(
                kind=kind,
                org_id=org_id,
                project_id=project_id,
                keeper_sync_run_id=keeper_sync_run_id,
            )
            await session.commit()
            return serialize_base32_id(job.public_id)
    msg = "no session"
    raise AssertionError(msg)


async def _seed_project(org_id: int, slug: str) -> int:
    """Seed a project in an org; return its integer primary key."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            proj_store = ProjectStore(session=session, logger=logger)
            project = await proj_store.create(
                org_id=org_id,
                data=ProjectCreate(
                    slug=slug,
                    title=f"Project {slug}",
                    source_url="https://example.com/example/repo",
                ),
            )
            await session.commit()
            return project.id
    msg = "no session"
    raise AssertionError(msg)


async def _seed_run(org_id: int) -> tuple[int, str]:
    """Seed a keeper-sync run; return ``(run_id, run_public_id_str)``."""
    async for session in db_session_dependency():
        async with session.begin():
            public_id = validate_base32_id(generate_base32_id())
            run = SqlKeeperSyncRun(
                public_id=public_id,
                org_id=org_id,
                kind="backfill",
                status="pending",
            )
            session.add(run)
            await session.flush()
            await session.refresh(run)
            run_id = run.id
            await session.commit()
            return run_id, serialize_base32_id(public_id)
    msg = "no session"
    raise AssertionError(msg)


@pytest.mark.asyncio
async def test_get_org_job_as_reader(client: AsyncClient) -> None:
    """A reader can fetch a job in their org and gets the QueueJob model."""
    await seed_org_with_admin(client, "read-org", _ADMIN)
    await seed_member("read-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("read-org")
    job_id = await _seed_job(org_id)

    response = await client.get(
        f"/docverse/orgs/read-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == job_id
    assert data["kind"] == "build_processing"
    assert data["self_url"].endswith(f"/orgs/read-org/jobs/{job_id}")


@pytest.mark.asyncio
async def test_get_org_job_cross_org_returns_404(client: AsyncClient) -> None:
    """A job belonging to another org returns 404 (no existence leak)."""
    await seed_org_with_admin(client, "owner-org", _ADMIN)
    await seed_org_with_admin(client, "other-org", _ADMIN)
    await seed_member("other-org", "reader-user", OrgRole.reader)

    owner_id = await _org_id("owner-org")
    job_id = await _seed_job(owner_id)

    # reader-user is a reader of other-org but the job lives in owner-org.
    response = await client.get(
        f"/docverse/orgs/other-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_org_job_no_role_returns_403(client: AsyncClient) -> None:
    """A user with no role in the org gets 403."""
    await seed_org_with_admin(client, "perm-org", _ADMIN)
    org_id = await _org_id("perm-org")
    job_id = await _seed_job(org_id)

    response = await client.get(
        f"/docverse/orgs/perm-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "nobody-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_org_job_not_found_returns_404(client: AsyncClient) -> None:
    """A nonexistent job in the org returns 404."""
    await seed_org_with_admin(client, "nf-org", _ADMIN)
    await seed_member("nf-org", "reader-user", OrgRole.reader)

    response = await client.get(
        "/docverse/orgs/nf-org/jobs/1000-0000-0000-05",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_org_jobs_newest_first(client: AsyncClient) -> None:
    """The listing returns the org's jobs newest-first with pagination."""
    await seed_org_with_admin(client, "list-org", _ADMIN)
    await seed_member("list-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("list-org")
    first = await _seed_job(org_id, kind=JobKind.build_processing)
    second = await _seed_job(org_id, kind=JobKind.publish_edition)
    third = await _seed_job(org_id, kind=JobKind.dashboard_build)

    response = await client.get(
        "/docverse/orgs/list-org/jobs",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    assert response.headers["X-Total-Count"] == "3"
    assert "Link" in response.headers
    ids = [job["id"] for job in response.json()]
    assert ids == [third, second, first]


@pytest.mark.asyncio
async def test_list_org_jobs_excludes_other_orgs(client: AsyncClient) -> None:
    """Jobs from other orgs never appear in the listing."""
    await seed_org_with_admin(client, "mine-org", _ADMIN)
    await seed_org_with_admin(client, "theirs-org", _ADMIN)
    await seed_member("mine-org", "reader-user", OrgRole.reader)
    mine_id = await _org_id("mine-org")
    theirs_id = await _org_id("theirs-org")
    mine = await _seed_job(mine_id)
    await _seed_job(theirs_id)

    response = await client.get(
        "/docverse/orgs/mine-org/jobs",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    assert response.headers["X-Total-Count"] == "1"
    assert [job["id"] for job in response.json()] == [mine]


@pytest.mark.asyncio
async def test_list_org_jobs_filter_by_kind(client: AsyncClient) -> None:
    """The ``kind`` query filter narrows the listing."""
    await seed_org_with_admin(client, "kind-org", _ADMIN)
    await seed_member("kind-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("kind-org")
    build = await _seed_job(org_id, kind=JobKind.build_processing)
    await _seed_job(org_id, kind=JobKind.publish_edition)

    response = await client.get(
        "/docverse/orgs/kind-org/jobs",
        params={"kind": "build_processing"},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    assert [job["id"] for job in response.json()] == [build]


@pytest.mark.asyncio
async def test_list_org_jobs_filter_by_status(client: AsyncClient) -> None:
    """The ``status`` query filter narrows the listing."""
    await seed_org_with_admin(client, "status-org", _ADMIN)
    await seed_member("status-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("status-org")
    # Freshly-created jobs are queued; filter should return only these.
    queued = await _seed_job(org_id)

    response = await client.get(
        "/docverse/orgs/status-org/jobs",
        params={"status": "queued"},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    assert [job["id"] for job in response.json()] == [queued]

    # No jobs are in a terminal state, so this filter is empty.
    empty = await client.get(
        "/docverse/orgs/status-org/jobs",
        params={"status": "completed"},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert empty.status_code == 200
    assert empty.json() == []


@pytest.mark.asyncio
async def test_list_org_jobs_filter_by_project(client: AsyncClient) -> None:
    """The ``project`` query filter narrows the listing to one project."""
    await seed_org_with_admin(client, "proj-org", _ADMIN)
    await seed_member("proj-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("proj-org")
    project_id = await _seed_project(org_id, "the-proj")
    scoped = await _seed_job(
        org_id, kind=JobKind.dashboard_build, project_id=project_id
    )
    await _seed_job(org_id, kind=JobKind.build_processing)

    response = await client.get(
        "/docverse/orgs/proj-org/jobs",
        params={"project": "the-proj"},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    assert [job["id"] for job in response.json()] == [scoped]


@pytest.mark.asyncio
async def test_list_org_jobs_unknown_project_returns_404(
    client: AsyncClient,
) -> None:
    """An unknown ``project`` slug returns 404."""
    await seed_org_with_admin(client, "np-org", _ADMIN)
    await seed_member("np-org", "reader-user", OrgRole.reader)

    response = await client.get(
        "/docverse/orgs/np-org/jobs",
        params={"project": "does-not-exist"},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_org_jobs_filter_by_run(client: AsyncClient) -> None:
    """The ``run`` query filter narrows the listing to one run's jobs."""
    await seed_org_with_admin(client, "run-org", _ADMIN)
    await seed_member("run-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("run-org")
    run_id, run_public_id = await _seed_run(org_id)
    attributed = await _seed_job(
        org_id,
        kind=JobKind.keeper_sync_project,
        keeper_sync_run_id=run_id,
    )
    await _seed_job(org_id, kind=JobKind.build_processing)

    response = await client.get(
        "/docverse/orgs/run-org/jobs",
        params={"run": run_public_id},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    ids = [job["id"] for job in response.json()]
    assert ids == [attributed]


@pytest.mark.asyncio
async def test_list_org_jobs_cross_org_run_returns_404(
    client: AsyncClient,
) -> None:
    """A ``run`` public id from another org returns 404 (no cross-org leak)."""
    await seed_org_with_admin(client, "asker-org", _ADMIN)
    await seed_org_with_admin(client, "runner-org", _ADMIN)
    await seed_member("asker-org", "reader-user", OrgRole.reader)
    runner_id = await _org_id("runner-org")
    _, other_run_public_id = await _seed_run(runner_id)

    response = await client.get(
        "/docverse/orgs/asker-org/jobs",
        params={"run": other_run_public_id},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_org_jobs_filters_combine(client: AsyncClient) -> None:
    """The ``kind`` and ``project`` filters combine conjunctively."""
    await seed_org_with_admin(client, "combo-org", _ADMIN)
    await seed_member("combo-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("combo-org")
    project_id = await _seed_project(org_id, "combo-proj")
    match = await _seed_job(
        org_id, kind=JobKind.dashboard_build, project_id=project_id
    )
    # Right kind, no project.
    await _seed_job(org_id, kind=JobKind.dashboard_build)
    # Right project, wrong kind.
    await _seed_job(
        org_id, kind=JobKind.build_processing, project_id=project_id
    )

    response = await client.get(
        "/docverse/orgs/combo-org/jobs",
        params={"kind": "dashboard_build", "project": "combo-proj"},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    assert [job["id"] for job in response.json()] == [match]


@pytest.mark.asyncio
async def test_list_org_jobs_paginates(client: AsyncClient) -> None:
    """A cursor from the Link header pages through the listing."""
    await seed_org_with_admin(client, "page-org", _ADMIN)
    await seed_member("page-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("page-org")
    seeded = [await _seed_job(org_id) for _ in range(5)]
    newest = list(reversed(seeded))

    first = await client.get(
        "/docverse/orgs/page-org/jobs",
        params={"limit": 2},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert first.status_code == 200
    assert first.headers["X-Total-Count"] == "5"
    assert [job["id"] for job in first.json()] == [newest[0], newest[1]]

    # Follow the "next" cursor from the Link header. On the first page
    # only the "next" link carries a cursor, so the first match is it.
    link = first.headers["Link"]
    assert 'rel="next"' in link
    match = re.search(r"cursor=([^&>]+)", link)
    assert match is not None
    cursor = match.group(1)

    second = await client.get(
        "/docverse/orgs/page-org/jobs",
        params={"limit": 2, "cursor": cursor},
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert second.status_code == 200
    assert [job["id"] for job in second.json()] == [newest[2], newest[3]]


@pytest.mark.asyncio
async def test_list_org_jobs_no_role_returns_403(client: AsyncClient) -> None:
    """A user with no role in the org gets 403."""
    await seed_org_with_admin(client, "noauth-org", _ADMIN)
    org_id = await _org_id("noauth-org")
    await _seed_job(org_id)

    response = await client.get(
        "/docverse/orgs/noauth-org/jobs",
        headers={"X-Auth-Request-User": "nobody-user"},
    )
    assert response.status_code == 403
