"""Tests for the org-scoped GET /orgs/:org/jobs/:job endpoint."""

from __future__ import annotations

import re
from typing import Any

import pytest
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency

from docverse.client.models import (
    BuildCreate,
    EditionCreate,
    EditionKind,
    OrgRole,
    ProjectCreate,
    TrackingMode,
)
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.domain.base32id import (
    generate_base32_id,
    serialize_base32_id,
    validate_base32_id,
)
from docverse.domain.queue import JobKind, JobStatus
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"
_HASH = "sha256:" + "a" * 64


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
    build_id: int | None = None,
    edition_id: int | None = None,
    keeper_sync_run_id: int | None = None,
    start: bool = False,
    phase: str | None = None,
    progress: dict[str, Any] | None = None,
) -> str:
    """Seed a queue job for an org; return its Base32 public id.

    ``start`` transitions the job to ``in_progress`` and ``phase`` /
    ``progress`` record a phase snapshot, mirroring how a worker advances
    a job — this lets the org-scoped ``GET`` be exercised over the same
    surface the legacy ``/queue/jobs`` endpoint used to cover.
    """
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            store = QueueJobStore(session=session, logger=logger)
            job = await store.create(
                kind=kind,
                org_id=org_id,
                project_id=project_id,
                build_id=build_id,
                edition_id=edition_id,
                keeper_sync_run_id=keeper_sync_run_id,
            )
            if start:
                await store.start(job.id)
            if phase is not None:
                await store.update_phase(job.id, phase, progress=progress)
            await session.commit()
            return serialize_base32_id(job.public_id)
    msg = "no session"
    raise AssertionError(msg)


async def _seed_project_build(
    org_id: int, project_slug: str
) -> tuple[int, int, str]:
    """Seed a project + build in an org.

    Returns ``(project_id, build_id, build_public_id_b32)``.
    """
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            proj_store = ProjectStore(session=session, logger=logger)
            build_store = BuildStore(session=session, logger=logger)
            project = await proj_store.create(
                org_id=org_id,
                data=ProjectCreate(
                    slug=project_slug,
                    title=f"Project {project_slug}",
                    source_url="https://example.com/example/repo",
                ),
            )
            build = await build_store.create(
                project_id=project.id,
                project_slug=project_slug,
                data=BuildCreate(git_ref="main", content_hash=_HASH),
                uploader="tester",
            )
            project_id = project.id
            build_id = build.id
            build_public = serialize_base32_id(build.public_id)
            await session.commit()
            return project_id, build_id, build_public
    msg = "no session"
    raise AssertionError(msg)


async def _seed_edition(project_id: int, slug: str) -> int:
    """Seed an edition in a project; return its integer primary key."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=logger)
            edition = await edition_store.create(
                project_id=project_id,
                data=EditionCreate(
                    slug=slug,
                    title=slug.title(),
                    kind=EditionKind.draft,
                    tracking_mode=TrackingMode.git_ref,
                ),
            )
            edition_id = edition.id
            await session.commit()
            return edition_id
    msg = "no session"
    raise AssertionError(msg)


async def _soft_delete_build(build_id: int) -> None:
    """Soft-delete a build so its back-reference URL degrades to null."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            build_store = BuildStore(session=session, logger=logger)
            await build_store.soft_delete(build_id=build_id)
            await session.commit()
            return
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


@pytest.mark.asyncio
async def test_list_org_jobs_memoizes_project_lookup(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A page of jobs sharing one project resolves its slug with one query.

    Guards the ``project_slug_cache`` memoization threaded from the list
    handler: subject-URL resolution must not issue a per-job project-store
    query when many jobs on a page share a project. Spy on
    ``ProjectStore.get_by_id`` and assert exactly one lookup across three
    jobs that all target the same project.
    """
    await seed_org_with_admin(client, "cache-org", _ADMIN)
    await seed_member("cache-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("cache-org")
    project_id, build_id, _build_public = await _seed_project_build(
        org_id, "cache-proj"
    )
    # Three build_processing jobs all sharing the one project (and build),
    # so each one's subject-URL resolution needs the project's slug.
    for _ in range(3):
        await _seed_job(
            org_id,
            kind=JobKind.build_processing,
            project_id=project_id,
            build_id=build_id,
        )

    lookups: list[int] = []
    original_get_by_id = ProjectStore.get_by_id

    async def counting_get_by_id(self: ProjectStore, project_pk: int) -> Any:
        lookups.append(project_pk)
        return await original_get_by_id(self, project_pk)

    monkeypatch.setattr(ProjectStore, "get_by_id", counting_get_by_id)

    response = await client.get(
        "/docverse/orgs/cache-org/jobs",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    assert response.headers["X-Total-Count"] == "3"
    # All three jobs resolve their build_url (and thus the project slug)...
    data = response.json()
    assert len(data) == 3
    for job in data:
        assert job["build_url"] is not None
    # ...but the shared project's slug is fetched exactly once.
    assert lookups == [project_id]


# ---------------------------------------------------------------------------
# Single-job representation coverage.
#
# These behaviors were previously exercised against the retired
# ``GET /queue/jobs/{job}`` endpoint; they are re-homed here against the
# org-scoped ``GET /orgs/{org}/jobs/{job}`` which is now their only surface.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_org_job_invalid_id_returns_422(client: AsyncClient) -> None:
    """A malformed Base32 job id is rejected with 422 for an authed reader."""
    await seed_org_with_admin(client, "badid-org", _ADMIN)
    await seed_member("badid-org", "reader-user", OrgRole.reader)

    response = await client.get(
        "/docverse/orgs/badid-org/jobs/not-a-valid-id",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_org_job_in_progress(client: AsyncClient) -> None:
    """A started job exposes its in_progress status, phase, and progress."""
    await seed_org_with_admin(client, "prog-org", _ADMIN)
    await seed_member("prog-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("prog-org")
    job_id = await _seed_job(
        org_id,
        start=True,
        phase="editions",
        progress={"editions_total": 2, "editions_completed": []},
    )

    response = await client.get(
        f"/docverse/orgs/prog-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == JobStatus.in_progress
    assert data["phase"] == "editions"
    assert data["progress"]["editions_total"] == 2
    assert data["date_started"] is not None


@pytest.mark.asyncio
async def test_get_org_job_build_processing_typed_progress(
    client: AsyncClient,
) -> None:
    """A build_processing job exposes its progress via the typed fields."""
    await seed_org_with_admin(client, "typed-org", _ADMIN)
    await seed_member("typed-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("typed-org")
    job_id = await _seed_job(
        org_id,
        phase="complete",
        progress={
            "message": "Build processing complete",
            "object_count": 2,
            "total_size_bytes": 4096,
            "editions_updated": [{"slug": "main", "action": "created"}],
            "editions_skipped": [{"slug": "stale"}],
            "publish_jobs": [
                {
                    "edition_slug": "main",
                    "publish_queue_job_public_id": "1000-0000-0000-05",
                }
            ],
        },
    )

    response = await client.get(
        f"/docverse/orgs/typed-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    progress = response.json()["progress"]
    assert progress["object_count"] == 2
    assert progress["editions_updated"][0]["slug"] == "main"
    assert progress["editions_updated"][0]["action"] == "created"
    assert progress["editions_skipped"][0]["slug"] == "stale"
    assert progress["publish_jobs"][0]["edition_slug"] == "main"
    assert (
        progress["publish_jobs"][0]["publish_queue_job_public_id"]
        == "1000-0000-0000-05"
    )


@pytest.mark.asyncio
async def test_get_org_job_non_build_progress_preserved(
    client: AsyncClient,
) -> None:
    """A non-build kind's progress round-trips unchanged via extra='allow'."""
    await seed_org_with_admin(client, "nb-org", _ADMIN)
    await seed_member("nb-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("nb-org")
    job_id = await _seed_job(
        org_id,
        kind=JobKind.keeper_sync_run_discovery,
        phase="complete",
        progress={
            "message": "Discovery complete",
            "in_scope_count": 5,
            "enqueued_count": 4,
        },
    )

    response = await client.get(
        f"/docverse/orgs/nb-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    progress = response.json()["progress"]
    assert progress["message"] == "Discovery complete"
    assert progress["in_scope_count"] == 5
    assert progress["enqueued_count"] == 4


@pytest.mark.asyncio
async def test_get_org_job_non_build_progress_omits_null_build_fields(
    client: AsyncClient,
) -> None:
    """A non-build job's progress JSON omits the six build-specific keys.

    ``from_domain`` validates any non-null progress into
    ``BuildProcessingProgress``; without the model serializer the six
    build-specific typed fields would leak as ``null`` for a non-build kind.
    Assert they are absent while the job's real keys survive.
    """
    await seed_org_with_admin(client, "nbnull-org", _ADMIN)
    await seed_member("nbnull-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("nbnull-org")
    job_id = await _seed_job(
        org_id,
        kind=JobKind.keeper_sync_run_discovery,
        phase="complete",
        progress={
            "message": "Discovery complete",
            "in_scope_count": 5,
            "enqueued_count": 4,
        },
    )

    response = await client.get(
        f"/docverse/orgs/nbnull-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    progress = response.json()["progress"]
    assert progress["message"] == "Discovery complete"
    assert progress["in_scope_count"] == 5
    assert progress["enqueued_count"] == 4
    for key in (
        "object_count",
        "total_size_bytes",
        "editions_updated",
        "editions_skipped",
        "publish_jobs",
        "edition_tracking_error",
    ):
        assert key not in progress


@pytest.mark.asyncio
async def test_get_org_job_build_processing_subject_url(
    client: AsyncClient,
) -> None:
    """A build_processing job links to its build via build_url/subject_url."""
    await seed_org_with_admin(client, "bp-org", _ADMIN)
    await seed_member("bp-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("bp-org")
    project_id, build_id, build_public = await _seed_project_build(
        org_id, "bp-proj"
    )
    job_id = await _seed_job(
        org_id,
        kind=JobKind.build_processing,
        project_id=project_id,
        build_id=build_id,
    )

    response = await client.get(
        f"/docverse/orgs/bp-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    data = response.json()
    suffix = f"/orgs/bp-org/projects/bp-proj/builds/{build_public}"
    assert data["build_url"].endswith(suffix)
    assert data["subject_url"] == data["build_url"]
    assert data["edition_url"] is None


@pytest.mark.asyncio
async def test_get_org_job_edition_url_when_targets_edition(
    client: AsyncClient,
) -> None:
    """A job targeting an edition exposes edition_url as the subject."""
    await seed_org_with_admin(client, "pe-org", _ADMIN)
    await seed_member("pe-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("pe-org")
    project_id, build_id, build_public = await _seed_project_build(
        org_id, "pe-proj"
    )
    edition_id = await _seed_edition(project_id, "main")
    job_id = await _seed_job(
        org_id,
        kind=JobKind.publish_edition,
        project_id=project_id,
        build_id=build_id,
        edition_id=edition_id,
    )

    response = await client.get(
        f"/docverse/orgs/pe-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["edition_url"].endswith(
        "/orgs/pe-org/projects/pe-proj/editions/main"
    )
    assert data["build_url"].endswith(
        f"/orgs/pe-org/projects/pe-proj/builds/{build_public}"
    )
    assert data["subject_url"] == data["edition_url"]


@pytest.mark.asyncio
async def test_get_org_job_subject_urls_null_when_no_resource(
    client: AsyncClient,
) -> None:
    """A job that targets no build/edition has null back-reference URLs."""
    await seed_org_with_admin(client, "nosub-org", _ADMIN)
    await seed_member("nosub-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("nosub-org")
    job_id = await _seed_job(org_id, kind=JobKind.keeper_sync_run_discovery)

    response = await client.get(
        f"/docverse/orgs/nosub-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["build_url"] is None
    assert data["edition_url"] is None
    assert data["subject_url"] is None


@pytest.mark.asyncio
async def test_get_org_job_build_url_null_when_build_deleted(
    client: AsyncClient,
) -> None:
    """A build_processing job whose build is soft-deleted has null URLs."""
    await seed_org_with_admin(client, "del-org", _ADMIN)
    await seed_member("del-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("del-org")
    project_id, build_id, _build_public = await _seed_project_build(
        org_id, "del-proj"
    )
    await _soft_delete_build(build_id)
    job_id = await _seed_job(
        org_id,
        kind=JobKind.build_processing,
        project_id=project_id,
        build_id=build_id,
    )

    response = await client.get(
        f"/docverse/orgs/del-org/jobs/{job_id}",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["build_url"] is None
    assert data["subject_url"] is None
    assert data["edition_url"] is None


@pytest.mark.asyncio
async def test_job_progress_schema_is_typed(client: AsyncClient) -> None:
    """The OpenAPI spec types ``QueueJob.progress`` as BuildProcessingProgress.

    Also guards against the schema-name collision with the unrelated
    edition-update request body: introducing the nested ``EditionUpdateRef``
    model must not rename the existing ``EditionUpdate`` component.
    """
    response = await client.get("/docverse/openapi.json")
    assert response.status_code == 200
    schemas = response.json()["components"]["schemas"]

    assert "BuildProcessingProgress" in schemas
    assert "EditionUpdateRef" in schemas
    assert "PublishJobRef" in schemas
    assert "EditionUpdate" in schemas

    progress_schema = schemas["QueueJob"]["properties"]["progress"]
    refs = {
        option.get("$ref")
        for option in progress_schema["anyOf"]
        if "$ref" in option
    }
    assert "#/components/schemas/BuildProcessingProgress" in refs

    progress_props = set(schemas["BuildProcessingProgress"]["properties"])
    assert {
        "message",
        "object_count",
        "total_size_bytes",
        "editions_updated",
        "editions_skipped",
        "publish_jobs",
        "edition_tracking_error",
    } <= progress_props

    assert schemas["BuildProcessingProgress"]["additionalProperties"] is True


@pytest.mark.asyncio
async def test_job_schema_exposes_subject_urls(client: AsyncClient) -> None:
    """The OpenAPI QueueJob schema declares the back-reference URL fields."""
    response = await client.get("/docverse/openapi.json")
    assert response.status_code == 200
    props = response.json()["components"]["schemas"]["QueueJob"]["properties"]
    for name in ("subject_url", "build_url", "edition_url"):
        assert name in props


# ---------------------------------------------------------------------------
# Retirement of the legacy surfaces (PRD #449).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_legacy_queue_job_endpoint_removed(client: AsyncClient) -> None:
    """``GET /queue/jobs/{job}`` no longer routes; it is absent from spec."""
    response = await client.get("/docverse/queue/jobs/1000-0000-0000-05")
    assert response.status_code == 404

    spec = (await client.get("/docverse/openapi.json")).json()
    assert not any("/queue/" in path for path in spec["paths"]), (
        "the /queue router must be fully retired from the OpenAPI spec"
    )


@pytest.mark.asyncio
async def test_keeper_sync_run_jobs_endpoint_removed(
    client: AsyncClient,
) -> None:
    """The run-scoped jobs subresource no longer routes nor appears in spec."""
    await seed_org_with_admin(client, "gone-org", _ADMIN)
    response = await client.get(
        "/docverse/orgs/gone-org/keeper-sync/runs/1000-0000-0000-05/jobs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404

    spec = (await client.get("/docverse/openapi.json")).json()
    assert not any(
        path.endswith("/keeper-sync/runs/{run}/jobs") for path in spec["paths"]
    )


@pytest.mark.asyncio
async def test_jobs_tag_replaces_queue(client: AsyncClient) -> None:
    """The OpenAPI ``jobs`` tag replaces the retired ``queue`` tag."""
    spec = (await client.get("/docverse/openapi.json")).json()
    tag_names = {tag["name"] for tag in spec.get("tags", [])}
    assert "jobs" in tag_names
    assert "queue" not in tag_names

    # The org-scoped jobs routes carry the ``jobs`` tag.
    op = spec["paths"]["/docverse/orgs/{org}/jobs/{job}"]["get"]
    assert "jobs" in op["tags"]


@pytest.mark.asyncio
async def test_events_subresource_reserved_not_implemented(
    client: AsyncClient,
) -> None:
    """``{job}/events`` is documented as reserved but not implemented."""
    await seed_org_with_admin(client, "sse-org", _ADMIN)
    await seed_member("sse-org", "reader-user", OrgRole.reader)
    org_id = await _org_id("sse-org")
    job_id = await _seed_job(org_id)

    # The reserved SSE subresource is not routed yet.
    response = await client.get(
        f"/docverse/orgs/sse-org/jobs/{job_id}/events",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 404

    spec = (await client.get("/docverse/openapi.json")).json()
    # It is documented (reserved) on the single-job endpoint's description
    # but never registered as its own path.
    assert "/docverse/orgs/{org}/jobs/{job}/events" not in spec["paths"]
    description = spec["paths"]["/docverse/orgs/{org}/jobs/{job}"]["get"][
        "description"
    ]
    assert "events" in description
    assert "text/event-stream" in description
