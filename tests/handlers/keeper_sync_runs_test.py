"""Tests for the org-scoped LTD Keeper sync run endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

import pytest
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency
from safir.http import PaginationLinkData
from sqlalchemy import select

from docverse.client.models import OrgRole
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import (
    generate_base32_id,
    serialize_base32_id,
    validate_base32_id,
)
from docverse.domain.queue import JobKind, JobStatus
from docverse.storage.organization_store import OrganizationStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"
_ORG = "ks-org"


def _make_run(**kwargs: Any) -> SqlKeeperSyncRun:
    """Build a run row with a minted ``public_id`` for seeding tests."""
    kwargs.setdefault("public_id", validate_base32_id(generate_base32_id()))
    return SqlKeeperSyncRun(**kwargs)


async def _seed_run(
    org_id: int, *, kind: str = "backfill", status: str = "pending"
) -> tuple[int, str]:
    """Seed a run row directly; return ``(primary_key, public_id_b32)``.

    The primary key is used to attribute seeded ``queue_jobs`` (the FK
    is the integer PK); the Base32 ``public_id`` is what the public API
    addresses the run by.
    """
    async for session in db_session_dependency():
        async with session.begin():
            row = _make_run(org_id=org_id, kind=kind, status=status)
            session.add(row)
            await session.flush()
            pk = row.id
            public = serialize_base32_id(row.public_id)
            await session.commit()
            return pk, public
    msg = "no session"
    raise AssertionError(msg)


async def _run_pk_for_public_id(public_id_b32: str) -> int:
    """Resolve a run's primary key from its Base32 ``public_id``."""
    public_id = validate_base32_id(public_id_b32)
    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlKeeperSyncRun.id).where(
                SqlKeeperSyncRun.public_id == public_id
            )
            return (await session.execute(stmt)).scalar_one()
    msg = "no session"
    raise AssertionError(msg)


async def _setup_org(client: AsyncClient) -> None:
    await seed_org_with_admin(client, _ORG, _ADMIN)


async def _enable_sync(
    client: AsyncClient,
    *,
    project_slugs: list[str] | Literal["*"] = "*",
) -> None:
    response = await client.put(
        f"/docverse/orgs/{_ORG}/keeper-sync",
        json={
            "enabled": True,
            "ltd_base_url": "https://keeper.lsst.codes/",
            "project_slugs": project_slugs,
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200


async def _seed_queue_job(
    *,
    org_id: int,
    run_id: int,
    status: JobStatus,
    subject_label: str | None = None,
) -> int:
    """Seed a queue_jobs row directly attributed to the run.

    Returns the row's primary key id.
    """
    async for session in db_session_dependency():
        async with session.begin():
            row = SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.keeper_sync_project.value,
                status=status.value,
                org_id=org_id,
                keeper_sync_run_id=run_id,
                subject_label=subject_label,
                date_completed=datetime.now(tz=UTC)
                if status
                in {
                    JobStatus.completed,
                    JobStatus.failed,
                    JobStatus.cancelled,
                    JobStatus.completed_with_errors,
                }
                else None,
            )
            session.add(row)
            await session.flush()
            row_id = row.id
            await session.commit()
            return row_id
    msg = "no session"
    raise AssertionError(msg)


async def _get_org_id() -> int:
    logger = structlog.get_logger("test")
    async for session in db_session_dependency():
        store = OrganizationStore(session=session, logger=logger)
        org = await store.get_by_slug(_ORG)
        assert org is not None
        return org.id
    msg = "no session"
    raise AssertionError(msg)


@pytest.mark.asyncio
async def test_post_run_returns_202_with_run_and_queue_job_link(
    client: AsyncClient,
) -> None:
    """``POST /runs`` creates a run, enqueues discovery, returns 202."""
    await _setup_org(client)
    await _enable_sync(client)

    response = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 202
    body: dict[str, Any] = response.json()
    assert body["run"]["status"] == "pending"
    assert body["run"]["kind"] == "backfill"
    # The discovery queue-job itself is run-attributed and starts queued,
    # so the freshly-created run already has one pending job on its books.
    assert body["run"]["pending_count"] == 1
    assert body["run"]["succeeded_count"] == 0
    assert body["run"]["failed_count"] == 0
    assert body["run"]["total_count"] == 1
    assert "self_url" in body["run"]
    # The run id is the Base32 public identifier, not the raw integer PK:
    # it is a hyphen-grouped string that decodes back to a non-negative int.
    assert isinstance(body["run"]["id"], str)
    assert "-" in body["run"]["id"]
    assert validate_base32_id(body["run"]["id"]) >= 0
    assert body["run"]["jobs_url"] == str(
        client.base_url.join(
            f"/docverse/orgs/{_ORG}/keeper-sync/runs/{body['run']['id']}/jobs"
        )
    )
    assert "queue_job_url" in body
    assert body["queue_job_id"]
    assert body["queue_job_url"].endswith(
        f"/queue/jobs/{body['queue_job_id']}"
    )


@pytest.mark.asyncio
async def test_post_run_returns_409_when_disabled(
    client: AsyncClient,
) -> None:
    """``POST /runs`` against a disabled config returns 409."""
    await _setup_org(client)
    # Default config is disabled, no PUT.
    response = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_post_run_409_when_non_terminal_run_exists(
    client: AsyncClient,
) -> None:
    """A second concurrent ``POST /runs`` returns 409."""
    await _setup_org(client)
    await _enable_sync(client)
    first = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert first.status_code == 202

    second = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_get_run_returns_aggregate_counters(
    client: AsyncClient,
) -> None:
    """``GET /runs/{id}`` aggregates counters from run-attributed jobs."""
    await _setup_org(client)
    await _enable_sync(client)
    create_response = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert create_response.status_code == 202
    run_b32: str = create_response.json()["run"]["id"]
    run_pk = await _run_pk_for_public_id(run_b32)
    org_id = await _get_org_id()

    # Seed mixed-status queue_jobs attributed to this run.
    await _seed_queue_job(
        org_id=org_id, run_id=run_pk, status=JobStatus.queued
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_pk, status=JobStatus.in_progress
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_pk, status=JobStatus.completed
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_pk, status=JobStatus.completed
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_pk, status=JobStatus.failed
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_pk, status=JobStatus.cancelled
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_b32}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    # 1 queued + 1 in_progress + 1 (discovery's own row) = 3 pending.
    assert body["pending_count"] == 3
    assert body["succeeded_count"] == 2
    assert body["failed_count"] == 2
    assert body["total_count"] == 7
    # date_last_activity reflects the MAX(coalesce(...)) across the
    # children and is non-null because the run has attributed jobs.
    assert body["date_last_activity"] is not None
    assert body["jobs_url"] == str(
        client.base_url.join(
            f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_b32}/jobs"
        )
    )


@pytest.mark.asyncio
async def test_get_run_date_last_activity_matches_max_child_event(
    client: AsyncClient,
) -> None:
    """``date_last_activity`` reflects the MAX coalesced child timestamp."""
    await _setup_org(client)
    org_id = await _get_org_id()
    # Seed a run directly so the response shape doesn't depend on the
    # discovery queue-job's auto-attributed timestamps.
    run_pk, run_b32 = await _seed_run(org_id, status="in_progress")

    # Three children, each completing at successively later instants.
    # The MAX(coalesce(...)) post-condition picks the latest one.
    completed_ids: list[int] = [
        await _seed_queue_job(
            org_id=org_id, run_id=run_pk, status=JobStatus.completed
        )
        for _ in range(3)
    ]

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_b32}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    last_activity = datetime.fromisoformat(body["date_last_activity"])
    # The third child's date_completed is the latest event the run has
    # observed; the response surfaces it as the run's date_last_activity.
    async for session in db_session_dependency():
        async with session.begin():
            stmt = select(SqlQueueJob.date_completed).where(
                SqlQueueJob.id == completed_ids[-1]
            )
            latest = (await session.execute(stmt)).scalar_one()
            assert latest is not None
            assert last_activity == latest


@pytest.mark.asyncio
async def test_get_run_date_last_activity_null_when_no_children(
    client: AsyncClient,
) -> None:
    """``date_last_activity`` is null on a run with no attributed jobs."""
    await _setup_org(client)
    org_id = await _get_org_id()
    _, run_b32 = await _seed_run(org_id, status="pending")

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_b32}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total_count"] == 0
    assert body["date_last_activity"] is None


@pytest.mark.asyncio
async def test_list_runs_surfaces_date_last_activity_per_row(
    client: AsyncClient,
) -> None:
    """``GET /runs`` exposes ``date_last_activity`` per row.

    A run with attributed children carries a non-null timestamp; a run
    without children carries ``null``. Both must coexist in a single
    page so the run-list aggregation is verified to keep its
    one-round-trip per page guarantee even when child rows are sparse.
    """
    await _setup_org(client)
    org_id = await _get_org_id()
    with_pk, with_b32 = await _seed_run(org_id, status="succeeded")
    _, without_b32 = await _seed_run(org_id, status="failed")

    await _seed_queue_job(
        org_id=org_id, run_id=with_pk, status=JobStatus.completed
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    runs = {r["id"]: r for r in response.json()}
    assert runs[with_b32]["date_last_activity"] is not None
    assert runs[without_b32]["date_last_activity"] is None


@pytest.mark.asyncio
async def test_get_run_404_for_unknown_run(client: AsyncClient) -> None:
    """A valid Base32 id with no matching run resolves to 404."""
    await _setup_org(client)
    unknown = serialize_base32_id(999999)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{unknown}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_run_422_for_malformed_run_id(client: AsyncClient) -> None:
    """A malformed (non-Base32) run id is rejected with 422, not 404/500."""
    await _setup_org(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/not-a-valid-id",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_run_404_for_run_in_other_org(
    client: AsyncClient,
) -> None:
    """A run belonging to another org cannot be fetched via this org."""
    await _setup_org(client)
    other_org = "ks-org-other"
    await seed_org_with_admin(client, other_org, _ADMIN)
    # Seed a run directly into the other org.
    logger = structlog.get_logger("test")
    async for session in db_session_dependency():
        async with session.begin():
            store = OrganizationStore(session=session, logger=logger)
            other = await store.get_by_slug(other_org)
            assert other is not None
            row = _make_run(org_id=other.id, kind="backfill", status="pending")
            session.add(row)
            await session.flush()
            run_b32 = serialize_base32_id(row.public_id)
            await session.commit()

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_b32}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_runs_returns_per_run_counters(
    client: AsyncClient,
) -> None:
    """``GET /runs`` returns correct counters scoped to each run."""
    await _setup_org(client)
    org_id = await _get_org_id()
    # Seed two terminal runs directly — counter aggregation must scope
    # per-run, not bleed across runs in the same org.
    run_a_pk, run_a_b32 = await _seed_run(org_id, status="succeeded")
    run_b_pk, run_b_b32 = await _seed_run(org_id, status="failed")

    await _seed_queue_job(
        org_id=org_id, run_id=run_a_pk, status=JobStatus.completed
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_a_pk, status=JobStatus.completed
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_b_pk, status=JobStatus.failed
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    runs = {r["id"]: r for r in response.json()}
    assert runs[run_a_b32]["succeeded_count"] == 2
    assert runs[run_a_b32]["failed_count"] == 0
    assert runs[run_a_b32]["total_count"] == 2
    assert runs[run_b_b32]["succeeded_count"] == 0
    assert runs[run_b_b32]["failed_count"] == 1
    assert runs[run_b_b32]["total_count"] == 1
    assert runs[run_a_b32]["jobs_url"] == str(
        client.base_url.join(
            f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_a_b32}/jobs"
        )
    )
    assert runs[run_b_b32]["jobs_url"] == str(
        client.base_url.join(
            f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_b_b32}/jobs"
        )
    )


@pytest.mark.asyncio
async def test_list_runs_returns_runs_newest_first(
    client: AsyncClient,
) -> None:
    """``GET /runs`` returns runs in date_started DESC order."""
    await _setup_org(client)
    org_id = await _get_org_id()
    # Seed three runs directly so we can control the order independently of
    # the partial unique index that disallows two non-terminal at once.
    async for session in db_session_dependency():
        async with session.begin():
            for status in ("succeeded", "failed", "succeeded"):
                row = _make_run(org_id=org_id, kind="backfill", status=status)
                session.add(row)
            await session.commit()

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    runs = response.json()
    assert len(runs) == 3
    starts = [r["date_started"] for r in runs]
    assert starts == sorted(starts, reverse=True)
    assert response.headers["X-Total-Count"] == "3"


@pytest.mark.asyncio
async def test_list_runs_filters_by_status(client: AsyncClient) -> None:
    """``GET /runs?status=...`` returns only runs in the given status."""
    await _setup_org(client)
    org_id = await _get_org_id()
    async for session in db_session_dependency():
        async with session.begin():
            for status in ("succeeded", "failed", "succeeded"):
                session.add(
                    _make_run(org_id=org_id, kind="backfill", status=status)
                )
            await session.commit()

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs?status=succeeded",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    runs = response.json()
    assert len(runs) == 2
    assert all(r["status"] == "succeeded" for r in runs)


@pytest.mark.asyncio
async def test_list_runs_paginates_with_cursor(client: AsyncClient) -> None:
    """``limit`` + ``cursor`` paginate through the run history."""
    await _setup_org(client)
    org_id = await _get_org_id()
    async for session in db_session_dependency():
        async with session.begin():
            for _ in range(3):
                session.add(
                    _make_run(
                        org_id=org_id, kind="backfill", status="succeeded"
                    )
                )
            await session.commit()

    first = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs?limit=2",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert first.status_code == 200
    page_one = first.json()
    assert len(page_one) == 2
    links = PaginationLinkData.from_header(first.headers.get("link"))
    assert links.next_url is not None
    second = await client.get(
        links.next_url, headers={"X-Auth-Request-User": _ADMIN}
    )
    assert second.status_code == 200
    page_two = second.json()
    assert len(page_two) == 1
    page_one_ids = {r["id"] for r in page_one}
    page_two_ids = {r["id"] for r in page_two}
    assert page_one_ids.isdisjoint(page_two_ids)


@pytest.mark.asyncio
async def test_post_run_403_for_non_admin(client: AsyncClient) -> None:
    await _setup_org(client)
    await _enable_sync(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_runs_403_for_non_admin(client: AsyncClient) -> None:
    await _setup_org(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_run_jobs_returns_subject_label(
    client: AsyncClient,
) -> None:
    """``GET /runs/{id}/jobs`` round-trips ``subject_label`` per child."""
    await _setup_org(client)
    org_id = await _get_org_id()
    run_pk, run_b32 = await _seed_run(org_id, status="in_progress")

    await _seed_queue_job(
        org_id=org_id,
        run_id=run_pk,
        status=JobStatus.completed,
        subject_label="sqr-001",
    )
    await _seed_queue_job(
        org_id=org_id,
        run_id=run_pk,
        status=JobStatus.in_progress,
        subject_label="sqr-002",
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_b32}/jobs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    jobs = response.json()
    assert len(jobs) == 2
    labels = {job["subject_label"] for job in jobs}
    assert labels == {"sqr-001", "sqr-002"}
    # Newest first: in_progress was inserted second.
    assert jobs[0]["subject_label"] == "sqr-002"
    assert response.headers["X-Total-Count"] == "2"
    # keeper_sync_run_id is exposed as the run's Base32 public id, not the PK.
    assert all(job["keeper_sync_run_id"] == run_b32 for job in jobs)


@pytest.mark.asyncio
async def test_get_run_jobs_filters_by_status(client: AsyncClient) -> None:
    """``?status=failed`` narrows the result set to failed children."""
    await _setup_org(client)
    org_id = await _get_org_id()
    run_pk, run_b32 = await _seed_run(org_id, status="in_progress")

    await _seed_queue_job(
        org_id=org_id,
        run_id=run_pk,
        status=JobStatus.completed,
        subject_label="ok-1",
    )
    await _seed_queue_job(
        org_id=org_id,
        run_id=run_pk,
        status=JobStatus.failed,
        subject_label="bad-1",
    )
    await _seed_queue_job(
        org_id=org_id,
        run_id=run_pk,
        status=JobStatus.failed,
        subject_label="bad-2",
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_b32}/jobs?status=failed",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    jobs = response.json()
    assert len(jobs) == 2
    assert {job["subject_label"] for job in jobs} == {"bad-1", "bad-2"}
    assert all(job["status"] == "failed" for job in jobs)
    assert response.headers["X-Total-Count"] == "2"


@pytest.mark.asyncio
async def test_get_run_jobs_paginates_with_cursor(
    client: AsyncClient,
) -> None:
    """``limit`` + ``cursor`` paginate through child queue jobs."""
    await _setup_org(client)
    org_id = await _get_org_id()
    run_pk, run_b32 = await _seed_run(org_id, status="in_progress")

    for index in range(3):
        await _seed_queue_job(
            org_id=org_id,
            run_id=run_pk,
            status=JobStatus.completed,
            subject_label=f"slug-{index}",
        )

    first = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_b32}/jobs?limit=2",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert first.status_code == 200
    page_one = first.json()
    assert len(page_one) == 2
    links = PaginationLinkData.from_header(first.headers.get("link"))
    assert links.next_url is not None

    second = await client.get(
        links.next_url, headers={"X-Auth-Request-User": _ADMIN}
    )
    assert second.status_code == 200
    page_two = second.json()
    assert len(page_two) == 1
    page_one_ids = {job["id"] for job in page_one}
    page_two_ids = {job["id"] for job in page_two}
    assert page_one_ids.isdisjoint(page_two_ids)


@pytest.mark.asyncio
async def test_get_run_jobs_404_for_unknown_run(client: AsyncClient) -> None:
    """A valid Base32 id with no matching run resolves to 404."""
    await _setup_org(client)
    unknown = serialize_base32_id(999999)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{unknown}/jobs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_run_jobs_422_for_malformed_run_id(
    client: AsyncClient,
) -> None:
    """A malformed (non-Base32) run id is rejected with 422, not 404/500."""
    await _setup_org(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/not-a-valid-id/jobs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_run_jobs_404_for_run_in_other_org(
    client: AsyncClient,
) -> None:
    """Cross-org access surfaces as 404, not 403."""
    await _setup_org(client)
    other_org = "ks-org-other"
    await seed_org_with_admin(client, other_org, _ADMIN)
    logger = structlog.get_logger("test")
    async for session in db_session_dependency():
        async with session.begin():
            store = OrganizationStore(session=session, logger=logger)
            other = await store.get_by_slug(other_org)
            assert other is not None
            row = _make_run(org_id=other.id, kind="backfill", status="pending")
            session.add(row)
            await session.flush()
            run_b32 = serialize_base32_id(row.public_id)
            await session.commit()

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_b32}/jobs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_run_jobs_403_for_non_admin(client: AsyncClient) -> None:
    await _setup_org(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)
    org_id = await _get_org_id()
    _, run_b32 = await _seed_run(org_id, status="pending")

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_b32}/jobs",
        headers={"X-Auth-Request-User": "reader-user"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_keeper_sync_url_fields_report_format_uri(
    client: AsyncClient,
) -> None:
    """The three HATEOAS URL fields advertise ``format: uri`` in OpenAPI.

    ``KeeperSyncRun.self_url``, ``KeeperSyncRunCreated.queue_job_url``,
    and ``KeeperSyncProjectRefreshAccepted.queue_job_url`` are typed
    ``HttpUrl`` on the client mirror so Pydantic emits the standard
    URL format string in the generated schema. The peer fields on the
    same models (``KeeperSyncRun.jobs_url`` and the URL fields on
    ``KeeperSyncProjectStatus``) are already ``HttpUrl``; this test
    locks the three formerly-``str`` fields into the same shape so a
    regression to plain ``str`` is caught at schema generation.

    The handler-side and client-side ``KeeperSyncRun`` schemas share a
    class name so FastAPI emits them under module-qualified keys; both
    must agree on ``format: uri`` because both are reachable from the
    public OpenAPI schema.
    """
    response = await client.get("/docverse/openapi.json")
    assert response.status_code == 200
    schemas = response.json()["components"]["schemas"]

    keeper_sync_run_schema_names = [
        name
        for name in schemas
        if name == "KeeperSyncRun" or name.endswith("__KeeperSyncRun")
    ]
    assert keeper_sync_run_schema_names, (
        "expected at least one KeeperSyncRun schema in OpenAPI"
    )

    targets: list[tuple[str, str]] = [
        *((name, "self_url") for name in keeper_sync_run_schema_names),
        ("KeeperSyncRunCreated", "queue_job_url"),
        ("KeeperSyncProjectRefreshAccepted", "queue_job_url"),
    ]
    for model_name, field_name in targets:
        field_schema = schemas[model_name]["properties"][field_name]
        assert field_schema.get("type") == "string", (
            f"{model_name}.{field_name} expected type=string,"
            f" got {field_schema!r}"
        )
        assert field_schema.get("format") == "uri", (
            f"{model_name}.{field_name} expected format=uri,"
            f" got {field_schema!r}"
        )
