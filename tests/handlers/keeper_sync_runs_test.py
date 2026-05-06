"""Tests for the org-scoped LTD Keeper sync run endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

import pytest
import structlog
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency

from docverse.client.models import OrgRole
from docverse.dbschema.keeper_sync_run import SqlKeeperSyncRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.storage.organization_store import OrganizationStore
from tests.conftest import seed_member, seed_org_with_admin

_ADMIN = "admin-user"
_ORG = "ks-org"


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
) -> None:
    """Seed a queue_jobs row directly attributed to the run."""
    async for session in db_session_dependency():
        async with session.begin():
            row = SqlQueueJob(
                public_id=validate_base32_id(generate_base32_id()),
                kind=JobKind.keeper_sync_project.value,
                status=status.value,
                org_id=org_id,
                keeper_sync_run_id=run_id,
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
            await session.commit()


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
    run_id: int = create_response.json()["run"]["id"]
    org_id = await _get_org_id()

    # Seed mixed-status queue_jobs attributed to this run.
    await _seed_queue_job(
        org_id=org_id, run_id=run_id, status=JobStatus.queued
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_id, status=JobStatus.in_progress
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_id, status=JobStatus.completed
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_id, status=JobStatus.completed
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_id, status=JobStatus.failed
    )
    await _seed_queue_job(
        org_id=org_id, run_id=run_id, status=JobStatus.cancelled
    )

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_id}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 200
    body = response.json()
    # 1 queued + 1 in_progress + 1 (discovery's own row) = 3 pending.
    assert body["pending_count"] == 3
    assert body["succeeded_count"] == 2
    assert body["failed_count"] == 2
    assert body["total_count"] == 7


@pytest.mark.asyncio
async def test_get_run_404_for_unknown_run(client: AsyncClient) -> None:
    await _setup_org(client)
    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/999",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


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
            row = SqlKeeperSyncRun(
                org_id=other.id, kind="backfill", status="pending"
            )
            session.add(row)
            await session.flush()
            run_id = row.id
            await session.commit()

    response = await client.get(
        f"/docverse/orgs/{_ORG}/keeper-sync/runs/{run_id}",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 404


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
                row = SqlKeeperSyncRun(
                    org_id=org_id, kind="backfill", status=status
                )
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
                    SqlKeeperSyncRun(
                        org_id=org_id, kind="backfill", status=status
                    )
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
                    SqlKeeperSyncRun(
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
    link = first.headers.get("Link", "")
    assert "next" in link

    # Extract the next cursor from the Link header.
    next_url = _extract_next_url(link)
    assert next_url is not None
    second = await client.get(
        next_url, headers={"X-Auth-Request-User": _ADMIN}
    )
    assert second.status_code == 200
    page_two = second.json()
    assert len(page_two) == 1
    page_one_ids = {r["id"] for r in page_one}
    page_two_ids = {r["id"] for r in page_two}
    assert page_one_ids.isdisjoint(page_two_ids)


def _extract_next_url(link_header: str) -> str | None:
    for part in link_header.split(","):
        section = part.strip()
        if section.endswith('rel="next"'):
            url = section.split(";")[0].strip()
            if url.startswith("<") and url.endswith(">"):
                return url[1:-1]
    return None


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
