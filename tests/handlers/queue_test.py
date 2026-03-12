"""Tests for the GET /queue/jobs/:job endpoint."""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.storage.queue_job_store import QueueJobStore


@pytest.mark.asyncio
async def test_get_queue_job(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Test retrieving a queue job by its public Base32 ID."""
    logger = structlog.get_logger("docverse")

    # First create an organization (queue jobs require org_id FK).
    org_response = await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "test-org",
            "title": "Test Organization",
            "base_domain": "test.example.com",
        },
    )
    assert org_response.status_code == 201

    # Create a queue job via the store.
    async with db_session.begin():
        store = QueueJobStore(session=db_session, logger=logger)
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await db_session.commit()

    job_id_str = serialize_base32_id(job.public_id)

    response = await client.get(f"/docverse/queue/jobs/{job_id_str}")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == job_id_str
    assert data["kind"] == "build_processing"
    assert data["status"] == "queued"
    assert data["self_url"].endswith(f"/queue/jobs/{job_id_str}")
    assert data["date_created"] is not None
    assert data["date_started"] is None
    assert data["date_completed"] is None
    assert data["phase"] is None
    assert data["progress"] is None
    assert data["errors"] is None


@pytest.mark.asyncio
async def test_get_queue_job_not_found(
    client: AsyncClient,
) -> None:
    """Test 404 for a nonexistent queue job."""
    response = await client.get("/docverse/queue/jobs/0000-0000-0000-00")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_queue_job_invalid_id(
    client: AsyncClient,
) -> None:
    """Test 404 for an invalid Base32 ID."""
    response = await client.get("/docverse/queue/jobs/not-a-valid-id")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_queue_job_in_progress(
    client: AsyncClient,
    db_session: async_scoped_session[AsyncSession],
) -> None:
    """Test retrieving a job that has been started."""
    logger = structlog.get_logger("docverse")

    # Create org first.
    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "test-org-2",
            "title": "Test Organization 2",
            "base_domain": "test2.example.com",
        },
    )

    # Create and start a queue job.
    async with db_session.begin():
        store = QueueJobStore(session=db_session, logger=logger)
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        started_job = await store.start(job.id)
        await store.update_phase(
            started_job.id,
            "editions",
            progress={"editions_total": 2, "editions_completed": []},
        )
        await db_session.commit()

    job_id_str = serialize_base32_id(job.public_id)
    response = await client.get(f"/docverse/queue/jobs/{job_id_str}")
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == JobStatus.in_progress
    assert data["phase"] == "editions"
    assert data["progress"]["editions_total"] == 2
    assert data["date_started"] is not None
