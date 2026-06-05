"""Tests for the GET /queue/jobs/:job endpoint."""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.storage.queue_job_store import QueueJobStore


@pytest.mark.asyncio
async def test_get_queue_job(
    client: AsyncClient,
    db_session: AsyncSession,
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
        headers={"X-Auth-Request-User": "superadmin"},
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
    response = await client.get("/docverse/queue/jobs/1000-0000-0000-05")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_queue_job_invalid_id(
    client: AsyncClient,
) -> None:
    """Test 422 for an invalid Base32 ID."""
    response = await client.get("/docverse/queue/jobs/not-a-valid-id")
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_queue_job_in_progress(
    client: AsyncClient,
    db_session: AsyncSession,
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
        headers={"X-Auth-Request-User": "superadmin"},
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


@pytest.mark.asyncio
async def test_get_queue_job_build_processing_typed_progress(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """A build_processing job exposes its progress via the typed fields."""
    logger = structlog.get_logger("docverse")

    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "typed-org",
            "title": "Typed Org",
            "base_domain": "typed.example.com",
        },
        headers={"X-Auth-Request-User": "superadmin"},
    )

    async with db_session.begin():
        store = QueueJobStore(session=db_session, logger=logger)
        job = await store.create(kind=JobKind.build_processing, org_id=1)
        await store.update_phase(
            job.id,
            "complete",
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
        await db_session.commit()

    job_id_str = serialize_base32_id(job.public_id)
    response = await client.get(f"/docverse/queue/jobs/{job_id_str}")
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
async def test_get_queue_job_non_build_progress_preserved(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """A non-build kind's progress round-trips unchanged via extra='allow'."""
    logger = structlog.get_logger("docverse")

    await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": "ks-org",
            "title": "Keeper Sync Org",
            "base_domain": "ks.example.com",
        },
        headers={"X-Auth-Request-User": "superadmin"},
    )

    async with db_session.begin():
        store = QueueJobStore(session=db_session, logger=logger)
        job = await store.create(
            kind=JobKind.keeper_sync_run_discovery, org_id=1
        )
        await store.update_phase(
            job.id,
            "complete",
            progress={
                "message": "Discovery complete",
                "in_scope_count": 5,
                "enqueued_count": 4,
            },
        )
        await db_session.commit()

    job_id_str = serialize_base32_id(job.public_id)
    response = await client.get(f"/docverse/queue/jobs/{job_id_str}")
    assert response.status_code == 200

    progress = response.json()["progress"]
    assert progress["message"] == "Discovery complete"
    assert progress["in_scope_count"] == 5
    assert progress["enqueued_count"] == 4


@pytest.mark.asyncio
async def test_queue_job_progress_schema_is_typed(
    client: AsyncClient,
) -> None:
    """The OpenAPI spec types ``QueueJob.progress`` as BuildProcessingProgress.

    Also guards against the schema-name collision with the unrelated
    edition-update request body: introducing the nested ``EditionUpdateRef``
    model must not rename the existing ``EditionUpdate`` component.
    """
    response = await client.get("/docverse/openapi.json")
    assert response.status_code == 200
    schemas = response.json()["components"]["schemas"]

    # The typed progress model and its nested entries are surfaced.
    assert "BuildProcessingProgress" in schemas
    assert "EditionUpdateRef" in schemas
    assert "PublishJobRef" in schemas

    # The unrelated edition-update request body keeps its own name.
    assert "EditionUpdate" in schemas

    # QueueJob.progress references the typed model rather than a free-form
    # object (so generated clients / api-types get real fields).
    progress_schema = schemas["QueueJob"]["properties"]["progress"]
    refs = {
        option.get("$ref")
        for option in progress_schema["anyOf"]
        if "$ref" in option
    }
    assert "#/components/schemas/BuildProcessingProgress" in refs

    # extra='allow' is reflected so other kinds round-trip generically.
    assert schemas["BuildProcessingProgress"]["additionalProperties"] is True
