"""The ``build_processing`` enqueue must not outrun its ``queue_jobs`` row.

``PATCH .../builds/{build}`` with ``status=uploaded`` used to hand arq a
``build_processing`` job from inside the still-uncommitted request
transaction, so a worker that picked the job up promptly could not see
the row the request was about to write. These tests pin the ordering
from the queue backend's point of view: by the time anything is handed
to arq, the row is durable and visible on an independent session.
"""

from __future__ import annotations

from typing import Any

import pytest
from httpx import AsyncClient
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select

from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.storage.queue_backend import ArqQueueBackend, EnqueuedJob
from tests.conftest import seed_build, seed_org_with_admin


async def _visible_build_processing_rows() -> list[tuple[int, str | None]]:
    """Read committed ``build_processing`` rows on an independent session.

    A second session sees only committed data, which is exactly what an
    arq worker handed the job would see.
    """
    sessions = db_session_dependency()
    session = await anext(sessions)
    try:
        result = await session.execute(
            select(SqlQueueJob.id, SqlQueueJob.backend_job_id).where(
                SqlQueueJob.kind == "build_processing"
            )
        )
        return [(row[0], row[1]) for row in result.all()]
    finally:
        await sessions.aclose()


async def _setup(client: AsyncClient) -> str:
    """Seed an org + project and return a pending build's base32 ID."""
    await seed_org_with_admin(client, "race-org", "testuser")
    await client.post(
        "/docverse/orgs/race-org/projects",
        json={
            "slug": "race-proj",
            "title": "Race Project",
            "source_url": "https://example.com/example/race",
        },
        headers={"X-Auth-Request-User": "testuser"},
    )
    return await seed_build("race-org", "race-proj")


@pytest.mark.asyncio
async def test_queue_job_row_visible_before_enqueue(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Arq only learns about the job after its row is committed."""
    build_id = await _setup(client)

    observed: list[list[tuple[int, str | None]]] = []
    original = ArqQueueBackend.enqueue

    async def spy(
        self: ArqQueueBackend,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> EnqueuedJob:
        if job_type == "build_processing":
            observed.append(await _visible_build_processing_rows())
        return await original(self, job_type, payload, queue_name=queue_name)

    monkeypatch.setattr(ArqQueueBackend, "enqueue", spy)

    response = await client.patch(
        f"/docverse/orgs/race-org/projects/race-proj/builds/{build_id}",
        json={"status": "uploaded"},
        headers={"X-Auth-Request-User": "testuser"},
    )
    assert response.status_code == 200
    assert observed, "build_processing was never enqueued"
    assert observed[0], (
        "arq was handed build_processing before its queue_jobs row was"
        " committed"
    )
