"""Tests for the excluded-path scrub endpoint.

``POST /orgs/{org}/projects/{project}/builds/scrub-excluded-paths``
queues one ``excluded_path_scrub`` job per project; these tests pin its
HTTP contract and the job resource it points at. What the job does is
covered by ``tests/worker/excluded_path_scrub_test.py``.
"""

from __future__ import annotations

import pytest
import structlog
from httpx import AsyncClient
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency

from docverse.models import (
    ExcludedPathScrubBuild,
    ExcludedPathScrubEntry,
    ExcludedPathScrubProgress,
    JobKind,
    JobStatus,
    OrgRole,
    QueueJob,
)
from docverse_server.domain.base32id import validate_base32_id
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.queues import MAINTENANCE_QUEUE_NAME
from tests.conftest import seed_member, seed_org_with_admin
from tests.support.arq_testing import count_jobs_by_name

_ORG = "scrub-org"
_PROJECT = "scrub-proj"
_ADMIN = "admin-user"
_URL = f"/docverse/orgs/{_ORG}/projects/{_PROJECT}/builds/scrub-excluded-paths"


async def _setup(client: AsyncClient) -> None:
    await seed_org_with_admin(client, _ORG, _ADMIN)
    response = await client.post(
        f"/docverse/orgs/{_ORG}/projects",
        json={
            "slug": _PROJECT,
            "title": "Scrub Project",
            "source_url": "https://example.com/example/scrub",
        },
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_scrub_returns_202_with_job_url(client: AsyncClient) -> None:
    """An admin's request is accepted with a link to the queued job."""
    await _setup(client)

    response = await client.post(
        _URL,
        json={"dry_run": True},
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 202
    body = response.json()
    assert isinstance(body["job_id"], str)
    assert body["job_url"].endswith(f"/orgs/{_ORG}/jobs/{body['job_id']}")
    assert response.headers["Location"] == body["job_url"]
    assert _maintenance_scrub_jobs() == 1


def _maintenance_scrub_jobs() -> int:
    """Count ``excluded_path_scrub`` jobs handed to the maintenance queue."""
    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    return count_jobs_by_name(
        mock_arq, "excluded_path_scrub", queue_name=MAINTENANCE_QUEUE_NAME
    )


@pytest.mark.asyncio
async def test_scrub_job_reports_through_the_client_model(
    client: AsyncClient,
) -> None:
    """The job URL serves a scrub job whose report the client model types.

    The request's ``dry_run`` is recorded in the report as soon as the
    job is queued, so it is visible before a worker picks the job up.
    """
    await _setup(client)
    accepted = await client.post(
        _URL,
        json={"dry_run": True},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert accepted.status_code == 202

    response = await client.get(
        accepted.json()["job_url"],
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 200
    job = QueueJob.model_validate(response.json())
    assert job.kind == JobKind.excluded_path_scrub
    assert job.status == JobStatus.queued
    assert job.subject_label == _PROJECT
    assert isinstance(job.progress, ExcludedPathScrubProgress)
    assert job.progress.dry_run is True
    assert job.progress.builds_examined == 0
    assert job.progress.builds == []


@pytest.mark.asyncio
async def test_scrub_defaults_to_a_real_run(client: AsyncClient) -> None:
    """An empty body records ``dry_run: false``."""
    await _setup(client)
    accepted = await client.post(
        _URL, json={}, headers={"X-Auth-Request-User": _ADMIN}
    )
    assert accepted.status_code == 202

    response = await client.get(
        accepted.json()["job_url"],
        headers={"X-Auth-Request-User": _ADMIN},
    )

    job = QueueJob.model_validate(response.json())
    assert isinstance(job.progress, ExcludedPathScrubProgress)
    assert job.progress.dry_run is False


@pytest.mark.asyncio
async def test_scrub_repeat_returns_the_active_job(
    client: AsyncClient,
) -> None:
    """A second request while one is queued gets the same job back."""
    await _setup(client)
    first = await client.post(
        _URL,
        json={"dry_run": True},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    assert first.status_code == 202

    second = await client.post(
        _URL,
        json={"dry_run": False},
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Location"] == first.headers["Location"]
    jobs = await client.get(
        f"/docverse/orgs/{_ORG}/jobs",
        headers={"X-Auth-Request-User": _ADMIN},
    )
    scrubs = [
        job
        for job in jobs.json()
        if job["kind"] == JobKind.excluded_path_scrub.value
    ]
    assert [job["id"] for job in scrubs] == [first.json()["job_id"]]
    assert _maintenance_scrub_jobs() == 1


@pytest.mark.asyncio
async def test_scrub_403_for_a_non_admin(client: AsyncClient) -> None:
    """Scrubbing is an admin operation; a reader is refused."""
    await _setup(client)
    await seed_member(_ORG, "reader-user", OrgRole.reader)

    response = await client.post(
        _URL,
        json={"dry_run": True},
        headers={"X-Auth-Request-User": "reader-user"},
    )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_scrub_404_for_an_unknown_project(client: AsyncClient) -> None:
    """A project that does not exist cannot be scrubbed."""
    await _setup(client)

    response = await client.post(
        f"/docverse/orgs/{_ORG}/projects/nope/builds/scrub-excluded-paths",
        json={"dry_run": True},
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 404


async def _write_progress(
    public_job_id: str, progress: dict[str, object]
) -> None:
    """Overwrite a job's ``progress`` as the scrub worker would."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            store = QueueJobStore(session=session, logger=logger)
            job = await store.get_by_public_id(
                validate_base32_id(public_job_id)
            )
            assert job is not None
            await store.update_progress(job.id, progress)
        return
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


@pytest.mark.asyncio
async def test_scrub_report_rows_round_trip_through_the_job_url(
    client: AsyncClient,
) -> None:
    """A report with rows reads back as typed rows and entries."""
    await _setup(client)
    accepted = await client.post(
        _URL,
        json={"dry_run": True},
        headers={"X-Auth-Request-User": _ADMIN},
    )
    report = ExcludedPathScrubProgress(
        dry_run=True,
        builds_examined=2,
        builds_affected=1,
        objects_removed=2,
        bytes_removed=1007,
        builds=[
            ExcludedPathScrubBuild(
                build_id="1txq-55pj-1x5m-16",
                build_url="https://example.test/builds/1txq-55pj-1x5m-16",
                edition_slugs=["main"],
                entries=[
                    ExcludedPathScrubEntry(
                        path=".buildinfo", object_count=1, total_size_bytes=7
                    ),
                    ExcludedPathScrubEntry(
                        path=".doctrees/",
                        object_count=1,
                        total_size_bytes=1000,
                    ),
                ],
            )
        ],
    )
    await _write_progress(
        accepted.json()["job_id"], report.model_dump(mode="json")
    )

    response = await client.get(
        accepted.json()["job_url"],
        headers={"X-Auth-Request-User": _ADMIN},
    )

    assert response.status_code == 200
    job = QueueJob.model_validate(response.json())
    assert job.progress == report
