"""Tests for docverse.client._client."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
import respx

from docverse.client._client import DocverseClient
from docverse.client._exceptions import (
    BuildProcessingError,
    DocverseClientError,
)
from docverse.client.models.builds import BuildStatus
from docverse.client.models.queue_enums import JobKind, JobStatus

BASE_URL = "https://docverse.example.com"
TOKEN = "test-token"  # noqa: S105

# Valid Crockford Base32 IDs with checksum (base32_lib.encode(N, ...))
BUILD_ID = "000000000195"
JOB_ID = "000000000292"


def _build_response(**overrides: Any) -> dict[str, Any]:
    """Return a dict matching the Build model shape."""
    data: dict[str, Any] = {
        "self_url": "/orgs/testorg/projects/testproj/builds/" + BUILD_ID,
        "project_url": "/orgs/testorg/projects/testproj",
        "id": BUILD_ID,
        "git_ref": "main",
        "alternate_name": None,
        "content_hash": "sha256:" + "a" * 64,
        "status": "pending",
        "upload_url": "https://storage.example.com/presigned-put",
        "queue_url": None,
        "object_count": None,
        "total_size_bytes": None,
        "uploader": "ci-bot",
        "annotations": None,
        "date_created": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        "date_uploaded": None,
        "date_completed": None,
    }
    data.update(overrides)
    return data


def _job_response(**overrides: Any) -> dict[str, Any]:
    """Return a dict matching the QueueJob model shape."""
    data: dict[str, Any] = {
        "self_url": "/queue/jobs/" + JOB_ID,
        "id": JOB_ID,
        "kind": "build_processing",
        "status": "completed",
        "phase": None,
        "progress": None,
        "errors": None,
        "date_created": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        "date_started": None,
        "date_completed": None,
    }
    data.update(overrides)
    return data


@pytest.mark.asyncio
async def test_create_build() -> None:
    """POST to builds endpoint with correct JSON and auth header."""
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.post("/orgs/myorg/projects/myproj/builds").mock(
            return_value=httpx.Response(201, json=_build_response())
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            build = await client.create_build(
                "myorg",
                "myproj",
                git_ref="main",
                content_hash="sha256:" + "a" * 64,
            )

        assert route.called
        request = route.calls[0].request
        assert request.headers["authorization"] == f"Bearer {TOKEN}"
        assert build.id == BUILD_ID
        assert build.status == BuildStatus.pending


@pytest.mark.asyncio
async def test_upload_tarball(tmp_path: Path) -> None:
    """PUT to presigned URL without Authorization header."""
    tarball = tmp_path / "docs.tar.gz"
    tarball.write_bytes(b"fake-tarball-content")

    presigned_url = "https://storage.example.com/presigned-put"

    with respx.mock() as router:
        route = router.put(presigned_url).mock(
            return_value=httpx.Response(200)
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            await client.upload_tarball(presigned_url, tarball)

        assert route.called
        request = route.calls[0].request
        # Security invariant: no auth token on presigned upload
        assert "authorization" not in request.headers


@pytest.mark.asyncio
async def test_complete_upload() -> None:
    """PATCH to build self_url with uploaded status."""
    build_url = "/orgs/testorg/projects/testproj/builds/" + BUILD_ID
    async with respx.mock(base_url=BASE_URL) as router:
        route = router.patch(build_url).mock(
            return_value=httpx.Response(
                200,
                json=_build_response(
                    status="uploaded",
                    queue_url="/queue/jobs/" + JOB_ID,
                ),
            )
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            build = await client.complete_upload(build_url)

        assert route.called
        assert build.status == BuildStatus.uploaded
        assert build.queue_url == "/queue/jobs/" + JOB_ID


@pytest.mark.asyncio
async def test_get_queue_job() -> None:
    """GET to queue URL returns parsed QueueJob."""
    queue_url = "/queue/jobs/" + JOB_ID
    async with respx.mock(base_url=BASE_URL) as router:
        router.get(queue_url).mock(
            return_value=httpx.Response(
                200,
                json=_job_response(status="completed"),
            )
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            job = await client.get_queue_job(queue_url)

    assert job.id == JOB_ID
    assert job.status == JobStatus.completed
    assert job.kind == JobKind.build_processing


@pytest.mark.asyncio
async def test_wait_for_job_completed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Polls twice then returns completed job."""
    monkeypatch.setattr("docverse.client._client._BACKOFF_INITIAL", 0)

    queue_url = "/queue/jobs/" + JOB_ID
    responses = [
        httpx.Response(200, json=_job_response(status="in_progress")),
        httpx.Response(200, json=_job_response(status="completed")),
    ]
    async with respx.mock(base_url=BASE_URL) as router:
        router.get(queue_url).mock(side_effect=responses)

        with patch("docverse.client._client.asyncio.sleep"):
            async with DocverseClient(BASE_URL, TOKEN) as client:
                job = await client.wait_for_job(queue_url)

    assert job.status == JobStatus.completed


@pytest.mark.asyncio
async def test_wait_for_job_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failed status raises BuildProcessingError with the job attached."""
    monkeypatch.setattr("docverse.client._client._BACKOFF_INITIAL", 0)

    queue_url = "/queue/jobs/" + JOB_ID
    async with respx.mock(base_url=BASE_URL) as router:
        router.get(queue_url).mock(
            return_value=httpx.Response(
                200,
                json=_job_response(status="failed", phase="inventory"),
            )
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            with pytest.raises(BuildProcessingError) as exc_info:
                await client.wait_for_job(queue_url)

    assert exc_info.value.job.status == JobStatus.failed
    assert exc_info.value.job.phase == "inventory"


@pytest.mark.asyncio
async def test_http_error() -> None:
    """Non-2xx response raises DocverseClientError with status_code."""
    async with respx.mock(base_url=BASE_URL) as router:
        router.post("/orgs/x/projects/y/builds").mock(
            return_value=httpx.Response(403, text="Forbidden")
        )
        async with DocverseClient(BASE_URL, TOKEN) as client:
            with pytest.raises(DocverseClientError) as exc_info:
                await client.create_build(
                    "x",
                    "y",
                    git_ref="main",
                    content_hash="sha256:" + "a" * 64,
                )

    assert exc_info.value.status_code == 403  # noqa: PLR2004
