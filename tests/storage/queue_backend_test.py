"""Tests for the ArqQueueBackend."""

import pytest
from safir.arq import MockArqQueue

from docverse.storage.queue_backend import ArqQueueBackend


@pytest.fixture
def queue_backend() -> ArqQueueBackend:
    return ArqQueueBackend(arq_queue=MockArqQueue())


@pytest.mark.asyncio
async def test_enqueue(queue_backend: ArqQueueBackend) -> None:
    job_id = await queue_backend.enqueue("test_task", {"key": "value"})
    assert isinstance(job_id, str)
    assert len(job_id) > 0


@pytest.mark.asyncio
async def test_enqueue_passes_payload_as_single_kwarg(
    queue_backend: ArqQueueBackend,
) -> None:
    """Verify payload is passed as a single ``payload`` kwarg, not spread."""
    mock_queue: MockArqQueue = queue_backend._arq_queue  # type: ignore[assignment]
    payload = {"key": "value", "n": 42}
    job_id = await queue_backend.enqueue("test_task", payload)
    # Inspect the stored job metadata to verify the payload was passed
    # as a single kwarg rather than spread as individual kwargs.
    default_queue = mock_queue.default_queue_name
    queue_jobs = mock_queue._job_metadata[default_queue]
    stored_job = queue_jobs[job_id]
    assert stored_job.kwargs == {"payload": payload}


@pytest.mark.asyncio
async def test_get_job_metadata(queue_backend: ArqQueueBackend) -> None:
    job_id = await queue_backend.enqueue("test_task", {"key": "value"})
    metadata = await queue_backend.get_job_metadata(job_id)
    assert metadata is not None
    assert metadata["id"] == job_id
    assert metadata["name"] == "test_task"
    assert metadata["status"] == "queued"


@pytest.mark.asyncio
async def test_get_job_metadata_not_found(
    queue_backend: ArqQueueBackend,
) -> None:
    metadata = await queue_backend.get_job_metadata("nonexistent")
    assert metadata is None


@pytest.mark.asyncio
async def test_get_job_result_unavailable(
    queue_backend: ArqQueueBackend,
) -> None:
    job_id = await queue_backend.enqueue("test_task", {"key": "value"})
    result = await queue_backend.get_job_result(job_id)
    assert result is None
