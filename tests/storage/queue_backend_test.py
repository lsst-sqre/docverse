"""Tests for the ArqQueueBackend."""

import pytest
from safir.arq import MockArqQueue

from docverse_server.storage.queue_backend import (
    ArqQueueBackend,
    NullQueueBackend,
)
from docverse_server.worker.queues import (
    KEEPER_SYNC_QUEUE_NAME,
    MAINTENANCE_QUEUE_NAME,
    POOL_QUEUE_NAMES,
)
from tests.support.arq_testing import get_jobs_by_name, register_queue


@pytest.fixture
def queue_backend() -> ArqQueueBackend:
    return ArqQueueBackend(
        arq_queue=MockArqQueue(default_queue_name="docverse:queue"),
        default_queue_name="docverse:queue",
    )


@pytest.mark.asyncio
async def test_enqueue(queue_backend: ArqQueueBackend) -> None:
    enqueued = await queue_backend.enqueue("test_task", {"key": "value"})
    assert isinstance(enqueued.id, str)
    assert len(enqueued.id) > 0
    assert enqueued.queue_name == "docverse:queue"


@pytest.mark.asyncio
async def test_enqueue_passes_payload_as_single_kwarg(
    queue_backend: ArqQueueBackend,
) -> None:
    """Verify payload is passed as a single ``payload`` kwarg, not spread."""
    mock_queue: MockArqQueue = queue_backend._arq_queue  # type: ignore[assignment]
    payload = {"key": "value", "n": 42}
    enqueued = await queue_backend.enqueue("test_task", payload)
    # Inspect the stored job metadata to verify the payload was passed
    # as a single kwarg rather than spread as individual kwargs.
    stored_jobs = get_jobs_by_name(
        mock_queue, "test_task", queue_name="docverse:queue"
    )
    assert len(stored_jobs) == 1
    assert stored_jobs[0].id == enqueued.id
    assert stored_jobs[0].kwargs == {"payload": payload}


@pytest.mark.asyncio
async def test_enqueue_uses_default_queue_name() -> None:
    """Verify enqueue uses the configured default queue name."""
    mock_queue = MockArqQueue(default_queue_name="docverse:queue")
    backend = ArqQueueBackend(
        arq_queue=mock_queue, default_queue_name="docverse:queue"
    )
    enqueued = await backend.enqueue("test_task", {"key": "value"})
    metadata = await backend.get_job_metadata(enqueued.id)
    assert metadata is not None
    assert metadata["queue_name"] == "docverse:queue"


@pytest.mark.asyncio
async def test_enqueue_override_queue_name() -> None:
    """Verify queue_name parameter overrides the default.

    The result reports where the job actually landed, which is what the
    caller records on ``queue_jobs.backend_queue_name``. That record is
    load-bearing for a queue outside this backend's configured set: the
    un-named fallback walk never reaches it, so only a named lookup
    finds the job.
    """
    mock_queue = MockArqQueue(default_queue_name="custom:queue")
    backend = ArqQueueBackend(
        arq_queue=mock_queue, default_queue_name="docverse:queue"
    )
    enqueued = await backend.enqueue(
        "test_task", {"key": "value"}, queue_name="custom:queue"
    )
    assert enqueued.queue_name == "custom:queue"

    metadata = await backend.get_job_metadata(
        enqueued.id, queue_name=enqueued.queue_name
    )
    assert metadata is not None
    assert metadata["queue_name"] == "custom:queue"
    assert await backend.get_job_metadata(enqueued.id) is None


@pytest.mark.asyncio
async def test_get_job_metadata_probes_the_configured_default_queue() -> None:
    """Lookups probe the queue this backend enqueues onto, in any process.

    The API process builds its ``ArqQueue`` through safir's
    ``arq_dependency``, which never passes a default queue name — so the
    wrapped queue's own default is arq's stock ``arq:queue`` while
    Docverse enqueues onto the configured ``docverse:queue``. A lookup
    that deferred to the *wrapped* queue's default would therefore find
    nothing in that process, and the abandoned sweep reads "nothing" as
    "arq lost this job".
    """
    mock_queue = MockArqQueue()
    assert mock_queue.default_queue_name != "docverse:queue"
    register_queue(mock_queue, "docverse:queue")
    backend = ArqQueueBackend(
        arq_queue=mock_queue, default_queue_name="docverse:queue"
    )

    enqueued = await backend.enqueue("test_task", {"key": "value"})
    metadata = await backend.get_job_metadata(enqueued.id)

    assert metadata is not None
    assert metadata["queue_name"] == "docverse:queue"


@pytest.mark.asyncio
async def test_get_job_metadata_probes_only_the_named_queue() -> None:
    """A ``queue_name`` argument pins the lookup to that one queue.

    The abandoned sweep passes the enqueueing queue recorded on the
    ``queue_jobs`` row, so a verified job is found with a single round
    trip instead of a walk over every pool queue.
    """
    mock_queue = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_queue, KEEPER_SYNC_QUEUE_NAME)
    backend = ArqQueueBackend(
        arq_queue=mock_queue,
        default_queue_name="docverse:queue",
        additional_queue_names=(KEEPER_SYNC_QUEUE_NAME,),
    )
    enqueued = await backend.enqueue(
        "keeper_sync_project", {"k": "v"}, queue_name=KEEPER_SYNC_QUEUE_NAME
    )

    found = await backend.get_job_metadata(
        enqueued.id, queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    missing = await backend.get_job_metadata(
        enqueued.id, queue_name="docverse:queue"
    )

    assert found is not None
    assert found["queue_name"] == KEEPER_SYNC_QUEUE_NAME
    assert missing is None


@pytest.mark.asyncio
async def test_get_job_metadata(queue_backend: ArqQueueBackend) -> None:
    enqueued = await queue_backend.enqueue("test_task", {"key": "value"})
    metadata = await queue_backend.get_job_metadata(enqueued.id)
    assert metadata is not None
    assert metadata["id"] == enqueued.id
    assert metadata["name"] == "test_task"
    assert metadata["status"] == "queued"


@pytest.mark.asyncio
async def test_null_backend_get_job_metadata_raises() -> None:
    """The null backend refuses lookups rather than answering "not found".

    ``None`` is the abandoned sweep's "arq lost this job" answer, so a
    null backend that returned it for every id would mass-fail healthy
    rows. Raising engages the sweep's error path instead.
    """
    with pytest.raises(RuntimeError):
        await NullQueueBackend().get_job_metadata("arq-job-id")


@pytest.mark.asyncio
async def test_pool_queue_names_covers_every_dedicated_pool() -> None:
    """The registry lists every queue Docverse runs a worker pool on.

    ``ArqQueueBackend``'s legacy-row fallback walks this tuple, so a pool
    queue missing from it reads as "arq lost the job" for every row
    enqueued before the queue name was recorded.
    """
    assert set(POOL_QUEUE_NAMES) == {
        KEEPER_SYNC_QUEUE_NAME,
        MAINTENANCE_QUEUE_NAME,
    }


@pytest.mark.asyncio
async def test_get_job_metadata_finds_job_on_a_dedicated_pool_queue() -> None:
    """The un-named lookup still walks every pool queue, for legacy rows.

    arq resolves a job's *status* through its queue's sorted set, so a
    job that is merely ``queued`` on a dedicated pool queue reads as
    "not found" when looked up under the default queue name alone. Rows
    predating ``queue_jobs.backend_queue_name`` cannot say which pool
    holds them, and the abandoned sweep (PRD #538) treats "no record" as
    "arq lost this job", so the un-named lookup keeps consulting every
    pool queue rather than reaping healthy keeper-sync and maintenance
    work.
    """
    mock_queue = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_queue, KEEPER_SYNC_QUEUE_NAME)
    backend = ArqQueueBackend(
        arq_queue=mock_queue,
        default_queue_name="docverse:queue",
        additional_queue_names=(KEEPER_SYNC_QUEUE_NAME,),
    )

    enqueued = await backend.enqueue(
        "keeper_sync_project", {"k": "v"}, queue_name=KEEPER_SYNC_QUEUE_NAME
    )
    metadata = await backend.get_job_metadata(enqueued.id)

    assert metadata is not None
    assert metadata["queue_name"] == KEEPER_SYNC_QUEUE_NAME


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
    enqueued = await queue_backend.enqueue("test_task", {"key": "value"})
    result = await queue_backend.get_job_result(enqueued.id)
    assert result is None
