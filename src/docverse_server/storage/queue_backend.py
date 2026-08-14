"""Queue backend abstraction for enqueuing background jobs."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable

from safir.arq import ArqQueue, JobMetadata, JobNotFound, JobResultUnavailable

__all__ = [
    "ArqQueueBackend",
    "NullQueueBackend",
    "QueueBackend",
]


@runtime_checkable
class QueueBackend(Protocol):
    """Backend-agnostic interface for enqueuing jobs and querying metadata.

    This protocol is from SQR-112 section queue-backend-protocol.
    """

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> str:
        """Enqueue a job.

        Parameters
        ----------
        job_type
            The task function name.
        payload
            Keyword arguments for the task.
        queue_name
            Override the default queue name.

        Returns
        -------
        str
            Backend-assigned job ID.
        """
        ...

    async def get_job_metadata(
        self, backend_job_id: str
    ) -> dict[str, Any] | None:
        """Get backend-specific metadata about a job (diagnostics only).

        Parameters
        ----------
        backend_job_id
            The backend-assigned job ID.

        Returns
        -------
        dict or None
            Metadata dict, or None if not found.
        """
        ...

    async def get_job_result(self, backend_job_id: str) -> object | None:
        """Get the result of a completed job.

        Parameters
        ----------
        backend_job_id
            The backend-assigned job ID.

        Returns
        -------
        object or None
            The result, or None if unavailable.
        """
        ...


class NullQueueBackend:
    """No-op queue backend for worker contexts.

    Used when constructing a BuildService that only needs
    status-transition methods, not job enqueueing.
    """

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> str:
        """Raise because this backend cannot enqueue."""
        msg = "NullQueueBackend cannot enqueue jobs"
        raise RuntimeError(msg)

    async def get_job_metadata(
        self,
        backend_job_id: str,
    ) -> dict[str, Any] | None:
        """Return None (no backend)."""
        return None

    async def get_job_result(
        self,
        backend_job_id: str,
    ) -> object | None:
        """Return None (no backend)."""
        return None


class ArqQueueBackend:
    """Queue backend wrapping safir's ArqQueue.

    Works with both RedisArqQueue and MockArqQueue.

    ``additional_queue_names`` lists the other queues Docverse runs
    (the dedicated keeper-sync and maintenance pools) so job *lookups*
    can find a job whichever pool it was enqueued onto; see
    :meth:`get_job_metadata`. It does not affect enqueueing, which
    always targets ``default_queue_name`` unless the caller overrides
    it per call.
    """

    def __init__(
        self,
        arq_queue: ArqQueue,
        *,
        default_queue_name: str = "arq:queue",
        additional_queue_names: Sequence[str] = (),
    ) -> None:
        self._arq_queue = arq_queue
        self._default_queue_name = default_queue_name
        # ``None`` first so the default lookup keeps deferring to the
        # wrapped queue's own default, exactly as before this fallback
        # existed.
        self._lookup_queue_names: tuple[str | None, ...] = (
            None,
            *(
                name
                for name in additional_queue_names
                if name != default_queue_name
            ),
        )

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> str:
        """Enqueue a job via arq."""
        metadata: JobMetadata = await self._arq_queue.enqueue(
            job_type,
            _queue_name=queue_name or self._default_queue_name,
            payload=payload,
        )
        return metadata.id

    async def get_job_metadata(
        self, backend_job_id: str
    ) -> dict[str, Any] | None:
        """Get arq job metadata as a dict, whichever pool queue holds it.

        arq stores a job's *definition* under a queue-agnostic key but
        resolves its *status* through the queue's sorted set, so a job
        that is merely ``queued`` on a dedicated pool queue looks
        missing when queried under the default queue name — safir turns
        that into ``JobNotFound``. Since ``None`` here means "arq has no
        record of this job", and the abandoned reaper sweep (PRD #538)
        fails a ``queue_jobs`` row on exactly that answer, the lookup
        walks every configured pool queue before concluding the job is
        gone. Otherwise every healthy keeper-sync or maintenance job
        would read as lost.
        """
        for queue_name in self._lookup_queue_names:
            try:
                metadata = await self._arq_queue.get_job_metadata(
                    backend_job_id, queue_name
                )
            except JobNotFound:
                continue
            return {
                "id": metadata.id,
                "name": metadata.name,
                "status": metadata.status.value,
                "enqueue_time": metadata.enqueue_time.isoformat(),
                "queue_name": metadata.queue_name,
            }
        return None

    async def get_job_result(self, backend_job_id: str) -> object | None:
        """Get the result of a completed arq job."""
        try:
            result = await self._arq_queue.get_job_result(backend_job_id)
        except (JobNotFound, JobResultUnavailable):
            return None
        return result.result  # type: ignore[no-any-return]
