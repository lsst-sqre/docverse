"""Queue backend abstraction for enqueuing background jobs."""

from __future__ import annotations

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
        job_type: str,  # noqa: ARG002
        payload: dict[str, Any],  # noqa: ARG002
        *,
        queue_name: str | None = None,  # noqa: ARG002
    ) -> str:
        """Raise because this backend cannot enqueue."""
        msg = "NullQueueBackend cannot enqueue jobs"
        raise RuntimeError(msg)

    async def get_job_metadata(
        self,
        backend_job_id: str,  # noqa: ARG002
    ) -> dict[str, Any] | None:
        """Return None (no backend)."""
        return None

    async def get_job_result(
        self,
        backend_job_id: str,  # noqa: ARG002
    ) -> object | None:
        """Return None (no backend)."""
        return None


class ArqQueueBackend:
    """Queue backend wrapping safir's ArqQueue.

    Works with both RedisArqQueue and MockArqQueue.
    """

    def __init__(self, arq_queue: ArqQueue) -> None:
        self._arq_queue = arq_queue

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> str:
        """Enqueue a job via arq."""
        metadata: JobMetadata = await self._arq_queue.enqueue(
            job_type, _queue_name=queue_name, payload=payload
        )
        return metadata.id

    async def get_job_metadata(
        self, backend_job_id: str
    ) -> dict[str, Any] | None:
        """Get arq job metadata as a dict."""
        try:
            metadata = await self._arq_queue.get_job_metadata(backend_job_id)
        except JobNotFound:
            return None
        return {
            "id": metadata.id,
            "name": metadata.name,
            "status": metadata.status.value,
            "enqueue_time": metadata.enqueue_time.isoformat(),
            "queue_name": metadata.queue_name,
        }

    async def get_job_result(self, backend_job_id: str) -> object | None:
        """Get the result of a completed arq job."""
        try:
            result = await self._arq_queue.get_job_result(backend_job_id)
        except (JobNotFound, JobResultUnavailable):
            return None
        return result.result  # type: ignore[no-any-return]
