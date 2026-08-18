"""Queue backend abstraction for enqueuing background jobs."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from safir.arq import ArqQueue, JobMetadata, JobNotFound, JobResultUnavailable

__all__ = [
    "ArqQueueBackend",
    "EnqueuedJob",
    "NullQueueBackend",
    "QueueBackend",
]


@dataclass(frozen=True, slots=True)
class EnqueuedJob:
    """Where a freshly enqueued job landed.

    ``enqueue`` returns both halves of a job's backend provenance rather
    than the id alone so a caller that records
    ``queue_jobs.backend_job_id`` structurally has
    ``queue_jobs.backend_queue_name`` in hand as well. The pairing is
    what lets the abandoned sweep verify a row against the queue it was
    actually enqueued onto instead of guessing from a hand-listed set of
    pool queues.
    """

    id: str
    """Backend-assigned job ID."""

    queue_name: str
    """Name of the queue the job was enqueued onto."""


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
    ) -> EnqueuedJob:
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
        EnqueuedJob
            The backend-assigned job ID and the queue it landed on.
        """
        ...

    async def get_job_metadata(
        self,
        backend_job_id: str,
        *,
        queue_name: str | None = None,
    ) -> dict[str, Any] | None:
        """Get backend-specific metadata about a job.

        Parameters
        ----------
        backend_job_id
            The backend-assigned job ID.
        queue_name
            Probe only this queue. ``None`` (the legacy-row fallback)
            lets the backend decide which queues to consult.

        Returns
        -------
        dict or None
            Metadata dict, or ``None`` if the backend has no record of
            the job.

        Notes
        -----
        ``None`` is load-bearing, not merely diagnostic: the abandoned
        reaper sweep (PRD #538) fails a ``queue_jobs`` row on exactly
        that answer. An implementation that cannot actually answer the
        question must raise rather than return ``None``.
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
    status-transition methods, not job enqueueing. Every method that
    would have to *answer* for a real queue raises instead of inventing
    a benign-looking value.
    """

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> EnqueuedJob:
        """Raise because this backend cannot enqueue."""
        msg = "NullQueueBackend cannot enqueue jobs"
        raise RuntimeError(msg)

    async def get_job_metadata(
        self,
        backend_job_id: str,
        *,
        queue_name: str | None = None,
    ) -> dict[str, Any] | None:
        """Raise because this backend cannot look a job up.

        Returning ``None`` — "no record of this job" — would be actively
        destructive: the abandoned reaper sweep fails a ``queue_jobs``
        row on that answer, so a queue-less :class:`Factory` path
        reaching a sweep would mass-fail every healthy candidate without
        anything raising. Raising engages the sweep's error path
        instead, matching :meth:`enqueue`.
        """
        msg = "NullQueueBackend cannot look up job metadata"
        raise RuntimeError(msg)

    async def get_job_result(
        self,
        backend_job_id: str,
    ) -> object | None:
        """Return None (no backend)."""
        return None


class ArqQueueBackend:
    """Queue backend wrapping safir's ArqQueue.

    Works with both RedisArqQueue and MockArqQueue.

    ``additional_queue_names`` lists the other queues Docverse runs (the
    dedicated keeper-sync and maintenance pools) so an *un-named* lookup
    can still find a job whichever pool it was enqueued onto; see
    :meth:`get_job_metadata`. It does not affect enqueueing, which
    always targets ``default_queue_name`` unless the caller overrides it
    per call.
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
        # This backend's *own* default first, never ``None``. Deferring
        # to the wrapped queue's default would probe the wrong queue in
        # the API process, where safir's ``arq_dependency`` builds the
        # ``RedisArqQueue`` with no default queue name at all and it
        # falls back to arq's stock ``arq:queue`` — a queue Docverse
        # never enqueues onto.
        self._lookup_queue_names: tuple[str, ...] = (
            default_queue_name,
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
    ) -> EnqueuedJob:
        """Enqueue a job via arq."""
        target_queue = queue_name or self._default_queue_name
        metadata: JobMetadata = await self._arq_queue.enqueue(
            job_type,
            _queue_name=target_queue,
            payload=payload,
        )
        return EnqueuedJob(id=metadata.id, queue_name=target_queue)

    async def get_job_metadata(
        self,
        backend_job_id: str,
        *,
        queue_name: str | None = None,
    ) -> dict[str, Any] | None:
        """Get arq job metadata as a dict, from the queue that holds it.

        arq stores a job's *definition* under a queue-agnostic key but
        resolves its *status* through the queue's sorted set, so a job
        that is merely ``queued`` on a dedicated pool queue looks
        missing when queried under any other queue's name — safir turns
        that into ``JobNotFound``. Since ``None`` here means "arq has no
        record of this job", and the abandoned reaper sweep (PRD #538)
        fails a ``queue_jobs`` row on exactly that answer, asking the
        wrong queue would read a healthy job as lost.

        ``queue_name`` is therefore the row's own recorded
        ``backend_queue_name``, giving one correct probe for any pool —
        including one added after this code was written. ``None`` is the
        fallback for rows predating that column: the lookup walks the
        configured default queue and then every additional pool queue
        before concluding the job is gone.
        """
        if queue_name is not None:
            candidates: tuple[str, ...] = (queue_name,)
        else:
            candidates = self._lookup_queue_names
        for candidate in candidates:
            try:
                metadata = await self._arq_queue.get_job_metadata(
                    backend_job_id, candidate
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
