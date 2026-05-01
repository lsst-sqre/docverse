"""Helpers for inspecting an in-memory ``MockArqQueue`` from tests.

``MockArqQueue`` does not expose a public API for enumerating enqueued
jobs — the test suite reaches in through the private ``_job_metadata``
mapping. Centralizing that access in one module keeps the
private-attribute coupling in a single place so a future ``safir.arq``
update only needs one fix-up site.

The helpers operate on either a single named queue (``queue_name``) or
the union across every queue the mock has seen (``queue_name=None``).
"""

from __future__ import annotations

from safir.arq import JobMetadata, MockArqQueue

__all__ = [
    "count_jobs_by_name",
    "get_jobs_by_name",
    "queue_names",
    "register_queue",
]


def register_queue(arq_queue: MockArqQueue, name: str) -> None:
    """Register an additional queue name on a ``MockArqQueue``.

    ``MockArqQueue`` only auto-creates the slot for its
    ``default_queue_name``; enqueueing into any other queue raises
    ``KeyError`` until that queue's name is added to ``_job_metadata``.
    """
    arq_queue._job_metadata.setdefault(name, {})


def get_jobs_by_name(
    arq_queue: MockArqQueue,
    name: str,
    *,
    queue_name: str | None = None,
) -> list[JobMetadata]:
    """Return every enqueued job whose ``name`` matches.

    Parameters
    ----------
    arq_queue
        The mock queue to inspect.
    name
        The arq function name (e.g. ``"dashboard_sync"``).
    queue_name
        If given, restrict to that single queue. If ``None`` (default),
        union jobs across every queue the mock has touched.
    """
    if queue_name is not None:
        per_queue = arq_queue._job_metadata.get(queue_name, {})
        return [job for job in per_queue.values() if job.name == name]
    return [
        job
        for queue in arq_queue._job_metadata.values()
        for job in queue.values()
        if job.name == name
    ]


def count_jobs_by_name(
    arq_queue: MockArqQueue,
    name: str,
    *,
    queue_name: str | None = None,
) -> int:
    """Count enqueued jobs whose ``name`` matches.

    See :func:`get_jobs_by_name` for the ``queue_name`` semantics.
    """
    return len(get_jobs_by_name(arq_queue, name, queue_name=queue_name))


def queue_names(arq_queue: MockArqQueue) -> set[str]:
    """Return the set of queue names that have received at least one job.

    Useful for asserting that jobs landed under the configured queue name
    rather than arq's default ``"arq:queue"``.
    """
    return set(arq_queue._job_metadata.keys())
