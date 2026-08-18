"""Deferred hand-off of ``queue_jobs`` rows to the queue backend.

Every ``queue_jobs`` row has two writers racing each other: the request
(or worker) that creates it, and the arq worker that arq hands the job
to. The worker can only see committed data, so any enqueue issued from
inside the transaction that writes the row is a race — arq may deliver
the job to a worker that then reads a database in which the row does not
exist yet.

The worker-side enqueue paths already avoid this by construction: the
keeper-sync tier cron, the run-discovery fan-out, the lifecycle-eval and
git-ref-audit dispatchers, and
:func:`~docverse_server.services.publish_enqueue.enqueue_publish_for_edition`
all commit the row, *then* enqueue, then stamp ``backend_job_id`` in a
short follow-up transaction. This module is that same discipline made
reusable for the request path, where the handler — not the service —
owns the commit and so a service cannot simply commit mid-method.

Services call :meth:`QueueDispatcher.defer` while the handler's
transaction is still open; the handler calls
:meth:`QueueDispatcher.dispatch` once it has committed. One dispatcher
is shared per :class:`~docverse_server.factory.Factory` (and therefore
per request / per worker job), so a service and its caller reach the
same pending list without threading it through return values.

Skipping the ``dispatch`` call is a fail-safe rather than a wedge: the
row stays ``queued`` with ``backend_job_id IS NULL``, which is exactly
the shape the reapers' orphan sweep reclaims.

Every request-path enqueue now routes through here — ``build_processing``
(the finding that prompted task #550), ``dashboard_build``,
``dashboard_sync``, ``publish_edition`` from the edition override and
rollback paths, and the two keeper-sync operator endpoints. Each of them
previously handed arq a job from inside the transaction that wrote its
row, and each had a distinct way to lose: ``build_processing`` tolerates
a missing row and processes the build anyway, leaving a forever-
``queued`` row that the abandoned sweep later stamps ``AbandonedQueueJob``
for a build that actually succeeded; every other kind resolves its row by
id and so raises ``JobNotFoundError`` instead, which arq records as a
plain failure rather than retrying — wedging whatever mutex or run the
row was holding.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.queue import QueueJob
from docverse_server.storage.queue_backend import QueueBackend
from docverse_server.storage.queue_job_store import QueueJobStore

__all__ = ["PendingEnqueue", "QueueDispatcher"]


@dataclass(frozen=True, slots=True)
class PendingEnqueue:
    """A ``queue_jobs`` row the queue backend has not been told about yet.

    Carries everything the enqueue needs so the deferred call is a pure
    replay: no store reads happen between ``defer`` and ``dispatch``.
    """

    queue_job_id: int
    """Internal id of the row this enqueue backs."""

    queue_job_public_id: str
    """Base32 public id of that row, for log context.

    Logged under ``dispatched_queue_job_id`` rather than the codebase's
    usual ``queue_job_id``: a dispatch often runs under a logger already
    bound to a *different* job — the ``publish_edition`` worker cascading
    a ``dashboard_build``, say — and reusing the key there would make one
    log line appear to describe two jobs at once. The value is still the
    public id, never the integer row id.
    """

    job_type: str
    """arq task function name."""

    payload: dict[str, Any] = field(default_factory=dict)
    """Keyword payload handed to the task function."""

    queue_name: str | None = None
    """Pool queue override, or ``None`` for the backend's default."""


class QueueDispatcher:
    """Hold enqueues until the transaction that wrote their rows commits.

    Bound to one session (one request or one worker job) and shared
    through :attr:`docverse_server.factory.Factory.queue_dispatcher`, so
    a service that defers and a handler that dispatches operate on the
    same pending list.
    """

    def __init__(
        self,
        *,
        session: AsyncSession,
        queue_backend: QueueBackend,
        queue_job_store: QueueJobStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._queue_backend = queue_backend
        self._queue_job_store = queue_job_store
        self._logger = logger
        self._pending: list[PendingEnqueue] = []

    @property
    def pending(self) -> tuple[PendingEnqueue, ...]:
        """Enqueues deferred so far and not yet dispatched."""
        return tuple(self._pending)

    def defer(
        self,
        *,
        queue_job: QueueJob,
        job_type: str,
        payload: dict[str, Any],
        queue_name: str | None = None,
    ) -> None:
        """Record an enqueue to issue once ``queue_job``'s row is durable.

        Call this from inside the transaction that inserted the row. The
        row's own ``queue_job_id`` / ``queue_job_public_id`` are added to
        ``payload`` here rather than at each call site: every worker
        function resolves its row from those two keys, so no caller can
        forget them and fall back to a ``backend_job_id`` lookup that is
        still ``NULL`` at delivery time.
        """
        public_id = serialize_base32_id(queue_job.public_id)
        self._pending.append(
            PendingEnqueue(
                queue_job_id=queue_job.id,
                queue_job_public_id=public_id,
                job_type=job_type,
                payload=payload
                | {
                    "queue_job_id": queue_job.id,
                    "queue_job_public_id": public_id,
                },
                queue_name=queue_name,
            )
        )

    async def dispatch(self) -> list[QueueJob]:
        """Enqueue everything deferred so far, stamping each row.

        Call this *after* committing the transaction that wrote the
        rows, with no transaction open: each stamp runs in its own short
        transaction, mirroring the worker-side two-phase enqueues.

        A backend failure propagates. The already-committed rows are
        then left ``queued`` with ``backend_job_id IS NULL`` — the same
        recoverable shape ``enqueue_publish_for_edition`` documents, and
        the one the reapers' orphan sweep reclaims — rather than a job
        arq knows about and no row to drive it.

        Returns
        -------
        list of QueueJob
            The stamped rows, in dispatch order. Empty when nothing was
            deferred, which makes the call safe to make unconditionally.
        """
        pending, self._pending = self._pending, []
        dispatched: list[QueueJob] = []
        for item in pending:
            enqueued = await self._queue_backend.enqueue(
                item.job_type, item.payload, queue_name=item.queue_name
            )
            async with self._session.begin():
                dispatched.append(
                    await self._queue_job_store.set_backend_job_id(
                        item.queue_job_id,
                        enqueued.id,
                        queue_name=enqueued.queue_name,
                    )
                )
                await self._session.commit()
            self._logger.debug(
                "Dispatched queue job",
                dispatched_queue_job_id=item.queue_job_public_id,
                job_type=item.job_type,
                backend_job_id=enqueued.id,
                backend_queue_name=enqueued.queue_name,
            )
        return dispatched
