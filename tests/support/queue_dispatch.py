"""Build a ``QueueDispatcher`` for tests that construct services directly.

Services that enqueue take a
:class:`~docverse_server.services.queue_dispatch.QueueDispatcher` rather
than a raw ``QueueBackend``, so the arq enqueue can be held until the
caller's transaction commits (task #550). Tests that go through
:class:`~docverse_server.factory.Factory` get one for free via
``factory.queue_dispatcher``; this helper serves the ones that assemble
a service by hand.

Call ``await dispatcher.dispatch()`` after the test's own commit when
the assertion depends on arq having seen the job or on the row's
``backend_job_id`` being stamped.
"""

from __future__ import annotations

import structlog
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession

from docverse_server.config import Configuration
from docverse_server.services.queue_dispatch import QueueDispatcher
from docverse_server.storage.queue_backend import (
    ArqQueueBackend,
    NullQueueBackend,
    QueueBackend,
)
from docverse_server.storage.queue_job_store import QueueJobStore

__all__ = ["make_dispatcher"]

_config = Configuration()


def make_dispatcher(
    session: AsyncSession,
    *,
    arq_queue: MockArqQueue | None = None,
    default_queue_name: str | None = None,
) -> QueueDispatcher:
    """Build a dispatcher over ``session`` and an optional mock queue.

    Without ``arq_queue`` the dispatcher is backed by
    :class:`~docverse_server.storage.queue_backend.NullQueueBackend`,
    which matches the worker-side factories that never enqueue.
    """
    logger: structlog.stdlib.BoundLogger = structlog.get_logger("docverse")
    queue_name = default_queue_name or _config.arq_queue_name
    backend: QueueBackend = (
        NullQueueBackend()
        if arq_queue is None
        else ArqQueueBackend(
            arq_queue=arq_queue, default_queue_name=queue_name
        )
    )
    return QueueDispatcher(
        session=session,
        queue_backend=backend,
        queue_job_store=QueueJobStore(session=session, logger=logger),
        logger=logger,
    )
