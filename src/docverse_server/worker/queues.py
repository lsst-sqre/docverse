"""arq queue-name constants for Docverse's dedicated worker pools.

The registry of every dedicated-pool queue name, decoupled from any one
worker function or service so a function module never has to own a
pool-wide constant — and so :data:`POOL_QUEUE_NAMES` can be complete by
construction rather than by a hand-maintained list somewhere else.
"""

from __future__ import annotations

__all__ = [
    "KEEPER_SYNC_QUEUE_NAME",
    "MAINTENANCE_QUEUE_NAME",
    "POOL_QUEUE_NAMES",
]


KEEPER_SYNC_QUEUE_NAME = "docverse:sync-queue"
"""arq queue name dedicated to LTD-sync work.

The queue is isolated from the regular ``docverse:queue`` so a noisy
backfill cannot starve ``build_processing`` and ``publish_edition``
jobs. Re-exported from ``docverse_server.services.keeper_sync_run``,
which owned it before this module became the registry.
"""

MAINTENANCE_QUEUE_NAME = "docverse:maintenance-queue"
"""arq queue name for the dedicated maintenance worker pool.

The pool is a catch-all for non-publishing periodic work — lifecycle
evaluation, the git-ref audit, and the cross-subsystem reaper
backstops — so its queue is isolated from the default ``docverse:queue``
and the ``docverse:sync-queue`` and one of those slow passes can never
starve ``build_processing`` / ``publish_edition`` or keeper-sync jobs.
``MaintenanceWorkerSettings`` binds this name; the dispatchers enqueue
their fan-out jobs onto it.
"""

POOL_QUEUE_NAMES = (KEEPER_SYNC_QUEUE_NAME, MAINTENANCE_QUEUE_NAME)
"""Every arq queue Docverse runs besides the configurable default one.

Handed to :class:`~docverse_server.storage.queue_backend.ArqQueueBackend`
as its legacy-row fallback: a ``queue_jobs`` row created before
``backend_queue_name`` existed cannot say which pool holds its job, so
the un-named lookup walks these queues before concluding arq lost it
(see ``ArqQueueBackend.get_job_metadata``). Rows written since carry
their own queue name and are probed directly.

Adding a pool queue means adding its constant *here*, next to the tuple,
so the fallback cannot silently miss it.
"""
