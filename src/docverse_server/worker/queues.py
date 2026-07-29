"""arq queue-name constants for Docverse's dedicated worker pools.

A neutral home for the dedicated-pool queue names, decoupled from any
one worker function. ``KEEPER_SYNC_QUEUE_NAME`` lives next to its
keeper-sync service for historical reasons; new pool queue names land
here so a function module never has to own a pool-wide constant.
"""

from __future__ import annotations

__all__ = ["MAINTENANCE_QUEUE_NAME"]


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
