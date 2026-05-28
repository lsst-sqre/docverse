"""arq worker function for the ``dashboard_sync_reaper`` cron backstop.

Mirrors :mod:`docverse.worker.functions.build_processing_reaper` for
``kind='dashboard_sync'`` rows. Per PRD #367 §"Reaper module shape"
this is the run-less variant: ``dashboard_sync`` does not aggregate
into a parent run row, so the reaper only sweeps stuck ``queue_jobs``
rows and finalises nothing.

Without reconciliation a wedged ``dashboard_sync`` leaves a binding's
``last_sync_queue_job`` showing a permanently in-progress sync after
a worker crash. Reaping flips the wedged row to ``failed`` so the
binding's template state can be retried on the next operator action.
"""

from __future__ import annotations

from typing import Any

from docverse.domain.queue import JobKind

from ._runless_reaper import sweep_runless_kind

__all__ = ["dashboard_sync_reaper"]


async def dashboard_sync_reaper(ctx: dict[str, Any]) -> str:
    """Cron-driven backstop that fails silently-stuck dashboard_sync rows.

    Thin shim over
    :func:`docverse.worker.functions._runless_reaper.sweep_runless_kind`;
    see that module for the shared sweep mechanics. Threshold defaults
    to 6 h via ``config.dashboard_sync_reaper_threshold_seconds``;
    non-prod can override with
    ``DOCVERSE_DASHBOARD_SYNC_REAPER_THRESHOLD_SECONDS``.
    """
    return await sweep_runless_kind(
        ctx,
        kind=JobKind.dashboard_sync,
        threshold_attr="dashboard_sync_reaper_threshold_seconds",
    )
