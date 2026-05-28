"""arq worker function for the ``dashboard_build_reaper`` cron backstop.

Mirrors :func:`docverse.worker.functions.lifecycle_reaper.lifecycle_reaper`
for ``kind='dashboard_build'`` rows. Per PRD #367 §"Reaper module shape"
this is the run-less variant: ``dashboard_build`` does not aggregate
into a parent run row, so the reaper only sweeps stuck ``queue_jobs``
rows and finalises nothing.

The user-visible motivation is the 409 on ``POST /dashboard/rebuild``
that an operator sees when a previous ``dashboard_build`` was lost by
arq: the partial unique index
``idx_queue_jobs_dashboard_build_active_uq`` keeps the wedged row in
view as long as it is ``queued`` or ``in_progress``. Reaping flips the
row to ``failed``, the partial index releases, and the next rebuild
can be enqueued.
"""

from __future__ import annotations

from typing import Any

from docverse.domain.queue import JobKind

from ._runless_reaper import sweep_runless_kind

__all__ = ["dashboard_build_reaper"]


async def dashboard_build_reaper(ctx: dict[str, Any]) -> str:
    """Cron-driven backstop that fails silently-stuck dashboard_build rows.

    Thin shim over
    :func:`docverse.worker.functions._runless_reaper.sweep_runless_kind`;
    see that module for the shared sweep mechanics. Threshold defaults
    to 30 min via ``config.dashboard_build_reaper_threshold_seconds``;
    non-prod can override with
    ``DOCVERSE_DASHBOARD_BUILD_REAPER_THRESHOLD_SECONDS``.
    """
    return await sweep_runless_kind(
        ctx,
        kind=JobKind.dashboard_build,
        threshold_attr="dashboard_build_reaper_threshold_seconds",
    )
