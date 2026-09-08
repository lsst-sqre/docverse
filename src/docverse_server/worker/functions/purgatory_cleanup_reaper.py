"""arq worker function for the ``purgatory_cleanup_reaper`` cron backstop.

Mirrors :mod:`docverse_server.worker.functions.dashboard_sync_reaper` for
``kind='purgatory_cleanup'`` rows. Per PRD #367 §"Reaper module shape"
this is the run-less variant: the nightly sweep keeps no parent run row
— PRD #596 says its record of a run is the dispatcher's log line plus
the per-org completion event — so the reaper only sweeps stuck
``queue_jobs`` rows and finalises nothing.

What a wedged row costs here is a whole organization's nightly sweep.
``purgatory_cleanup`` rows are guarded by the per-org mutex
``idx_queue_jobs_purgatory_cleanup_active_uq``, so a row left
``in_progress`` by an OOM-killed worker — or left ``queued`` by a
dispatcher that crashed between the SQL commit and ``arq_queue.enqueue``
— makes ``create_unless_active`` step over that org on every following
tick. The org's expired builds then keep their object-store content
indefinitely, which is the exact cost the sweep exists to stop. Reaping
the row to ``failed`` releases the mutex, and the next nightly tick
resumes from the oldest unstamped build.
"""

from __future__ import annotations

from typing import Any

from docverse_server.domain.queue import JobKind

from ._runless_reaper import sweep_runless_kind

__all__ = ["purgatory_cleanup_reaper"]


async def purgatory_cleanup_reaper(ctx: dict[str, Any]) -> str:
    """Cron-driven backstop that fails stuck purgatory_cleanup rows.

    Thin shim over
    :func:`docverse_server.worker.functions._runless_reaper.sweep_runless_kind`;
    see that module for the shared sweep mechanics. Threshold defaults
    to 6 h via ``config.purgatory_cleanup_reaper_threshold_seconds``;
    non-prod can override with
    ``DOCVERSE_PURGATORY_CLEANUP_REAPER_THRESHOLD_SECONDS``.
    """
    return await sweep_runless_kind(
        ctx,
        kind=JobKind.purgatory_cleanup,
        threshold_attr="purgatory_cleanup_reaper_threshold_seconds",
    )
