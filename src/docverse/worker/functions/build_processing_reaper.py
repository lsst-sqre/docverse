"""arq worker function for the ``build_processing_reaper`` cron backstop.

Mirrors :mod:`docverse.worker.functions.dashboard_build_reaper` for
``kind='build_processing'`` rows. Per PRD #367 §"Reaper module shape"
this is the run-less variant: ``build_processing`` does not aggregate
into a parent run row, so the reaper only sweeps stuck ``queue_jobs``
rows and finalises nothing.

Without reconciliation a wedged ``build_processing`` leaves an
uploaded build that never gets registered as ready — invisible to
operators today but corrosive: the project never sees progress on its
new release. Reaping flips the wedged row to ``failed`` so the build
state can be retried on the next operator action.
"""

from __future__ import annotations

from typing import Any

from docverse.domain.queue import JobKind

from ._runless_reaper import sweep_runless_kind

__all__ = ["build_processing_reaper"]


async def build_processing_reaper(ctx: dict[str, Any]) -> str:
    """Cron-driven backstop that fails silently-stuck build_processing rows.

    Thin shim over
    :func:`docverse.worker.functions._runless_reaper.sweep_runless_kind`;
    see that module for the shared sweep mechanics. Threshold defaults
    to 8 h via ``config.build_processing_reaper_threshold_seconds``;
    non-prod can override with
    ``DOCVERSE_BUILD_PROCESSING_REAPER_THRESHOLD_SECONDS``.
    """
    return await sweep_runless_kind(
        ctx,
        kind=JobKind.build_processing,
        threshold_attr="build_processing_reaper_threshold_seconds",
    )
