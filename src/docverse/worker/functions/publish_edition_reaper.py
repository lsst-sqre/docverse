"""arq worker function for the ``publish_edition_reaper`` cron backstop.

Mirrors :mod:`docverse.worker.functions.dashboard_build_reaper` for
``kind='publish_edition'`` rows. Per PRD #367 §"Reaper module shape"
this is the run-less variant: ``publish_edition`` does not aggregate
into a parent run row, so the reaper only sweeps stuck ``queue_jobs``
rows and finalises nothing.

Without reconciliation a wedged ``publish_edition`` leaves an edition
in ``publishing`` status that never reaches the CDN — invisible to
operators today but corrosive: the CDN silently stays behind the
edition's intended target build. Reaping flips the wedged row to
``failed`` so the publish state can be retried on the next operator
action.
"""

from __future__ import annotations

from typing import Any

from docverse.domain.queue import JobKind

from ._runless_reaper import sweep_runless_kind

__all__ = ["publish_edition_reaper"]


async def publish_edition_reaper(ctx: dict[str, Any]) -> str:
    """Cron-driven backstop that fails silently-stuck publish_edition rows.

    Thin shim over
    :func:`docverse.worker.functions._runless_reaper.sweep_runless_kind`;
    see that module for the shared sweep mechanics. Threshold defaults
    to 4 h via ``config.publish_edition_reaper_threshold_seconds``;
    non-prod can override with
    ``DOCVERSE_PUBLISH_EDITION_REAPER_THRESHOLD_SECONDS``.
    """
    return await sweep_runless_kind(
        ctx,
        kind=JobKind.publish_edition,
        threshold_attr="publish_edition_reaper_threshold_seconds",
    )
