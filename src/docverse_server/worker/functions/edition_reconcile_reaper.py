"""arq worker function for the ``edition_reconcile_reaper`` cron backstop.

Mirrors :mod:`docverse_server.worker.functions.purgatory_cleanup_reaper`
for ``kind='edition_reconcile'`` rows. Per PRD #367 §"Reaper module
shape" this is the run-less variant: the reconciliation loop keeps no
parent run row — PRD #612 says its record of a tick is the dispatcher's
log line plus the per-org completion event — so the reaper only sweeps
stuck ``queue_jobs`` rows and finalises nothing.

What a wedged row costs here is a whole organization's reconciliation.
``edition_reconcile`` rows are guarded by the per-org mutex
``idx_queue_jobs_edition_reconcile_active_uq``, so a row left
``in_progress`` by an OOM-killed worker — or left ``queued`` by a
dispatcher that crashed between the SQL commit and ``arq_queue.enqueue``
— makes ``create_unless_active`` step over that org on every following
tick. The org's drifted editions then keep serving whatever the CDN
happens to hold: nothing, or a build several releases behind. Reaping
the row to ``failed`` releases the mutex, and the next tick re-plans the
org from current state.

That cadence is why this reaper's threshold is the tight one on the
pool. Its siblings back up daily or operator-triggered work and can
afford six hours; the loop here runs twice an hour, so an hour is
already twelve missed passes' worth of grace. There is no risk in the
tighter window: a reconciliation pass is bounded work — read the org's
editions, read the edge, enqueue at most
``edition_reconcile_max_actions_per_job`` publishes — and the publishes
it enqueues run on the default pool under their own queue rows, so
reaping the reconcile row never cancels repair work already in flight.
"""

from __future__ import annotations

from typing import Any

from docverse_server.domain.queue import JobKind

from ._runless_reaper import sweep_runless_kind

__all__ = ["edition_reconcile_reaper"]


async def edition_reconcile_reaper(ctx: dict[str, Any]) -> str:
    """Cron-driven backstop that fails stuck edition_reconcile rows.

    Thin shim over
    :func:`docverse_server.worker.functions._runless_reaper.sweep_runless_kind`;
    see that module for the shared sweep mechanics. Threshold defaults
    to 1 h via ``config.edition_reconcile_reaper_threshold_seconds``;
    non-prod can override with
    ``DOCVERSE_EDITION_RECONCILE_REAPER_THRESHOLD_SECONDS``.
    """
    return await sweep_runless_kind(
        ctx,
        kind=JobKind.edition_reconcile,
        threshold_attr="edition_reconcile_reaper_threshold_seconds",
    )
