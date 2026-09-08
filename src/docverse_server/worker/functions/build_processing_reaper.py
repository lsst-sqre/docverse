"""arq worker function for the ``build_processing_reaper`` cron backstop.

Mirrors :mod:`docverse_server.worker.functions.dashboard_build_reaper` for
``kind='build_processing'`` rows. Per PRD #367 §"Reaper module shape"
this is the run-less variant: ``build_processing`` does not aggregate
into a parent run row, so the reaper sweeps stuck ``queue_jobs`` rows
and finalises no parent.

Without reconciliation a wedged ``build_processing`` leaves an
uploaded build that never gets registered as ready — invisible to
operators today but corrosive: the project never sees progress on its
new release. Reaping flips the wedged row to ``failed`` so the build
state can be retried on the next operator action.

This kind alone also carries the shared sweep's ``extra_sweep`` hook
(PRD #577): failing a queue job says nothing about the ``builds`` row
it was working on, and a build stranded in ``processing`` reads as
in-flight forever — including the rows today's stale-guard bug (#575)
already left behind, which this sweep heals on the first tick after
deploy.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.queue import JobKind
from docverse_server.factory import Factory

from ._runless_reaper import ExtraSweepResult, sweep_runless_kind

__all__ = ["build_processing_reaper"]


async def _sweep_stranded_builds(
    *, factory: Factory, threshold: timedelta
) -> ExtraSweepResult:
    """Retire builds left in ``processing`` with no live queue job.

    The build-side counterpart to the three queue-job sweeps: a row
    whose worker is gone — because it was never transitioned, or
    because one of those sweeps just failed its job earlier in this
    very transaction — is transitioned to ``failed`` so ``processing``
    goes back to meaning "a worker is on it".

    Reuses the reaper's own threshold rather than a second knob: the
    build and the job it belongs to went quiet at the same moment, so
    the patience an operator configures for one is the patience they
    mean for the other. The threshold is handed over as the interval it
    is, leaving the store to subtract it from the database's clock the
    way the queue-job sweeps do — a cutoff computed here from the
    worker's own ``datetime.now()`` would disagree with theirs at the
    boundary whenever the two clocks are skewed.

    The shared reaper calls this once per transaction, so it must be
    idempotent: the second pass simply finds nothing left to claim
    unless the abandoned reaps in its own transaction freed a build.
    """
    build_store = factory.create_build_store()
    stranded = await build_store.fail_stranded_processing(idle_after=threshold)
    return ExtraSweepResult(
        log_key="stranded_builds",
        public_ids=[
            serialize_base32_id(build.public_id) for build in stranded
        ],
    )


async def build_processing_reaper(ctx: dict[str, Any]) -> str:
    """Cron backstop for stuck build_processing jobs and their builds.

    Thin shim over
    :func:`docverse_server.worker.functions._runless_reaper.sweep_runless_kind`;
    see that module for the shared sweep mechanics. Threshold defaults
    to 8 h via ``config.build_processing_reaper_threshold_seconds``;
    non-prod can override with
    ``DOCVERSE_BUILD_PROCESSING_REAPER_THRESHOLD_SECONDS``. The same
    threshold bounds the stranded-build sweep passed as ``extra_sweep``.
    """
    return await sweep_runless_kind(
        ctx,
        kind=JobKind.build_processing,
        threshold_attr="build_processing_reaper_threshold_seconds",
        extra_sweep=_sweep_stranded_builds,
    )
