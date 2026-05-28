"""Shared template for the run-less reaper modules.

A "run-less" reaper is the cron-driven backstop for a :class:`JobKind`
that does not aggregate into a parent run row — currently
``dashboard_build``, ``publish_edition``, ``build_processing``, and
``dashboard_sync``. Each sweeps stuck ``queue_jobs`` rows (silent
in-progress + orphan queued) in one transaction and finalises nothing.

Per PRD #367 §"Reaper module shape", each kind still ships its own
arq-registered function so cron staggering, logger name, log-event
text, and per-kind operator narrative stay independent. The function
in each module is a thin shim that delegates the actual sweep to
:func:`sweep_runless_kind` below.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse.config import config
from docverse.domain.queue import JobKind

__all__ = ["ORPHAN_IDLE_WINDOW", "sweep_runless_kind"]


# Window before a queued row with no ``backend_job_id`` is treated as
# orphaned. Matches the lifecycle and keeper-sync reapers' equivalent
# constant (5 min) so staleness checks across reapers stay aligned —
# long enough never to race a healthy concurrent enqueue, short enough
# to free a wedged row on the next reaper tick.
ORPHAN_IDLE_WINDOW = timedelta(minutes=5)


async def sweep_runless_kind(
    ctx: dict[str, Any],
    *,
    kind: JobKind,
    threshold_attr: str,
) -> str:
    """Sweep silent + orphan rows for one run-less ``kind``.

    Used by the per-kind reaper modules. Reads the configured threshold
    from ``config.<threshold_attr>`` at invocation time (so non-prod
    overrides via ``DOCVERSE_<KIND>_REAPER_THRESHOLD_SECONDS`` take
    effect immediately), runs both sweeps in one transaction, and
    emits ``logger.warning`` with counts and reaped public IDs when
    anything was reaped, ``logger.debug`` otherwise.

    The structlog ``event`` strings are f-string-built from
    ``kind.value`` so they match the per-kind literals the original
    explicit reapers emitted — log dashboards keying off
    ``"Reaped stuck <kind> queue jobs"`` keep working.
    """
    logger = structlog.get_logger(f"docverse.worker.{kind.value}_reaper")
    threshold = timedelta(seconds=getattr(config, threshold_attr))
    # Event strings are built once per invocation as locals so the
    # ``logger.{warning,debug}`` calls below stay free of f-strings
    # (ruff G004) while keeping the literal byte-for-byte identical to
    # what each per-kind reaper emitted before the refactor.
    warning_event = f"Reaped stuck {kind.value} queue jobs"
    debug_event = f"No stuck {kind.value} queue jobs to reap"

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()

        async with session.begin():
            silent = await queue_job_store.fail_silent_jobs(
                kind, idle_after=threshold
            )
            orphan = await queue_job_store.fail_orphaned_jobs(
                kind, idle_after=ORPHAN_IDLE_WINDOW
            )

        reaped_count = len(silent) + len(orphan)
        if reaped_count:
            logger.warning(
                warning_event,
                reaped_count=reaped_count,
                silent_count=len(silent),
                orphan_count=len(orphan),
                reaped_public_ids=sorted(
                    qj.public_id for qj in (*silent, *orphan)
                ),
            )
        else:
            logger.debug(debug_event)
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)
