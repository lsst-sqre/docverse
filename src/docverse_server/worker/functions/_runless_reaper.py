"""Shared template for the run-less reaper modules.

A "run-less" reaper is the cron-driven backstop for a :class:`JobKind`
that does not aggregate into a parent run row — currently
``dashboard_build``, ``publish_edition``, ``build_processing``, and
``dashboard_sync``. Each sweeps stuck ``queue_jobs`` rows and finalises
nothing. Three passes cover the three ways a row goes stuck: silent
(``in_progress`` but the worker died), orphan (``queued`` and never
reached arq), and abandoned (``queued``, reached arq, and arq then lost
the job — PRD #538). A kind may add a fourth, kind-specific pass of its
own through the ``extra_sweep`` hook — ``build_processing`` uses it to
retire builds its queue jobs left behind (PRD #577).

Per PRD #367 §"Reaper module shape", each kind still ships its own
arq-registered function so cron staggering, logger name, log-event
text, and per-kind operator narrative stay independent. The function
in each module is a thin shim that delegates the actual sweep to
:func:`sweep_runless_kind` below.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol

import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse_server.config import config
from docverse_server.domain.queue import JobKind
from docverse_server.worker.functions._reaper_log import (
    create_reaped_jobs_payload,
    create_reaped_public_ids,
)

if TYPE_CHECKING:
    from docverse_server.factory import Factory

__all__ = [
    "ORPHAN_IDLE_WINDOW",
    "ExtraSweep",
    "ExtraSweepResult",
    "sweep_runless_kind",
]


@dataclass(frozen=True)
class ExtraSweepResult:
    """What one kind-specific extra sweep claimed during a reaper tick.

    The sweep reports serialized public ids rather than rows because
    the only thing the shared reaper does with them is log them, and
    the rows themselves are of a type only the owning kind knows.
    """

    log_key: str
    """Warning-payload key the ids are logged under (e.g.
    ``"stranded_builds"``)."""

    public_ids: list[str]
    """Base32 public ids of the rows the sweep claimed, in reap order.

    Counted into the tick's ``reaped_count``, so a tick whose only
    finding came from this sweep still logs at ``warning``.
    """


class ExtraSweep(Protocol):
    """A kind-specific extra pass run by :func:`sweep_runless_kind`.

    Called inside the reaper's first transaction, after the silent and
    orphan sweeps and before the abandoned sweep's candidate query, so
    it observes the rows those two just failed and commits with them.
    """

    async def __call__(
        self, *, factory: Factory, threshold: timedelta
    ) -> ExtraSweepResult:
        """Run the sweep and report the rows it claimed."""
        ...


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
    extra_sweep: ExtraSweep | None = None,
) -> str:
    """Sweep silent + orphan + abandoned rows for one run-less ``kind``.

    Used by the per-kind reaper modules. Reads the configured threshold
    from ``config.<threshold_attr>`` at invocation time (so non-prod
    overrides via ``DOCVERSE_<KIND>_REAPER_THRESHOLD_SECONDS`` take
    effect immediately), runs all three sweeps, and emits
    ``logger.warning`` with counts, reaped public IDs, and per-row
    sweep/``backend_job_id`` detail when anything was reaped,
    ``logger.debug`` otherwise.

    The tick runs in two transactions rather than one (task #548). The
    first carries the silent and orphan sweeps plus the abandoned
    sweep's candidate query; the queue-backend round trips then happen
    with no transaction open; the second applies whatever those
    verified, re-checking each row is still ``queued``. Keeping the
    backend outside the first transaction is what stops a stalled Redis
    — the post-outage scenario the abandoned sweep exists for — from
    blowing the tick past arq's job timeout and rolling back the silent
    and orphan reaps along with it.

    ``extra_sweep`` is the opt-in fourth pass: a kind that also has
    non-queue-job wreckage to clear (``build_processing`` and its
    stranded builds, PRD #577) passes a callable here, and its findings
    are counted into ``reaped_count`` and logged under the key the
    result names. Kinds that pass nothing behave exactly as before,
    down to the log payload's keys.

    The structlog ``event`` strings are f-string-built from
    ``kind.value`` so they match the per-kind literals the original
    explicit reapers emitted — log dashboards keying off
    ``"Reaped stuck <kind> queue jobs"`` keep working.
    """
    logger = structlog.get_logger(
        f"docverse_server.worker.{kind.value}_reaper"
    )
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
        # The abandoned sweep asks arq whether it still knows each
        # candidate job, so the reapers now need a queue backend — the
        # only reaper dependency beyond the store (PRD #538 §Summary,
        # "Reaper dependency change").
        queue_backend = factory.create_queue_backend()

        # First transaction: the two backend-free sweeps, plus the
        # abandoned sweep's candidate query. Committing here is what
        # keeps a stalled Redis from taking this work down with it —
        # arq's job timeout cancels the tick with a ``CancelledError``,
        # a ``BaseException`` no ``except Exception`` soft-abort can
        # catch, so anything still uncommitted when the backend hangs is
        # lost and re-lost on every following tick (task #548).
        async with session.begin():
            silent = await queue_job_store.fail_silent_jobs(
                kind, idle_after=threshold
            )
            orphan = await queue_job_store.fail_orphaned_jobs(
                kind, idle_after=ORPHAN_IDLE_WINDOW
            )
            # Deliberately after the two sweeps above: a kind-specific
            # sweep is likely to key off the rows they just failed —
            # ``build_processing``'s does, since a build only looks
            # stranded once the silent sweep has released its job.
            extra = (
                await extra_sweep(factory=factory, threshold=threshold)
                if extra_sweep is not None
                else None
            )
            # Reuses the kind's silent threshold rather than the much
            # shorter orphan window: an abandoned row *did* reach arq,
            # so it deserves the same benefit of the doubt a running
            # job gets before being declared dead.
            candidates = await queue_job_store.select_abandoned_jobs(
                kind, idle_after=threshold
            )

        # Backend round trips with no transaction open at all: nothing
        # to roll back, no rows locked, and the sweeps above already
        # durable however long arq takes to answer.
        reaps = await queue_job_store.verify_abandoned_candidates(
            candidates, queue_backend=queue_backend
        )

        # Second transaction, deliberately short: apply the verified
        # reaps, re-checking each row is still ``queued`` in case a
        # worker picked it up while the backend was being asked.
        async with session.begin():
            abandoned = await queue_job_store.apply_abandoned_reaps(reaps)

        by_sweep = (
            ("silent", silent),
            ("orphan", orphan),
            ("abandoned", abandoned),
        )
        # An absent hook contributes no key at all, so the three kinds
        # without one log exactly the payload they logged before.
        extra_count = len(extra.public_ids) if extra is not None else 0
        extra_payload = (
            {extra.log_key: extra.public_ids} if extra is not None else {}
        )
        reaped_count = len(silent) + len(orphan) + len(abandoned) + extra_count
        if reaped_count:
            logger.warning(
                warning_event,
                reaped_count=reaped_count,
                silent_count=len(silent),
                orphan_count=len(orphan),
                abandoned_count=len(abandoned),
                reaped_public_ids=create_reaped_public_ids(by_sweep),
                reaped_jobs=create_reaped_jobs_payload(by_sweep),
                **extra_payload,
            )
        else:
            logger.debug(debug_event)
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)
