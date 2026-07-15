"""Shared helper for finalising a lifecycle_eval run.

Mirrors :mod:`docverse.services.keeper_sync_finalisation` so per-org
``lifecycle_eval`` workers can roll the parent ``lifecycle_eval_runs``
row to its terminal status once every attributed child ``queue_jobs``
row is terminal. Lives next to the keeper-sync helper rather than
inside the evaluator package because the evaluator is deliberately
pure and unaware of run-level orchestration.
"""

from __future__ import annotations

from docverse.client.models import LifecycleEvalRunStatus

from docverse.exceptions import NotFoundError
from docverse.storage.lifecycle_eval_run_store import LifecycleEvalRunStore

__all__ = ["maybe_finalise_lifecycle_run"]


async def maybe_finalise_lifecycle_run(
    *,
    run_store: LifecycleEvalRunStore,
    run_id: int,
) -> None:
    """Transition a lifecycle_eval run to terminal once all children terminal.

    Mirrors ``maybe_finalise_run`` from the keeper-sync subsystem (see
    :func:`docverse.services.keeper_sync_finalisation.maybe_finalise_run`
    for the rationale on the terminal pre-check). Two per-org workers
    finishing concurrently can each compute a different terminal status
    (one sees every child completed and picks ``succeeded`` just as the
    second's failure commits, so the second picks ``partial_failure``).
    Without the terminal pre-check the second caller would hit the
    transition state machine's terminal→terminal guard and raise
    ``InvalidJobStateError``, rolling back the surrounding
    ``session.begin()`` and undoing that child's terminal transition.
    Swallow the conflict here so the loser of the race exits cleanly
    and lets the winning terminal status stand.
    """
    activity = await run_store.aggregate_activity(run_id=run_id)
    if activity.total_count == 0 or activity.pending_count > 0:
        return
    new_status = (
        LifecycleEvalRunStatus.partial_failure
        if activity.failed_count > 0
        else LifecycleEvalRunStatus.succeeded
    )
    run = await run_store.get(run_id)
    if run is None:
        msg = f"Lifecycle eval run {run_id} not found"
        raise NotFoundError(msg)
    if run.status in {
        LifecycleEvalRunStatus.succeeded,
        LifecycleEvalRunStatus.partial_failure,
        LifecycleEvalRunStatus.failed,
    }:
        return
    await run_store.transition_status(run_id=run_id, new_status=new_status)
