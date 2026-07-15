"""Shared helper for finalising a ``git_ref_audit`` run.

Mirrors :mod:`docverse.services.lifecycle_finalisation` so per-org
``git_ref_audit`` workers can roll the parent ``git_ref_audit_runs``
row to its terminal status once every attributed child ``queue_jobs``
row is terminal. The two finalisers stay separate (one per run table)
rather than parameterising a single helper: each owns its own store
type and status enum, and the saved Python LoC would be negligible
next to the unhelpful generic shape.
"""

from __future__ import annotations

from docverse.client.models import GitRefAuditRunStatus

from docverse.exceptions import NotFoundError
from docverse.storage.git_ref_audit_run_store import GitRefAuditRunStore

__all__ = ["maybe_finalise_git_ref_audit_run"]


async def maybe_finalise_git_ref_audit_run(
    *,
    run_store: GitRefAuditRunStore,
    run_id: int,
) -> None:
    """Transition a git_ref_audit run to terminal once all children terminal.

    Two per-org workers finishing concurrently can each compute a
    different terminal status (one sees every child completed and
    picks ``succeeded`` just as the second's failure commits, so the
    second picks ``partial_failure``). Without the terminal pre-check
    the second caller would hit the transition state machine's
    terminal→terminal guard and raise ``InvalidJobStateError``,
    rolling back the surrounding ``session.begin()`` and undoing that
    child's terminal transition. Swallow the conflict here so the
    loser of the race exits cleanly and lets the winning terminal
    status stand. Same rationale as
    :func:`docverse.services.lifecycle_finalisation
    .maybe_finalise_lifecycle_run`.
    """
    activity = await run_store.aggregate_activity(run_id=run_id)
    if activity.total_count == 0 or activity.pending_count > 0:
        return
    new_status = (
        GitRefAuditRunStatus.partial_failure
        if activity.failed_count > 0
        else GitRefAuditRunStatus.succeeded
    )
    run = await run_store.get(run_id)
    if run is None:
        msg = f"Git ref audit run {run_id} not found"
        raise NotFoundError(msg)
    if run.status in {
        GitRefAuditRunStatus.succeeded,
        GitRefAuditRunStatus.partial_failure,
        GitRefAuditRunStatus.failed,
    }:
        return
    await run_store.transition_status(run_id=run_id, new_status=new_status)
