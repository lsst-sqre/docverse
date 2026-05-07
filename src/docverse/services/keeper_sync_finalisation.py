"""Shared helper for finalising a keeper-sync run.

Both the ``keeper_sync_project`` worker (after a project sync's child
``QueueJob`` reaches a terminal state) and the ``publish_edition``
worker (after a publish job that was attributed to a keeper-sync run
completes) need to roll the parent ``keeper_sync_runs`` row to a
terminal status once every attributed child has reached terminal. The
logic is the same in both places, so it lives here.
"""

from __future__ import annotations

from docverse.client.models import KeeperSyncRunStatus
from docverse.exceptions import NotFoundError
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore

__all__ = ["maybe_finalise_run"]


async def maybe_finalise_run(
    *,
    run_store: KeeperSyncRunStore,
    run_id: int,
) -> None:
    """Transition the run to a terminal status once all children are terminal.

    Idempotent re-entry on the same terminal status is handled by
    ``transition_status``'s same-status fast path. The explicit terminal
    pre-check guards a different case: two child finalisers racing each
    other can compute *different* terminal statuses (e.g. one sees all
    children completed and picks ``succeeded`` just as another child's
    failure commits, so the second finaliser picks ``partial_failure``).
    Without the pre-check, the second caller would hit
    ``transition_status``'s terminal→terminal guard and raise
    ``InvalidJobStateError``, which would roll back the surrounding
    ``session.begin()`` and undo that child's terminal transition. We
    swallow the conflict here so the loser of the race exits cleanly
    and lets the winning terminal status stand.
    """
    activity = await run_store.aggregate_activity(run_id=run_id)
    if activity.total_count == 0 or activity.pending_count > 0:
        return
    new_status = (
        KeeperSyncRunStatus.partial_failure
        if activity.failed_count > 0
        else KeeperSyncRunStatus.succeeded
    )
    run = await run_store.get(run_id)
    if run is None:
        msg = f"Keeper sync run {run_id} not found"
        raise NotFoundError(msg)
    if run.status in {
        KeeperSyncRunStatus.succeeded,
        KeeperSyncRunStatus.partial_failure,
        KeeperSyncRunStatus.failed,
    }:
        return
    await run_store.transition_status(run_id=run_id, new_status=new_status)
