"""Shared helper for finalising a keeper-sync run.

Both the ``keeper_sync_project`` worker (after a project sync's child
``QueueJob`` reaches a terminal state) and the ``publish_edition``
worker (after a publish job that was attributed to a keeper-sync run
completes) need to roll the parent ``keeper_sync_runs`` row to a
terminal status once every attributed child has reached terminal. The
logic is the same in both places, so it lives here.

When :func:`maybe_finalise_run` actually drives a run terminal it returns
the finalised run paired with its child-job activity; callers publish the
``keeper_sync_run_completed`` Sasquatch metric from that result via
:func:`publish_run_completed`, after their transaction commits (SQR-112).
"""

from __future__ import annotations

from datetime import timedelta

import sentry_sdk
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import KeeperSyncRunStatus
from docverse.domain.keeper_sync_run import KeeperSyncRunWithActivity
from docverse.exceptions import NotFoundError
from docverse.metrics import KeeperSyncRunCompletedEvent
from docverse.metrics.events import DocverseEvents
from docverse.storage.keeper_sync_run_store import KeeperSyncRunStore
from docverse.storage.organization_store import OrganizationStore

__all__ = ["maybe_finalise_run", "publish_run_completed"]


_TERMINAL_STATUSES = frozenset(
    {
        KeeperSyncRunStatus.succeeded,
        KeeperSyncRunStatus.partial_failure,
        KeeperSyncRunStatus.failed,
    }
)


async def maybe_finalise_run(
    *,
    run_store: KeeperSyncRunStore,
    run_id: int,
) -> KeeperSyncRunWithActivity | None:
    """Transition the run to a terminal status once all children are terminal.

    Returns the finalised run paired with the child-job activity it was
    computed from when (and only when) this call actually drove the run
    terminal; returns ``None`` otherwise (children still pending, no
    attributed children, or the run was already terminal). Callers use
    the return value to publish ``keeper_sync_run_completed`` after their
    surrounding transaction commits.

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
    (returning ``None``, so it publishes nothing) and lets the winning
    terminal status stand.
    """
    activity = await run_store.aggregate_activity(run_id=run_id)
    if activity.total_count == 0 or activity.pending_count > 0:
        return None
    new_status = (
        KeeperSyncRunStatus.partial_failure
        if activity.failed_count > 0
        else KeeperSyncRunStatus.succeeded
    )
    run = await run_store.get(run_id)
    if run is None:
        msg = f"Keeper sync run {run_id} not found"
        raise NotFoundError(msg)
    if run.status in _TERMINAL_STATUSES:
        return None
    finalised = await run_store.transition_status(
        run_id=run_id, new_status=new_status
    )
    return KeeperSyncRunWithActivity(run=finalised, activity=activity)


async def publish_run_completed(
    *,
    events: DocverseEvents | None,
    session: AsyncSession,
    org_store: OrganizationStore,
    completion: KeeperSyncRunWithActivity | None,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Emit one ``keeper_sync_run_completed`` metric for a finalised run.

    A no-op when ``events`` is unset (a worker ctx wired without metrics)
    or ``completion`` is ``None`` (this caller did not actually finalise
    the run — children still pending, or another finaliser won the race).
    A keeper-sync run spans many projects, so it is org-scoped and
    ``project`` is always ``None``; ``success`` is ``True`` only for a
    clean ``succeeded`` finalisation, ``False`` for ``partial_failure`` /
    ``failed``.

    Fully best-effort: callers invoke this *after* their finalisation
    transaction has committed, so it swallows and logs any error — a
    metrics-backend outage (already covered by ``raise_on_error=False``)
    *or* a DB error during its own ``org_id`` resolution — rather than
    letting it propagate and disrupt (or retry) an already-committed run
    finalisation.

    The org slug is resolved from the run's ``org_id`` so a single helper
    serves every finaliser regardless of what its payload carried.
    """
    if events is None or completion is None:
        return
    try:
        run = completion.run
        activity = completion.activity
        async with session.begin():
            org = await org_store.get_by_id(run.org_id)
        organization = org.slug if org is not None else str(run.org_id)
        elapsed = (
            run.date_finished - run.date_started
            if run.date_finished is not None
            else timedelta(0)
        )
        await events.keeper_sync_run_completed.publish(
            KeeperSyncRunCompletedEvent(
                organization=organization,
                project=None,
                success=run.status == KeeperSyncRunStatus.succeeded,
                total_count=activity.total_count,
                succeeded_count=activity.succeeded_count,
                failed_count=activity.failed_count,
                elapsed=elapsed,
            )
        )
    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        logger.exception("Failed to publish keeper_sync_run_completed metric")
