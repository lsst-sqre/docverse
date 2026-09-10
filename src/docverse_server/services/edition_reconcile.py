"""Per-organization edition reconciliation (PRD #612).

The service between the ``edition_reconcile`` arq job and the pure
planner in :mod:`docverse_server.domain.edition_reconcile`: it reads the
organization's state, hands it to the planner, and applies whatever the
planner decided.

Its shape follows from what "applying" means here. Every action is an
*enqueue*, not a publish — the loop never touches a CDN itself, it puts
a ``publish_edition`` job back on the queue and lets the ordinary
publish path do the work. That keeps the reconciler's blast radius to
"jobs that should already have been enqueued", and it is why a failure
applying one action is contained rather than fatal: the remaining
editions of an organization have nothing to do with the one whose
enqueue raised, and abandoning them would let one bad edition strand a
whole org's drift indefinitely.

The reads are deliberately three batched queries rather than a walk:
one for the org's editions, one for their current builds' history rows,
one for the live publish jobs. An org with thousands of editions is the
normal case for this loop, so anything per-edition would put the tick's
cost on the organization's size squared.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import sentry_sdk
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse_server.domain.edition_build_history import EditionBuildHistory
from docverse_server.domain.edition_reconcile import (
    EditionReconcilePlan,
    EditionRepublish,
    plan_edition_reconcile,
)
from docverse_server.domain.organization import Organization
from docverse_server.metrics import EditionPublishTrigger
from docverse_server.services.publish_enqueue import (
    enqueue_publish_for_edition,
)
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.queue_backend import QueueBackend
from docverse_server.storage.queue_job_store import QueueJobStore

__all__ = [
    "RECONCILE_GRACE_WINDOW",
    "EditionReconcileOutcome",
    "EditionReconcileService",
]

RECONCILE_GRACE_WINDOW = timedelta(minutes=5)
"""How recent a pair's own timestamps must be to be left alone.

Deliberately equal to
:data:`docverse_server.worker.functions._runless_reaper.ORPHAN_IDLE_WINDOW`,
and pinned to it by a test rather than imported from it — the worker
package imports this one, so the dependency cannot run the other way.

The two agreeing is what makes the reconciler and the orphan sweeps tell
one story about a two-phase enqueue: the sweep waits this long before
calling a ``queued`` row with no backend job id abandoned, so the
reconciler waiting exactly as long means it never re-drives a pair whose
enqueue the sweep would still consider in progress.
"""


@dataclass(slots=True)
class EditionReconcileOutcome:
    """What one organization's reconciliation tick did.

    One tally rendered two ways — onto the ``queue_jobs`` row's
    ``progress`` and (from task #618) into the tick's metrics event — so
    the number an operator reads off the queue and the number Sasquatch
    charts cannot diverge.
    """

    editions_scanned: int = 0
    """Editions the tick considered, every bucket included."""

    republished: int = 0
    """Publishes this tick put back on the queue."""

    republish_failed: int = 0
    """Actions the tick planned and could not enqueue."""

    in_flight_skipped: int = 0
    """Pairs a live ``publish_edition`` job still holds."""

    grace_skipped: int = 0
    """Pairs too young to tell apart from an enqueue in progress."""

    failed_left_alone: int = 0
    """Pairs reading ``failed``; reported for operators, never re-driven."""

    retired_build_skipped: int = 0
    """Pointers at a soft-deleted or purged build."""

    tombstoned: int = 0
    """Soft-deleted editions (task #616 turns these into unpublishes)."""

    unpointed: int = 0
    """Live editions that have never had a current build."""

    healthy: int = 0
    """Pairs already converged."""

    capped: int = 0
    """Actions the per-job cap left for the next tick."""

    failed_editions: list[str] = field(default_factory=list)
    """``project/edition`` of every action counted under failure."""

    @property
    def has_errors(self) -> bool:
        """Whether the job should end ``completed_with_errors``."""
        return self.republish_failed > 0

    def as_progress(self) -> dict[str, Any]:
        """Render the tally as the ``queue_jobs.progress`` JSONB body."""
        return {
            "editions_scanned": self.editions_scanned,
            "republished": self.republished,
            "republish_failed": self.republish_failed,
            "in_flight_skipped": self.in_flight_skipped,
            "grace_skipped": self.grace_skipped,
            "failed_left_alone": self.failed_left_alone,
            "retired_build_skipped": self.retired_build_skipped,
            "tombstoned": self.tombstoned,
            "unpointed": self.unpointed,
            "healthy": self.healthy,
            "capped": self.capped,
            "failed_editions": list(self.failed_editions),
        }


class EditionReconcileService:
    """Re-drive one organization's lost and stalled edition publishes."""

    def __init__(
        self,
        *,
        session: AsyncSession,
        edition_store: EditionStore,
        history_store: EditionBuildHistoryStore,
        queue_job_store: QueueJobStore,
        queue_backend: QueueBackend,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._edition_store = edition_store
        self._history_store = history_store
        self._queue_job_store = queue_job_store
        self._queue_backend = queue_backend
        self._logger = logger

    async def reconcile_org(
        self, org: Organization, *, limit: int
    ) -> EditionReconcileOutcome:
        """Plan and apply one organization's reconciliation tick.

        Called with **no transaction open**: the reads run in one short
        transaction that closes before anything is enqueued, and
        :func:`~docverse_server.services.publish_enqueue.enqueue_publish_for_edition`
        opens its own two-phase transactions per action. Holding the
        read transaction across the enqueues would pin a connection
        idle-in-transaction for the length of the whole tick and, worse,
        would make Phase A's commit-before-enqueue guarantee a lie.

        Parameters
        ----------
        org
            The organization to reconcile.
        limit
            Maximum actions this tick may apply.

        Returns
        -------
        EditionReconcileOutcome
            The tally, whether or not anything needed doing.
        """
        plan = await self._plan(org, limit=limit)
        outcome = EditionReconcileOutcome(
            editions_scanned=plan.editions_scanned,
            in_flight_skipped=plan.in_flight_skipped,
            grace_skipped=plan.grace_skipped,
            failed_left_alone=plan.failed_left_alone,
            retired_build_skipped=plan.retired_build_skipped,
            tombstoned=plan.tombstoned,
            unpointed=plan.unpointed,
            healthy=plan.healthy,
            capped=plan.capped,
        )
        for action in plan.republish:
            await self._apply_republish(org, action, outcome)
        return outcome

    async def _plan(
        self, org: Organization, *, limit: int
    ) -> EditionReconcilePlan:
        """Read the organization's state and run the pure planner.

        The three reads share one transaction so the plan describes a
        single consistent instant: a publish job finishing between the
        history read and the live-job read would otherwise look like a
        stalled pair with no job behind it, which is precisely the shape
        the loop re-drives.
        """
        async with self._session.begin():
            editions = (
                await self._edition_store.list_org_editions_for_reconcile(
                    org_id=org.id
                )
            )
            pairs = [
                (edition.edition_id, edition.current_build_id)
                for edition in editions
                if edition.current_build_id is not None
            ]
            history_rows = (
                await self._history_store.list_by_edition_build_pairs(pairs)
            )
            live_pairs = await self._queue_job_store.list_live_publish_pairs(
                org_id=org.id
            )
        return plan_edition_reconcile(
            editions=editions,
            history_pairs=_latest_by_pair(history_rows),
            live_publish_pairs=live_pairs,
            pointers={},
            now=datetime.now(tz=UTC),
            grace=RECONCILE_GRACE_WINDOW,
            limit=limit,
        )

    async def _apply_republish(
        self,
        org: Organization,
        action: EditionRepublish,
        outcome: EditionReconcileOutcome,
    ) -> None:
        """Enqueue one drifted pair's publish, isolating its failure.

        A raise here is counted and logged rather than propagated. The
        editions after this one are independent of it, and the tick is
        the org's only recovery path — letting one edition abort the
        pass would strand every drift behind it until an operator
        noticed.
        """
        label = f"{action.project_slug}/{action.edition_slug}"
        try:
            await enqueue_publish_for_edition(
                session=self._session,
                edition_store=self._edition_store,
                history_store=self._history_store,
                queue_job_store=self._queue_job_store,
                queue_backend=self._queue_backend,
                org_id=org.id,
                project_id=action.project_id,
                project_slug=action.project_slug,
                edition_id=action.edition_id,
                edition_slug=action.edition_slug,
                build_id=action.build_id,
                build_public_id=action.build_public_id,
                trigger_override=EditionPublishTrigger.reconcile,
            )
        except Exception as exc:
            sentry_sdk.capture_exception(exc)
            outcome.republish_failed += 1
            outcome.failed_editions.append(label)
            self._logger.exception(
                "Failed to re-drive a drifted edition publish",
                project=action.project_slug,
                edition=action.edition_slug,
                build=action.build_public_id,
                reason=action.reason.value,
                phase="reconcile",
            )
            return
        outcome.republished += 1
        self._logger.info(
            "Re-drove a drifted edition publish",
            project=action.project_slug,
            edition=action.edition_slug,
            build=action.build_public_id,
            reason=action.reason.value,
            phase="reconcile",
        )


def _latest_by_pair(
    rows: list[EditionBuildHistory],
) -> dict[tuple[int, int], EditionBuildHistory]:
    """Index history rows by pair, keeping the most recent of each.

    ``edition_build_history`` has no unique constraint on
    ``(edition_id, build_id)`` and ``record()`` appends, so an edition
    rolled back onto a build it once served has two rows for the pair.
    ``list_by_edition_build_pairs`` returns them position-ordered with
    the most recent first, so the first row seen for a pair wins and the
    later duplicates are dropped.
    """
    latest: dict[tuple[int, int], EditionBuildHistory] = {}
    for row in rows:
        latest.setdefault((row.edition_id, row.build_id), row)
    return latest
