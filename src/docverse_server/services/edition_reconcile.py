"""Per-organization edition reconciliation (PRD #612).

The service between the ``edition_reconcile`` arq job and the pure
planner in :mod:`docverse_server.domain.edition_reconcile`: it reads the
organization's state, hands it to the planner, and applies whatever the
planner decided.

Its shape follows from what "applying" means here, which is different
for the two kinds of action.

A republish is an *enqueue*, never a publish: the loop puts a
``publish_edition`` job back on the queue and lets the ordinary publish
path — with its edition lock, its deleted-build guard and its own
retries — do the work. That keeps the reconciler's blast radius to
"jobs that should already have been enqueued".

Each enqueue re-tests its pair first, inside the enqueue's own
transaction, because the plan describes an instant that has passed:
the read transaction closed before the CDN read-back, and up to a
whole cap's worth of other actions may have run since. An edition
repointed in that window, or a pair another driver has meanwhile
enqueued, is dropped rather than re-driven — see
:meth:`EditionReconcileService._recheck_republish`.

An unpublish has no such path to defer to; nothing enqueues a key
delete, so the loop performs it, through
:class:`~docverse_server.services.edition_publishing.EditionPublishingService`
so that removing a pointer means exactly what it means everywhere else.
Deleting the key of an edition the database has already tombstoned is
the narrowest write the loop could make: the row it is reconciling is
gone, and the only thing the key can still do is serve deleted content.

Both kinds are applied one at a time with their failures contained. The
remaining editions of an organization have nothing to do with the one
that raised, and the tick is the org's only recovery path, so abandoning
them would let a single bad edition strand a whole org's drift
indefinitely.

The reads are deliberately batched rather than a walk: one query for the
org's editions, one for their current builds' history rows, one for the
live publish jobs, and one chunked CDN read-back for every pointer. An
org with thousands of editions is the normal case for this loop, so
anything per-edition would put the tick's cost on the organization's
size squared.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import sentry_sdk
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse_server.domain.edition_build_history import EditionBuildHistory
from docverse_server.domain.edition_pointer import (
    EditionPointer,
    edition_pointer_key,
)
from docverse_server.domain.edition_reconcile import (
    EditionReconcilePlan,
    EditionRepublish,
    EditionUnpublish,
    ReconcileEdition,
    plan_edition_reconcile,
)
from docverse_server.domain.organization import Organization
from docverse_server.metrics import EditionPublishTrigger
from docverse_server.services.edition_publishing import (
    EditionPublisherProvider,
    EditionPublishingService,
)
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


class _ApplySkip(StrEnum):
    """Why an action the planner chose was dropped at apply time.

    The apply-time half of the planner's own ``_Skip`` vocabulary, and
    named to match it. ``in_flight`` feeds the very counter the
    planner's ``in_flight_skipped`` bucket feeds, because a pair skipped
    for a live publish is the same finding whichever of the two reads
    noticed it. ``superseded`` has no plan-time sibling: the planner
    cannot observe a repoint that happens after its own transaction
    closed.
    """

    superseded = "superseded"
    in_flight = "in_flight"

    @property
    def message(self) -> str:
        """The log line this skip is reported under."""
        if self is _ApplySkip.superseded:
            return "Skipped a superseded edition republish"
        return "Skipped an in-flight edition republish"


class _StalePlanError(Exception):
    """One planned republish no longer holds when it reaches the queue.

    Raised from the re-check that
    :func:`~docverse_server.services.publish_enqueue.enqueue_publish_for_edition`
    runs as its Phase A transaction's first statement, so the enqueue is
    abandoned with nothing written. Private to this module: it is a
    decision the loop makes about its own plan, never an error anything
    outside handles, and the helper it travels through deliberately
    passes it back untouched.
    """

    def __init__(
        self, skip: _ApplySkip, *, current_build_id: int | None
    ) -> None:
        super().__init__(f"Republish dropped at apply time: {skip.value}")
        self.skip = skip
        """Which of the two apply-time races dropped the action."""

        self.current_build_id = current_build_id
        """The build the edition actually pointed at under the lock."""


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

    pointers_read: int = 0
    """Keys the org's edge answered this tick's read-back with.

    Read against :attr:`cdn_checked` and :attr:`editions_scanned`: the
    first says the edge was asked at all, the second how many keys went
    out, and this one how many came back with something. A tick where
    the three diverge sharply is an org whose edge has lost keys
    wholesale, which no single edition's bucket would show.
    """

    republished: int = 0
    """Publishes this tick put back on the queue."""

    republish_failed: int = 0
    """Actions the tick planned and could not enqueue."""

    unpublished: int = 0
    """Stranded CDN keys this tick deleted."""

    unpublish_failed: int = 0
    """Keys the tick planned to delete and could not."""

    in_flight_skipped: int = 0
    """Pairs a live ``publish_edition`` job still holds.

    Counts both the pairs the planner's snapshot already saw a job for
    and the ones that acquired one between the plan and the enqueue. The
    two are the same finding — a publish is in progress, so the
    reconciler stands down — and an operator reading the counter wants
    how many pairs were mid-publish, not which of two reads noticed.
    """

    superseded_skipped: int = 0
    """Planned republishes the edition had moved off before the enqueue.

    No planner bucket corresponds: the planner cannot see this, because
    the repoint happens after its transaction closed. Distinct from
    :attr:`in_flight_skipped` because it accuses something different —
    a steady trickle here is an org whose editions are re-pointed faster
    than a tick takes to walk them, which is the read that says the
    per-job cap wants lowering rather than that anything is broken.
    """

    grace_skipped: int = 0
    """Pairs too young to tell apart from an enqueue in progress."""

    failed_left_alone: int = 0
    """Pairs reading ``failed``; reported for operators, never re-driven."""

    retired_build_skipped: int = 0
    """Pointers at a soft-deleted or purged build."""

    tombstoned: int = 0
    """Soft-deleted editions the edge already serves nothing for."""

    unpointed: int = 0
    """Live editions that have never had a current build."""

    unexpected_pointers: int = 0
    """Keys for editions with no build; reported, never acted on."""

    healthy: int = 0
    """Pairs already converged."""

    capped: int = 0
    """Actions the per-job cap left for the next tick."""

    cdn_checked: bool = False
    """Whether this tick read the org's edge back at all.

    ``False`` for an organization with no ``cdn_service_label``, and the
    one number an operator needs before reading the rest: without it a
    tick reporting no drift could equally mean "nothing is wrong" or
    "nothing was looked at".
    """

    republished_editions: list[str] = field(default_factory=list)
    """``project/edition`` of every publish this tick re-drove.

    The identity a drifted edition actually has: ``editions`` carries no
    public id — an edition is addressed by its slug within its project
    everywhere in the tree, and that pair *is* its CDN key — so the
    base32 rendering the reaper log payloads use has nothing here to
    render. Sorted at the log call rather than here, because the order
    an action was applied in is the order the per-action ``info`` lines
    are already in and only the summary wants them collated.
    """

    unpublished_editions: list[str] = field(default_factory=list)
    """``project/edition`` of every stranded key this tick deleted."""

    failed_editions: list[str] = field(default_factory=list)
    """``project/edition`` of every action counted under failure."""

    @property
    def has_errors(self) -> bool:
        """Whether the job should end ``completed_with_errors``."""
        return self.republish_failed > 0 or self.unpublish_failed > 0

    @property
    def acted(self) -> bool:
        """Whether the tick touched, or tried to touch, a CDN key."""
        return bool(
            self.republished
            or self.unpublished
            or self.republish_failed
            or self.unpublish_failed
        )

    def as_progress(self) -> dict[str, Any]:
        """Render the tally as the ``queue_jobs.progress`` JSONB body."""
        return {
            "editions_scanned": self.editions_scanned,
            "pointers_read": self.pointers_read,
            "republished": self.republished,
            "republish_failed": self.republish_failed,
            "unpublished": self.unpublished,
            "unpublish_failed": self.unpublish_failed,
            "in_flight_skipped": self.in_flight_skipped,
            "superseded_skipped": self.superseded_skipped,
            "grace_skipped": self.grace_skipped,
            "failed_left_alone": self.failed_left_alone,
            "retired_build_skipped": self.retired_build_skipped,
            "tombstoned": self.tombstoned,
            "unpointed": self.unpointed,
            "unexpected_pointers": self.unexpected_pointers,
            "healthy": self.healthy,
            "capped": self.capped,
            "cdn_checked": self.cdn_checked,
            "failed_editions": list(self.failed_editions),
        }

    def as_log_payload(self) -> dict[str, Any]:
        """Render the tally as the drift warning's structured payload.

        The counters of :meth:`as_progress` plus the editions behind the
        two action counts. They are deliberately not on the queue row:
        the row is what an operator reaches for *later*, and by then the
        re-driven publishes have their own ``queue_jobs`` rows and the
        deleted keys are gone, so repeating a capped tick's whole work
        list there would grow the JSONB without adding an answer. The
        log line is read at the moment of the repair, which is the one
        moment the names are the only record.
        """
        return {
            **self.as_progress(),
            "republished_editions": sorted(self.republished_editions),
            "unpublished_editions": sorted(self.unpublished_editions),
        }


class EditionReconcileService:
    """Converge one organization's editions with what its CDN serves."""

    def __init__(
        self,
        *,
        session: AsyncSession,
        edition_store: EditionStore,
        history_store: EditionBuildHistoryStore,
        queue_job_store: QueueJobStore,
        queue_backend: QueueBackend,
        publisher_provider: EditionPublisherProvider,
        publishing_service: EditionPublishingService,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._edition_store = edition_store
        self._history_store = history_store
        self._queue_job_store = queue_job_store
        self._queue_backend = queue_backend
        self._publisher_provider = publisher_provider
        self._publishing_service = publishing_service
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
            pointers_read=plan.pointers_read,
            in_flight_skipped=plan.in_flight_skipped,
            grace_skipped=plan.grace_skipped,
            failed_left_alone=plan.failed_left_alone,
            retired_build_skipped=plan.retired_build_skipped,
            tombstoned=plan.tombstoned,
            unpointed=plan.unpointed,
            unexpected_pointers=plan.unexpected_pointers,
            healthy=plan.healthy,
            capped=plan.capped,
            cdn_checked=plan.cdn_checked,
        )
        for action in plan.republish:
            await self._apply_republish(org, action, outcome)
        for removal in plan.unpublish:
            await self._apply_unpublish(org, removal, outcome)
        return outcome

    async def _plan(
        self, org: Organization, *, limit: int
    ) -> EditionReconcilePlan:
        """Read the organization's state and run the pure planner.

        The reads share one transaction but **not** one snapshot: the
        session runs at the database default of READ COMMITTED, so each
        SELECT sees whatever had committed when it started. The order
        the two publish-state reads are issued in is therefore
        load-bearing, and it is live publish pairs first, history rows
        second.

        That order is the one whose skew is harmless. A publish job that
        commits ``published`` and then completes its queue row between
        the two reads is caught by the live read while it still holds
        the pair, and the history row the second read then finds says
        ``published`` — either way the pair is not a candidate. A
        publish that *starts* in the same gap is missed by the live read
        but shows up in history as a freshly bumped ``pending`` or
        ``publishing`` row, which the grace window skips. Reading
        history first inverts both: a finishing publish reads
        ``publishing`` with no live job behind it, which is exactly the
        ``stalled_publish`` shape, and the tick spends a redundant job
        and a hostname purge resetting a converged pair to ``pending``.

        Widening the transaction to REPEATABLE READ would buy a real
        snapshot at the cost of serialization failures the job has no
        retry for (``max_tries=1``), for a race the read order already
        settles.

        The CDN read-back is in neither read's snapshot — it is an HTTP
        call to somebody else's system — so it runs after the
        transaction closes rather than holding a connection open across
        it. The resulting skew is bounded by one round trip and is
        covered by the grace window on both sides: a pointer that moved
        inside it belongs to a publish far younger than ``grace``.
        """
        async with self._session.begin():
            editions = (
                await self._edition_store.list_org_editions_for_reconcile(
                    org_id=org.id
                )
            )
            live_pairs = await self._queue_job_store.list_live_publish_pairs(
                org_id=org.id
            )
            pairs = [
                (edition.edition_id, edition.current_build_id)
                for edition in editions
                if edition.current_build_id is not None
            ]
            history_rows = (
                await self._history_store.list_by_edition_build_pairs(pairs)
            )
            publisher = None
            if org.cdn_service_label is not None:
                publisher = await self._publisher_provider(
                    org_id=org.id, service_label=org.cdn_service_label
                )
        pointers: Mapping[str, EditionPointer | None] | None = None
        if publisher is not None:
            async with publisher:
                pointers = await publisher.get_pointers(_keys(editions))
        return plan_edition_reconcile(
            editions=editions,
            history_pairs=_latest_by_pair(history_rows),
            live_publish_pairs=live_pairs,
            pointers=pointers,
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

        The action is re-tested before it is applied, in the enqueue's
        own transaction — see :meth:`_recheck_republish`. The plan it
        came from was made in a transaction that has since closed, and
        up to ``limit`` other actions may have been applied since, so
        "this pair is drifted" is a claim about a moment that has
        passed by the time the pair reaches the queue.
        """
        label = f"{action.project_slug}/{action.edition_slug}"

        async def _recheck() -> None:
            await self._recheck_republish(org, action)

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
                precheck=_recheck,
            )
        except _StalePlanError as stale:
            if stale.skip is _ApplySkip.superseded:
                outcome.superseded_skipped += 1
            else:
                outcome.in_flight_skipped += 1
            self._logger.info(
                stale.skip.message,
                project=action.project_slug,
                edition=action.edition_slug,
                edition_id=action.edition_id,
                planned_build_id=action.build_id,
                current_build_id=stale.current_build_id,
                build=action.build_public_id,
                reason=action.reason.value,
                phase="reconcile",
            )
            return
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
        outcome.republished_editions.append(label)
        self._logger.info(
            "Re-drove a drifted edition publish",
            project=action.project_slug,
            edition=action.edition_slug,
            build=action.build_public_id,
            reason=action.reason.value,
            phase="reconcile",
        )

    async def _recheck_republish(
        self, org: Organization, action: EditionRepublish
    ) -> None:
        """Re-test one planned republish inside the enqueue transaction.

        Runs as the first statement of
        :func:`~docverse_server.services.publish_enqueue.enqueue_publish_for_edition`'s
        Phase A, and raises :exc:`_StalePlanError` to abort it with
        nothing written. Both conditions it tests are races the plan
        cannot close on its own, because the plan's transaction commits
        before the CDN read-back and before the first of up to ``limit``
        enqueues:

        * The edition may have been **repointed** — by tracking, by an
          API rollback, by keeper-sync — since the pair was chosen.
          Enqueuing the planned build then publishes superseded content,
          and the ``EDITION_UPDATE`` advisory lock does not save it: that
          lock serializes publish jobs by pickup order, not enqueue
          order, so the stale job can win and leave the edge on the old
          build until the next tick notices. The read takes a row lock,
          so the answer holds for the rest of Phase A and a repoint
          arriving mid-transaction waits rather than slipping past.
        * Another driver may have **enqueued the same pair** since the
          snapshot was read. Nothing in ``queue_jobs`` refuses a second
          ``publish_edition`` row for a pair — the loop deliberately
          does not add a mutex index for it — so without this the pair
          would get two jobs, two KV writes, and two
          ``edition_published`` events.

        Order matters: an edition that has moved on is superseded
        whatever is in flight for the build it has left, and that is the
        more specific thing to tell an operator.
        """
        current_build_id = await self._edition_store.lock_current_build_id(
            edition_id=action.edition_id
        )
        if current_build_id != action.build_id:
            raise _StalePlanError(
                _ApplySkip.superseded, current_build_id=current_build_id
            )
        if await self._queue_job_store.has_live_publish_job(
            org_id=org.id,
            edition_id=action.edition_id,
            build_id=action.build_id,
        ):
            raise _StalePlanError(
                _ApplySkip.in_flight, current_build_id=current_build_id
            )

    async def _apply_unpublish(
        self,
        org: Organization,
        action: EditionUnpublish,
        outcome: EditionReconcileOutcome,
    ) -> None:
        """Delete one stranded key, isolating its failure.

        Goes through
        `~docverse_server.services.edition_publishing.EditionPublishingService`
        rather than the publisher this tick already opened for the
        read-back, so the loop removes a pointer by exactly the same
        route the delete path does — one place decides what unpublishing
        an edition means, and the reconciler cannot drift from it.

        The call runs inside a transaction because it re-reads the
        organization to resolve the publisher, and this service is
        invoked with none open; the enclosing ``begin()`` also keeps
        that read from leaving an implicit transaction behind for the
        next action to trip over.
        """
        label = f"{action.project_slug}/{action.edition_slug}"
        try:
            async with self._session.begin():
                await self._publishing_service.unpublish(
                    org_id=org.id,
                    project_slug=action.project_slug,
                    edition_slug=action.edition_slug,
                )
        except Exception as exc:
            sentry_sdk.capture_exception(exc)
            outcome.unpublish_failed += 1
            outcome.failed_editions.append(label)
            self._logger.exception(
                "Failed to remove a stranded edition pointer",
                project=action.project_slug,
                edition=action.edition_slug,
                phase="reconcile",
            )
            return
        outcome.unpublished += 1
        outcome.unpublished_editions.append(label)
        self._logger.info(
            "Removed a stranded edition pointer",
            project=action.project_slug,
            edition=action.edition_slug,
            phase="reconcile",
        )


def _keys(editions: Sequence[ReconcileEdition]) -> list[str]:
    """Build the CDN key of every edition the tick will consider.

    Tombstones included, and deliberately: a soft-deleted edition's
    stranded key is one of the two drifts the read-back exists to find,
    and it is the only one nothing else in the tree would ever notice.
    """
    return [
        edition_pointer_key(edition.project_slug, edition.edition_slug)
        for edition in editions
    ]


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
