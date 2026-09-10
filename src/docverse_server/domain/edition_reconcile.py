"""Domain models for the edition reconciliation loop (PRD #612)."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from docverse.models.queue_enums import PublishStatus

from .base32id import serialize_base32_id
from .edition_build_history import EditionBuildHistory

__all__ = [
    "EditionReconcilePlan",
    "EditionRepublish",
    "ReconcileEdition",
    "ReconcileReason",
    "plan_edition_reconcile",
]


@dataclass(frozen=True, slots=True)
class ReconcileEdition:
    """One organization's edition, as the reconciliation loop reads it.

    A flat join of the three rows a reconciliation decision needs — the
    edition, its project, and the build the edition currently points at
    — rather than the :class:`~docverse_server.domain.edition.Edition`
    domain model. Two of those needs are the reason:

    * The loop is **org-scoped**, so it has to name the project of every
      edition it considers. Both the CDN key (``{project_slug}/
      {edition_slug}``) and the ``publish_edition`` payload are built
      from the slug, and re-reading it per edition would be an N+1 over
      an org's whole edition set.
    * A decision turns on whether the current build's *content* still
      exists, which ``Edition`` cannot say: it carries the build's
      public id but neither ``date_deleted`` nor ``date_purged``, and
      re-driving a publish at a purged build would write a CDN pointer
      to objects the purgatory sweep has already reclaimed.

    Soft-deleted editions are in scope, so :attr:`date_deleted` is a
    first-class field rather than a filter applied before the loop sees
    the row: an edition whose tombstone was committed but whose CDN key
    outlived it is one of the drifts the loop exists to repair.
    """

    edition_id: int
    """Internal id, and the loop's deterministic ordering key."""

    edition_slug: str
    """Second half of the edition's CDN key."""

    project_id: int
    """Owning project, as the ``publish_edition`` queue row records it."""

    project_slug: str
    """First half of the CDN key, and the publish payload's project."""

    date_updated: datetime
    """When the edition row last changed.

    The grace-window anchor for an edition with no history row yet: a
    pointer that has only just moved is a publish still being enqueued,
    not drift.
    """

    date_deleted: datetime | None
    """Tombstone stamp; ``None`` for a live edition."""

    current_build_id: int | None
    """Build the edition serves, or ``None`` if it has never had one."""

    current_build_public_id: int | None
    """Base32 public id of :attr:`current_build_id`, unserialized."""

    current_build_storage_prefix: str | None
    """Object-store prefix the CDN pointer for this build must name."""

    current_build_date_deleted: datetime | None
    """Soft-delete stamp on the current build, if any."""

    current_build_date_purged: datetime | None
    """Reclamation stamp on the current build, if any."""

    @property
    def current_build_retired(self) -> bool:
        """Whether the current build's content is gone or on its way out.

        Either stamp is disqualifying and for the same reason: the
        purgatory sweep reclaims a soft-deleted build's objects once its
        organization's retention elapses, so publishing a pointer at a
        merely-deleted build races that reclamation just as surely as
        publishing at an already-purged one.
        """
        return (
            self.current_build_date_deleted is not None
            or self.current_build_date_purged is not None
        )


class ReconcileReason(StrEnum):
    """Why the planner decided one pair needs re-driving.

    Carried on the action so the warning log and the queue-job progress
    can say *which* failure mode an org is accumulating: a run of
    ``lost_enqueue`` points at something dropping enqueues between the
    pointer move and the queue, while a run of ``stalled_publish``
    points at workers dying mid-publish. The two want very different
    follow-up, so collapsing them into "drift" would cost an operator
    the only diagnosis the loop can give for free.
    """

    lost_enqueue = "lost_enqueue"
    """No publish was ever enqueued for the pair.

    Either no ``edition_build_history`` row exists for it or the row's
    ``publish_status`` is still NULL. ``enqueue_publish_for_edition`` is
    the only writer of that column, so NULL is proof the enqueue never
    ran — the #513 keeper-sync rule, generalized to every project.
    """

    stalled_publish = "stalled_publish"
    """A publish started for the pair and never finished.

    The pair reads ``pending`` or ``publishing`` with no live
    ``publish_edition`` job behind it: a lost Phase B whose orphan row
    the sweep failed, or a publish whose worker died and whose row the
    reaper failed without ever clearing the pair.
    """


@dataclass(frozen=True, slots=True)
class EditionRepublish:
    """One ``(edition, build)`` pair the loop will re-drive.

    Carries everything ``enqueue_publish_for_edition`` needs, so
    applying the plan is a loop over these with no further reads.
    """

    edition_id: int
    edition_slug: str
    project_id: int
    project_slug: str
    build_id: int
    build_public_id: str
    reason: ReconcileReason


@dataclass(frozen=True, slots=True)
class EditionReconcilePlan:
    """What one organization's reconciliation tick should do.

    Every edition the tick scanned lands in exactly one bucket, and the
    counters are as much of the output as :attr:`republish` is: an org
    with nothing to do still reports what it looked at, which is what
    turns "the loop ran and found nothing" into evidence rather than
    silence.
    """

    republish: tuple[EditionRepublish, ...]
    """Actions to apply, ordered by edition id and cut at the cap."""

    editions_scanned: int
    """Editions considered, including every skipped bucket."""

    capped: int
    """Actions the cap left for the next tick."""

    in_flight_skipped: int = 0
    """Pairs a live ``publish_edition`` job still holds."""

    grace_skipped: int = 0
    """Pairs too young to distinguish from an enqueue in progress."""

    failed_left_alone: int = 0
    """Pairs reading ``failed``, reported for operators and untouched."""

    retired_build_skipped: int = 0
    """Pointers at a soft-deleted or purged build."""

    tombstoned: int = 0
    """Soft-deleted editions (task #616 turns these into unpublishes)."""

    unpointed: int = 0
    """Live editions that have never had a current build."""

    healthy: int = 0
    """Pairs already reading ``published``."""

    @property
    def has_actions(self) -> bool:
        """Whether this tick will change anything."""
        return bool(self.republish)


class _Skip(StrEnum):
    """Bucket an edition falls into when the tick has nothing to do.

    Private to the planner: the buckets are already public as the plan's
    counters, and naming them once here is what keeps
    :func:`plan_edition_reconcile` a tally over
    :func:`_classify_edition` rather than a nest of counters.
    """

    tombstoned = "tombstoned"
    unpointed = "unpointed"
    retired_build_skipped = "retired_build_skipped"
    in_flight_skipped = "in_flight_skipped"
    failed_left_alone = "failed_left_alone"
    healthy = "healthy"
    grace_skipped = "grace_skipped"


def _classify_edition(
    edition: ReconcileEdition,
    *,
    history: EditionBuildHistory | None,
    live: bool,
    now: datetime,
    grace: timedelta,
) -> EditionRepublish | _Skip:
    """Apply the decision table to one edition.

    Returns the action to take, or the bucket that explains why there is
    none. The order of the checks is the table's order and is
    load-bearing at two points: an in-flight job outranks every state
    the pair could be recorded in (a publish in progress is allowed to
    leave the pair looking stalled), and the grace window is consulted
    *last*, so an edition that was never a candidate is not counted as
    one the window held back.
    """
    if edition.date_deleted is not None:
        # Task #616 turns a tombstone with a live pointer into an
        # unpublish; with no read-back there is nothing to decide.
        return _Skip.tombstoned
    build_id = edition.current_build_id
    public_id = edition.current_build_public_id
    if build_id is None or public_id is None:
        return _Skip.unpointed
    if edition.current_build_retired:
        return _Skip.retired_build_skipped
    if live:
        return _Skip.in_flight_skipped

    if history is not None and history.publish_status is not None:
        status = history.publish_status
        if status is PublishStatus.failed:
            return _Skip.failed_left_alone
        if status is PublishStatus.published:
            return _Skip.healthy
        reason = ReconcileReason.stalled_publish
    else:
        reason = ReconcileReason.lost_enqueue

    settled_at = edition.date_updated
    if history is not None:
        settled_at = max(settled_at, history.date_created)
    if now - settled_at < grace:
        return _Skip.grace_skipped

    return EditionRepublish(
        edition_id=edition.edition_id,
        edition_slug=edition.edition_slug,
        project_id=edition.project_id,
        project_slug=edition.project_slug,
        build_id=build_id,
        build_public_id=serialize_base32_id(public_id),
        reason=reason,
    )


def plan_edition_reconcile(
    *,
    editions: Sequence[ReconcileEdition],
    history_pairs: Mapping[tuple[int, int], EditionBuildHistory],
    live_publish_pairs: AbstractSet[tuple[int, int]],
    pointers: Mapping[str, object],
    now: datetime,
    grace: timedelta,
    limit: int,
) -> EditionReconcilePlan:
    """Decide what one organization's reconciliation tick should do.

    Pure: no I/O, no clock, no randomness. Everything the decision table
    consults is an argument, which is what lets the whole table be
    pinned case by case in ``tests/domain/edition_reconcile_test.py``
    without a database.

    Parameters
    ----------
    editions
        Every edition the org owns, tombstones included.
    history_pairs
        The most recent ``edition_build_history`` row for each
        ``(edition_id, build_id)`` pair the caller asked about. A pair
        absent from this mapping has no history row at all, which
        differs from a row whose ``publish_status`` is NULL only in
        provenance — both mean no publish was ever enqueued.
    live_publish_pairs
        Pairs a ``publish_edition`` job is genuinely working on right
        now (``in_progress``, or ``queued`` with a backend job id).
    pointers
        What the CDN actually serves, keyed by ``{project}/{edition}``.
        Accepted now and unused: the pointer rules arrive with task
        #616, and taking the argument from the start means that task
        adds rules to this function rather than reshaping its callers.
    now
        The tick's clock.
    grace
        How recent a pair's own timestamps must be for the tick to
        assume an enqueue is still in progress rather than lost.
        Defaults, at the call site, to the same ``ORPHAN_IDLE_WINDOW``
        the orphan sweeps age rows out against, so the two agree about
        when a two-phase enqueue has had its chance.
    limit
        Maximum actions this tick may return.

    Returns
    -------
    EditionReconcilePlan
        The actions, ordered by edition id and truncated at ``limit``,
        plus one counter per bucket the table sorts editions into.

    Notes
    -----
    Editions are walked in ascending id order rather than sorted after
    the fact, so the cap always cuts the same prefix of the same work
    list. A capped organization therefore re-attempts the same pairs
    every tick until they converge, instead of sampling a rotating
    window that could starve its tail indefinitely.
    """
    actions: list[EditionRepublish] = []
    skips: Counter[_Skip] = Counter()

    for edition in sorted(editions, key=lambda e: e.edition_id):
        pair = (edition.edition_id, edition.current_build_id or 0)
        outcome = _classify_edition(
            edition,
            history=history_pairs.get(pair),
            live=pair in live_publish_pairs,
            now=now,
            grace=grace,
        )
        if isinstance(outcome, EditionRepublish):
            actions.append(outcome)
        else:
            skips[outcome] += 1

    return EditionReconcilePlan(
        republish=tuple(actions[:limit]),
        editions_scanned=len(editions),
        capped=max(len(actions) - limit, 0),
        in_flight_skipped=skips[_Skip.in_flight_skipped],
        grace_skipped=skips[_Skip.grace_skipped],
        failed_left_alone=skips[_Skip.failed_left_alone],
        retired_build_skipped=skips[_Skip.retired_build_skipped],
        tombstoned=skips[_Skip.tombstoned],
        unpointed=skips[_Skip.unpointed],
        healthy=skips[_Skip.healthy],
    )
