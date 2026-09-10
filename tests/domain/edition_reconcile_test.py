"""Tests for the pure edition-reconciliation planner (PRD #612).

The planner is the whole decision table of the reconciliation loop with
no I/O in it, so this module is where each rule is pinned case by case:
what counts as drift, what counts as still-in-flight work the loop must
keep its hands off, and what counts as damage only an operator should
touch. The service and worker tests exercise the wiring; the semantics
live here.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from docverse.models.queue_enums import PublishStatus
from docverse_server.domain.edition_build_history import EditionBuildHistory
from docverse_server.domain.edition_reconcile import (
    EditionReconcilePlan,
    ReconcileEdition,
    ReconcileReason,
    plan_edition_reconcile,
)

NOW = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
"""The instant every case is planned at."""

GRACE = timedelta(minutes=5)
"""Default grace window, matching ``ORPHAN_IDLE_WINDOW``."""

SETTLED = NOW - timedelta(hours=2)
"""A timestamp comfortably outside the grace window."""

LIMIT = 100


def _edition(
    *,
    edition_id: int = 1,
    slug: str = "main",
    build_id: int | None = 5000,
    date_updated: datetime = SETTLED,
    date_deleted: datetime | None = None,
    build_deleted: datetime | None = None,
    build_purged: datetime | None = None,
) -> ReconcileEdition:
    """Build a live edition pointing at a live build, unless told otherwise."""
    return ReconcileEdition(
        edition_id=edition_id,
        edition_slug=slug,
        project_id=7,
        project_slug="proj",
        date_updated=date_updated,
        date_deleted=date_deleted,
        current_build_id=build_id,
        current_build_public_id=None if build_id is None else 123456789,
        current_build_storage_prefix=(
            None if build_id is None else "proj/builds/abc"
        ),
        current_build_date_deleted=build_deleted,
        current_build_date_purged=build_purged,
    )


def _history(
    *,
    edition: ReconcileEdition,
    status: PublishStatus | None,
    date_created: datetime = SETTLED,
) -> EditionBuildHistory:
    """Build the history row for an edition's current build."""
    assert edition.current_build_id is not None
    return EditionBuildHistory(
        id=edition.edition_id * 10,
        edition_id=edition.edition_id,
        build_id=edition.current_build_id,
        position=1,
        publish_status=status,
        date_created=date_created,
    )


def _plan(
    editions: list[ReconcileEdition],
    *,
    history: dict[tuple[int, int], EditionBuildHistory] | None = None,
    live: set[tuple[int, int]] | None = None,
    limit: int = LIMIT,
) -> EditionReconcilePlan:
    """Run the planner with the module's fixed clock and grace."""
    return plan_edition_reconcile(
        editions=editions,
        history_pairs=history or {},
        live_publish_pairs=live or set(),
        pointers={},
        now=NOW,
        grace=GRACE,
        limit=limit,
    )


def test_missing_history_pair_is_republished() -> None:
    """No history row at all means the publish was never enqueued.

    The #513 rule generalized off keeper-sync: a pointer moved and
    nothing followed it, which for a client-upload project has had no
    recovery path at all until now.
    """
    edition = _edition()

    plan = _plan([edition])

    assert len(plan.republish) == 1
    action = plan.republish[0]
    assert action.edition_id == edition.edition_id
    assert action.build_id == edition.current_build_id
    assert action.reason == ReconcileReason.lost_enqueue
    assert plan.editions_scanned == 1


def test_null_publish_status_pair_is_republished() -> None:
    """A history row whose status never left NULL is the same drift.

    ``enqueue_publish_for_edition`` is the only writer of the pair's
    ``publish_status``, so a recorded row still reading NULL says a
    pointer moved without a publish behind it — the pre-enqueue-era
    imports included.
    """
    edition = _edition()
    history = _history(edition=edition, status=None)

    plan = _plan([edition], history={(edition.edition_id, 5000): history})

    assert len(plan.republish) == 1
    assert plan.republish[0].reason == ReconcileReason.lost_enqueue


def test_pending_pair_with_no_live_job_is_republished() -> None:
    """A ``pending`` pair whose queue row was orphan-reaped is drift.

    This is the lost Phase B: Phase A committed the pair ``pending`` and
    a ``queued`` row with a NULL ``backend_job_id``, the enqueue never
    happened, and the orphan sweep failed the row without touching the
    pair. Nothing else in the tree re-drives it.
    """
    edition = _edition()
    history = _history(edition=edition, status=PublishStatus.pending)

    plan = _plan([edition], history={(edition.edition_id, 5000): history})

    assert len(plan.republish) == 1
    assert plan.republish[0].reason == ReconcileReason.stalled_publish


def test_publishing_pair_with_no_live_job_is_republished() -> None:
    """A ``publishing`` pair the silent-worker reaper failed is drift.

    ``publish_edition_reaper`` fails the ``queue_jobs`` row and leaves
    the edition and its history row reading ``publishing`` forever, so
    the pair is exactly as stuck as the reaper's own docstring implies
    it is not.
    """
    edition = _edition()
    history = _history(edition=edition, status=PublishStatus.publishing)

    plan = _plan([edition], history={(edition.edition_id, 5000): history})

    assert len(plan.republish) == 1
    assert plan.republish[0].reason == ReconcileReason.stalled_publish


def test_failed_pair_is_left_alone() -> None:
    """A ``failed`` pair is reported, never re-driven.

    ``publish_edition`` marks a pair ``failed`` only once arq's retries
    are exhausted or the error was non-retryable, so re-driving it here
    would loop on whatever is actually broken. Operators own these.
    """
    edition = _edition()
    history = _history(edition=edition, status=PublishStatus.failed)

    plan = _plan([edition], history={(edition.edition_id, 5000): history})

    assert plan.republish == ()
    assert plan.failed_left_alone == 1


def test_pair_with_a_live_job_is_skipped_in_flight() -> None:
    """A publish genuinely on the queue holds its pair."""
    edition = _edition()
    history = _history(edition=edition, status=PublishStatus.publishing)

    plan = _plan(
        [edition],
        history={(edition.edition_id, 5000): history},
        live={(edition.edition_id, 5000)},
    )

    assert plan.republish == ()
    assert plan.in_flight_skipped == 1


def test_pair_younger_than_grace_is_skipped() -> None:
    """A pair mid-enqueue is left alone until the window passes.

    Phase A commits the pair ``pending`` before Phase B writes the
    backend job id, so for a moment a perfectly healthy enqueue looks
    exactly like a lost one. The grace window is what keeps the loop
    from racing it.
    """
    edition = _edition()
    history = _history(
        edition=edition,
        status=PublishStatus.pending,
        date_created=NOW - timedelta(minutes=1),
    )

    plan = _plan([edition], history={(edition.edition_id, 5000): history})

    assert plan.republish == ()
    assert plan.grace_skipped == 1


def test_recently_repointed_edition_is_skipped() -> None:
    """A pointer that has only just moved is not yet drift.

    With no history row the pair has no timestamp of its own, so the
    edition's own ``date_updated`` is the grace anchor: ``build_
    processing`` commits ``set_current_build`` before it enqueues, and
    the gap between the two must not read as a lost enqueue.
    """
    edition = _edition(date_updated=NOW - timedelta(seconds=30))

    plan = _plan([edition])

    assert plan.republish == ()
    assert plan.grace_skipped == 1


def test_deleted_current_build_is_skipped() -> None:
    """A pointer at a soft-deleted build is never re-driven.

    The purgatory sweep reclaims a deleted build's objects once the
    org's retention elapses, so a pointer written now could outlive the
    content it names.
    """
    edition = _edition(build_deleted=SETTLED)

    plan = _plan([edition])

    assert plan.republish == ()
    assert plan.retired_build_skipped == 1


def test_purged_current_build_is_skipped() -> None:
    """A pointer at reclaimed content is never re-driven."""
    edition = _edition(build_deleted=SETTLED, build_purged=SETTLED)

    plan = _plan([edition])

    assert plan.republish == ()
    assert plan.retired_build_skipped == 1


def test_published_pair_is_healthy() -> None:
    """With no CDN read-back a ``published`` pair is converged."""
    edition = _edition()
    history = _history(edition=edition, status=PublishStatus.published)

    plan = _plan([edition], history={(edition.edition_id, 5000): history})

    assert plan.republish == ()
    assert plan.healthy == 1


def test_edition_without_a_current_build_is_not_republished() -> None:
    """An edition that has never had a build has nothing to publish."""
    edition = _edition(build_id=None)

    plan = _plan([edition])

    assert plan.republish == ()
    assert plan.unpointed == 1


def test_tombstoned_edition_is_not_republished() -> None:
    """A soft-deleted edition is never re-published.

    Task #616 turns this bucket into an unpublish once the loop can read
    the edge back; until then a tombstone is simply never a republish
    candidate.
    """
    edition = _edition(date_deleted=SETTLED)

    plan = _plan([edition])

    assert plan.republish == ()
    assert plan.tombstoned == 1


def test_cap_truncates_by_edition_id_and_reports_the_remainder() -> None:
    """The cap cuts a stable prefix and says how much it left.

    Ordering by edition id (not by discovery order) is what makes a
    capped org resume deterministically: the same prefix is attempted
    every tick until those pairs converge, rather than a rotating
    sample that starves the tail.
    """
    editions = [
        _edition(edition_id=eid, slug=f"e{eid}", build_id=eid * 100)
        for eid in (5, 1, 9, 3)
    ]

    plan = _plan(editions, limit=2)

    assert [action.edition_id for action in plan.republish] == [1, 3]
    assert plan.capped == 2
    assert plan.editions_scanned == 4


def test_uncapped_plan_reports_nothing_left_over() -> None:
    """A plan inside the cap reports ``capped == 0``."""
    editions = [
        _edition(edition_id=eid, slug=f"e{eid}", build_id=eid * 100)
        for eid in (1, 2)
    ]

    plan = _plan(editions, limit=10)

    assert len(plan.republish) == 2
    assert plan.capped == 0
