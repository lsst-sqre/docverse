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
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.edition_build_history import EditionBuildHistory
from docverse_server.domain.edition_pointer import (
    EditionPointer,
    edition_pointer_key,
)
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

PUBLIC_ID = 123456789
"""Public id of the build every fixture edition points at."""

BUILD_ID = serialize_base32_id(PUBLIC_ID)
"""The base32 rendering a matching pointer has to carry."""

PREFIX = "proj/builds/abc"
"""``builds.storage_prefix`` a matching pointer has to carry."""


def _edition(
    *,
    edition_id: int = 1,
    slug: str = "main",
    build_id: int | None = 5000,
    date_updated: datetime = SETTLED,
    date_deleted: datetime | None = None,
    project_deleted: datetime | None = None,
    build_deleted: datetime | None = None,
    build_purged: datetime | None = None,
) -> ReconcileEdition:
    """Build a live edition pointing at a live build, unless told otherwise."""
    return ReconcileEdition(
        edition_id=edition_id,
        edition_slug=slug,
        project_id=7,
        project_slug="proj",
        project_date_deleted=project_deleted,
        date_updated=date_updated,
        date_deleted=date_deleted,
        current_build_id=build_id,
        current_build_public_id=None if build_id is None else PUBLIC_ID,
        current_build_storage_prefix=None if build_id is None else PREFIX,
        current_build_date_deleted=build_deleted,
        current_build_date_purged=build_purged,
    )


def _pointer(
    *,
    build_id: str = BUILD_ID,
    prefix: str = PREFIX,
) -> EditionPointer:
    """Build the pointer a converged edge would serve."""
    return EditionPointer(
        build_public_id=build_id, r2_prefix=prefix, cache_profile=None
    )


def _pointers(
    *pairs: tuple[ReconcileEdition, EditionPointer | None],
) -> dict[str, EditionPointer | None]:
    """Build a read-back mapping keyed the way the publisher keys it."""
    return {
        edition_pointer_key(
            edition.project_slug, edition.edition_slug
        ): pointer
        for edition, pointer in pairs
    }


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
    pointers: dict[str, EditionPointer | None] | None = None,
    limit: int = LIMIT,
) -> EditionReconcilePlan:
    """Run the planner with the module's fixed clock and grace.

    ``pointers`` defaults to ``None``, the no-CDN organization: most of
    the decision table is decided on database state alone, and passing
    an empty mapping instead would assert the far stronger "the edge was
    read and serves nothing for any of these editions".
    """
    return plan_edition_reconcile(
        editions=editions,
        history_pairs=history or {},
        live_publish_pairs=live or set(),
        pointers=pointers,
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


def test_published_pair_on_a_no_cdn_org_is_healthy() -> None:
    """Without an edge to read, a ``published`` pair is taken at its word.

    An organization with no ``cdn_service_label`` has nothing for the
    loop to compare the database against, so ``published`` is the whole
    of the truth available and the tick must not manufacture drift out
    of the pointers it could not read.
    """
    edition = _edition()
    history = _history(edition=edition, status=PublishStatus.published)

    plan = _plan([edition], history={(edition.edition_id, 5000): history})

    assert plan.republish == ()
    assert plan.healthy == 1
    assert plan.cdn_checked is False


def test_edition_without_a_current_build_is_not_republished() -> None:
    """An edition that has never had a build has nothing to publish."""
    edition = _edition(build_id=None)

    plan = _plan([edition])

    assert plan.republish == ()
    assert plan.unpointed == 1


def test_tombstoned_edition_without_a_pointer_is_only_counted() -> None:
    """A soft-deleted edition the edge already forgot needs nothing.

    A tombstone is never a republish candidate, and once its key is gone
    there is nothing left to unpublish either — the ordinary delete path
    got there first, which is the steady state this bucket reports.
    """
    edition = _edition(date_deleted=SETTLED)

    plan = _plan(
        [edition],
        pointers=_pointers((edition, None)),
    )

    assert plan.republish == ()
    assert plan.unpublish == ()
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


def test_published_pair_with_a_missing_pointer_is_republished() -> None:
    """A ``published`` pair the edge serves nothing for is drift.

    The database's own record of the publish is not evidence that the
    key survived: a KV write that was acknowledged and lost, or a key
    deleted out from under the pair, leaves exactly this shape and is
    invisible to every other check in the tree.
    """
    edition = _edition()
    history = _history(edition=edition, status=PublishStatus.published)

    plan = _plan(
        [edition],
        history={(edition.edition_id, 5000): history},
        pointers=_pointers((edition, None)),
    )

    assert len(plan.republish) == 1
    assert plan.republish[0].reason == ReconcileReason.pointer_missing
    assert plan.cdn_checked is True


def test_published_pair_with_a_stale_build_id_is_republished() -> None:
    """A pointer naming a build the edition has moved off is drift.

    The publish that should have moved the pointer never landed, so the
    edge is still serving a superseded build. Nothing in the database
    disagrees with itself here — only the comparison finds it.
    """
    edition = _edition()
    history = _history(edition=edition, status=PublishStatus.published)

    plan = _plan(
        [edition],
        history={(edition.edition_id, 5000): history},
        pointers=_pointers(
            (edition, _pointer(build_id=serialize_base32_id(999)))
        ),
    )

    assert len(plan.republish) == 1
    assert plan.republish[0].reason == ReconcileReason.pointer_stale
    assert plan.republish[0].build_public_id == BUILD_ID


def test_published_pair_with_a_stale_prefix_is_republished() -> None:
    """The prefix is compared independently of the build id.

    A content-hash migration re-homes a build's objects without minting
    a new build, so a pointer can carry the right ``build_id`` and a
    prefix that names objects nothing writes to any more.
    """
    edition = _edition()
    history = _history(edition=edition, status=PublishStatus.published)

    plan = _plan(
        [edition],
        history={(edition.edition_id, 5000): history},
        pointers=_pointers((edition, _pointer(prefix="proj/builds/old"))),
    )

    assert len(plan.republish) == 1
    assert plan.republish[0].reason == ReconcileReason.pointer_stale


def test_published_pair_with_a_matching_pointer_is_healthy() -> None:
    """Database and edge agreeing is the whole point of the loop."""
    edition = _edition()
    history = _history(edition=edition, status=PublishStatus.published)

    plan = _plan(
        [edition],
        history={(edition.edition_id, 5000): history},
        pointers=_pointers((edition, _pointer())),
    )

    assert plan.republish == ()
    assert plan.healthy == 1
    assert plan.cdn_checked is True


def test_pointer_drift_younger_than_grace_is_skipped() -> None:
    """A pointer read moments after its write is not yet evidence.

    Workers KV is eventually consistent, so a bulk read can answer with
    the value a just-completed publish replaced. The same window that
    keeps the loop off an enqueue in progress keeps it off a write the
    edge has not finished propagating.
    """
    edition = _edition(date_updated=NOW - timedelta(minutes=1))
    history = _history(
        edition=edition,
        status=PublishStatus.published,
        date_created=NOW - timedelta(minutes=1),
    )

    plan = _plan(
        [edition],
        history={(edition.edition_id, 5000): history},
        pointers=_pointers((edition, _pointer(prefix="proj/builds/old"))),
    )

    assert plan.republish == ()
    assert plan.grace_skipped == 1


def test_tombstoned_edition_with_a_pointer_is_unpublished() -> None:
    """A key that outlived its edition is deleted.

    The soft-delete path removes the pointer after the tombstone
    commits, so a pod dying between the two strands a key that keeps
    serving a deleted edition's content indefinitely. The unique index
    on ``(project, lower(slug))`` covers tombstones, so a stranded key
    can never belong to a live edition of the same name.
    """
    edition = _edition(date_deleted=SETTLED)

    plan = _plan([edition], pointers=_pointers((edition, _pointer())))

    assert plan.republish == ()
    assert len(plan.unpublish) == 1
    action = plan.unpublish[0]
    assert action.edition_id == edition.edition_id
    assert action.edition_slug == edition.edition_slug
    assert action.project_slug == edition.project_slug
    assert plan.tombstoned == 0


def test_deleted_project_edition_with_a_pointer_is_unpublished() -> None:
    """A project's tombstone deletes its editions' keys too.

    The cascade that tombstones a project's editions landed without a
    backfill, so a project soft-deleted before it still owns editions
    whose own ``date_deleted`` is NULL. Their keys are as stranded as
    any other tombstone's — the project they belong to is gone — so the
    planner takes the tombstone leg on the project's stamp as readily
    as on the edition's.
    """
    edition = _edition(project_deleted=SETTLED)

    plan = _plan([edition], pointers=_pointers((edition, _pointer())))

    assert plan.republish == ()
    assert len(plan.unpublish) == 1
    assert plan.unpublish[0].edition_id == edition.edition_id
    assert plan.tombstoned == 0


def test_deleted_project_edition_without_a_pointer_is_counted() -> None:
    """A live-looking edition under a dead project is never republished.

    Without the project's stamp this edition reads as drift of the
    plainest kind — no history row at all — and the republish it earns
    is unsatisfiable: ``publish_edition`` resolves the project by slug
    through a lookup that filters tombstones, so the job raises before
    it can mark the pair failed and the next tick re-drives the same
    pair forever.
    """
    edition = _edition(project_deleted=SETTLED)

    plan = _plan([edition], pointers=_pointers((edition, None)))

    assert plan.republish == ()
    assert plan.unpublish == ()
    assert plan.tombstoned == 1


def test_unpointed_edition_with_a_pointer_is_reported_only() -> None:
    """A key for an edition that never had a build is reported, not acted on.

    Nothing in the tree should be able to produce this, which is exactly
    why the loop refuses to guess: publishing needs a build it does not
    have, and deleting a key it cannot explain would destroy the one
    piece of evidence an operator has.
    """
    edition = _edition(build_id=None)

    plan = _plan([edition], pointers=_pointers((edition, _pointer())))

    assert plan.republish == ()
    assert plan.unpublish == ()
    assert plan.unexpected_pointers == 1
    assert plan.unpointed == 0


def test_unpublishes_count_against_the_same_cap() -> None:
    """One cap covers both action kinds, cut in edition-id order.

    The cap exists to bound a tick's blast radius, and an unpublish is
    as much of a CDN write as a republish, so counting them separately
    would let a drifted org do twice the work the operator asked for.
    """
    live = _edition(edition_id=1, slug="live", build_id=100)
    dead = _edition(
        edition_id=2, slug="dead", build_id=200, date_deleted=SETTLED
    )
    later = _edition(edition_id=3, slug="later", build_id=300)

    plan = _plan(
        [live, dead, later],
        pointers=_pointers(
            (live, None), (dead, _pointer()), (later, _pointer())
        ),
        limit=2,
    )

    assert [action.edition_id for action in plan.republish] == [1]
    assert [action.edition_id for action in plan.unpublish] == [2]
    assert plan.capped == 1


def test_plan_counts_the_pointers_the_edge_answered_with() -> None:
    """``pointers_read`` is how many keys the edge actually serves.

    ``editions_scanned`` already says how many keys the tick asked
    about, so the number worth carrying alongside ``cdn_checked`` is how
    many came back with something. Here two of the three editions are
    published at the edge and the third's key is gone, which is exactly
    the case where "we read the edge" and "the edge has these editions"
    have to be separable numbers.
    """
    served = _edition(edition_id=1, slug="main")
    also_served = _edition(edition_id=2, slug="v1")
    forgotten = _edition(edition_id=3, slug="v2")
    editions = [served, also_served, forgotten]
    history = {
        (edition.edition_id, 5000): _history(
            edition=edition, status=PublishStatus.published
        )
        for edition in editions
    }

    plan = _plan(
        editions,
        history=history,
        pointers=_pointers(
            (served, _pointer()),
            (also_served, _pointer()),
            (forgotten, None),
        ),
    )

    assert plan.editions_scanned == 3
    assert plan.cdn_checked is True
    assert plan.pointers_read == 2


def test_plan_reads_no_pointers_without_a_cdn() -> None:
    """An org whose edge was never read reports zero pointers read.

    The companion to ``cdn_checked is False``: a tick that looked at
    nothing must not report a pointer count that could be mistaken for
    an edge serving nothing.
    """
    edition = _edition()
    history = _history(edition=edition, status=PublishStatus.published)

    plan = _plan([edition], history={(edition.edition_id, 5000): history})

    assert plan.cdn_checked is False
    assert plan.pointers_read == 0
