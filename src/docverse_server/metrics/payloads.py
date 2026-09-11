"""Sasquatch metrics event payloads for Docverse.

Every payload derives from :class:`DocverseEventBase`, which carries the
two dimensions every Docverse metric is sliced by — ``organization`` and
``project``. Payloads are deliberately **scalar-only** (the Avro/InfluxDB
backing store rejects nested structures; see
:meth:`safir.metrics.EventPayload.validate_structure`), and durations are
expressed as :class:`datetime.timedelta`.
"""

from __future__ import annotations

from datetime import timedelta

from safir.metrics import EventPayload

from .enums import (
    EditionPublishTrigger,
    LifecycleAction,
    LifecycleActionTrigger,
    LifecycleReapAction,
    MembershipChangeAction,
    MetricsEditionKind,
    MetricsOrgRole,
    MetricsPrincipalType,
)

__all__ = [
    "BuildProcessedEvent",
    "BuildUploadedEvent",
    "DashboardBuiltEvent",
    "DocverseEventBase",
    "EditionLifecycleEvent",
    "EditionPublishedEvent",
    "EditionReconcileCompletedEvent",
    "KeeperSyncRunCompletedEvent",
    "LifecycleActionEvent",
    "MembershipChangedEvent",
    "ProjectLifecycleEvent",
    "PurgatoryCleanupCompletedEvent",
    "ResourceInventoryEvent",
]


class DocverseEventBase(EventPayload):
    """Common dimensions shared by every Docverse metrics event.

    Every Docverse metric is analysed per organization, and almost all
    of them per project; carrying both on a shared base keeps the slice
    dimensions consistent across the catalog. ``project`` is ``None`` for
    org-scoped events (e.g. a future ``resource_inventory`` org row).
    """

    organization: str
    """Slug of the organization the event belongs to."""

    project: str | None
    """Slug of the project, or ``None`` for org-scoped events."""


class BuildUploadedEvent(DocverseEventBase):
    """A client signalled that a build's upload is complete.

    Emitted from the ``PATCH .../builds/{build}`` handler when the build
    transitions ``pending -> processing``. The provenance fields are
    copied from the build's annotations (``None`` where the uploader did
    not annotate them) and give SQuaRE adoption/source signal.
    """

    uploader: str
    """Principal (user or bot) that uploaded the build."""

    commit_sha: str | None
    """Git commit SHA the build was produced from, if annotated."""

    github_repository: str | None
    """``owner/repo`` that produced the build, if annotated."""

    github_run_id: str | None
    """GitHub Actions run ID, if annotated."""

    github_actor: str | None
    """GitHub user or app that triggered the run, if annotated."""

    ci_platform: str | None
    """CI platform that produced the build, if annotated."""


class BuildProcessedEvent(DocverseEventBase):
    """A build finished processing in the ``build_processing`` worker.

    Covers all three terminal outcomes: a successful unpack+upload, a
    failed one (``success=False``), and a stale build that was skipped
    because a newer build for the same ``(project, git_ref)`` superseded
    it (``stale_skipped=True``).
    """

    success: bool
    """Whether processing completed without error."""

    object_count: int | None
    """Number of objects uploaded, or ``None`` when nothing was uploaded."""

    total_size_bytes: int | None
    """Total uploaded size in bytes, or ``None`` when nothing was uploaded."""

    editions_updated: int
    """Number of editions repointed at this build."""

    editions_skipped: int
    """Number of tracking editions left unchanged."""

    stale_skipped: bool
    """Whether this build was skipped as superseded by a newer build."""

    elapsed: timedelta
    """Wall-clock time the worker spent on this build."""


class DashboardBuiltEvent(DocverseEventBase):
    """A dashboard finished building in the ``dashboard_build`` worker.

    Covers both terminal outcomes: a successful render+upload and a
    failed one (``success=False``). The object counters are ``None`` on
    the failure path, where nothing was uploaded.
    """

    success: bool
    """Whether the dashboard build completed without error."""

    object_count: int | None
    """Number of dashboard artifacts uploaded, or ``None`` on failure."""

    total_size_bytes: int | None
    """Total uploaded dashboard size in bytes, or ``None`` on failure."""

    elapsed: timedelta
    """Wall-clock time the worker spent on this dashboard build."""


class EditionPublishedEvent(DocverseEventBase):
    """An edition's current build finished publishing to the CDN.

    Emitted from the ``publish_edition`` worker's success terminal.
    """

    edition_kind: MetricsEditionKind
    """Kind of the published edition."""

    trigger: EditionPublishTrigger
    """What flow drove this publish (build fan-out vs. keeper-sync)."""

    elapsed: timedelta
    """Wall-clock time the worker spent on this publish."""


class EditionReconcileCompletedEvent(DocverseEventBase):
    """One organization's ``edition_reconcile`` tick finished.

    Emitted by the per-org loop once, after its ``queue_jobs`` row's
    terminal transition. Like the ``purgatory_cleanup`` sweep it keeps
    no run table, so this event is the durable record that the
    reconciler ran and what it found. The loop is dispatched per
    organization and one tick spans every project in it, so the event is
    org-scoped and ``project`` is always ``None``; a repair's
    project-scoped detail arrives as the ``edition_published`` event the
    re-driven publish itself emits, tagged ``trigger=reconcile``.

    Published for **every** tick, including the ones that found nothing.
    That is the point of it: a loop whose value is the claim "the edge
    still agrees with the database" has to say so on the ticks where
    nothing happened, or a silent reconciler and a dead one look the
    same on a dashboard.

    The counters are per-tick deltas over the editions this org owns.
    ``republished`` next to ``edition_published``'s ``reconcile``
    trigger is how much of an environment's publish traffic is the
    system healing itself; ``capped`` above zero on consecutive ticks is
    an organization drifting faster than one tick's cap can repair.
    """

    editions_scanned: int
    """Editions the tick considered, every bucket included."""

    pointers_read: int
    """Keys the org's edge answered the read-back with.

    Zero whenever ``cdn_checked`` is ``False``, which is the pair of
    fields that keeps "no edge to read" apart from "an edge serving
    nothing" — the second being a whole organization's worth of drift.
    """

    republished: int
    """Publishes the tick put back on the queue."""

    unpublished: int
    """Stranded CDN keys the tick deleted."""

    in_flight_skipped: int
    """Pairs a live ``publish_edition`` job still held.

    Both halves of that gate: the pairs the plan's snapshot already saw
    a job for, and the pairs that acquired one before the enqueue ran.
    """

    superseded_skipped: int
    """Planned republishes the edition had moved off before the enqueue.

    Repoints that landed after the plan was made. A steady trickle is an
    organization whose editions move faster than one tick takes to walk
    them — a reason to lower the per-job cap, not a failure.
    """

    failed_left_alone: int
    """Pairs reading ``failed``; reported for operators, never re-driven."""

    unexpected_pointers: int
    """Keys for editions with no build to publish; reported, not acted on."""

    capped: int
    """Actions the per-job cap left for the next tick."""

    cdn_checked: bool
    """Whether the tick read the organization's edge back at all."""

    elapsed: timedelta
    """Wall-clock time the loop spent on this organization."""


class ProjectLifecycleEvent(DocverseEventBase):
    """A project was created, updated, or deleted via the projects handler.

    Consolidates the project management verbs into one event keyed by
    ``action`` (SQR-112 D4). Published from the FastAPI projects handler
    after the operation's final commit.
    """

    action: LifecycleAction
    """Which management operation occurred (create/update/delete)."""


class EditionLifecycleEvent(DocverseEventBase):
    """An edition was created, updated, deleted, or rolled back.

    Consolidates the edition management verbs into one event keyed by
    ``action`` (SQR-112 D4). Published from the FastAPI editions handler
    after the operation's final commit.
    """

    action: LifecycleAction
    """Which management operation occurred (create/update/delete/rollback)."""

    edition_kind: MetricsEditionKind
    """Kind of the edition the operation acted on."""


class KeeperSyncRunCompletedEvent(DocverseEventBase):
    """An LTD-keeper backfill run reached a terminal status.

    Emitted when
    :func:`docverse_server.services.keeper_sync_finalisation.maybe_finalise_run`
    actually rolls a ``keeper_sync_runs`` row to a terminal status (from
    any of the worker paths that finalise a run: ``keeper_sync_project``,
    ``publish_edition``, or the keeper-sync reaper). A keeper-sync run
    spans many projects, so it is org-scoped and ``project`` is always
    ``None``. ``success`` is ``True`` only when every attributed child
    job completed cleanly (run status ``succeeded``); a run with any
    failed child finalises ``partial_failure`` and reports
    ``success=False``.
    """

    success: bool
    """Whether the run finalised with no failed child jobs."""

    total_count: int
    """Total number of queue jobs attributed to the run."""

    succeeded_count: int
    """Number of attributed jobs that completed cleanly."""

    failed_count: int
    """Number of attributed jobs that failed (or soft-failed)."""

    elapsed: timedelta
    """Wall-clock time from the run starting to its terminal transition."""


class LifecycleActionEvent(DocverseEventBase):
    """A maintenance worker retired a resource.

    Emitted once per reap by the ``lifecycle_eval``, ``git_ref_audit``
    and ``purgatory_cleanup`` workers (SQR-112 D7). ``action`` records
    what drove the reap and ``trigger`` records which worker performed
    it; both are dedicated metrics enums selected at the emission site.

    The first two workers soft-delete rows; ``purgatory_cleanup`` reaps
    the other end of the same lifecycle, permanently reclaiming a
    long-deleted build's object-store content. One event type spans both
    so a consumer can follow a resource from the rule that retired it
    through to the sweep that freed its bytes, rather than joining two
    schemas to answer that.

    The event is project-scoped — every reaped row belongs to a known
    project, so ``project`` is always set, including when that project
    has itself been soft-deleted (the cascade is exactly how a deleted
    project's builds reach the sweep). ``success`` is ``True`` because a
    reap is published only once its commit is durable: the soft-delete
    transaction for the reapers, the ``date_purged`` stamp for the
    sweep. It is carried for schema uniformity with the other flow
    events (SQR-112 D3) and leaves room for a future soft-failure reap
    path.
    """

    action: LifecycleReapAction
    """What drove the reap: a lifecycle rule, or retention elapsing."""

    trigger: LifecycleActionTrigger
    """Which worker performed the reap."""

    success: bool
    """Whether the reap committed successfully (always ``True`` today)."""


class PurgatoryCleanupCompletedEvent(DocverseEventBase):
    """One organization's ``purgatory_cleanup`` tick finished.

    Emitted by the per-org sweep once, after its final commit. The job
    keeps no run table — the only other trace a tick leaves is its
    ``queue_jobs`` row, which retention will eventually take — so this
    event is the durable record that the sweep ran and what it took
    back. Retention is an organization setting and one tick spans every
    project in the org, so the event is org-scoped and ``project`` is
    always ``None``; the per-build detail arrives as one
    ``lifecycle_action`` per purged build, which is project-scoped.

    The counters are per-tick deltas, not gauges: summing
    ``bytes_reclaimed`` over a window is how much storage the sweep gave
    back in that window. The standing footprint it has yet to reclaim is
    the ``resource_inventory`` gauge's ``purgatory_bytes``, and the two
    are meant to be read against each other.

    A tick that could not run at all — the organization vanished, no
    staging store is configured, its credential will not decrypt —
    publishes nothing and fails its queue row instead. Reporting it here
    would put a tick that reclaimed nothing *because it never started*
    next to one that reclaimed nothing because there was nothing to do.
    """

    success: bool
    """Whether every build the tick attempted was reclaimed."""

    builds_purged: int
    """Builds whose content went and whose row the tick stamped."""

    builds_failed: int
    """Builds the tick attempted and could not complete."""

    builds_skipped_referenced: int
    """Builds held back because a live edition still serves them."""

    objects_deleted: int
    """Objects removed from under the purged builds' prefixes."""

    bytes_reclaimed: int
    """Summed ``total_size_bytes`` of the builds this tick purged."""

    capped: bool
    """Whether the per-job cap, not the backlog, ended the work list."""

    elapsed: timedelta
    """Wall-clock time the sweep spent on this organization."""


class ResourceInventoryEvent(DocverseEventBase):
    """A daily census snapshot of active resources (SQR-112 D8).

    Emitted by the ``inventory_census`` worker once per org (org-scoped,
    ``project=None``, ``project_count`` set) and once per non-deleted
    project (project-scoped, ``project`` set, ``project_count=None``).
    Every field is a self-contained absolute-count gauge queried
    downstream with ``last()``: soft-deleted projects/editions/builds are
    excluded and ``total_build_bytes`` is the summed footprint of exactly
    the active builds counted by ``build_count``.

    The two ``purgatory_*`` gauges are the exception to that exclusion,
    and they count the complement rather than a subset: a build is in
    ``build_count``/``total_build_bytes`` while it is live and in
    ``purgatory_build_count``/``purgatory_bytes`` once it is soft-deleted
    but not yet purged, never in both. They make the reap-pending
    footprint visible — storage the ``purgatory_cleanup`` sweep still
    owes back to the bucket — and fall to zero as that sweep runs.
    """

    project_count: int | None
    """Active projects in the org; ``None`` on a project-scoped row."""

    edition_count: int
    """Active editions in scope (org-wide on an org row, else the project)."""

    build_count: int
    """Active builds in scope (org-wide on an org row, else the project)."""

    total_build_bytes: int
    """Summed ``total_size_bytes`` of the active builds in scope."""

    purgatory_build_count: int
    """Soft-deleted, not-yet-purged builds in scope."""

    purgatory_bytes: int
    """Summed ``total_size_bytes`` of the purgatory builds in scope."""


class MembershipChangedEvent(DocverseEventBase):
    """An organization member was added or removed.

    Emitted from the members handler: ``post_member`` (``action=add``)
    and ``delete_member`` (``action=remove``), each published after the
    operation's commit. Membership is org-scoped, so ``project`` is
    always ``None``; the principal's role and identity are carried as
    dedicated metrics enums mapped from the API at the emission site.
    """

    action: MembershipChangeAction
    """Whether the member was added or removed."""

    role: MetricsOrgRole
    """Role the membership grants (or granted, for a removal)."""

    principal_type: MetricsPrincipalType
    """Whether the principal is a user or a group."""

    principal: str
    """Username or group name the membership applies to."""
