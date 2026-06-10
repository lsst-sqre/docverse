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
    "KeeperSyncRunCompletedEvent",
    "MembershipChangedEvent",
    "ProjectLifecycleEvent",
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
    :func:`docverse.services.keeper_sync_finalisation.maybe_finalise_run`
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
