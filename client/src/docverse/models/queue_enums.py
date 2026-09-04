"""Enumerations for queue job resources."""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "JobKind",
    "JobStatus",
    "PublishStatus",
    "RetiredBuildStatus",
]


class JobKind(StrEnum):
    """Kind of background job.

    - ``build_processing`` — unpack an uploaded build tarball, inventory
      its objects, and evaluate edition tracking.
    - ``edition_update`` — move an edition's build pointer.
    - ``publish_edition`` — publish an edition's current build to the
      CDN / object store serving path.
    - ``dashboard_build`` — regenerate one project's dashboard from
      database state.
    - ``dashboard_sync`` — re-fetch a dashboard template from its bound
      GitHub repository.
    - ``lifecycle_eval`` — evaluate an organization's lifecycle rules
      and soft-delete matching editions and builds.
    - ``git_ref_audit`` — audit tracked git refs against the source
      repository (feeds the ``ref_deleted`` lifecycle rule).
    - ``purgatory_cleanup`` — permanently delete soft-deleted builds
      whose purgatory retention has elapsed.
    - ``credential_reencrypt`` — re-encrypt stored credentials after a
      key rotation.
    - ``keeper_sync_run_discovery`` — fan out per-project sync jobs for
      a keeper-sync run.
    - ``keeper_sync_project`` — sync one LTD product into Docverse.
    """

    build_processing = "build_processing"
    edition_update = "edition_update"
    publish_edition = "publish_edition"
    dashboard_build = "dashboard_build"
    dashboard_sync = "dashboard_sync"
    lifecycle_eval = "lifecycle_eval"
    git_ref_audit = "git_ref_audit"
    purgatory_cleanup = "purgatory_cleanup"
    credential_reencrypt = "credential_reencrypt"
    keeper_sync_run_discovery = "keeper_sync_run_discovery"
    keeper_sync_project = "keeper_sync_project"


class JobStatus(StrEnum):
    """Status of a queue job.

    - ``queued`` — the job is enqueued and waiting for a worker.
    - ``in_progress`` — a worker is executing the job.
    - ``completed`` — the job finished successfully.
    - ``completed_with_errors`` — the job finished, but some of its
      work failed; see the job's ``errors`` field.
    - ``failed`` — the job failed; see the job's ``errors`` field.
    - ``cancelled`` — the job was cancelled before completing.
    """

    queued = "queued"
    in_progress = "in_progress"
    completed = "completed"
    completed_with_errors = "completed_with_errors"
    failed = "failed"
    cancelled = "cancelled"


class PublishStatus(StrEnum):
    """CDN publish state for an edition or edition build history entry.

    - ``pending`` — the edition points at a build that has not been
      published yet.
    - ``publishing`` — a ``publish_edition`` job is copying the build
      to the serving path.
    - ``published`` — the build is live at the edition's published URL.
    - ``failed`` — the most recent publish attempt failed.
    """

    pending = "pending"
    publishing = "publishing"
    published = "published"
    failed = "failed"


class RetiredBuildStatus(StrEnum):
    """State a build was found in when its worker stood down.

    Reported as ``BuildProcessingProgress.retired_status`` on a
    ``build_processing`` job that completed without publishing anything
    because somebody else retired the build underneath it — a delete, a
    lifecycle reap, the stranded-build sweep, a supersession, or a purge
    that removed the row outright. The value names *what the worker
    found*, so an operator does not have to guess which of the several
    possible retirements happened. The commonest are:

    - ``cancelled`` — the build was deleted (or lifecycle-reaped) and
      cancelled. Always accompanied by ``deleted_skipped``.
    - ``failed`` — the stranded-build sweep or the silent-job reaper
      failed it, usually because the upload ran past the reaper
      threshold.
    - ``superseded`` — a newer build for the same ``(project, git_ref)``
      took over.
    - ``missing`` — the row was gone by the time the worker looked.

    This is :class:`~docverse.models.BuildStatus` plus ``missing``, and
    the overlap is deliberate rather than duplication. Every
    ``BuildStatus`` is a member because the worker reports whatever
    status its re-read returned, and the re-read can return a *live*
    status: ``BuildStore.soft_delete`` stamps ``date_deleted`` without
    touching the status, so a deleted row can still read ``processing``
    or ``completed`` and is retired just as firmly. A narrower enum
    would reject a payload the worker really writes, turning a job
    listing into a validation error. ``missing`` is the member with no
    ``BuildStatus`` counterpart, and cannot become one: there is no row
    to write it to, and ``BuildStatus`` is persisted behind the
    ``builds_status_check`` constraint.
    """

    pending = "pending"
    uploaded = "uploaded"
    processing = "processing"
    completed = "completed"
    failed = "failed"
    superseded = "superseded"
    cancelled = "cancelled"
    missing = "missing"
