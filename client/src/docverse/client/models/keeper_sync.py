"""Pydantic models for LTD Keeper sync configuration and runs."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from .editions import EditionKind

__all__ = [
    "KeeperSyncConfig",
    "KeeperSyncEditionDiff",
    "KeeperSyncEditionStatus",
    "KeeperSyncProjectRefreshAccepted",
    "KeeperSyncProjectStateSummary",
    "KeeperSyncProjectStatus",
    "KeeperSyncResourceType",
    "KeeperSyncRun",
    "KeeperSyncRunCreated",
    "KeeperSyncRunKind",
    "KeeperSyncRunStatus",
    "KeeperSyncTierCohort",
    "KeeperSyncTierName",
    "KeeperSyncTierStatus",
    "KeeperSyncTombstone",
    "KeeperSyncTombstoneReason",
]


_DEFAULT_LTD_BASE_URL = "https://keeper.lsst.codes"


class KeeperSyncConfig(BaseModel):
    """LTD Keeper sync configuration for an organization.

    Stored as a JSONB blob on the ``organizations`` row and validated
    through this model on read and write. ``GET /orgs/{org}/keeper-sync``
    returns a default-disabled instance when no config has been
    persisted; ``PUT`` replaces the stored config wholesale.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False,
        description="Whether LTD Keeper sync is enabled on the organization.",
    )

    ltd_base_url: HttpUrl = Field(
        default=HttpUrl(_DEFAULT_LTD_BASE_URL),
        description="Base URL of the LTD Keeper API (v1 shape).",
    )

    project_slugs: list[str] | Literal["*"] = Field(
        default_factory=list,
        description=(
            'LTD project slugs to sync, or ``"*"`` for every project'
            " visible on the LTD instance."
        ),
    )


class KeeperSyncRunKind(StrEnum):
    """Kind of LTD Keeper sync run."""

    backfill = "backfill"
    resync = "resync"
    reconcile = "reconcile"


class KeeperSyncRunStatus(StrEnum):
    """Lifecycle status of a keeper sync run.

    ``pending`` — the discovery job has been enqueued but has not yet
    fanned out any children. ``in_progress`` — discovery has enqueued
    at least one child sync job. ``succeeded`` / ``partial_failure`` /
    ``failed`` are terminal states.
    """

    pending = "pending"
    in_progress = "in_progress"
    succeeded = "succeeded"
    partial_failure = "partial_failure"
    failed = "failed"


class KeeperSyncRun(BaseModel):
    """Response model for a keeper sync run resource."""

    model_config = ConfigDict(from_attributes=True)

    self_url: HttpUrl = Field(description="URL to this run resource.")

    jobs_url: HttpUrl = Field(
        description=(
            "URL to ``GET`` for the run's child queue-job listing"
            " (``get_org_keeper_sync_run_jobs``). Always present so"
            " clients can paginate the run's children without"
            " constructing the URL by hand."
        )
    )

    id: str = Field(
        description="Public Crockford Base32 identifier for the run."
    )

    kind: KeeperSyncRunKind = Field(description="Kind of run.")

    status: KeeperSyncRunStatus = Field(description="Lifecycle status.")

    pending_count: int = Field(
        description=(
            "Number of fanned-out child queue jobs still in a non-terminal"
            " state (queued or in_progress)."
        )
    )

    succeeded_count: int = Field(
        description="Number of fanned-out child queue jobs that succeeded."
    )

    failed_count: int = Field(
        description=(
            "Number of fanned-out child queue jobs that ended in a failure"
            " state (failed or cancelled)."
        )
    )

    total_count: int = Field(
        description=(
            "Total number of fanned-out child queue jobs attributed to"
            " this run."
        )
    )

    date_started: datetime = Field(
        description="Timestamp when the run row was created."
    )

    date_finished: datetime | None = Field(
        default=None,
        description="Timestamp when the run reached a terminal status.",
    )

    date_last_activity: datetime | None = Field(
        default=None,
        description=(
            "Most-recent state-transition timestamp across the run's"
            " attributed child queue jobs. Operators can poll this to"
            " detect a stuck run without paginating its children."
            " ``null`` while the run has no attributed queue jobs yet."
        ),
    )


class KeeperSyncRunCreated(BaseModel):
    """Response body returned by ``POST /orgs/{org}/keeper-sync/runs``."""

    model_config = ConfigDict(from_attributes=True)

    run: KeeperSyncRun = Field(description="The newly created run.")

    job_id: str = Field(
        description=(
            "Public Base32 identifier for the enqueued"
            " ``keeper_sync_run_discovery`` queue job."
        )
    )

    job_url: HttpUrl = Field(
        description="URL of the enqueued discovery job resource."
    )


KeeperSyncTierName = Literal["main", "discovery", "other"]
"""Tier-cron identifier surfaced in the project-status response.

Mirrors :class:`docverse.services.keeper_sync.scheduler.Tier` so the
public schema does not depend on the server-side enum's import path.
"""


KeeperSyncTierCohort = Literal["hot", "dormant", "unseen"]
"""Tier-cohort label surfaced in the project-status response.

``hot`` — the project is polled on every tick of the tier's cadence.
``dormant`` — the project is rate-limited to one poll per tier dormant
interval. ``unseen`` — the tier has never observed this project.
"""


class KeeperSyncTierStatus(BaseModel):
    """Per-tier cohort + jitter-aware schedule for a project.

    Surfaced once per tier (``main`` / ``discovery`` / ``other``) in
    the project-status response. The values come from the same pure
    planner the tier-cron worker functions consult, so an operator can
    read off the same decision the worker would make.
    """

    model_config = ConfigDict(from_attributes=True)

    tier: KeeperSyncTierName = Field(description="Tier-cron identifier.")

    cohort: KeeperSyncTierCohort = Field(
        description=(
            "Cohort label: ``hot`` polls every tick, ``dormant`` is"
            " rate-limited, ``unseen`` has no observation yet."
        )
    )

    date_last_polled: datetime | None = Field(
        default=None,
        description=(
            "Wall-clock time of the most recent poll for this tier, as"
            " recorded in the per-tier ``date_<tier>_last_polled``"
            " annotation. ``null`` when the project has never been"
            " polled by this tier or the annotation is missing /"
            " malformed."
        ),
    )

    date_next_due: datetime | None = Field(
        default=None,
        description=(
            "Earliest wall-clock time at which the next *poll* (not"
            " necessarily an enqueue) for this tier may run. For hot,"
            " unseen, and dormant-without-last-polled cohorts this is"
            " the next tier-cron tick — there is no per-project"
            " calendar gate. For dormant cohorts with a recorded"
            " last-polled annotation this is ``last_polled +"
            " dormant_interval`` (jitter-aware: dormant projects'"
            " next-due timestamps are spread across the dormant"
            " interval by stable_hash_fraction(slug)). Change-"
            " detection and per-subject mutex gates downstream of the"
            " planner may still suppress the actual enqueue."
        ),
    )


class KeeperSyncProjectStateSummary(BaseModel):
    """Operator-readable subset of a project-resource ``keeper_sync_state``.

    Only fields useful for diagnostics are exposed; internal columns
    like ``last_seen_etag`` are omitted to keep the schema minimal.
    """

    model_config = ConfigDict(from_attributes=True)

    ltd_slug: str = Field(description="LTD product slug for this project.")

    date_last_synced: datetime | None = Field(
        default=None,
        description="Most recent successful sync timestamp.",
    )

    date_rebuilt_seen: datetime | None = Field(
        default=None,
        description=(
            "Most recent ``date_rebuilt`` observed on the LTD ``main``"
            " edition. Used by the dormancy gate."
        ),
    )

    annotations: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Raw ``annotations`` JSONB from the state row. Includes"
            " per-tier last-polled timestamps and the cached"
            " ``main_edition_*`` pointer."
        ),
    )


class KeeperSyncEditionStatus(BaseModel):
    """One Docverse-side edition with its keeper-sync attribution.

    Reflects a left-join from the Docverse ``editions`` table to
    ``keeper_sync_state``: every Docverse edition appears, but the LTD
    columns (``ltd_id`` / ``ltd_slug`` / ``date_last_synced``) are
    populated only when keeper-sync has imported the edition.
    """

    model_config = ConfigDict(from_attributes=True)

    edition_url: HttpUrl = Field(
        description=(
            "Canonical ``GET /orgs/{org}/projects/{project}/editions/"
            "{edition}`` URL for this edition."
        )
    )

    slug: str = Field(description="Docverse edition slug.")

    kind: EditionKind = Field(
        description="Docverse edition kind (``main``, ``draft``, ...)."
    )

    ltd_id: int | None = Field(
        default=None,
        description=(
            "LTD edition id from the linked ``keeper_sync_state`` row,"
            " or ``null`` when no row links this edition."
        ),
    )

    ltd_slug: str | None = Field(
        default=None,
        description=(
            "LTD edition slug from the linked state row, or ``null``"
            " when no row links this edition."
        ),
    )

    date_last_synced: datetime | None = Field(
        default=None,
        description=(
            "Most recent successful sync timestamp for this edition,"
            " or ``null`` if not yet synced."
        ),
    )


class KeeperSyncEditionDiff(BaseModel):
    """LTD vs Docverse edition reconciliation diff.

    Populated only when the project-status endpoint is called with
    ``?ltd=true``; otherwise omitted from the response. ``missing_in_
    docverse`` lists LTD edition slugs visible to the live LTD API but
    not represented by any ``keeper_sync_state`` row in this org;
    ``missing_in_ltd`` lists keeper-sync-tracked LTD edition slugs that
    the live LTD API no longer returns (candidates for soft-deletion).
    """

    model_config = ConfigDict(from_attributes=True)

    missing_in_docverse: list[str] = Field(
        default_factory=list,
        description=(
            "LTD edition slugs visible to LTD but not represented by a"
            " keeper-sync state row in this org."
        ),
    )

    missing_in_ltd: list[str] = Field(
        default_factory=list,
        description=(
            "LTD edition slugs tracked by keeper-sync state rows but no"
            " longer returned by the live LTD edition listing."
        ),
    )


class KeeperSyncProjectStatus(BaseModel):
    """Operator-readable summary of one project's keeper-sync state.

    Returned by ``GET /orgs/{org}/keeper-sync/projects/{ltd_slug}``.
    Combines the project-resource state row, per-tier cohort
    explanations, and a Docverse-side edition listing left-joined with
    keeper-sync state. When the request includes ``?ltd=true`` the
    response also carries an ``edition_diff`` with a live-LTD
    reconciliation result.
    """

    model_config = ConfigDict(from_attributes=True)

    self_url: HttpUrl = Field(
        description=(
            "Canonical ``GET /orgs/{org}/keeper-sync/projects/{ltd_slug}``"
            " URL for this project's keeper-sync status. Lets clients"
            " paginating the org-wide project listing drill into a"
            " single project without constructing the URL by hand."
        )
    )

    org_url: HttpUrl = Field(
        description=(
            "Canonical ``GET /orgs/{org}`` URL for the Docverse"
            " organization this report is scoped to."
        )
    )

    project_url: HttpUrl | None = Field(
        default=None,
        description=(
            "Canonical ``GET /orgs/{org}/projects/{project}`` URL for"
            " the Docverse project, or ``null`` when no Docverse"
            " project has been imported yet for this LTD slug."
        ),
    )

    sync_refresh_url: HttpUrl = Field(
        description=(
            "URL to ``POST`` for an immediate one-shot sync of this"
            " project (``post_org_keeper_sync_project_refresh``). Always"
            " present so operators can trigger a refresh from the"
            " status response without constructing the URL by hand."
        )
    )

    editions_sync_url: HttpUrl = Field(
        description=(
            "URL to ``GET`` for the paginated keeper-sync editions"
            " collection for this project"
            " (``get_org_keeper_sync_project_editions``). Always"
            " present so operators can scan the full edition list"
            " without constructing the URL by hand."
        )
    )

    ltd_slug: str = Field(
        description="LTD product slug the report is scoped to."
    )

    project_state: KeeperSyncProjectStateSummary | None = Field(
        default=None,
        description=(
            "Project-resource ``keeper_sync_state`` row, or ``null``"
            " when no row exists yet (never-seen project)."
        ),
    )

    tier_status: list[KeeperSyncTierStatus] = Field(
        description=(
            "One entry per tier-cron in fixed order:"
            " ``main``, ``discovery``, ``other``."
        )
    )

    main_edition: KeeperSyncEditionStatus | None = Field(
        default=None,
        description=(
            "Embedded summary of the project's ``__main`` edition"
            " (``kind=main``), left-joined with its keeper-sync state"
            " row. ``null`` when no Docverse project exists yet for"
            " this LTD slug, or when the project has not yet been"
            " auto-created with a ``__main`` edition. The full"
            " edition list is paginated via ``editions_sync_url``."
        ),
    )

    edition_diff: KeeperSyncEditionDiff | None = Field(
        default=None,
        description=(
            "Live-LTD reconciliation diff. Present only when the"
            " request was made with ``?ltd=true``; otherwise omitted."
        ),
    )


class KeeperSyncProjectRefreshAccepted(BaseModel):
    """Response body returned by the per-project refresh endpoint.

    Returned by
    ``POST /orgs/{org}/keeper-sync/projects/{ltd_slug}/refresh``. The
    refresh is a tier-cron-equivalent one-shot trigger — no run row is
    created, only a ``keeper_sync_project`` queue job is enqueued, so
    the envelope is a thin wrapper around the queue-job link.
    """

    model_config = ConfigDict(from_attributes=True)

    job_id: str = Field(
        description=(
            "Public Base32 identifier for the enqueued"
            " ``keeper_sync_project`` queue job."
        )
    )

    job_url: HttpUrl = Field(description="URL of the enqueued job resource.")


class KeeperSyncResourceType(StrEnum):
    """LTD resource types tracked by keeper-sync.

    Mirrors :class:`docverse.storage.keeper_sync.ResourceType` as a
    public wire value so the admin tombstone API can filter requests
    and shape responses without leaking the server-side enum's import
    path. Builds are not tombstoned today, but the value is kept so
    operator-facing filters do not silently reject a valid resource
    type the schema otherwise carries.
    """

    project = "project"
    edition = "edition"
    build = "build"


class KeeperSyncTombstoneReason(StrEnum):
    """Why a ``keeper_sync_state`` row was tombstoned.

    Wire-stable enum surfaced by the admin tombstone API. Mirrors
    :class:`docverse.storage.keeper_sync.TombstoneReason`.
    """

    manual_delete = "manual_delete"
    """Operator-driven soft-delete of the Docverse-side resource."""

    lifecycle_delete = "lifecycle_delete"
    """Automated soft-delete by ``lifecycle_eval`` / ``git_ref_audit`` /
    the ``ref_deleted`` webhook."""

    lifecycle_preemptive = "lifecycle_preemptive"
    """Sync itself short-circuited an LTD edition that the lifecycle
    rules would immediately delete, before the build content was
    copied. No matching Docverse row exists in this case."""


class KeeperSyncTombstone(BaseModel):
    """One tombstoned ``keeper_sync_state`` row.

    Returned by ``GET /orgs/{org}/keeper-sync/tombstones`` and
    ``DELETE /orgs/{org}/keeper-sync/tombstones/{tombstone}`` (the
    DELETE returns 204 with no body, so this model only appears on
    list responses today; keeping it as a sibling of the list-entry
    shape lets a future "get one tombstone" endpoint reuse it).
    """

    model_config = ConfigDict(from_attributes=True)

    self_url: HttpUrl = Field(
        description=(
            "Canonical URL of the DELETE endpoint that would clear this"
            " tombstone (``delete_org_keeper_sync_tombstone``). The"
            " endpoint accepts only ``DELETE``; GET-on-self is not"
            " modelled because the list response already carries every"
            " field a single-tombstone fetch would return."
        )
    )

    id: str = Field(
        description=(
            "Public Crockford Base32 identifier for the tombstoned"
            " ``keeper_sync_state`` row. Use this id with the DELETE"
            " endpoint to clear the tombstone."
        )
    )

    resource_type: KeeperSyncResourceType = Field(
        description=(
            "LTD resource type the tombstone applies to. ``project``"
            " vetoes re-import of the LTD product entirely; ``edition``"
            " vetoes one specific LTD edition; ``build`` is reserved"
            " (no caller writes build-level tombstones today)."
        )
    )

    ltd_slug: str = Field(
        description=(
            "LTD-side slug for the tombstoned resource. For ``project``"
            " rows this is the LTD product slug; for ``edition`` rows"
            " it is the LTD edition slug (e.g. ``main`` or ``v1.0``)."
        )
    )

    ltd_id: int | None = Field(
        default=None,
        description=(
            "LTD-side numeric id for the tombstoned resource."
            " Populated for ``edition`` and ``build`` rows; ``null``"
            " for ``project`` rows (LTD products are slug-only)."
        ),
    )

    docverse_id: int | None = Field(
        default=None,
        description=(
            "Docverse-side row id the tombstone is paired to."
            " ``null`` for ``lifecycle_preemptive`` rows that veto an"
            " LTD edition that was never imported (so no Docverse row"
            " exists)."
        ),
    )

    date_tombstoned: datetime = Field(
        description="Wall-clock time the tombstone was recorded."
    )

    tombstone_reason: KeeperSyncTombstoneReason = Field(
        description="Why this row was tombstoned."
    )

    tombstone_note: str | None = Field(
        default=None,
        description=(
            "Optional operator-facing note the writer attached to this"
            " tombstone. ``null`` for automated writes (lifecycle and"
            " preemptive paths attach no note today)."
        ),
    )

    display_path: str = Field(
        description=(
            "Docverse-side display path for the tombstoned resource."
            " For ``project`` rows: the Docverse project slug. For"
            " ``edition`` rows: ``<project_slug>/<edition_slug>``."
            " Falls back to the LTD slug when no Docverse row is"
            " linked (``lifecycle_preemptive`` rows) or the linked"
            " row has been hard-deleted out from under the tombstone."
        )
    )
