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
    "KeeperSyncRun",
    "KeeperSyncRunCreated",
    "KeeperSyncRunKind",
    "KeeperSyncRunStatus",
    "KeeperSyncTierCohort",
    "KeeperSyncTierName",
    "KeeperSyncTierStatus",
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

    self_url: str = Field(description="URL to this run resource.")

    id: int = Field(description="Numeric identifier for the run.")

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

    queue_job_id: str = Field(
        description=(
            "Public Base32 identifier for the enqueued"
            " ``keeper_sync_run_discovery`` queue job."
        )
    )

    queue_job_url: str = Field(
        description="URL of the enqueued discovery queue job resource."
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
            "Wall-clock time at which the planner will next greenlight"
            " a poll. ``null`` when the next cron tick will poll"
            " unconditionally (hot, unseen, or dormant without a"
            " last-polled annotation). Jitter-aware: dormant projects'"
            " next-due timestamps are spread across the dormant"
            " interval by stable_hash_fraction(slug)."
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

    editions: list[KeeperSyncEditionStatus] = Field(
        default_factory=list,
        description=(
            "Docverse-side editions of the project, left-joined with"
            " ``keeper_sync_state`` rows on ``docverse_id``. Empty"
            " when ``project_state`` is ``null``."
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

    queue_job_id: str = Field(
        description=(
            "Public Base32 identifier for the enqueued"
            " ``keeper_sync_project`` queue job."
        )
    )

    queue_job_url: str = Field(
        description="URL of the enqueued queue job resource."
    )
