"""Pydantic models for LTD Keeper sync configuration and runs."""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    ValidationInfo,
    field_validator,
)

from ._examples import (
    EXAMPLE_EDITION_URL,
    EXAMPLE_JOB_ID,
    EXAMPLE_JOB_URL,
    EXAMPLE_ORG_URL,
    EXAMPLE_PROJECT_URL,
    EXAMPLE_RUN_ID,
    EXAMPLE_TOMBSTONE_ID,
)
from .editions import EditionKind

__all__ = [
    "KeeperSyncConfig",
    "KeeperSyncConfigUpdate",
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
    "KeeperSyncScopePreview",
    "KeeperSyncTierCohort",
    "KeeperSyncTierName",
    "KeeperSyncTierStatus",
    "KeeperSyncTombstone",
    "KeeperSyncTombstoneReason",
]


_DEFAULT_LTD_BASE_URL = "https://keeper.lsst.codes"

_MAX_SLUG_PATTERNS = 100
"""Maximum number of entries accepted in one slug-pattern field."""

_MAX_SLUG_PATTERN_LENGTH = 256
"""Maximum length, in characters, of one slug pattern."""

_PATTERN_ECHO_LENGTH = 64
"""How much of an over-long pattern is echoed back in its error."""


def _validate_slug_patterns(
    patterns: list[str], field_name: str | None
) -> list[str]:
    """Validate one slug-pattern field's entries.

    Patterns are org-admin-supplied and are matched against short LTD
    slugs, so the count and length caps — rather than a match timeout or
    an alternative regex engine — are what bound the cost of a
    pathological pattern (PRD #667 §Out of scope).

    Parameters
    ----------
    patterns
        The field's candidate patterns.
    field_name
        Name of the field being validated, echoed in error messages so
        a 422 points at the offending field.

    Returns
    -------
    list of str
        The patterns, unchanged.

    Raises
    ------
    ValueError
        If there are too many patterns, if one is too long, or if one
        does not compile as a Python regular expression.
    """
    field = field_name or "slug patterns"
    if len(patterns) > _MAX_SLUG_PATTERNS:
        msg = (
            f"{field} accepts at most {_MAX_SLUG_PATTERNS} patterns;"
            f" got {len(patterns)}"
        )
        raise ValueError(msg)
    for pattern in patterns:
        if len(pattern) > _MAX_SLUG_PATTERN_LENGTH:
            echo = pattern[:_PATTERN_ECHO_LENGTH]
            msg = (
                f"{field} pattern {echo!r}... is"
                f" {len(pattern)} characters long; the limit is"
                f" {_MAX_SLUG_PATTERN_LENGTH}"
            )
            raise ValueError(msg)
        try:
            re.compile(pattern)
        except re.error as exc:
            msg = (
                f"{field} pattern {pattern!r} is not a valid Python"
                f" regular expression: {exc}"
            )
            raise ValueError(msg) from exc
    return patterns


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
        examples=[["sqr-112", "dmtn-001"]],
    )

    project_slug_patterns: list[str] = Field(
        default_factory=list,
        description=(
            "Python regular expressions that *add* LTD project slugs to"
            " the sync scope, on top of ``project_slugs``. Matched with"
            " :func:`re.fullmatch` and case-sensitively, so ``sqr-1``"
            " matches only ``sqr-1`` — not ``sqr-10`` or ``sqr-100``."
            f" At most {_MAX_SLUG_PATTERNS} entries, each at most"
            f" {_MAX_SLUG_PATTERN_LENGTH} characters."
        ),
        examples=[[r"sqr-\d+", r"dmtn-\d+"]],
    )

    exclude_project_slugs: list[str] = Field(
        default_factory=list,
        description=(
            "LTD project slugs removed from the sync scope. Excludes"
            " always win: a slug listed here is out of scope even when"
            ' ``project_slugs`` is ``"*"`` or an include pattern'
            " matches it."
        ),
        examples=[["www"]],
    )

    exclude_project_slug_patterns: list[str] = Field(
        default_factory=list,
        description=(
            "Python regular expressions that *remove* LTD project slugs"
            " from the sync scope. Matched with :func:`re.fullmatch` and"
            " case-sensitively. Excludes always win over both"
            " ``project_slugs`` and ``project_slug_patterns``."
            f" At most {_MAX_SLUG_PATTERNS} entries, each at most"
            f" {_MAX_SLUG_PATTERN_LENGTH} characters."
        ),
        examples=[[r"test-.*"]],
    )

    @field_validator("project_slug_patterns", "exclude_project_slug_patterns")
    @classmethod
    def _check_patterns(
        cls, value: list[str], info: ValidationInfo
    ) -> list[str]:
        """Reject pattern lists that are too long or do not compile."""
        return _validate_slug_patterns(value, info.field_name)

    def is_in_scope(self, slug: str) -> bool:
        """Report whether one LTD project slug is in the sync scope.

        See :meth:`filter_in_scope` for the rule.
        """
        return bool(self.filter_in_scope([slug]))

    def filter_in_scope(self, slugs: Iterable[str]) -> list[str]:
        """Filter LTD project slugs down to the ones in the sync scope.

        A slug is *included* when ``project_slugs`` is ``"*"``, when it
        is listed in ``project_slugs``, or when it fully matches one of
        ``project_slug_patterns``. It is *in scope* when it is included
        and is neither listed in ``exclude_project_slugs`` nor fully
        matched by one of ``exclude_project_slug_patterns`` — excludes
        always win.

        Pattern matching uses :func:`re.fullmatch` and is
        case-sensitive.

        Parameters
        ----------
        slugs
            LTD project slugs to filter, typically in LTD listing order.

        Returns
        -------
        list of str
            The in-scope slugs, in the order they were given, so
            successive passes over the same LTD listing fan out
            deterministically.
        """
        configured = self.project_slugs
        wildcard = configured == "*"
        listed = set() if isinstance(configured, str) else set(configured)
        excluded = set(self.exclude_project_slugs)
        include_patterns = [
            re.compile(pattern) for pattern in self.project_slug_patterns
        ]
        exclude_patterns = [
            re.compile(pattern)
            for pattern in self.exclude_project_slug_patterns
        ]
        return [
            slug
            for slug in slugs
            if (
                (
                    wildcard
                    or slug in listed
                    or any(p.fullmatch(slug) for p in include_patterns)
                )
                and slug not in excluded
                and not any(p.fullmatch(slug) for p in exclude_patterns)
            )
        ]


class KeeperSyncConfigUpdate(BaseModel):
    """Partial update for an organization's LTD Keeper sync configuration.

    Request model for ``PATCH /orgs/{org}/keeper-sync``, applied with
    JSON-Merge-Patch semantics: every field is optional, and only the fields
    present in the request body are changed — omitted fields are left
    untouched. ``project_slugs``, when provided, **replaces the stored array
    wholesale** (there is no append semantics; send the full desired list, or
    ``"*"`` for every project). ``extra="forbid"`` rejects unknown fields, and
    ``model_dump(exclude_unset=True)`` is what distinguishes "omitted" from an
    explicit value. Use ``PUT`` for a full replacement of the config.

    An explicit JSON ``null`` for any field is rejected with a 422: these
    config fields are non-nullable, so RFC 7386's null-as-remove semantics
    have no meaning here. Omit a field to leave it unchanged.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool | None = Field(
        default=None,
        description="Whether LTD Keeper sync is enabled on the organization.",
    )

    ltd_base_url: HttpUrl | None = Field(
        default=None,
        description="Base URL of the LTD Keeper API (v1 shape).",
    )

    project_slugs: list[str] | Literal["*"] | None = Field(
        default=None,
        description=(
            'LTD project slugs to sync, or ``"*"`` for every project'
            " visible on the LTD instance. When provided, replaces the"
            " stored list wholesale (no append semantics)."
        ),
        examples=[["sqr-112", "dmtn-001"]],
    )

    project_slug_patterns: list[str] | None = Field(
        default=None,
        description=(
            "Python regular expressions that *add* LTD project slugs to"
            " the sync scope. When provided, replaces the stored list"
            " wholesale (no append semantics). Validated exactly as on"
            " ``PUT``."
        ),
        examples=[[r"sqr-\d+", r"dmtn-\d+"]],
    )

    exclude_project_slugs: list[str] | None = Field(
        default=None,
        description=(
            "LTD project slugs removed from the sync scope. When"
            " provided, replaces the stored list wholesale (no append"
            " semantics)."
        ),
        examples=[["www"]],
    )

    exclude_project_slug_patterns: list[str] | None = Field(
        default=None,
        description=(
            "Python regular expressions that *remove* LTD project slugs"
            " from the sync scope. When provided, replaces the stored"
            " list wholesale (no append semantics). Validated exactly as"
            " on ``PUT``."
        ),
        examples=[[r"test-.*"]],
    )

    @field_validator("project_slug_patterns", "exclude_project_slug_patterns")
    @classmethod
    def _check_patterns(
        cls, value: list[str] | None, info: ValidationInfo
    ) -> list[str] | None:
        """Apply the ``KeeperSyncConfig`` pattern rules to an update.

        ``None`` passes through untouched so
        :meth:`_reject_explicit_null` owns the explicit-null 422.
        """
        if value is None:
            return None
        return _validate_slug_patterns(value, info.field_name)

    @field_validator(
        "enabled",
        "ltd_base_url",
        "project_slugs",
        "project_slug_patterns",
        "exclude_project_slugs",
        "exclude_project_slug_patterns",
    )
    @classmethod
    def _reject_explicit_null(cls, value: object) -> object:
        """Reject an explicit ``null`` for any config field.

        The validator is skipped for unset defaults, so it only fires when
        a field is explicitly sent as ``null``. These fields are
        non-nullable in the stored config, so null-as-remove has no
        meaning; omit a field to leave it unchanged.
        """
        if value is None:
            msg = (
                "keeper-sync config fields may not be null; omit a field to"
                " leave it unchanged"
            )
            raise ValueError(msg)
        return value


class KeeperSyncScopePreview(BaseModel):
    """Side-effect-free resolution of a keeper-sync scope against LTD.

    Response body of ``POST /orgs/{org}/keeper-sync/scope-preview``. The
    endpoint merges an optional :class:`KeeperSyncConfigUpdate` over the
    stored config exactly as ``PATCH`` would, resolves the result against
    the *live* LTD product listing, and reports what that scope covers —
    without persisting the candidate config or enqueueing any work.

    Saving a wider scope is not inert: the tier crons act on the stored
    config at their next tick. This report is how an operator checks a
    candidate scope *before* saving it, which is what syncing lsst.io in
    waves by document series needs.

    ``in_scope_slugs`` is the scope as the *config* resolves it, so a
    slug appears there even when a tombstone will make sync skip it;
    ``tombstoned_slugs`` names that subset. The preview works whether or
    not sync is ``enabled``.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "ltd_count": 1640,
                    "in_scope_count": 3,
                    "in_scope_slugs": ["sqr-060", "sqr-112", "dmtn-201"],
                    "new_slugs": ["dmtn-201"],
                    "tombstoned_slugs": ["sqr-060"],
                    "unmatched_project_slugs": ["sqr-9999"],
                }
            ]
        }
    )

    ltd_count: int = Field(
        description=(
            "Number of distinct product slugs the live LTD instance"
            " listed when the preview ran."
        ),
        examples=[1640],
    )

    in_scope_count: int = Field(
        description=(
            "Size of ``in_scope_slugs``. This is how many child"
            " ``keeper_sync_project`` jobs a backfill launched with the"
            " same config would fan out — so the run's ``total_count``"
            " is this plus one, the discovery job attributing itself to"
            " the run. Slugs skipped for a tombstone or for an"
            " already-running per-project job lower the child count"
            " further."
        ),
        examples=[3],
    )

    in_scope_slugs: list[str] = Field(
        default_factory=list,
        description=(
            "The resolved scope, in LTD listing order — the order in"
            " which a backfill would fan its children out. Includes"
            " slugs that a tombstone will make sync skip; see"
            " ``tombstoned_slugs``."
        ),
        examples=[["sqr-060", "sqr-112", "dmtn-201"]],
    )

    new_slugs: list[str] = Field(
        default_factory=list,
        description=(
            "In-scope slugs with no keeper-sync state row on this"
            " organization yet — what the next backfill would import for"
            " the first time. In LTD listing order."
        ),
        examples=[["dmtn-201"]],
    )

    tombstoned_slugs: list[str] = Field(
        default_factory=list,
        description=(
            "In-scope slugs that sync will skip because their"
            " project-resource state row is tombstoned. Clear the"
            " tombstone via ``DELETE /orgs/{org}/keeper-sync/tombstones/"
            "{tombstone}`` to bring one back. In LTD listing order."
        ),
        examples=[["sqr-060"]],
    )

    unmatched_project_slugs: list[str] = Field(
        default_factory=list,
        description=(
            "Entries of ``project_slugs`` or ``exclude_project_slugs``"
            " that the live LTD listing does not contain — the typo"
            " catcher. Pattern fields are not checked here: a pattern"
            " matching nothing is a legitimate way to stage a future"
            " wave. Listed in config order, ``project_slugs`` first."
        ),
        examples=[["sqr-9999"]],
    )


class KeeperSyncRunKind(StrEnum):
    """Kind of LTD Keeper sync run.

    - ``backfill`` — full import of the configured LTD projects into
      Docverse; the kind created by ``POST /orgs/{org}/keeper-sync/
      runs`` today.
    - ``resync`` — reserved for a re-import of already-synced projects;
      not created today.
    - ``reconcile`` — reserved for a diff-and-repair pass against LTD;
      not created today.
    """

    backfill = "backfill"
    resync = "resync"
    reconcile = "reconcile"


class KeeperSyncRunStatus(StrEnum):
    """Lifecycle status of a keeper sync run.

    - ``pending`` — the discovery job has been enqueued but has not yet
      fanned out any children.
    - ``in_progress`` — discovery has enqueued at least one child sync
      job.
    - ``succeeded`` — terminal; every child job succeeded.
    - ``partial_failure`` — terminal; some child jobs failed.
    - ``failed`` — terminal; the run failed outright (e.g. the
      discovery job itself failed).
    """

    pending = "pending"
    in_progress = "in_progress"
    succeeded = "succeeded"
    partial_failure = "partial_failure"
    failed = "failed"


class KeeperSyncRun(BaseModel):
    """Response model for a keeper sync run resource."""

    model_config = ConfigDict(from_attributes=True)

    self_url: HttpUrl = Field(
        description="URL to this run resource.",
        examples=[f"{EXAMPLE_ORG_URL}/keeper-sync/runs/{EXAMPLE_RUN_ID}"],
    )

    jobs_url: HttpUrl = Field(
        description=(
            "URL to ``GET`` for the run's child queue-job listing"
            " (the org-scoped jobs collection filtered to this run,"
            " ``GET /orgs/{org}/jobs?run={id}``). Always present so"
            " clients can paginate the run's children without"
            " constructing the URL by hand."
        ),
        examples=[f"{EXAMPLE_ORG_URL}/jobs?run={EXAMPLE_RUN_ID}"],
    )

    id: str = Field(
        description="Public Crockford Base32 identifier for the run.",
        examples=[EXAMPLE_RUN_ID],
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
        ),
        examples=[EXAMPLE_JOB_ID],
    )

    job_url: HttpUrl = Field(
        description="URL of the enqueued discovery job resource.",
        examples=[EXAMPLE_JOB_URL],
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

    ltd_slug: str = Field(
        description="LTD product slug for this project.",
        examples=["pipelines"],
    )

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
        ),
        examples=[EXAMPLE_EDITION_URL],
    )

    slug: str = Field(
        description="Docverse edition slug.",
        examples=["v1"],
    )

    kind: EditionKind = Field(
        description="Docverse edition kind (``main``, ``draft``, ...)."
    )

    ltd_id: int | None = Field(
        default=None,
        description=(
            "LTD edition id from the linked ``keeper_sync_state`` row,"
            " or ``null`` when no row links this edition. This is the"
            " numeric id from the legacy LTD Keeper API, not a Docverse"
            " identifier."
        ),
        examples=[4289],
    )

    ltd_slug: str | None = Field(
        default=None,
        description=(
            "LTD edition slug from the linked state row, or ``null``"
            " when no row links this edition."
        ),
        examples=["v1"],
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
        ),
        examples=[f"{EXAMPLE_ORG_URL}/keeper-sync/projects/pipelines"],
    )

    org_url: HttpUrl = Field(
        description=(
            "Canonical ``GET /orgs/{org}`` URL for the Docverse"
            " organization this report is scoped to."
        ),
        examples=[EXAMPLE_ORG_URL],
    )

    project_url: HttpUrl | None = Field(
        default=None,
        description=(
            "Canonical ``GET /orgs/{org}/projects/{project}`` URL for"
            " the Docverse project, or ``null`` when no Docverse"
            " project has been imported yet for this LTD slug."
        ),
        examples=[EXAMPLE_PROJECT_URL],
    )

    sync_refresh_url: HttpUrl = Field(
        description=(
            "URL to ``POST`` for an immediate one-shot sync of this"
            " project (``post_org_keeper_sync_project_refresh``). Always"
            " present so operators can trigger a refresh from the"
            " status response without constructing the URL by hand."
        ),
        examples=[f"{EXAMPLE_ORG_URL}/keeper-sync/projects/pipelines/refresh"],
    )

    editions_sync_url: HttpUrl = Field(
        description=(
            "URL to ``GET`` for the paginated keeper-sync editions"
            " collection for this project"
            " (``get_org_keeper_sync_project_editions``). Always"
            " present so operators can scan the full edition list"
            " without constructing the URL by hand."
        ),
        examples=[
            f"{EXAMPLE_ORG_URL}/keeper-sync/projects/pipelines/editions"
        ],
    )

    ltd_slug: str = Field(
        description="LTD product slug the report is scoped to.",
        examples=["pipelines"],
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
        ),
        examples=[EXAMPLE_JOB_ID],
    )

    job_url: HttpUrl = Field(
        description="URL of the enqueued job resource.",
        examples=[EXAMPLE_JOB_URL],
    )


class KeeperSyncResourceType(StrEnum):
    """LTD resource types tracked by keeper-sync.

    - ``project`` — the tombstone vetoes re-import of the LTD product
      entirely.
    - ``edition`` — the tombstone vetoes one specific LTD edition.
    - ``build`` — reserved; builds are not tombstoned today, but the
      value is kept so operator-facing filters do not silently reject
      a valid resource type the schema otherwise carries.
    """

    project = "project"
    edition = "edition"
    build = "build"


class KeeperSyncTombstoneReason(StrEnum):
    """Why a ``keeper_sync_state`` row was tombstoned.

    - ``manual_delete`` — an operator soft-deleted the Docverse-side
      resource; the tombstone stops sync from re-importing it.
    - ``lifecycle_delete`` — an automated process (``lifecycle_eval``,
      ``git_ref_audit``, or the ``ref_deleted`` webhook) soft-deleted
      the Docverse-side resource.
    - ``lifecycle_preemptive`` — sync itself short-circuited an LTD
      edition that the lifecycle rules would immediately delete,
      before the build content was copied. No matching Docverse row
      exists in this case.
    """

    manual_delete = "manual_delete"
    lifecycle_delete = "lifecycle_delete"
    lifecycle_preemptive = "lifecycle_preemptive"


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
        ),
        examples=[
            f"{EXAMPLE_ORG_URL}/keeper-sync/tombstones/{EXAMPLE_TOMBSTONE_ID}"
        ],
    )

    id: str = Field(
        description=(
            "Public Crockford Base32 identifier for the tombstoned"
            " ``keeper_sync_state`` row. Use this id with the DELETE"
            " endpoint to clear the tombstone."
        ),
        examples=[EXAMPLE_TOMBSTONE_ID],
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
        ),
        examples=["v1"],
    )

    ltd_id: int | None = Field(
        default=None,
        description=(
            "LTD-side numeric id for the tombstoned resource,"
            " from the legacy LTD Keeper API."
            " Populated for ``edition`` and ``build`` rows; ``null``"
            " for ``project`` rows (LTD products are slug-only)."
        ),
        examples=[4289],
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
        ),
        examples=["pipelines/v1"],
    )
