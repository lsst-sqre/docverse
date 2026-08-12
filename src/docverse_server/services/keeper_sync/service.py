"""High-level orchestration for one LTD product / edition / build slice.

The :class:`KeeperSyncService` is the tracer-bullet entry point that
walks one project end-to-end: it fetches LTD's view of the resource
through :class:`docverse_server.storage.ltd.LtdClient`, looks up or
creates the matching Docverse rows by delegating to the existing
``ProjectService`` / ``BuildStore`` / ``EditionStore``, and copies the
build content into Docverse R2 via :class:`BuildContentCopier`. The
:class:`KeeperSyncStateStore` row is the idempotency key: a re-run
with unchanged LTD state short-circuits.

This slice covers only the ``git_refs`` LTD edition mode; other modes
raise :class:`NotImplementedError` from
:func:`docverse_server.services.keeper_sync.mappers.map_edition_tracking` and
are filled in by issue #289.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import sentry_sdk
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionAutocreationConfig,
    ProjectCreate,
    ProjectGitHubBindingCreate,
    TrackingMode,
)
from docverse.models.editions import DefaultEditionConfig
from docverse.models.projects import parse_github_url
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.edition import Edition
from docverse_server.domain.edition_autocreation import (
    DEFAULT_EDITION_AUTOCREATION,
    resolve_edition_autocreation,
)
from docverse_server.domain.edition_kind import is_kind_promotion
from docverse_server.domain.lifecycle import (
    DraftInactivityRule,
    RefDeletedRule,
)
from docverse_server.domain.organization import Organization
from docverse_server.domain.project import Project
from docverse_server.domain.semver_aggregate import (
    SemverAggregateSpec,
    semver_aggregate_specs,
)
from docverse_server.domain.slug import (
    AnySlugRewriteRule,
    resolve_slug_rewrite_rules,
)
from docverse_server.domain.version import (
    SemverVersion,
    accepts_version_advance,
    parse_stable_semver,
)
from docverse_server.exceptions import (
    KeeperSyncInvariantError,
    KeeperSyncSystemicFailureError,
    NotFoundError,
)
from docverse_server.services.keeper_sync_tombstone import (
    KeeperSyncTombstoneService,
)
from docverse_server.services.lifecycle.evaluator import (
    LifecycleEvaluationContext,
    evaluate_lifecycle,
    filter_rule_set,
    resolve_rule_set,
)
from docverse_server.services.project import (
    DEFAULT_EDITION_SLUG,
    ProjectService,
)
from docverse_server.services.project_github_binding import (
    ProjectGitHubBindingResolver,
)
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.github import (
    GitHubRefSetFetcher,
    RepositoryNotAccessibleError,
    RepositoryRefFetchError,
)
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse_server.storage.ltd import (
    LtdBuild,
    LtdClient,
    LtdEdition,
    LtdEditionMode,
    LtdProduct,
    LtdSourceAccessDeniedError,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore

from .copier import EMPTY_MANIFEST_HASH, CopyResult
from .mappers import (
    EditionKindDerivation,
    derive_edition_kind,
    derive_edition_slug,
    derive_edition_source_prefix,
    map_edition_tracking,
)

__all__ = [
    "DEFAULT_ORPHAN_RECLAIM_MAX_AGE",
    "MAX_CONSECUTIVE_EDITION_FAILURES",
    "AggregateEditionOutcome",
    "BuildSyncOutcome",
    "CopyCallable",
    "EditionSyncFailure",
    "EditionSyncOutcome",
    "KeeperSyncContext",
    "KeeperSyncService",
    "ManifestCallable",
    "ProjectSyncResult",
]

#: Default age threshold for treating a ``pending`` keeper-sync
#: placeholder build as orphaned. The placeholder is created in the
#: same flow that immediately copies bucket contents and finalizes the
#: build; the whole cycle should complete in well under this. Anything
#: older is assumed to be from a worker that crashed between the
#: placeholder commit and the finalize commit.
DEFAULT_ORPHAN_RECLAIM_MAX_AGE = timedelta(hours=1)

#: Number of *consecutive* per-edition failures that ``sync_project``
#: treats as a systemic outage rather than a run of independent
#: per-edition faults.
#:
#: The per-edition failure boundary exists for the isolated case — LTD's
#: oldest uploads carry no public-read object ACL, so one permanently
#: unreadable build must not strand the releases behind it. That same
#: isolation is actively harmful when the fault is shared: a mid-run LTD
#: outage or a database failure marks every remaining edition failed one
#: by one, the loop finishes, and the run rolls up ``completed`` on what
#: was really a 3-of-80 import.
#:
#: The threshold is derived from the largest *contiguous* run of
#: permanent per-edition faults LTD actually produces. Measured on
#: roundtable-dev 2026-08-12: documenteer's build slugs 1-204 deny
#: anonymous ``GetObject`` and 205+ read, 60 of its editions point at
#: builds inside that block, and LTD lists editions oldest-first — so
#: those 60 arrive back-to-back. (Only 19 failed the 2026-08-12 run
#: because the other 41 were ``lifecycle_preemptive`` tombstones that
#: short-circuited; after the PRD's manual tombstone purge and resync
#: all 60 reach ``sync_edition``.) The un-ACL'd uploads are therefore
#: strictly contiguous, not "scattered through a product's history" as
#: this constant was originally justified — a premise that went
#: unchallenged only because a tombstone short-circuit used to reset the
#: counter, which kept the guard from ever firing.
#:
#: Seventy-five sits above that 60-edition block with headroom for a
#: slightly deeper one on another product, and still aborts inside the
#: first ~27% of documenteer's 279 editions, so a genuine outage costs
#: 75 attempts rather than a whole product's edition list. The bias
#: toward a high threshold follows the asymmetry of the two errors: a
#: false abort is permanent and total — documenteer would stop at
#: edition 60-something of 279 on every poll, forever, and none of the
#: readable editions behind the denial block would ever import —
#: whereas a missed abort is loud and recoverable, since the run
#: finishes ``completed_with_errors`` with an exact
#: ``edition_failure_count`` and ``aggregate_activity`` rolls the parent
#: run up ``partial_failure``.
#:
#: The edition-prefix fallback (#516) now recovers that whole
#: contiguous block from ``<product>/v/<edition-slug>/``, so those
#: denials should stop reaching this counter at all — a denial only
#: survives to become an edition failure when the published copy is
#: missing or denied too. The threshold is deliberately *not* lowered on
#: the strength of that reasoning alone: a roundtable-dev resync has to
#: confirm the 19 documenteer failures actually drop to zero first, and
#: the asymmetry above still says to err high. Lowering it is separate
#: work, and #516's
#: :class:`~docverse_server.storage.ltd.LtdSourceAccessDeniedError` is
#: the Docverse-side type that would also let a denial be discounted
#: from the *systemic* signal by error type rather than by count.
MAX_CONSECUTIVE_EDITION_FAILURES = 75

#: Type alias for the ``(source_prefix, dest_prefix) -> CopyResult``
#: callable the service consumes. Tests inject a fake; the production
#: factory wires it onto a real :class:`BuildContentCopier`.
CopyCallable = Callable[[str, str], Awaitable[CopyResult]]

#: Type alias for the ``(source_prefix) -> manifest_hash`` callable
#: used for dual-upload convergence: the service computes the inbound
#: LTD manifest hash via this callable, then short-circuits the upload
#: when an existing Docverse build for the project already carries it.
ManifestCallable = Callable[[str], Awaitable[str]]

#: Placeholder hash on a freshly-created synced build row. Overwritten
#: with the real manifest hash once the copier has run. The regex on
#: :class:`BuildCreate.content_hash` requires ``sha256:<64 hex>``.
_PLACEHOLDER_CONTENT_HASH = f"sha256:{'0' * 64}"

#: Username recorded as the build's uploader for synced builds.
_SYNC_UPLOADER = "keeper-sync"


@dataclass(frozen=True)
class BuildSyncOutcome:
    """What ``sync_build`` did with one LTD build.

    ``docverse_build_public_id`` carries the public Base32 form of the
    Docverse build's id so the keeper-sync worker can pass it into the
    publish-enqueue helper without re-loading the build row.
    """

    docverse_build_id: int | None
    """``None`` when the call short-circuited (state matched LTD)."""

    docverse_build_public_id: str | None
    """``None`` when the call short-circuited (state matched LTD)."""

    short_circuited: bool
    content_hash: str | None
    object_count: int | None
    total_size_bytes: int | None


@dataclass(frozen=True)
class _ResolvedBuildSource:
    """The LTD bucket prefix one build sync actually reads, plus its hash.

    Both the manifest hash and the copy must read the *same* prefix or
    the ``content_hash`` recorded on the build would not describe the
    bytes that were uploaded. Bundling the two together makes that
    impossible to get wrong: the prefix is resolved once, and the hash
    that comes back is the hash *of that prefix*.
    """

    prefix: str
    """The resolved LTD prefix, always terminated by ``/``."""

    manifest_hash: str
    """Manifest hash of :attr:`prefix`'s contents."""

    from_edition_prefix: bool
    """``True`` when the build prefix denied and ``v/`` recovered it."""

    @property
    def annotations(self) -> dict[str, Any]:
        """Provenance to record on the build's ``keeper_sync_state`` row.

        A build recovered from the edition prefix carries a
        ``bucket_root_dir`` whose bytes came from somewhere else, so
        "where did this content come from" has to be answerable after
        the fact — from the database, not just from a log line that has
        since rotated away.
        """
        return {
            "ltd_source_prefix": self.prefix,
            "ltd_source_prefix_origin": (
                "edition" if self.from_edition_prefix else "build"
            ),
        }


@dataclass(frozen=True)
class AggregateEditionOutcome:
    """One semver aggregate edition the backfill created or advanced.

    Emitted only when the pass actually moved a row — a freshly created
    ``N`` / ``N.M`` edition, or an existing aggregate whose pointer
    advanced onto this build. The steady state (aggregate already on the
    build, or held back by the version guard) emits nothing, so the
    worker's publish fan-out stays a no-op on repeat polls.
    """

    docverse_edition_id: int
    docverse_slug: str
    docverse_build_id: int
    docverse_build_public_id: str


@dataclass(frozen=True)
class EditionSyncOutcome:
    """What ``sync_edition`` did with one LTD edition.

    ``docverse_project_id`` / ``docverse_project_slug`` are carried on
    every outcome so per-edition consumers (e.g. the worker's
    ``on_edition_synced`` publish-enqueue callback) have the project
    context the publish helper needs without re-querying the project
    store from the closure.

    ``docverse_edition_id`` is ``None`` when the call short-circuited
    on a tombstoned ``keeper_sync_state`` row whose ``docverse_id`` is
    also ``None`` (the ``lifecycle_preemptive`` case writes such rows
    for LTD editions that were never imported).
    """

    docverse_edition_id: int | None
    docverse_slug: str
    docverse_project_id: int
    docverse_project_slug: str
    build_outcome: BuildSyncOutcome | None
    short_circuited: bool
    """``True`` when the call returned without contacting LTD at all.

    Set only by the tombstone short-circuit, which returns before
    ``_ensure_edition`` on the strength of a single ``keeper_sync_state``
    read. ``sync_project`` reads this to keep such a return out of its
    consecutive-failure accounting: a database read succeeds *especially*
    when LTD is down, so treating it as evidence of a healthy edition
    reset the systemic-failure counter mid-outage.

    Deliberately explicit rather than inferred from ``build_outcome is
    None``, which is also true of a legitimately build-less edition that
    really was synced.
    """

    aggregate_outcomes: tuple[AggregateEditionOutcome, ...] = ()
    """Semver aggregates this edition's release build created or moved.

    Empty on every path that did not run the backfill (no build, a
    tombstone short-circuit, a non-semver ref, aggregates switched off)
    and in the steady state where the aggregates already point at the
    build.
    """


@dataclass(frozen=True)
class EditionSyncFailure:
    """One LTD edition whose :meth:`sync_edition` call raised.

    Carries the LTD-side identity rather than the Docverse-side one:
    a failure can fire before the edition has a Docverse row at all
    (an ``AccessDenied`` on the LTD build's objects leaves the edition
    created but the build missing), so the LTD slug/id is the only
    identifier guaranteed to exist. ``error_message`` is ``str(exc)``,
    kept short enough to live in a ``queue_jobs.progress`` JSONB blob;
    the full traceback goes to Sentry and the structured log.
    """

    ltd_edition_id: int
    ltd_edition_slug: str
    error_type: str
    error_message: str


@dataclass(frozen=True)
class ProjectSyncResult:
    """What ``sync_project`` did with one LTD product.

    ``docverse_project_id`` is ``None`` only when the call
    short-circuited on a tombstoned ``keeper_sync_state`` project row
    whose ``docverse_id`` was never populated — practically a
    defensive shape; the only production path that writes a project
    tombstone is the manual-delete chokepoint, which always carries a
    Docverse project id.

    ``edition_failures`` is empty on a clean sync; a non-empty tuple
    means the run was *partial* — those LTD editions raised and were
    skipped, every other edition still synced.
    """

    docverse_project_id: int | None
    docverse_project_slug: str
    edition_outcomes: list[EditionSyncOutcome]
    edition_failures: tuple[EditionSyncFailure, ...] = ()


@dataclass(frozen=True)
class KeeperSyncContext:
    """Bundle of stores and services that ``KeeperSyncService`` reads.

    Grouping the persistence plumbing here keeps the service constructor
    readable: callers pass one ``context`` instead of six individual
    stores, and tests can build a context once and reuse it.
    """

    org_store: OrganizationStore
    project_store: ProjectStore
    project_service: ProjectService
    edition_store: EditionStore
    build_store: BuildStore
    state_store: KeeperSyncStateStore


class KeeperSyncService:
    """Orchestrate sync for one LTD product / edition / build path."""

    def __init__(
        self,
        *,
        session: AsyncSession,
        context: KeeperSyncContext,
        ltd_client: LtdClient,
        copy_callable: CopyCallable,
        manifest_callable: ManifestCallable,
        logger: structlog.stdlib.BoundLogger,
        orphan_reclaim_max_age: timedelta = DEFAULT_ORPHAN_RECLAIM_MAX_AGE,
        tombstone_service: KeeperSyncTombstoneService | None = None,
        binding_resolver: ProjectGitHubBindingResolver | None = None,
        ref_set_fetcher: GitHubRefSetFetcher | None = None,
    ) -> None:
        self._session = session
        self._org_store = context.org_store
        self._project_store = context.project_store
        self._project_service = context.project_service
        self._edition_store = context.edition_store
        self._build_store = context.build_store
        self._state_store = context.state_store
        self._ltd_client = ltd_client
        self._copy_callable = copy_callable
        self._manifest_callable = manifest_callable
        self._logger = logger
        self._orphan_reclaim_max_age = orphan_reclaim_max_age
        self._tombstone_service = tombstone_service
        self._binding_resolver = binding_resolver
        self._ref_set_fetcher = ref_set_fetcher

    async def sync_project(
        self,
        *,
        org_id: int,
        ltd_slug: str,
        on_edition_synced: (
            Callable[[EditionSyncOutcome], Awaitable[None]] | None
        ) = None,
    ) -> ProjectSyncResult:
        """Sync one LTD product (and all its editions) into Docverse.

        Each edition has its own failure boundary: an exception out of
        :meth:`sync_edition` is captured to Sentry, logged with the LTD
        edition's slug/id, recorded on the returned
        :attr:`ProjectSyncResult.edition_failures`, and the loop moves
        on. One unreadable LTD build therefore costs one edition rather
        than every edition behind it in the list — LTD's oldest uploads
        carry no public-read ACL, so a single permanently-``AccessDenied``
        build used to strand the whole project's release history.
        ``edition_failures`` is the partial-success signal (bounded retry
        of transient copy errors is deliberately out of scope here); the
        worker records it on the job's ``progress`` and finishes the job
        ``completed_with_errors``.

        That isolation is bounded by
        :data:`MAX_CONSECUTIVE_EDITION_FAILURES`. A run of that many
        *consecutive* failures is read as a systemic fault — a mid-run
        LTD outage, a dead database — rather than a run of independent
        per-edition ones, and raises
        :exc:`~docverse_server.exceptions.KeeperSyncSystemicFailureError`
        (chained from the last edition's exception) instead of walking
        the remaining editions. Without the abort, a systemic fault
        would mark every remaining edition failed one at a time and
        still let the run roll up green over a 3-of-80 import.

        Only an edition that *did work* resets the counter, so scattered
        isolated failures keep their per-edition boundary while editions
        the loop passed over stay invisible to the breaker: an edition in
        ``skip_ltd_ids`` never reaches :meth:`sync_edition`, and one whose
        ``keeper_sync_state`` row is tombstoned returns a
        ``short_circuited`` outcome from a lone database read. Neither
        contacts LTD, so neither is evidence that an outage has ended.

        ``on_edition_synced`` runs once per :meth:`sync_edition` return,
        before the next iteration begins. Callbacks fire after each
        edition's ``session.begin()`` blocks have committed, so they
        may open their own transactions safely. Exceptions raised by
        the callback are caught and logged so a single failure does not
        stop the rest of the project sync; the tail-end self-heal pass
        in the worker picks up any edition the callback failed to act
        on. The default ``None`` preserves all non-worker call sites.

        Short-circuits before the LTD product fetch when the project's
        ``keeper_sync_state`` row is tombstoned: the operator has
        deleted the project on the Docverse side and the migration
        must not re-import it. Returns a result with empty
        ``edition_outcomes`` so worker post-sync passes (e.g. the
        self-heal pass) iterate nothing.
        """
        async with self._session.begin():
            project_state = await self._state_store.get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug=ltd_slug,
                include_tombstoned=True,
            )
        if project_state is not None and project_state.date_tombstoned:
            self._logger.info(
                "Sync short-circuited: project tombstoned",
                ltd_slug=ltd_slug,
                tombstone_reason=project_state.tombstone_reason,
            )
            return ProjectSyncResult(
                docverse_project_id=project_state.docverse_id,
                docverse_project_slug=ltd_slug,
                edition_outcomes=[],
            )

        ltd_product = await self._ltd_client.get_product(ltd_slug)
        async with self._session.begin():
            org, project = await self._ensure_project(
                org_id=org_id, ltd_product=ltd_product
            )
        # Resolved once per project and threaded through both kind
        # derivation call sites (the proactive pass and sync_edition)
        # so the rule set is parsed a single time per sync.
        rewrite_rules = _resolve_rewrite_rules(org=org, project=project)
        # Same story for the autocreation config: project-over-org, one
        # resolution per sync, threaded into every sync_edition call.
        autocreation = resolve_edition_autocreation(
            project=project.edition_autocreation,
            org=org.edition_autocreation,
        )
        ltd_editions = await self._ltd_client.list_editions_for_product(
            ltd_slug
        )
        skip_ltd_ids = await self._proactive_lifecycle_pass(
            org=org,
            project=project,
            ltd_editions=ltd_editions,
            rewrite_rules=rewrite_rules,
        )
        outcomes: list[EditionSyncOutcome] = []
        failures: list[EditionSyncFailure] = []
        consecutive_failures = 0
        for ltd_edition in ltd_editions:
            if ltd_edition.ltd_id in skip_ltd_ids:
                continue
            try:
                outcome = await self.sync_edition(
                    org_id=org.id,
                    org_slug=org.slug,
                    project=project,
                    ltd_edition=ltd_edition,
                    rewrite_rules=rewrite_rules,
                    autocreation=autocreation,
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                self._logger.exception(
                    "Edition sync failed; skipping edition and continuing",
                    ltd_edition_id=ltd_edition.ltd_id,
                    ltd_edition_slug=ltd_edition.slug,
                    project_id=project.id,
                    project=project.slug,
                )
                failures.append(
                    EditionSyncFailure(
                        ltd_edition_id=ltd_edition.ltd_id,
                        ltd_edition_slug=ltd_edition.slug,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                )
                consecutive_failures += 1
                if consecutive_failures >= MAX_CONSECUTIVE_EDITION_FAILURES:
                    recent = [
                        failure.ltd_edition_slug
                        for failure in failures[-consecutive_failures:]
                    ]
                    self._logger.exception(
                        "Aborting project sync: consecutive edition failures"
                        " indicate a systemic outage",
                        consecutive_failures=consecutive_failures,
                        failed_ltd_edition_slugs=recent,
                        project_id=project.id,
                        project=project.slug,
                        ltd_slug=ltd_slug,
                    )
                    raise KeeperSyncSystemicFailureError(
                        ltd_slug=ltd_slug,
                        consecutive_failures=consecutive_failures,
                        failed_ltd_edition_slugs=recent,
                    ) from exc
                continue
            # Only an edition that actually did work counts as evidence
            # that the fault was per-edition rather than systemic. A
            # tombstone short-circuit returns on a single
            # ``keeper_sync_state`` read without touching LTD — it
            # succeeds *especially* during an outage — so it leaves the
            # counter alone, exactly as the ``skip_ltd_ids`` ``continue``
            # above does. Resetting here let any project with tombstones
            # scattered through its edition list defeat the breaker with
            # an alternating fail/skip pattern.
            if not outcome.short_circuited:
                consecutive_failures = 0
            outcomes.append(outcome)
            if on_edition_synced is not None:
                try:
                    await on_edition_synced(outcome)
                except Exception as exc:
                    sentry_sdk.capture_exception(exc)
                    self._logger.exception(
                        "on_edition_synced callback raised; continuing",
                        docverse_slug=outcome.docverse_slug,
                    )
        return ProjectSyncResult(
            docverse_project_id=project.id,
            docverse_project_slug=project.slug,
            edition_outcomes=outcomes,
            edition_failures=tuple(failures),
        )

    async def _proactive_lifecycle_pass(
        self,
        *,
        org: Organization,
        project: Project,
        ltd_editions: list[LtdEdition],
        rewrite_rules: Sequence[AnySlugRewriteRule] = (),
    ) -> set[int]:
        """Tombstone LTD editions a lifecycle rule would delete on import.

        Runs once per :meth:`sync_project` invocation, between the LTD
        editions list and the per-edition fan-out. For every LTD edition
        that has no Docverse row and no existing tombstone, builds a
        transient domain :class:`Edition` and runs
        :func:`evaluate_lifecycle` filtered to ``draft_inactivity`` and
        ``ref_deleted``; ``build_history_orphan`` is excluded because it
        matches against the Docverse build-history chain that only
        exists post-import (the post-import ``lifecycle_eval`` pass owns
        it).

        Returns the set of LTD edition ids the caller must skip — those
        are tombstoned ``lifecycle_preemptive`` and never reach
        ``sync_edition``. An empty set means "nothing tombstoned
        proactively; iterate every LTD edition normally."

        The proactive pass is a no-op when any of ``tombstone_service``,
        ``binding_resolver``, or ``ref_set_fetcher`` is unconfigured —
        the production factory wires all three; tests that exercise
        only the non-proactive paths can omit them.

        The per-project GitHub ref pre-fetch happens here (once,
        outside any open write transaction) and is shared across every
        edition. A ``RepositoryNotAccessibleError`` or
        ``RepositoryRefFetchError`` is caught, logged, and downgrades
        the pass — ``ref_deleted`` cannot match without ``live_refs``,
        so those projects fall through to KEEP and the regular
        ``git_ref_audit`` cron catches up on its own schedule.
        """
        if (
            self._tombstone_service is None
            or self._binding_resolver is None
            or self._ref_set_fetcher is None
        ):
            return set()
        if not ltd_editions:
            return set()
        rule_set = filter_rule_set(
            resolve_rule_set(
                org_rules=org.lifecycle_rules,
                project_rules=project.lifecycle_rules,
            ),
            include=(DraftInactivityRule, RefDeletedRule),
        )
        if not rule_set.root:
            return set()

        # Pre-load state rows for this project's LTD editions in one
        # round-trip. We need both ``date_tombstoned`` (skip already-
        # vetoed editions so we do not overwrite the recorded reason)
        # and ``docverse_id`` (skip already-imported editions — the
        # regular ``lifecycle_eval`` pass owns those, and the transient
        # built from LTD metadata could not see ``lifecycle_exempt`` or
        # other Docverse-side state).
        ltd_ids = [e.ltd_id for e in ltd_editions]
        async with self._session.begin():
            state_rows = await self._state_store.list_for_org(
                org_id=org.id,
                resource_type=ResourceType.edition,
                ltd_ids=ltd_ids,
                include_tombstoned=True,
            )
        state_by_ltd_id = {
            row.ltd_id: row for row in state_rows if row.ltd_id is not None
        }

        live_refs = await self._fetch_live_refs(project=project)
        now = _now()
        skip_ltd_ids: set[int] = set()
        for ltd_edition in ltd_editions:
            state = state_by_ltd_id.get(ltd_edition.ltd_id)
            if state is not None and (
                state.date_tombstoned is not None
                or state.docverse_id is not None
            ):
                continue
            transient = _transient_edition_from_ltd(
                ltd_edition=ltd_edition,
                project_id=project.id,
                rewrite_rules=rewrite_rules,
            )
            if transient is None:
                continue
            decision = evaluate_lifecycle(
                rule_set=rule_set,
                context=LifecycleEvaluationContext(
                    editions=[transient],
                    builds=[],
                    edition_build_history=[],
                    now=now,
                    live_refs=live_refs,
                ),
            )
            matched_rule = decision.edition_matches.get(transient.id)
            if matched_rule is None:
                continue
            async with self._session.begin():
                await self._tombstone_service.record(
                    org_id=org.id,
                    resource_type=ResourceType.edition,
                    ltd_id=ltd_edition.ltd_id,
                    ltd_slug=ltd_edition.slug,
                    reason=TombstoneReason.lifecycle_preemptive,
                )
            self._logger.info(
                "Proactive lifecycle delete: tombstoning LTD edition",
                ltd_edition_id=ltd_edition.ltd_id,
                ltd_edition_slug=ltd_edition.slug,
                rule_type=matched_rule,
                org_id=org.id,
                project_id=project.id,
                project=project.slug,
            )
            skip_ltd_ids.add(ltd_edition.ltd_id)
        return skip_ltd_ids

    async def _fetch_live_refs(
        self, *, project: Project
    ) -> frozenset[str] | None:
        """Resolve binding + fetch the project's live ref set, once.

        Returns ``None`` when the project has no GitHub binding (the
        ``ref_deleted`` rule simply does not fire) or when the GitHub
        round-trip fails — both are accepted "rule disabled, KEEP wins"
        outcomes. ``draft_inactivity`` is unaffected because it does
        not read ``live_refs``.

        ``ProjectGitHubBindingResolver.resolve`` owns its own short
        read transaction and mints the GitHub installation token
        *after* that transaction has closed; the caller must therefore
        **not** wrap this method in ``session.begin()``. Mirrors
        :func:`git_ref_audit._fetch_refs_per_project`.
        """
        if self._binding_resolver is None or self._ref_set_fetcher is None:
            msg = (
                "_fetch_live_refs requires binding_resolver and "
                "ref_set_fetcher to be configured; caller must "
                "short-circuit when they are None"
            )
            raise KeeperSyncInvariantError(msg)
        binding = await self._binding_resolver.resolve(project.id)
        if binding is None:
            return None
        try:
            ref_set = await self._ref_set_fetcher.fetch(
                owner=binding.owner,
                repo=binding.repo,
                auth=binding.auth,
                logger=self._logger,
            )
        except RepositoryNotAccessibleError as exc:
            self._logger.info(
                "Proactive lifecycle: GitHub repository not accessible,"
                " ref_deleted disabled for this pass",
                owner=exc.owner,
                repo=exc.repo,
                installation_id=binding.installation_id,
                project_id=project.id,
                project=project.slug,
            )
            return None
        except RepositoryRefFetchError as exc:
            self._logger.warning(
                "Proactive lifecycle: GitHub ref fetch failed,"
                " ref_deleted disabled for this pass",
                owner=exc.owner,
                repo=exc.repo,
                installation_id=binding.installation_id,
                project_id=project.id,
                project=project.slug,
                error=str(exc),
            )
            return None
        return ref_set.all

    async def _ensure_project(
        self, *, org_id: int, ltd_product: LtdProduct
    ) -> tuple[Organization, Project]:
        """Return ``(org, project)``, creating the project if missing."""
        org = await self._org_store.get_by_id(org_id)
        if org is None:
            msg = f"Organization id={org_id} not found"
            raise NotFoundError(msg)
        existing = await self._project_store.get_by_slug(
            org_id=org_id, slug=ltd_product.slug
        )
        if existing is None:
            # LTD ``doc_repo`` is almost always a github.com URL; route
            # it into the structured ``github`` binding (the single
            # source of truth) and leave ``source_url`` for the rare
            # non-GitHub repo. The ProjectCreate validator rejects a
            # github.com ``source_url`` outright, so this split is
            # mandatory, not cosmetic.
            doc_repo = str(ltd_product.doc_repo)
            parsed = parse_github_url(doc_repo)
            if parsed is not None:
                owner, repo = parsed
                github = ProjectGitHubBindingCreate(owner=owner, repo=repo)
                source_url = None
            else:
                github = None
                source_url = doc_repo
            org, project, _ = await self._project_service.create(
                org_slug=org.slug,
                data=ProjectCreate(
                    slug=ltd_product.slug,
                    title=ltd_product.title,
                    source_url=source_url,
                    github=github,
                    default_edition=DefaultEditionConfig(
                        tracking_mode=TrackingMode.git_ref,
                        tracking_params={"git_ref": "main"},
                    ),
                ),
            )
        else:
            project = existing
        await self._state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.project,
            ltd_slug=ltd_product.slug,
            docverse_id=project.id,
            date_last_synced=_now(),
        )
        return org, project

    async def sync_edition(
        self,
        *,
        org_id: int,
        project: Project,
        ltd_edition: LtdEdition,
        org_slug: str | None = None,
        rewrite_rules: Sequence[AnySlugRewriteRule] = (),
        autocreation: EditionAutocreationConfig | None = None,
    ) -> EditionSyncOutcome:
        """Sync one LTD edition (and its current build) into Docverse.

        ``rewrite_rules`` are the project's resolved slug-rewrite rules,
        consulted when deriving the edition's kind for ``git_refs`` /
        ``manual`` editions. The empty default still picks up the
        built-in version heuristics.

        ``autocreation`` is the project's resolved edition-autocreation
        config, which gates the semver aggregate backfill;
        :meth:`sync_project` resolves it project-over-org once per
        project and passes it in. ``None`` falls back to the built-in
        defaults for the direct callers that do not resolve it.

        Short-circuits *before* ``_ensure_edition`` runs when the
        edition's ``keeper_sync_state`` row is tombstoned. The check
        sits ahead of ``_ensure_edition`` so a tombstoned-but-still-
        soft-deleted edition row cannot trip the ``create_internal``
        slug clash that previously raised "lost ON CONFLICT race"
        (the canonical Docverse uniqueness index ignores
        ``date_deleted``, so a soft-deleted row keeps its slug
        reserved against re-import).
        """
        async with self._session.begin():
            edition_state = await self._state_store.get(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_edition.ltd_id,
                include_tombstoned=True,
            )
        if edition_state is not None and edition_state.date_tombstoned:
            self._logger.info(
                "Sync short-circuited: edition tombstoned",
                ltd_edition_id=ltd_edition.ltd_id,
                ltd_edition_slug=ltd_edition.slug,
                tombstone_reason=edition_state.tombstone_reason,
            )
            return EditionSyncOutcome(
                docverse_edition_id=edition_state.docverse_id,
                docverse_slug=derive_edition_slug(ltd_edition.slug),
                docverse_project_id=project.id,
                docverse_project_slug=project.slug,
                build_outcome=None,
                short_circuited=True,
            )

        # ``manual`` editions need the published build's git_refs to
        # synthesize a Docverse ``git_ref`` tracking pair. Other modes
        # ignore the build for mapping; we skip the extra fetch when we
        # can.
        ltd_build_for_mapping: LtdBuild | None = None
        if (
            ltd_edition.mode == LtdEditionMode.manual
            and ltd_edition.build_url is not None
        ):
            ltd_build_for_mapping = await self._ltd_client.get_build_by_url(
                str(ltd_edition.build_url)
            )

        tracking_mode, tracking_params = map_edition_tracking(
            ltd_edition, build=ltd_build_for_mapping
        )
        kind_derivation = derive_edition_kind(
            ltd_edition,
            git_ref=tracking_params.get("git_ref"),
            rules=rewrite_rules,
        )
        docverse_slug = derive_edition_slug(ltd_edition.slug)
        self._logger.debug(
            "Derived keeper-sync edition kind",
            ltd_edition_id=ltd_edition.ltd_id,
            ltd_edition_slug=ltd_edition.slug,
            ltd_mode=ltd_edition.mode,
            edition_kind=kind_derivation.kind.value,
            kind_source=kind_derivation.source.value,
            kind_detail=kind_derivation.detail,
            docverse_slug=docverse_slug,
            project_id=project.id,
        )

        async with self._session.begin():
            edition = await self._ensure_edition(
                project_id=project.id,
                docverse_slug=docverse_slug,
                kind_derivation=kind_derivation,
                title=ltd_edition.title,
                tracking_mode=tracking_mode,
                tracking_params=tracking_params,
            )
            await self._state_store.upsert(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=ltd_edition.ltd_id,
                ltd_slug=ltd_edition.slug,
                docverse_id=edition.id,
                date_last_synced=_now(),
                date_rebuilt_seen=ltd_edition.date_rebuilt,
                annotations={
                    "ltd_mode": ltd_edition.mode,
                    "ltd_tracked_refs": ltd_edition.tracked_refs,
                },
            )

        build_outcome: BuildSyncOutcome | None = None
        aggregate_outcomes: tuple[AggregateEditionOutcome, ...] = ()
        if ltd_edition.build_url is not None:
            build_outcome = await self.sync_build(
                org_id=org_id,
                org_slug=org_slug,
                project=project,
                edition=edition,
                ltd_edition=ltd_edition,
                ltd_build=ltd_build_for_mapping,
            )
            if build_outcome.docverse_build_id is not None:
                # The backfill runs after ``sync_build`` has committed the
                # build row and repointed the edition, so the edition's
                # import is already durable by the time we get here.
                # Letting a backfill error escape would hand the whole
                # edition to ``sync_project``'s per-edition boundary:
                # no outcome is emitted, so ``on_edition_synced`` never
                # enqueues the publish and the tail-end self-heal (which
                # iterates only ``edition_outcomes``) cannot recover it —
                # a fully-imported edition would be reported failed and
                # its CDN pointer never published this run. Swallow it
                # here instead, exactly as the ``on_edition_synced``
                # callback's failures are swallowed in ``sync_project``:
                # the aggregates are a derived nicety, and the next poll
                # re-runs the backfill for this build anyway.
                try:
                    aggregate_outcomes = (
                        await self._backfill_semver_aggregates(
                            project_id=project.id,
                            git_ref=tracking_params.get("git_ref"),
                            build_id=build_outcome.docverse_build_id,
                            autocreation=(
                                autocreation or DEFAULT_EDITION_AUTOCREATION
                            ),
                        )
                    )
                except Exception as exc:
                    sentry_sdk.capture_exception(exc)
                    self._logger.exception(
                        "Semver aggregate backfill failed; edition sync"
                        " still succeeded",
                        ltd_edition_id=ltd_edition.ltd_id,
                        ltd_edition_slug=ltd_edition.slug,
                        docverse_slug=edition.slug,
                        project_id=project.id,
                        project=project.slug,
                    )

        return EditionSyncOutcome(
            docverse_edition_id=edition.id,
            # Report the *persisted* edition's slug, not the keeper-derived
            # ``docverse_slug``. When ``_ensure_edition`` adopts a
            # differently-slugged native edition on the same git_ref (PRD
            # #409: native ``tickets-DM-54686`` vs keeper ``DM-54686``), the
            # two disagree. Both publish paths key off
            # ``outcome.docverse_slug`` via ``get_by_slug``
            # (publish_edition / _resolve_self_heal_target), so reporting the
            # keeper slug would miss the row — the build would fail to
            # publish (NotFoundError) or be silently skipped.
            docverse_slug=edition.slug,
            docverse_project_id=project.id,
            docverse_project_slug=project.slug,
            build_outcome=build_outcome,
            short_circuited=False,
            aggregate_outcomes=aggregate_outcomes,
        )

    async def _backfill_semver_aggregates(
        self,
        *,
        project_id: int,
        git_ref: str | None,
        build_id: int,
        autocreation: EditionAutocreationConfig,
    ) -> tuple[AggregateEditionOutcome, ...]:
        """Create/point the ``N`` / ``N.M`` aggregates for a release.

        The migration counterpart of
        ``EditionTrackingService._auto_create_version_editions``: without
        it, an imported project's dashboard would list bare release
        editions where a natively built one groups them under ``15`` and
        ``15.2``. Both paths build their rows from
        :func:`~docverse_server.domain.semver_aggregate.semver_aggregate_specs`
        so they cannot drift.

        The gate is the ref's own semver grammar
        (:func:`~docverse_server.domain.version.parse_stable_semver`) —
        neither the persisted kind nor the rule-derived one. The
        persisted kind would couple the aggregates to unrelated manual
        PATCHes of a single edition; the derived kind would let any
        user rule that merely reshapes a slug shadow the built-in
        ``SemverRule`` and silently switch the backfill off, since user
        rules match first. The native path gates on the same
        classification, so a migrated project keeps rendering the same
        dashboard groups as a natively built one, and
        ``edition_autocreation.semver_aggregates`` stays the explicit
        opt-out on both.

        Returns one outcome per aggregate this call actually moved, for
        the worker to publish; see :class:`AggregateEditionOutcome`.
        """
        if not autocreation.semver_aggregates:
            return ()
        version = parse_stable_semver(git_ref)
        if version is None:
            return ()
        specs = semver_aggregate_specs(version)
        if not specs:
            return ()
        outcomes: list[AggregateEditionOutcome] = []
        async with self._session.begin():
            build = await self._build_store.get_by_id(build_id)
            if build is not None:
                for spec in specs:
                    outcome = await self._ensure_aggregate_edition(
                        project_id=project_id,
                        spec=spec,
                        build=build,
                        version=version,
                    )
                    if outcome is not None:
                        outcomes.append(outcome)
        return tuple(outcomes)

    async def _ensure_aggregate_edition(
        self,
        *,
        project_id: int,
        spec: SemverAggregateSpec,
        build: Build,
        version: SemverVersion,
    ) -> AggregateEditionOutcome | None:
        """Ensure one aggregate exists and points at *build*.

        Returns ``None`` — leaving the row untouched — when the slug is
        occupied by an edition that is not this aggregate (an operator's
        own ``15``, or a natively created one on a different tracking
        mode), when the version guard rejects an older release, or when
        the pointer is already where it should be.
        """
        edition = await self._edition_store.get_by_slug(
            project_id=project_id, slug=spec.slug
        )
        if edition is None:
            edition = await self._edition_store.create_internal(
                project_id=project_id,
                slug=spec.slug,
                title=spec.title,
                kind=spec.kind,
                tracking_mode=spec.tracking_mode,
                tracking_params=spec.tracking_params,
            )
            self._logger.info(
                "Auto-created semver aggregate edition for synced release",
                project_id=project_id,
                edition_id=edition.id,
                edition_slug=edition.slug,
                edition_kind=spec.kind.value,
                tracking_mode=spec.tracking_mode.value,
            )
        # Also catches ``create_internal`` losing its ON CONFLICT race to
        # a concurrent writer that inserted a differently-tracked row.
        if edition.tracking_mode != spec.tracking_mode:
            self._logger.info(
                "Semver aggregate slug held by a non-aggregate edition;"
                " leaving it untouched",
                project_id=project_id,
                edition_id=edition.id,
                edition_slug=edition.slug,
                edition_kind=edition.kind.value,
                tracking_mode=edition.tracking_mode.value,
            )
            return None
        if not _aggregate_accepts(edition, version):
            return None
        if edition.current_build_id == build.id:
            return None
        updated = await self._edition_store.set_current_build(
            edition_id=edition.id, build_id=build.id, skip_date_guard=True
        )
        if updated is None:
            return None
        self._logger.info(
            "Pointed semver aggregate at synced release build",
            project_id=project_id,
            edition_id=edition.id,
            edition_slug=edition.slug,
            build_id=build.id,
        )
        return AggregateEditionOutcome(
            docverse_edition_id=edition.id,
            docverse_slug=edition.slug,
            docverse_build_id=build.id,
            docverse_build_public_id=serialize_base32_id(build.public_id),
        )

    async def _ensure_edition(
        self,
        *,
        project_id: int,
        docverse_slug: str,
        kind_derivation: EditionKindDerivation,
        title: str,
        tracking_mode: TrackingMode,
        tracking_params: dict[str, Any],
    ) -> Edition:
        """Look up or create an edition; refresh its tracking and kind.

        ``kind_derivation`` seeds a newly created edition's kind
        outright. On an existing edition — whether matched by slug,
        adopted by ``git_ref``, or handed back by a ``create_internal``
        that lost its ``ON CONFLICT`` race — it is applied through
        :meth:`_refresh_kind`, which only ever promotes.
        """
        edition = await self._edition_store.get_by_slug(
            project_id=project_id, slug=docverse_slug
        )
        if edition is None and docverse_slug == DEFAULT_EDITION_SLUG:
            msg = (
                f"Default edition {DEFAULT_EDITION_SLUG!r} missing for "
                f"project_id={project_id}; project creation should have"
                " auto-created it"
            )
            raise RuntimeError(msg)
        if edition is None:
            # PRD #409: native auto-creation and keeper-sync derive an
            # edition's slug from a branch differently, so the same
            # ``git_ref`` can be slugged two ways (native
            # ``tickets-DM-54686`` vs keeper-sync ``DM-54686``). After a
            # slug miss, consult the shared git_ref lookup: if a
            # differently-slugged edition already tracks this ref, adopt
            # it — refresh its tracking and keep its slug — rather than
            # insert a duplicate row on the same ref.
            if tracking_mode == TrackingMode.git_ref:
                adopted = (
                    await self._edition_store.get_git_ref_tracking_edition(
                        project_id=project_id,
                        git_ref=tracking_params["git_ref"],
                    )
                )
                if adopted is not None:
                    await self._refresh_tracking(
                        edition=adopted,
                        tracking_mode=tracking_mode,
                        tracking_params=tracking_params,
                    )
                    self._logger.info(
                        "Adopted existing git_ref edition for keeper-sync"
                        " import",
                        project_id=project_id,
                        git_ref=tracking_params["git_ref"],
                        adopted_slug=adopted.slug,
                        keeper_slug=docverse_slug,
                        edition_id=adopted.id,
                    )
                    return await self._refresh_kind(
                        edition=adopted, kind_derivation=kind_derivation
                    )
            edition = await self._edition_store.create_internal(
                project_id=project_id,
                slug=docverse_slug,
                title=title,
                kind=kind_derivation.kind,
                tracking_mode=tracking_mode,
                tracking_params=tracking_params,
            )
        # Applied on both paths. ``create_internal`` is race-tolerant: a
        # concurrent writer (e.g. the native ``track_build`` path handling
        # an upload for the same ref) that inserted this
        # ``(project, lower(slug))`` between our SELECT and INSERT wins the
        # ``ON CONFLICT DO NOTHING``, and we get *its* row back — an
        # existing edition exactly like a ``get_by_slug`` hit, whose
        # tracking columns and kind still need realigning with LTD. The
        # store returns the winner verbatim with no "did I insert?" signal,
        # so both refreshes run unconditionally; on a genuinely fresh row
        # they are no-ops, since it already carries these tracking params
        # and ``_refresh_kind`` is promote-only (its ``(kind, kind)`` pair
        # is never a promotion).
        await self._refresh_tracking(
            edition=edition,
            tracking_mode=tracking_mode,
            tracking_params=tracking_params,
        )
        return await self._refresh_kind(
            edition=edition, kind_derivation=kind_derivation
        )

    async def _refresh_tracking(
        self,
        *,
        edition: Edition,
        tracking_mode: TrackingMode,
        tracking_params: dict[str, Any],
    ) -> None:
        """Realign an existing edition's tracking columns with LTD."""
        await self._edition_store.update_tracking(
            edition_id=edition.id,
            tracking_mode=tracking_mode,
            tracking_params=tracking_params,
        )

    async def _refresh_kind(
        self,
        *,
        edition: Edition,
        kind_derivation: EditionKindDerivation,
    ) -> Edition:
        """Apply a freshly derived kind to an existing edition.

        Promote-only per the shared
        :func:`~docverse_server.domain.edition_kind.is_kind_promotion`
        policy — the same gate the native ``track_build`` path applies —
        so the only write this can make is ``draft`` -> ``release``, and
        only on an edition whose kind no operator has pinned by hand.
        Everything else is a no-op that returns ``edition`` unchanged.

        Returns the edition with the promoted kind applied so the caller
        reports the row's post-sync state rather than the stale read.
        """
        if not is_kind_promotion(
            edition.kind,
            kind_derivation.kind,
            kind_manually_set=edition.kind_manually_set,
        ):
            return edition
        await self._edition_store.update_kind(
            edition_id=edition.id, kind=kind_derivation.kind
        )
        self._logger.info(
            "Promoted keeper-sync edition kind",
            project_id=edition.project_id,
            edition_id=edition.id,
            edition_slug=edition.slug,
            previous_kind=edition.kind.value,
            edition_kind=kind_derivation.kind.value,
            kind_source=kind_derivation.source.value,
            kind_detail=kind_derivation.detail,
        )
        return edition.model_copy(update={"kind": kind_derivation.kind})

    async def sync_build(
        self,
        *,
        org_id: int,
        project: Project,
        edition: Edition,
        ltd_edition: LtdEdition,
        ltd_build: LtdBuild | None = None,
        org_slug: str | None = None,
    ) -> BuildSyncOutcome:
        """Sync the LTD edition's current build into Docverse.

        Short-circuits when ``keeper_sync_state`` already records a
        Docverse build whose ``date_rebuilt_seen`` matches LTD's.

        ``ltd_build`` may be passed in by callers that already fetched
        the build (e.g. ``sync_edition`` does so for ``manual`` editions
        to derive the tracking pair). Skipping the refetch saves a round
        trip to LTD.

        The bucket prefix the content is read from is resolved once, by
        :meth:`_resolve_build_source`, and that one prefix feeds both the
        manifest hash and the copy — see that method for the
        ``AccessDenied`` recovery it performs.
        """
        if ltd_edition.build_url is None:
            msg = "sync_build requires an edition with build_url set"
            raise ValueError(msg)
        if ltd_build is None:
            ltd_build = await self._ltd_client.get_build_by_url(
                str(ltd_edition.build_url)
            )
        if not ltd_build.uploaded:
            msg = (
                f"LTD build id={ltd_build.ltd_id} for edition"
                f" {ltd_edition.slug!r} reports uploaded=False; refusing"
                " to sync a half-uploaded build"
            )
            raise RuntimeError(msg)

        async with self._session.begin():
            existing_state = await self._state_store.get(
                org_id=org_id,
                resource_type=ResourceType.build,
                ltd_id=ltd_build.ltd_id,
            )

        if existing_state is not None and (
            existing_state.date_rebuilt_seen == ltd_edition.date_rebuilt
            and existing_state.docverse_id is not None
        ):
            async with self._session.begin():
                await self._state_store.upsert(
                    org_id=org_id,
                    resource_type=ResourceType.build,
                    ltd_id=ltd_build.ltd_id,
                    ltd_slug=ltd_build.slug,
                    date_last_synced=_now(),
                )
            self._logger.info(
                "Sync short-circuited: state matches LTD",
                ltd_build_id=ltd_build.ltd_id,
                edition_slug=edition.slug,
            )
            return BuildSyncOutcome(
                docverse_build_id=existing_state.docverse_id,
                docverse_build_public_id=None,
                short_circuited=True,
                content_hash=existing_state.content_hash,
                object_count=None,
                total_size_bytes=None,
            )

        source = await self._resolve_build_source(
            ltd_build=ltd_build, ltd_edition=ltd_edition, project=project
        )
        manifest_hash = source.manifest_hash
        async with self._session.begin():
            existing_build = (
                await self._build_store.get_completed_by_content_hash(
                    project_id=project.id, content_hash=manifest_hash
                )
            )
            if existing_build is not None:
                if edition.current_build_id != existing_build.id:
                    # Convergence intentionally targets the oldest
                    # completed build with this content hash so the
                    # canonical row is stable; bypass the date guard
                    # because re-pointing the edition backwards to that
                    # row is the de-duplicating choice when the user-
                    # visible content is identical.
                    await self._edition_store.set_current_build(
                        edition_id=edition.id,
                        build_id=existing_build.id,
                        skip_date_guard=True,
                    )
                await self._state_store.upsert(
                    org_id=org_id,
                    resource_type=ResourceType.build,
                    ltd_id=ltd_build.ltd_id,
                    ltd_slug=ltd_build.slug,
                    docverse_id=existing_build.id,
                    date_last_synced=_now(),
                    date_rebuilt_seen=ltd_edition.date_rebuilt,
                    content_hash=manifest_hash,
                    annotations=source.annotations,
                )
        if existing_build is not None:
            self._logger.info(
                "Sync converged on existing Docverse build",
                ltd_build_id=ltd_build.ltd_id,
                docverse_build_public_id=serialize_base32_id(
                    existing_build.public_id
                ),
                edition_slug=edition.slug,
                content_hash=manifest_hash,
            )
            return BuildSyncOutcome(
                docverse_build_id=existing_build.id,
                docverse_build_public_id=serialize_base32_id(
                    existing_build.public_id
                ),
                short_circuited=True,
                content_hash=manifest_hash,
                object_count=None,
                total_size_bytes=None,
            )

        async with self._session.begin():
            await self._reclaim_orphan_placeholders(
                project_id=project.id,
                project_slug=project.slug,
                org_slug=org_slug,
                edition_slug=edition.slug,
                ltd_edition=ltd_edition,
            )
            new_build = await self._create_synced_build(
                project=project, ltd_edition=ltd_edition
            )

        copy_result = await self._copy_callable(
            source.prefix, new_build.storage_prefix
        )

        async with self._session.begin():
            await self._finalize_synced_build(
                build=new_build,
                edition=edition,
                project_slug=project.slug,
                org_slug=org_slug,
                copy_result=copy_result,
            )
            await self._state_store.upsert(
                org_id=org_id,
                resource_type=ResourceType.build,
                ltd_id=ltd_build.ltd_id,
                ltd_slug=ltd_build.slug,
                docverse_id=new_build.id,
                date_last_synced=_now(),
                date_rebuilt_seen=ltd_edition.date_rebuilt,
                content_hash=copy_result.content_hash,
                annotations=source.annotations,
            )

        self._logger.info(
            "Synced LTD build into Docverse",
            ltd_build_id=ltd_build.ltd_id,
            docverse_build_public_id=serialize_base32_id(new_build.public_id),
            edition_slug=edition.slug,
            content_hash=copy_result.content_hash,
            ltd_source_prefix=source.prefix,
            ltd_source_prefix_origin=source.annotations[
                "ltd_source_prefix_origin"
            ],
        )
        return BuildSyncOutcome(
            docverse_build_id=new_build.id,
            docverse_build_public_id=serialize_base32_id(new_build.public_id),
            short_circuited=False,
            content_hash=copy_result.content_hash,
            object_count=copy_result.object_count,
            total_size_bytes=copy_result.total_size_bytes,
        )

    async def _resolve_build_source(
        self,
        *,
        ltd_build: LtdBuild,
        ltd_edition: LtdEdition,
        project: Project,
    ) -> _ResolvedBuildSource:
        """Pick the LTD prefix this build's content is readable from.

        ``LtdBuild.bucket_root_dir`` — ``<product>/builds/<build-slug>/``
        — is the primary source and stays the source everywhere it
        works, so already-synced projects re-hash to exactly the same
        value and nothing is re-copied.

        LTD's earliest uploads, though, wrote those build objects with no
        public-read ACL, and :class:`~docverse_server.storage.ltd.LtdS3Source`
        is deliberately anonymous, so every ``GetObject`` under the
        prefix answers ``AccessDenied`` (#516). LTD's publish step also
        copied the same bytes to the edition prefix,
        ``<product>/v/<edition-slug>/``, and *that* copy is public —
        it is what Fastly serves. Measured on documenteer 2026-08-12:
        1,863 objects across the 19 denied editions, 100% readable from
        the edition prefix, object counts matching the build prefixes
        exactly.

        So a denial — and *only* a denial — falls back to the edition
        prefix. An empty or missing build prefix hashes to the empty
        manifest as before rather than silently importing whatever the
        edition prefix holds, and the default edition has no ``v/``
        sibling to fall back to at all (LTD serves it from the product
        root), which :func:`derive_edition_source_prefix` enforces.

        A fallback that finds nothing — denied again, or an edition
        prefix with no keys under it — re-raises the denial, leaving the
        edition to ``sync_project``'s per-edition failure boundary
        exactly as before this recovery path existed.
        """
        build_prefix = _ensure_trailing_slash(ltd_build.bucket_root_dir)
        try:
            manifest_hash = await self._manifest_callable(build_prefix)
        except LtdSourceAccessDeniedError:
            edition_prefix = derive_edition_source_prefix(
                bucket_root_dir=ltd_build.bucket_root_dir,
                ltd_edition_slug=ltd_edition.slug,
            )
            if edition_prefix is None:
                raise
            self._logger.info(
                "LTD build prefix is unreadable; retrying from the"
                " edition prefix",
                ltd_build_id=ltd_build.ltd_id,
                ltd_edition_slug=ltd_edition.slug,
                ltd_build_prefix=build_prefix,
                ltd_edition_prefix=edition_prefix,
                project=project.slug,
            )
            manifest_hash = await self._manifest_callable(edition_prefix)
            if manifest_hash == EMPTY_MANIFEST_HASH:
                # Nothing was published under the edition prefix either.
                # Importing the empty manifest would finalize a
                # zero-object build and leave the edition serving 404s
                # while the sync reported success, so the denial stands
                # and the edition stays retryable.
                self._logger.warning(
                    "LTD edition prefix is empty; cannot recover the"
                    " unreadable build",
                    ltd_build_id=ltd_build.ltd_id,
                    ltd_edition_slug=ltd_edition.slug,
                    ltd_build_prefix=build_prefix,
                    ltd_edition_prefix=edition_prefix,
                    project=project.slug,
                )
                raise
            self._logger.info(
                "Recovered LTD build content from the edition prefix",
                ltd_build_id=ltd_build.ltd_id,
                ltd_edition_slug=ltd_edition.slug,
                ltd_build_prefix=build_prefix,
                ltd_edition_prefix=edition_prefix,
                content_hash=manifest_hash,
                project=project.slug,
            )
            return _ResolvedBuildSource(
                prefix=edition_prefix,
                manifest_hash=manifest_hash,
                from_edition_prefix=True,
            )
        return _ResolvedBuildSource(
            prefix=build_prefix,
            manifest_hash=manifest_hash,
            from_edition_prefix=False,
        )

    async def _reclaim_orphan_placeholders(
        self,
        *,
        project_id: int,
        ltd_edition: LtdEdition,
        project_slug: str | None = None,
        org_slug: str | None = None,
        edition_slug: str | None = None,
    ) -> None:
        """Fail stale ``pending`` placeholders left by a crashed prior run.

        ``sync_build`` writes the placeholder build row in one
        transaction, copies bucket content outside the database, then
        finalizes the build in a second transaction. A worker that
        crashes between those two commits leaves a ``pending`` build
        row behind that will never advance. Without reclaim, every
        retry simply adds another orphan. This finds prior orphans for
        the same ``(project_id, git_ref)`` that were created with the
        keeper-sync uploader more than
        ``self._orphan_reclaim_max_age`` ago and transitions them to
        ``failed`` so they stop showing up in the project's build
        list.
        """
        if not ltd_edition.tracked_refs:
            return
        git_ref = ltd_edition.tracked_refs[0]
        cutoff = _now() - self._orphan_reclaim_max_age
        orphans = await self._build_store.list_pending_older_than(
            project_id=project_id,
            git_ref=git_ref,
            uploader=_SYNC_UPLOADER,
            older_than=cutoff,
        )
        if not orphans:
            return
        reclaimed_ids: list[int] = []
        for orphan in orphans:
            await self._build_store.transition_status(
                build_id=orphan.id,
                new_status=BuildStatus.failed,
                org_slug=org_slug,
                project_slug=project_slug,
                edition_slug=edition_slug,
            )
            reclaimed_ids.append(orphan.id)
        self._logger.warning(
            "Reclaimed orphaned keeper-sync placeholder builds",
            reclaimed_build_ids=reclaimed_ids,
            project_id=project_id,
            git_ref=git_ref,
        )

    async def _create_synced_build(
        self, *, project: Project, ltd_edition: LtdEdition
    ) -> Build:
        """Insert a placeholder build row that the copier can write into."""
        if not ltd_edition.tracked_refs:
            msg = (
                f"LTD edition {ltd_edition.slug!r} has no tracked_refs;"
                " cannot derive git_ref for synced build"
            )
            raise ValueError(msg)
        return await self._build_store.create(
            project_id=project.id,
            project_slug=project.slug,
            data=BuildCreate(
                git_ref=ltd_edition.tracked_refs[0],
                content_hash=_PLACEHOLDER_CONTENT_HASH,
            ),
            uploader=_SYNC_UPLOADER,
        )

    async def _finalize_synced_build(
        self,
        *,
        build: Build,
        edition: Edition,
        copy_result: CopyResult,
        project_slug: str | None = None,
        org_slug: str | None = None,
    ) -> None:
        """Mark the build complete and atomically point the edition at it.

        Runs inside a single ``session.begin()`` so a crash between the
        build-side update and the edition-side update cannot leave the
        edition pointing at a build that does not exist or vice versa.
        """
        await self._build_store.update_content_hash(
            build_id=build.id,
            content_hash=copy_result.content_hash,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slug=edition.slug,
        )
        await self._build_store.update_inventory(
            build_id=build.id,
            object_count=copy_result.object_count,
            total_size_bytes=copy_result.total_size_bytes,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slug=edition.slug,
        )
        await self._build_store.transition_status(
            build_id=build.id,
            new_status=BuildStatus.processing,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slug=edition.slug,
        )
        await self._build_store.transition_status(
            build_id=build.id,
            new_status=BuildStatus.completed,
            org_slug=org_slug,
            project_slug=project_slug,
            edition_slug=edition.slug,
        )
        await self._edition_store.set_current_build(
            edition_id=edition.id,
            build_id=build.id,
            skip_date_guard=True,
        )


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _ensure_trailing_slash(prefix: str) -> str:
    return prefix if prefix.endswith("/") else f"{prefix}/"


def _aggregate_accepts(edition: Edition, candidate: SemverVersion) -> bool:
    """Report whether *candidate* may advance an aggregate's pointer.

    LTD hands editions back in no particular version order, so without a
    guard a project with 81 releases would leave ``15`` pointing at
    whichever ``15.x.y`` happened to sync last.

    The policy itself lives in
    :func:`~docverse_server.domain.version.accepts_version_advance`,
    shared with the version-mode arm of
    ``EditionTrackingService._should_update`` so a change to one cannot
    drift migrated aggregate pointers away from natively built ones.
    Callers reach here only once ``edition.tracking_mode`` has been
    confirmed to equal the aggregate spec's, so the mode is always a
    semver one.
    """
    return accepts_version_advance(
        candidate=candidate,
        current_build_id=edition.current_build_id,
        current_build_git_ref=edition.current_build_git_ref,
        mode=edition.tracking_mode,
    )


def _resolve_rewrite_rules(
    *, org: Organization, project: Project
) -> list[AnySlugRewriteRule]:
    """Resolve the project's effective slug-rewrite rules.

    Mirrors :class:`~docverse_server.services.edition_tracking.EditionTrackingService`:
    a project's rule list, when set, entirely replaces the org's — there
    is no merging (SQR-112 inheritance rule). "Set" means not ``None``,
    so an explicit ``[]`` opts the project out of the org's rules.
    """  # noqa: E501
    return resolve_slug_rewrite_rules(
        project=project.slug_rewrite_rules,
        org=org.slug_rewrite_rules,
    )


def _transient_edition_from_ltd(
    *,
    ltd_edition: LtdEdition,
    project_id: int,
    rewrite_rules: Sequence[AnySlugRewriteRule] = (),
) -> Edition | None:
    """Build a transient ``Edition`` from an LTD edition for proactive eval.

    The proactive lifecycle pass evaluates :func:`evaluate_lifecycle`
    against editions that may not have a Docverse row yet, so we
    synthesise a domain :class:`Edition` from the LTD edition's
    metadata. Only ``draft_inactivity`` and ``ref_deleted`` participate
    in the proactive pass, so the transient only needs the fields
    those two predicates read: ``kind``, ``tracking_mode``,
    ``tracking_params``, ``lifecycle_exempt``, ``date_deleted``, and
    ``date_updated``.

    ``kind`` in particular must come from the same mode-first
    derivation :meth:`KeeperSyncService.sync_edition` uses, or the
    proactive pass would evaluate a release-to-be as a ``draft`` and
    tombstone it ``lifecycle_preemptive`` before it is ever imported.

    Returns ``None`` when the LTD edition's mode requires the
    currently-published build to map (``manual``) — the proactive
    evaluator deliberately does not fetch builds (defeats the
    bandwidth-saving point). Those editions fall through to
    ``sync_edition`` and the regular ``lifecycle_eval`` pass handles
    them post-import. ``date_updated`` mirrors LTD's ``date_rebuilt``
    when set (LTD's analogue of Docverse's edition-touch timestamp)
    and falls back to ``date_created`` otherwise.
    """
    try:
        tracking_mode, tracking_params = map_edition_tracking(
            ltd_edition, build=None
        )
    except ValueError:
        return None
    return Edition(
        id=ltd_edition.ltd_id,
        slug=derive_edition_slug(ltd_edition.slug),
        title=ltd_edition.title,
        project_id=project_id,
        kind=derive_edition_kind(
            ltd_edition,
            git_ref=tracking_params.get("git_ref"),
            rules=rewrite_rules,
        ).kind,
        tracking_mode=tracking_mode,
        tracking_params=tracking_params or None,
        lifecycle_exempt=False,
        date_created=ltd_edition.date_created,
        date_updated=ltd_edition.date_rebuilt or ltd_edition.date_created,
        date_deleted=None,
    )
