"""High-level orchestration for one LTD product / edition / build slice.

The :class:`KeeperSyncService` is the tracer-bullet entry point that
walks one project end-to-end: it fetches LTD's view of the resource
through :class:`docverse.storage.ltd.LtdClient`, looks up or
creates the matching Docverse rows by delegating to the existing
``ProjectService`` / ``BuildStore`` / ``EditionStore``, and copies the
build content into Docverse R2 via :class:`BuildContentCopier`. The
:class:`KeeperSyncStateStore` row is the idempotency key: a re-run
with unchanged LTD state short-circuits.

This slice covers only the ``git_refs`` LTD edition mode; other modes
raise :class:`NotImplementedError` from
:func:`docverse.services.keeper_sync.mappers.map_edition_tracking` and
are filled in by issue #289.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import sentry_sdk
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    BuildCreate,
    BuildStatus,
    EditionKind,
    ProjectCreate,
    ProjectGitHubBindingCreate,
    TrackingMode,
)
from docverse.client.models.editions import DefaultEditionConfig
from docverse.client.models.projects import parse_github_url
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.lifecycle import DraftInactivityRule, RefDeletedRule
from docverse.domain.organization import Organization
from docverse.domain.project import Project
from docverse.exceptions import KeeperSyncInvariantError, NotFoundError
from docverse.services.keeper_sync_tombstone import KeeperSyncTombstoneService
from docverse.services.lifecycle.evaluator import (
    LifecycleEvaluationContext,
    evaluate_lifecycle,
    filter_rule_set,
    resolve_rule_set,
)
from docverse.services.project import DEFAULT_EDITION_SLUG, ProjectService
from docverse.services.project_github_binding import (
    ProjectGitHubBindingResolver,
)
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.github import (
    GitHubRefSetFetcher,
    RepositoryNotAccessibleError,
    RepositoryRefFetchError,
)
from docverse.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
    TombstoneReason,
)
from docverse.storage.ltd import (
    LtdBuild,
    LtdClient,
    LtdEdition,
    LtdEditionMode,
    LtdProduct,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

from .copier import CopyResult
from .mappers import (
    derive_edition_kind,
    derive_edition_slug,
    map_edition_tracking,
)

__all__ = [
    "DEFAULT_ORPHAN_RECLAIM_MAX_AGE",
    "BuildSyncOutcome",
    "CopyCallable",
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


@dataclass(frozen=True)
class ProjectSyncResult:
    """What ``sync_project`` did with one LTD product.

    ``docverse_project_id`` is ``None`` only when the call
    short-circuited on a tombstoned ``keeper_sync_state`` project row
    whose ``docverse_id`` was never populated — practically a
    defensive shape; the only production path that writes a project
    tombstone is the manual-delete chokepoint, which always carries a
    Docverse project id.
    """

    docverse_project_id: int | None
    docverse_project_slug: str
    edition_outcomes: list[EditionSyncOutcome]


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

    def __init__(  # noqa: PLR0913
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
        ltd_editions = await self._ltd_client.list_editions_for_product(
            ltd_slug
        )
        skip_ltd_ids = await self._proactive_lifecycle_pass(
            org=org,
            project=project,
            ltd_editions=ltd_editions,
        )
        outcomes: list[EditionSyncOutcome] = []
        for ltd_edition in ltd_editions:
            if ltd_edition.ltd_id in skip_ltd_ids:
                continue
            outcome = await self.sync_edition(
                org_id=org.id,
                org_slug=org.slug,
                project=project,
                ltd_edition=ltd_edition,
            )
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
        )

    async def _proactive_lifecycle_pass(
        self,
        *,
        org: Organization,
        project: Project,
        ltd_editions: list[LtdEdition],
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
                ltd_edition=ltd_edition, project_id=project.id
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
    ) -> EditionSyncOutcome:
        """Sync one LTD edition (and its current build) into Docverse.

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
        kind = derive_edition_kind(ltd_edition.slug)
        docverse_slug = derive_edition_slug(ltd_edition.slug)

        async with self._session.begin():
            edition = await self._ensure_edition(
                project_id=project.id,
                docverse_slug=docverse_slug,
                kind=kind,
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
        if ltd_edition.build_url is not None:
            build_outcome = await self.sync_build(
                org_id=org_id,
                org_slug=org_slug,
                project=project,
                edition=edition,
                ltd_edition=ltd_edition,
                ltd_build=ltd_build_for_mapping,
            )

        return EditionSyncOutcome(
            docverse_edition_id=edition.id,
            docverse_slug=docverse_slug,
            docverse_project_id=project.id,
            docverse_project_slug=project.slug,
            build_outcome=build_outcome,
        )

    async def _ensure_edition(  # noqa: PLR0913
        self,
        *,
        project_id: int,
        docverse_slug: str,
        kind: EditionKind,
        title: str,
        tracking_mode: TrackingMode,
        tracking_params: dict[str, Any],
    ) -> Edition:
        """Look up or create an edition; refresh its tracking config."""
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
            edition = await self._edition_store.create_internal(
                project_id=project_id,
                slug=docverse_slug,
                title=title,
                kind=kind,
                tracking_mode=tracking_mode,
                tracking_params=tracking_params,
            )
        else:
            await self._refresh_tracking(
                edition=edition,
                tracking_mode=tracking_mode,
                tracking_params=tracking_params,
            )
        return edition

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

    async def sync_build(  # noqa: PLR0913
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

        manifest_hash = await self._manifest_callable(
            _ensure_trailing_slash(ltd_build.bucket_root_dir)
        )
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
            _ensure_trailing_slash(ltd_build.bucket_root_dir),
            new_build.storage_prefix,
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
            )

        self._logger.info(
            "Synced LTD build into Docverse",
            ltd_build_id=ltd_build.ltd_id,
            docverse_build_public_id=serialize_base32_id(new_build.public_id),
            edition_slug=edition.slug,
            content_hash=copy_result.content_hash,
        )
        return BuildSyncOutcome(
            docverse_build_id=new_build.id,
            docverse_build_public_id=serialize_base32_id(new_build.public_id),
            short_circuited=False,
            content_hash=copy_result.content_hash,
            object_count=copy_result.object_count,
            total_size_bytes=copy_result.total_size_bytes,
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


def _transient_edition_from_ltd(
    *, ltd_edition: LtdEdition, project_id: int
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
        kind=derive_edition_kind(ltd_edition.slug),
        tracking_mode=tracking_mode,
        tracking_params=tracking_params or None,
        lifecycle_exempt=False,
        date_created=ltd_edition.date_created,
        date_updated=ltd_edition.date_rebuilt or ltd_edition.date_created,
        date_deleted=None,
    )
