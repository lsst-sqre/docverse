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

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    BuildCreate,
    BuildStatus,
    EditionKind,
    ProjectCreate,
    TrackingMode,
)
from docverse.client.models.editions import DefaultEditionConfig
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.organization import Organization
from docverse.domain.project import Project
from docverse.exceptions import NotFoundError
from docverse.services.project import DEFAULT_EDITION_SLUG, ProjectService
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
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
    """

    docverse_edition_id: int
    docverse_slug: str
    docverse_project_id: int
    docverse_project_slug: str
    build_outcome: BuildSyncOutcome | None


@dataclass(frozen=True)
class ProjectSyncResult:
    """What ``sync_project`` did with one LTD product."""

    docverse_project_id: int
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
        """
        ltd_product = await self._ltd_client.get_product(ltd_slug)
        async with self._session.begin():
            org, project = await self._ensure_project(
                org_id=org_id, ltd_product=ltd_product
            )
        ltd_editions = await self._ltd_client.list_editions_for_product(
            ltd_slug
        )
        outcomes: list[EditionSyncOutcome] = []
        for ltd_edition in ltd_editions:
            outcome = await self.sync_edition(
                org_id=org.id,
                project=project,
                ltd_edition=ltd_edition,
            )
            outcomes.append(outcome)
            if on_edition_synced is not None:
                try:
                    await on_edition_synced(outcome)
                except Exception:
                    self._logger.exception(
                        "on_edition_synced callback raised; continuing",
                        docverse_slug=outcome.docverse_slug,
                    )
        return ProjectSyncResult(
            docverse_project_id=project.id,
            docverse_project_slug=project.slug,
            edition_outcomes=outcomes,
        )

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
            org, project, _ = await self._project_service.create(
                org_slug=org.slug,
                data=ProjectCreate(
                    slug=ltd_product.slug,
                    title=ltd_product.title,
                    doc_repo=str(ltd_product.doc_repo),
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
    ) -> EditionSyncOutcome:
        """Sync one LTD edition (and its current build) into Docverse."""
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

    async def sync_build(
        self,
        *,
        org_id: int,
        project: Project,
        edition: Edition,
        ltd_edition: LtdEdition,
        ltd_build: LtdBuild | None = None,
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
                project_id=project.id, ltd_edition=ltd_edition
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
        self, *, project_id: int, ltd_edition: LtdEdition
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
                build_id=orphan.id, new_status=BuildStatus.failed
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
    ) -> None:
        """Mark the build complete and atomically point the edition at it.

        Runs inside a single ``session.begin()`` so a crash between the
        build-side update and the edition-side update cannot leave the
        edition pointing at a build that does not exist or vice versa.
        """
        await self._build_store.update_content_hash(
            build_id=build.id, content_hash=copy_result.content_hash
        )
        await self._build_store.update_inventory(
            build_id=build.id,
            object_count=copy_result.object_count,
            total_size_bytes=copy_result.total_size_bytes,
        )
        await self._build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.processing
        )
        await self._build_store.transition_status(
            build_id=build.id, new_status=BuildStatus.completed
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
