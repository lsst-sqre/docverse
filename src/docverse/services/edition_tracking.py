"""Service for edition tracking — matching builds to editions."""

from __future__ import annotations

from typing import Literal

import structlog

from docverse.client.models import EditionKind, TrackingMode
from docverse.domain.build import Build
from docverse.domain.edition import Edition
from docverse.domain.edition_tracking import (
    EditionTrackingOutcome,
    EditionTrackingResult,
)
from docverse.domain.slug import (
    InvalidSlugError,
    SlugDerivationResult,
    derive_edition_slug,
    parse_slug_rewrite_rules,
)
from docverse.domain.version import (
    LsstDocVersion,
    SemverVersion,
    parse_version_for_mode,
)
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

# Tracking modes that use version comparison instead of date-based guards.
_VERSION_MODES = frozenset(
    {
        TrackingMode.semver_release,
        TrackingMode.semver_major,
        TrackingMode.semver_minor,
        TrackingMode.eups_major_release,
        TrackingMode.eups_weekly_release,
        TrackingMode.eups_daily_release,
        TrackingMode.lsst_doc,
    }
)


class EditionTrackingService:
    """Orchestrate edition tracking when a build completes.

    Derives an edition slug from the build's git ref, finds or
    auto-creates matching editions, updates their pointers (with a
    stale-build guard), and records build history.
    """

    def __init__(
        self,
        edition_store: EditionStore,
        history_store: EditionBuildHistoryStore,
        project_store: ProjectStore,
        org_store: OrganizationStore,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._edition_store = edition_store
        self._history_store = history_store
        self._project_store = project_store
        self._org_store = org_store
        self._logger = logger

    async def track_build(self, build: Build) -> EditionTrackingResult:
        """Evaluate tracking rules for a completed build.

        Parameters
        ----------
        build
            The build to track.

        Returns
        -------
        EditionTrackingResult
            Which editions were updated, created, or skipped.

        Raises
        ------
        RuntimeError
            If the build's project or organization cannot be found
            (data integrity violation).
        """
        # 1. Load project
        project = await self._project_store.get_by_id(build.project_id)
        if project is None:
            msg = (
                f"Project id={build.project_id} not found"
                f" for build id={build.id}"
            )
            raise RuntimeError(msg)

        # 2. Load org
        org = await self._org_store.get_by_id(project.org_id)
        if org is None:
            msg = (
                f"Organization id={project.org_id} not found"
                f" for project id={project.id}"
            )
            raise RuntimeError(msg)

        # 3. Resolve effective rewrite rules
        raw_rules = project.slug_rewrite_rules or org.slug_rewrite_rules
        rules = parse_slug_rewrite_rules(raw_rules)

        # 4. Derive slug
        try:
            derivation = derive_edition_slug(
                build.git_ref, rules, alternate_name=build.alternate_name
            )
        except InvalidSlugError as exc:
            self._logger.warning(
                "Invalid slug derived from git ref",
                git_ref=build.git_ref,
                error=str(exc),
                build_id=build.id,
                project_id=project.id,
            )
            return EditionTrackingResult(derived_slug=None, suppressed=False)

        if derivation is None:
            self._logger.info(
                "Git ref suppressed by ignore rule",
                git_ref=build.git_ref,
                build_id=build.id,
                project_id=project.id,
            )
            return EditionTrackingResult(derived_slug=None, suppressed=True)

        self._logger.info(
            "Derived edition slug",
            slug=derivation.slug,
            edition_kind=derivation.edition_kind,
            git_ref=build.git_ref,
            build_id=build.id,
            project_id=project.id,
        )

        # 5. Find matching editions (includes version-based modes)
        editions = await self._edition_store.find_matching_editions(
            project_id=project.id,
            git_ref=build.git_ref,
            alternate_name=build.alternate_name,
        )

        # 6. Auto-create git_ref edition if no match from slug path
        created_ids: set[int] = set()
        if not editions:
            new_edition = await self._auto_create_edition(
                project_id=project.id, derivation=derivation
            )
            if new_edition is not None:
                editions = [new_edition]
                created_ids.add(new_edition.id)

        # 7. Auto-create semver_major / semver_minor editions
        (
            version_editions,
            version_created_ids,
        ) = await self._auto_create_version_editions(
            project_id=project.id, build=build
        )
        created_ids |= version_created_ids
        for ve in version_editions:
            if not any(e.id == ve.id for e in editions):
                editions.append(ve)

        # 8. Update each edition
        outcomes = await self._update_editions(
            editions, build, created_ids=created_ids
        )

        return EditionTrackingResult(
            derived_slug=derivation.slug,
            suppressed=False,
            outcomes=outcomes,
        )

    async def _update_editions(
        self,
        editions: list[Edition],
        build: Build,
        *,
        created_ids: set[int],
    ) -> list[EditionTrackingOutcome]:
        """Apply build to each matched edition, returning outcomes."""
        outcomes: list[EditionTrackingOutcome] = []
        for edition in editions:
            outcome = await self._try_update_edition(
                edition,
                build,
                auto_created=edition.id in created_ids,
            )
            outcomes.append(outcome)
        return outcomes

    async def _try_update_edition(
        self,
        edition: Edition,
        build: Build,
        *,
        auto_created: bool,
    ) -> EditionTrackingOutcome:
        """Attempt to update a single edition's build pointer."""
        if not self._should_update(edition, build):
            self._logger.info(
                "Version guard skipped edition",
                edition_slug=edition.slug,
                edition_id=edition.id,
                build_id=build.id,
            )
            return EditionTrackingOutcome(
                edition_id=edition.id,
                slug=edition.slug,
                build_id=build.id,
                action="skipped",
            )

        is_version_mode = edition.tracking_mode in _VERSION_MODES
        skip_date_guard = is_version_mode and not (
            edition.tracking_mode == TrackingMode.lsst_doc
            and build.git_ref == "main"
            and edition.current_build_git_ref == "main"
        )
        updated = await self._edition_store.set_current_build(
            edition_id=edition.id,
            build_id=build.id,
            skip_date_guard=skip_date_guard,
        )
        if updated is None:
            self._logger.info(
                "Stale build skipped for edition",
                edition_slug=edition.slug,
                edition_id=edition.id,
                build_id=build.id,
            )
            return EditionTrackingOutcome(
                edition_id=edition.id,
                slug=edition.slug,
                build_id=build.id,
                action="skipped",
            )

        await self._history_store.record(
            edition_id=edition.id, build_id=build.id
        )
        action: Literal["updated", "created"] = (
            "created" if auto_created else "updated"
        )
        self._logger.info(
            "Edition pointer updated",
            edition_slug=edition.slug,
            edition_id=edition.id,
            build_id=build.id,
            action=action,
        )
        return EditionTrackingOutcome(
            edition_id=edition.id,
            slug=edition.slug,
            build_id=build.id,
            action=action,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _should_update(self, edition: Edition, build: Build) -> bool:
        """Check whether *build* should update *edition*.

        For non-version tracking modes (``git_ref``, ``alternate_git_ref``),
        always returns ``True`` — the date-based stale guard in
        ``set_current_build`` handles ordering.

        For version-based modes, parses both the candidate and current
        git refs and returns ``True`` only when the candidate is >=
        the current version.
        """
        if edition.tracking_mode not in _VERSION_MODES:
            return True

        # Special handling for lsst_doc + main ref
        if edition.tracking_mode == TrackingMode.lsst_doc:
            return self._should_update_lsst_doc(edition, build)

        candidate = parse_version_for_mode(
            build.git_ref, edition.tracking_mode
        )
        if candidate is None:
            return False

        # No current build → accept any parseable version
        if (
            edition.current_build_id is None
            or edition.current_build_git_ref is None
        ):
            return True

        current = parse_version_for_mode(
            edition.current_build_git_ref, edition.tracking_mode
        )
        if current is None:
            # Current ref is unparseable (e.g. leftover "main") → accept
            return True

        return candidate >= current

    def _should_update_lsst_doc(self, edition: Edition, build: Build) -> bool:
        """Version guard for ``lsst_doc`` tracking mode.

        - main→main: accepted (fall through to date guard)
        - main→version: always accepted (upgrade from main)
        - version→main: rejected
        - version→version: compare parsed versions
        """
        current_ref = edition.current_build_git_ref

        # No current build → accept anything
        if edition.current_build_id is None or current_ref is None:
            return True

        candidate_is_main = build.git_ref == "main"
        current_is_main = current_ref == "main"

        if current_is_main:
            # main→main or main→version (always upgrade)
            return (
                candidate_is_main
                or LsstDocVersion.parse(build.git_ref) is not None
            )
        if candidate_is_main:
            # version→main: reject
            return False

        # version → version: compare parsed versions
        candidate_v = LsstDocVersion.parse(build.git_ref)
        current_v = LsstDocVersion.parse(current_ref)
        if candidate_v is None:
            return False
        return current_v is None or candidate_v >= current_v

    async def _auto_create_edition(
        self,
        *,
        project_id: int,
        derivation: SlugDerivationResult,
    ) -> Edition | None:
        """Auto-create an edition from a slug derivation result.

        If an edition with the derived slug already exists (race guard),
        the existing edition is returned instead.
        """
        # Race guard: check if another worker already created it
        existing = await self._edition_store.get_by_slug(
            project_id=project_id, slug=derivation.slug
        )
        if existing is not None:
            self._logger.info(
                "Edition already exists, skipping auto-create",
                slug=derivation.slug,
                project_id=project_id,
            )
            return existing

        edition = await self._edition_store.create_internal(
            project_id=project_id,
            slug=derivation.slug,
            title=derivation.slug,
            kind=derivation.edition_kind,
            tracking_mode=derivation.tracking_mode,
            tracking_params=derivation.tracking_params,
        )
        self._logger.info(
            "Auto-created edition",
            slug=edition.slug,
            kind=edition.kind,
            tracking_mode=edition.tracking_mode,
            project_id=project_id,
        )
        return edition

    async def _auto_create_version_editions(
        self,
        *,
        project_id: int,
        build: Build,
    ) -> tuple[list[Edition], set[int]]:
        """Auto-create ``semver_major`` / ``semver_minor`` editions.

        Only triggers for stable semver tags (no prerelease).  Uses
        ``create_internal`` because single-digit slugs like ``"2"``
        don't pass ``EditionCreate``'s slug pattern.

        Returns a tuple of (matched editions, IDs of newly created ones).
        """
        sv = SemverVersion.parse(build.git_ref)
        if sv is None or sv.prerelease is not None:
            return [], set()

        matched: list[Edition] = []
        created_ids: set[int] = set()

        # --- semver_major ---
        major_slug = str(sv.major)
        major_edition = await self._edition_store.get_by_slug(
            project_id=project_id, slug=major_slug
        )
        if major_edition is None:
            major_edition = await self._edition_store.create_internal(
                project_id=project_id,
                slug=major_slug,
                title=f"Latest {sv.major}.x",
                kind=EditionKind.major,
                tracking_mode=TrackingMode.semver_major,
                tracking_params={"major_version": sv.major},
            )
            self._logger.info(
                "Auto-created semver_major edition",
                slug=major_slug,
                project_id=project_id,
            )
            matched.append(major_edition)
            created_ids.add(major_edition.id)
        elif major_edition.tracking_mode == TrackingMode.semver_major:
            matched.append(major_edition)

        # --- semver_minor ---
        minor_slug = f"{sv.major}.{sv.minor}"
        minor_edition = await self._edition_store.get_by_slug(
            project_id=project_id, slug=minor_slug
        )
        if minor_edition is None:
            minor_edition = await self._edition_store.create_internal(
                project_id=project_id,
                slug=minor_slug,
                title=f"Latest {sv.major}.{sv.minor}.x",
                kind=EditionKind.minor,
                tracking_mode=TrackingMode.semver_minor,
                tracking_params={
                    "major_version": sv.major,
                    "minor_version": sv.minor,
                },
            )
            self._logger.info(
                "Auto-created semver_minor edition",
                slug=minor_slug,
                project_id=project_id,
            )
            matched.append(minor_edition)
            created_ids.add(minor_edition.id)
        elif minor_edition.tracking_mode == TrackingMode.semver_minor:
            matched.append(minor_edition)

        return matched, created_ids
