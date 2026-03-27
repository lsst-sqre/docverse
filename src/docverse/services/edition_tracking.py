"""Service for edition tracking — matching builds to editions."""

from __future__ import annotations

from typing import Literal

import structlog

from docverse.client.models import EditionCreate
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
from docverse.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore


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

        # 5. Find matching editions
        editions = await self._edition_store.find_matching_editions(
            project_id=project.id,
            git_ref=build.git_ref,
            alternate_name=build.alternate_name,
        )

        # 6. Auto-create if no match
        auto_created = False
        if not editions:
            new_edition = await self._auto_create_edition(
                project_id=project.id, derivation=derivation
            )
            if new_edition is not None:
                editions = [new_edition]
                auto_created = True

        # 7. Update each edition
        outcomes: list[EditionTrackingOutcome] = []
        for edition in editions:
            updated = await self._edition_store.set_current_build(
                edition_id=edition.id, build_id=build.id
            )
            if updated is not None:
                await self._history_store.record(
                    edition_id=edition.id, build_id=build.id
                )
                action: Literal["updated", "created"] = (
                    "created"
                    if auto_created and len(editions) == 1
                    else "updated"
                )
                outcomes.append(
                    EditionTrackingOutcome(
                        edition_id=edition.id,
                        slug=edition.slug,
                        build_id=build.id,
                        action=action,
                    )
                )
                self._logger.info(
                    "Edition pointer updated",
                    edition_slug=edition.slug,
                    edition_id=edition.id,
                    build_id=build.id,
                    action=action,
                )
            else:
                outcomes.append(
                    EditionTrackingOutcome(
                        edition_id=edition.id,
                        slug=edition.slug,
                        build_id=build.id,
                        action="skipped",
                    )
                )
                self._logger.info(
                    "Stale build skipped for edition",
                    edition_slug=edition.slug,
                    edition_id=edition.id,
                    build_id=build.id,
                )

        return EditionTrackingResult(
            derived_slug=derivation.slug,
            suppressed=False,
            outcomes=outcomes,
        )

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

        edition = await self._edition_store.create(
            project_id=project_id,
            data=EditionCreate(
                slug=derivation.slug,
                title=derivation.slug,
                kind=derivation.edition_kind,
                tracking_mode=derivation.tracking_mode,
                tracking_params=derivation.tracking_params,
            ),
        )
        self._logger.info(
            "Auto-created edition",
            slug=edition.slug,
            kind=edition.kind,
            tracking_mode=edition.tracking_mode,
            project_id=project_id,
        )
        return edition
