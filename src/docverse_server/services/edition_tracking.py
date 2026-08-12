"""Service for edition tracking — matching builds to editions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import structlog

from docverse.models import EditionAutocreationConfig, TrackingMode
from docverse_server.domain.build import Build
from docverse_server.domain.edition import Edition
from docverse_server.domain.edition_autocreation import (
    resolve_edition_autocreation,
)
from docverse_server.domain.edition_kind import is_kind_promotion
from docverse_server.domain.edition_tracking import (
    EditionTrackingOutcome,
    EditionTrackingResult,
)
from docverse_server.domain.semver_aggregate import semver_aggregate_specs
from docverse_server.domain.slug import (
    InvalidSlugError,
    SlugDerivationResult,
    derive_edition_slug,
    resolve_slug_rewrite_rules,
)
from docverse_server.domain.version import (
    LsstDocVersion,
    accepts_version_advance,
    parse_stable_semver,
    parse_version_for_mode,
)
from docverse_server.services.lock_service import LockKey, LockService
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore

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


@dataclass(frozen=True)
class EditionTrackingDeps:
    """Collaborators required by :class:`EditionTrackingService`.

    ``lock_service`` is optional: when set (workers), each
    ``set_current_build`` call is wrapped in an EDITION_UPDATE advisory
    lock so it serializes against concurrent ``publish_edition`` jobs
    and admin-triggered edition pointer changes. ``None`` for
    non-worker call paths (unit tests, out-of-scope code paths).
    """

    edition_store: EditionStore
    history_store: EditionBuildHistoryStore
    project_store: ProjectStore
    org_store: OrganizationStore
    logger: structlog.stdlib.BoundLogger
    lock_service: LockService | None = None


class EditionTrackingService:
    """Orchestrate edition tracking when a build completes.

    Derives an edition slug from the build's git ref, finds or
    auto-creates matching editions, updates their pointers (with a
    stale-build guard), and records build history.
    """

    def __init__(self, deps: EditionTrackingDeps) -> None:
        self._deps = deps

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
        project = await self._deps.project_store.get_by_id(build.project_id)
        if project is None:
            msg = (
                f"Project id={build.project_id} not found"
                f" for build id={build.id}"
            )
            raise RuntimeError(msg)

        # 2. Load org
        org = await self._deps.org_store.get_by_id(project.org_id)
        if org is None:
            msg = (
                f"Organization id={project.org_id} not found"
                f" for project id={project.id}"
            )
            raise RuntimeError(msg)

        # 3. Resolve effective rewrite rules. Project-over-org, keyed on
        # None (unset) rather than falsiness, so an explicit `[]` opts
        # the project out of the org's rules instead of inheriting them.
        rules = resolve_slug_rewrite_rules(
            project=project.slug_rewrite_rules,
            org=org.slug_rewrite_rules,
        )

        # 4. Derive slug
        try:
            derivation = derive_edition_slug(
                build.git_ref, rules, alternate_name=build.alternate_name
            )
        except InvalidSlugError as exc:
            self._deps.logger.warning(
                "Invalid slug derived from git ref",
                git_ref=build.git_ref,
                error=str(exc),
                build_id=build.id,
                project_id=project.id,
            )
            return EditionTrackingResult(derived_slug=None, suppressed=False)

        if derivation is None:
            self._deps.logger.info(
                "Git ref suppressed by ignore rule",
                git_ref=build.git_ref,
                build_id=build.id,
                project_id=project.id,
            )
            return EditionTrackingResult(derived_slug=None, suppressed=True)

        self._deps.logger.info(
            "Derived edition slug",
            slug=derivation.slug,
            edition_kind=derivation.edition_kind,
            matched_rule_type=derivation.matched_rule_type,
            git_ref=build.git_ref,
            build_id=build.id,
            project_id=project.id,
        )

        # 5. Find matching editions (includes version-based modes)
        editions = await self._deps.edition_store.find_matching_editions(
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
        autocreation = resolve_edition_autocreation(
            project=project.edition_autocreation,
            org=org.edition_autocreation,
        )
        (
            version_editions,
            version_created_ids,
        ) = await self._auto_create_version_editions(
            project_id=project.id,
            build=build,
            autocreation=autocreation,
        )
        created_ids |= version_created_ids
        for ve in version_editions:
            if not any(e.id == ve.id for e in editions):
                editions.append(ve)

        # 8. Heal kinds derived before the built-in version rules landed
        editions = await self._refresh_kinds(
            editions, derivation=derivation, project_id=project.id
        )

        # 9. Update each edition
        outcomes = await self._update_editions(
            editions,
            build,
            created_ids=created_ids,
            org_id=org.id,
            project_id=project.id,
        )

        return EditionTrackingResult(
            derived_slug=derivation.slug,
            suppressed=False,
            outcomes=outcomes,
        )

    async def _refresh_kinds(
        self,
        editions: list[Edition],
        *,
        derivation: SlugDerivationResult,
        project_id: int,
    ) -> list[Edition]:
        """Re-apply the derived kind to already-existing editions.

        ``find_matching_editions`` only ever moves an edition's build
        pointer, so an edition created before the built-in version rules
        existed keeps whatever kind it was stamped with — a
        version-slugged edition stays ``draft`` forever, holding the
        short CDN cache profile and staying eligible for
        ``draft_inactivity`` soft-deletion, while the byte-identical
        edition on a keeper-sync-migrated project is promoted to
        ``release`` on every sync. Re-deriving here closes that gap.

        The write is gated by the shared
        :func:`~docverse_server.domain.edition_kind.is_kind_promotion`
        policy — the same one keeper-sync's ``_refresh_kind`` applies —
        so the gate alone makes the sweep safe across the whole matched
        set: the ``__main`` edition (``main``), the ``N`` / ``N.M``
        aggregates (``major`` / ``minor``), an ``alternate``, and every
        row just auto-created with this very derivation are all no-ops,
        because none of them is a ``(draft, release)`` pair. Any edition
        whose kind an operator PATCHed by hand — including one demoted
        from ``release`` to ``draft``, which *is* such a pair — is held
        back by its ``kind_manually_set`` flag instead.

        Returns the list with promoted rows replaced by their post-write
        state, so downstream steps see the kind that is in the database.
        """
        refreshed: list[Edition] = []
        for edition in editions:
            if not is_kind_promotion(
                edition.kind,
                derivation.edition_kind,
                kind_manually_set=edition.kind_manually_set,
            ):
                refreshed.append(edition)
                continue
            await self._deps.edition_store.update_kind(
                edition_id=edition.id, kind=derivation.edition_kind
            )
            self._deps.logger.info(
                "Promoted edition kind",
                edition_slug=edition.slug,
                edition_id=edition.id,
                project_id=project_id,
                previous_kind=edition.kind.value,
                edition_kind=derivation.edition_kind.value,
                matched_rule_type=derivation.matched_rule_type,
            )
            refreshed.append(
                edition.model_copy(update={"kind": derivation.edition_kind})
            )
        return refreshed

    async def _update_editions(
        self,
        editions: list[Edition],
        build: Build,
        *,
        created_ids: set[int],
        org_id: int,
        project_id: int,
    ) -> list[EditionTrackingOutcome]:
        """Apply build to each matched edition, returning outcomes."""
        outcomes: list[EditionTrackingOutcome] = []
        for edition in editions:
            outcome = await self._try_update_edition(
                edition,
                build,
                auto_created=edition.id in created_ids,
                org_id=org_id,
                project_id=project_id,
            )
            outcomes.append(outcome)
        return outcomes

    async def _try_update_edition(
        self,
        edition: Edition,
        build: Build,
        *,
        auto_created: bool,
        org_id: int,
        project_id: int,
    ) -> EditionTrackingOutcome:
        """Attempt to update a single edition's build pointer."""
        if not self._should_update(edition, build):
            self._deps.logger.info(
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
        updated = await self._set_current_build_locked(
            org_id=org_id,
            project_id=project_id,
            edition_id=edition.id,
            build_id=build.id,
            skip_date_guard=skip_date_guard,
        )
        if updated is None:
            self._deps.logger.info(
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

        # Backfill alternate_name on existing alt-mode editions whose row
        # predates the column. The matching layer guarantees the build's
        # alternate_name equals the edition's tracking_params alternate_name,
        # so a NULL column is the only state that needs writing.
        if (
            updated.tracking_mode == TrackingMode.alternate_git_ref
            and updated.alternate_name is None
            and build.alternate_name is not None
        ):
            await self._deps.edition_store.set_alternate_name(
                edition_id=updated.id, alternate_name=build.alternate_name
            )

        await self._deps.history_store.record(
            edition_id=edition.id, build_id=build.id
        )
        action: Literal["updated", "created"] = (
            "created" if auto_created else "updated"
        )
        self._deps.logger.info(
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

    async def _set_current_build_locked(
        self,
        *,
        org_id: int,
        project_id: int,
        edition_id: int,
        build_id: int,
        skip_date_guard: bool,
    ) -> Edition | None:
        """Update the edition pointer under an EDITION_UPDATE lock.

        When ``self._deps.lock_service`` is configured (worker call paths),
        the call serializes against concurrent ``publish_edition`` jobs
        and admin-triggered edition pointer changes for the same
        edition. With no lock service (unit-test call paths), the
        update runs unwrapped — non-worker callers are explicitly
        out of scope per the design (SQR-112).
        """
        if self._deps.lock_service is None:
            return await self._deps.edition_store.set_current_build(
                edition_id=edition_id,
                build_id=build_id,
                skip_date_guard=skip_date_guard,
            )
        lock_key = LockKey.for_edition_update(
            org_id=org_id,
            project_id=project_id,
            edition_id=edition_id,
        )
        async with self._deps.lock_service.acquire(lock_key):
            return await self._deps.edition_store.set_current_build(
                edition_id=edition_id,
                build_id=build_id,
                skip_date_guard=skip_date_guard,
            )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _should_update(self, edition: Edition, build: Build) -> bool:
        """Check whether *build* should update *edition*.

        For non-version tracking modes (``git_ref``, ``alternate_git_ref``),
        always returns ``True`` — the date-based stale guard in
        ``set_current_build`` handles ordering.

        For version-based modes, parses the candidate ref and defers to
        `accepts_version_advance` — the guard keeper-sync's aggregate
        backfill shares, so migrated and natively built editions cannot
        end up on different releases.
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

        return accepts_version_advance(
            candidate=candidate,
            current_build_id=edition.current_build_id,
            current_build_git_ref=edition.current_build_git_ref,
            mode=edition.tracking_mode,
        )

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
        existing = await self._deps.edition_store.get_by_slug(
            project_id=project_id, slug=derivation.slug
        )
        if existing is not None:
            self._deps.logger.info(
                "Edition already exists, skipping auto-create",
                slug=derivation.slug,
                project_id=project_id,
            )
            return existing

        edition = await self._deps.edition_store.create_internal(
            project_id=project_id,
            slug=derivation.slug,
            title=derivation.slug,
            kind=derivation.edition_kind,
            tracking_mode=derivation.tracking_mode,
            tracking_params=derivation.tracking_params,
            alternate_name=derivation.tracking_params.get("alternate_name"),
        )
        self._deps.logger.info(
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
        autocreation: EditionAutocreationConfig,
    ) -> tuple[list[Edition], set[int]]:
        """Auto-create ``semver_major`` / ``semver_minor`` editions.

        Only triggers for stable semver tags (no prerelease), and only
        when the resolved ``autocreation`` config enables
        ``semver_aggregates``.  Uses ``create_internal`` because
        single-digit slugs like ``"2"`` don't pass ``EditionCreate``'s
        slug pattern.

        The gate is the ref's own semver grammar
        (:func:`~docverse_server.domain.version.parse_stable_semver`),
        not the kind ``track_build``'s slug derivation landed on —
        keeper-sync's ``_backfill_semver_aggregates`` gates on the same
        classification so the two paths stay in step. Reading the
        derived kind instead would let any user rule that merely
        reshapes the slug switch the aggregates off by accident: user
        rules run ahead of the built-in ``SemverRule``, so an org whose
        ``prefix_strip`` rule exists only to drop the ``v`` from
        ``v16.0.0`` leaves the kind at that rule's ``draft`` default and
        would silently never see a ``16`` / ``16.0`` row. Operators who
        genuinely want no aggregates say so with
        ``edition_autocreation.semver_aggregates = false``.

        The autocreation gate is autocreation-only, matching the
        config's name: an aggregate edition that already exists —
        auto-created before the knob was turned off, or created by hand
        — is still matched and advanced by ``find_matching_editions`` in
        ``track_build``. Only the implicit creation of new ``N`` /
        ``N.M`` rows stops. Keeper-sync's backfill applies the knob the
        same way, so a migrated project's ``/v/15`` does not freeze
        where an identically configured native project's keeps moving.

        Returns a tuple of (matched editions, IDs of newly created ones).
        """
        if not autocreation.semver_aggregates:
            return [], set()

        sv = parse_stable_semver(build.git_ref)
        if sv is None:
            return [], set()

        matched: list[Edition] = []
        created_ids: set[int] = set()
        for spec in semver_aggregate_specs(sv):
            edition = await self._deps.edition_store.get_by_slug(
                project_id=project_id, slug=spec.slug
            )
            if edition is None:
                edition = await self._deps.edition_store.create_internal(
                    project_id=project_id,
                    slug=spec.slug,
                    title=spec.title,
                    kind=spec.kind,
                    tracking_mode=spec.tracking_mode,
                    tracking_params=spec.tracking_params,
                )
                self._deps.logger.info(
                    "Auto-created semver aggregate edition",
                    slug=spec.slug,
                    tracking_mode=spec.tracking_mode,
                    project_id=project_id,
                )
                matched.append(edition)
                created_ids.add(edition.id)
            elif edition.tracking_mode == spec.tracking_mode:
                matched.append(edition)

        return matched, created_ids
