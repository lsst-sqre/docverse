"""Service for edition tracking — matching builds to editions."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Literal

import structlog

from docverse.models import (
    EditionAutocreationConfig,
    EditionKind,
    EditionKindSource,
    TrackingMode,
)
from docverse_server.domain.build import Build
from docverse_server.domain.edition import (
    Edition,
    ReservedSlug,
    SlugReservation,
)
from docverse_server.domain.edition_autocreation import (
    resolve_edition_autocreation,
)
from docverse_server.domain.edition_tracking import (
    EditionTrackingOutcome,
    EditionTrackingResult,
    RevivedEdition,
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


def _derivation_governs(
    edition: Edition, derivation: SlugDerivationResult
) -> bool:
    """Report whether *derivation* describes *edition*'s kind.

    ``find_matching_editions`` returns everything a build advances, not
    just the edition the ref derivation names: a build on ``1.0.0``
    also matches the ``1`` / ``1.0`` aggregates, and a build on ``main``
    matches the project's ``__main`` edition. Those rows carry
    *structural* kinds that no ref derivation computes, so applying the
    ref's kind to them would demote ``__main`` to ``draft`` (which
    ``ck_editions_main_slug_kind`` rejects outright) and flatten the
    aggregates into ordinary releases.

    The derivation therefore governs exactly the editions it could have
    created: those tracking the mode it emits — ``git_ref``, or
    ``alternate_git_ref`` when the build carries an alternate scope —
    and never the default edition, whose ``main`` kind is a property of
    the project rather than of any ref.
    """
    return (
        edition.kind is not EditionKind.main
        and edition.tracking_mode == derivation.tracking_mode
    )


#: Tracking modes whose matcher only ever accepts a release ref — a final
#: EUPS release, a weekly tag, or a daily tag. An edition in one of them
#: can never point at a branch or a release candidate, so the mode alone
#: certifies it as a ``release``, whatever the build's own ref derives:
#: a daily tag derives ``draft``, yet the ``daily`` edition following it
#: is a release. ``lsst_doc`` is absent because it falls back to the
#: default branch until a version tag exists; ``eups_major_release`` is
#: left out by PRD #864, so its kind stays whatever it was created with.
_RELEASE_TRACKING_MODES = frozenset(
    {
        TrackingMode.eups_release,
        TrackingMode.eups_weekly_release,
        TrackingMode.eups_daily_release,
    }
)


@dataclass(frozen=True, slots=True)
class _KindTarget:
    """The kind tracking converges an edition on, and what decided it.

    ``matched_rule_type`` is the slug-rewrite rule behind a ref
    derivation, or ``None`` when the edition's tracking mode decided.
    """

    kind: EditionKind
    matched_rule_type: str | None


def _kind_target(
    edition: Edition, derivation: SlugDerivationResult
) -> _KindTarget | None:
    """Return the kind *edition* should carry, or ``None`` if none governs.

    A release-only tracking mode (see ``_RELEASE_TRACKING_MODES``)
    certifies ``release`` on its own; otherwise the ref derivation
    governs exactly the editions :func:`_derivation_governs` admits. The
    default edition's ``main`` kind belongs to the project, so neither
    rewrites it.
    """
    if edition.kind is EditionKind.main:
        return None
    if edition.tracking_mode in _RELEASE_TRACKING_MODES:
        return _KindTarget(kind=EditionKind.release, matched_rule_type=None)
    if _derivation_governs(edition, derivation):
        return _KindTarget(
            kind=derivation.edition_kind,
            matched_rule_type=derivation.matched_rule_type,
        )
    return None


#: Tracking modes in which an edition follows one literal git ref. A
#: match in one of them is the edition the build's ref already has, so
#: it suppresses auto-creation: ``__main`` tracking ``main`` keeps a
#: ``main`` build from spawning a ``main`` edition beside it. A match in
#: any other mode is an *alias* — ``__main`` following the newest
#: release, ``weekly`` the newest weekly — that stands for a whole
#: family of refs and owns none of them.
_LITERAL_REF_MODES = frozenset(
    {TrackingMode.git_ref, TrackingMode.alternate_git_ref}
)


def _owns_ref(edition: Edition, derivation: SlugDerivationResult) -> bool:
    """Report whether matched *edition* is the build's own ref edition.

    It is when it tracks the literal ref (see ``_LITERAL_REF_MODES``) or
    already holds the derived slug — compared case-insensitively, as
    ``uq_editions_project_lower_slug`` does, so an alias edition that
    happens to carry the ref's slug is advanced once rather than
    returned again by auto-creation's existing-slug guard.
    """
    return (
        edition.tracking_mode in _LITERAL_REF_MODES
        or edition.slug.lower() == derivation.slug.lower()
    )


# Tracking modes that use version comparison instead of date-based guards.
_VERSION_MODES = frozenset(
    {
        TrackingMode.semver_release,
        TrackingMode.semver_major,
        TrackingMode.semver_minor,
        TrackingMode.eups_major_release,
        TrackingMode.eups_release,
        TrackingMode.eups_weekly_release,
        TrackingMode.eups_daily_release,
        TrackingMode.lsst_doc,
    }
)


@dataclass(frozen=True, slots=True)
class _RefEdition:
    """The edition auto-creation settles the build's ref on.

    ``created`` is whether this tracking inserted or revived it, which
    reports it ``created``. An edition that already held the derived
    slug is reused instead, so it reports ``updated`` like any other
    existing edition the build advances. ``revived`` is the revived
    subset of ``created``.
    """

    edition: Edition
    created: bool
    revived: bool


@dataclass(slots=True)
class _VersionEditions:
    """The semver aggregates a stable release build reaches.

    ``matched`` lists every aggregate the build advances; ``created_ids``
    the ones this tracking inserted or revived, and ``revived_ids`` the
    revived subset of those.
    """

    matched: list[Edition] = field(default_factory=list)
    created_ids: set[int] = field(default_factory=set)
    revived_ids: set[int] = field(default_factory=set)


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

    Derives an edition slug from the build's git ref, finds matching
    editions, auto-creates the ref's own edition when no match owns the
    ref, updates their pointers (with a stale-build guard), and records
    build history.
    """

    def __init__(self, deps: EditionTrackingDeps) -> None:
        self._deps = deps

    async def track_build(self, build: Build) -> EditionTrackingResult:
        """Evaluate tracking rules for a completed build.

        The build advances every edition whose tracking matches its ref,
        and the edition its ref derives (``/v/<slug>/``) is auto-created
        — or revived, if lifecycle rules reaped it — unless a match
        already owns that ref: one tracking the literal ref (``git_ref``
        or ``alternate_git_ref``), or one already holding the derived
        slug. Matches in every other mode are aliases for a family of
        refs (``__main`` in ``eups_release``, ``weekly``, ``latest``,
        the semver aggregates, an ``lsst_doc`` edition), so they advance
        *beside* the per-ref edition rather than absorbing the build.
        ``__main`` tracking ``main`` therefore keeps a ``main`` build
        from spawning a ``main`` edition, while a ``v30_0_13`` build
        both advances an ``eups_release`` ``__main`` and creates
        ``v30_0_13``. A slug an operator deleted stays deleted. Stable
        semver tags also auto-create their ``N`` / ``N.M`` aggregates,
        as ``edition_autocreation.semver_aggregates`` allows.

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

        # 5. Find matching editions (includes version-based modes). The
        #    project's default branch is resolved once here: it is the
        #    pre-release ref an ``lsst_doc`` edition falls back to, both
        #    when matching and in the version guard below.
        default_branch = project.effective_default_branch
        editions = await self._deps.edition_store.find_matching_editions(
            project_id=project.id,
            git_ref=build.git_ref,
            alternate_name=build.alternate_name,
            default_branch=default_branch,
        )

        # 6. Take the head-of-transaction lock, before steps 7 and 8
        #    can lock an edition row. Their inserts take only the
        #    project's ``FOR KEY SHARE``, which conflicts with nothing
        #    the soft-delete cascade takes, but a revive updates an
        #    existing edition row, and holding that while asking for
        #    the project would walk ``editions -> projects``. Neither
        #    step can produce the default edition — ``__main`` is never
        #    deleted, so never revived — so the matches in hand already
        #    say whether this build reaches it.
        await self._lock_project_for_default_repoint(editions)

        # 7. Auto-create (or revive) the derived edition unless a match
        #    already owns the build's ref (see ``_owns_ref``). Alias
        #    matches — ``__main`` in ``eups_release``, ``weekly``,
        #    ``latest`` — advance alongside it rather than absorbing the
        #    build, so every release, weekly, or daily keeps its own
        #    ``/v/<slug>/`` edition. The ref's edition joins ``editions``
        #    so steps 9 and 10 converge, point, and publish it; only one
        #    this step inserted or revived reports ``created``.
        created_ids: set[int] = set()
        revived_ids: set[int] = set()
        ref_edition = await self._auto_create_edition(
            editions, project_id=project.id, derivation=derivation
        )
        if ref_edition is not None:
            editions.append(ref_edition.edition)
            if ref_edition.created:
                created_ids.add(ref_edition.edition.id)
            if ref_edition.revived:
                revived_ids.add(ref_edition.edition.id)

        # 8. Auto-create (or revive) semver_major / semver_minor editions
        autocreation = resolve_edition_autocreation(
            project=project.edition_autocreation,
            org=org.edition_autocreation,
        )
        version_editions = await self._auto_create_version_editions(
            project_id=project.id,
            build=build,
            autocreation=autocreation,
        )
        created_ids |= version_editions.created_ids
        revived_ids |= version_editions.revived_ids
        for ve in version_editions.matched:
            if not any(e.id == ve.id for e in editions):
                editions.append(ve)

        # 9. Converge kinds on the current derivation
        editions = await self._refresh_kinds(
            editions,
            derivation=derivation,
            org_id=org.id,
            project_id=project.id,
        )

        # 10. Update each edition. A revived edition is in
        #     ``created_ids``: it is new to everything that skips
        #     soft-deleted rows, so it earns the same publish.
        outcomes = await self._update_editions(
            editions,
            build,
            created_ids=created_ids,
            org_id=org.id,
            project_id=project.id,
            default_branch=default_branch,
        )

        return EditionTrackingResult(
            derived_slug=derivation.slug,
            suppressed=False,
            outcomes=outcomes,
            revived=[
                RevivedEdition(
                    edition_id=edition.id, slug=edition.slug, kind=edition.kind
                )
                for edition in editions
                if edition.id in revived_ids
            ],
        )

    async def _lock_project_for_default_repoint(
        self, editions: list[Edition]
    ) -> None:
        """Take the project row when this build reaches ``__main``.

        ``track_build`` is a **composite writer** in the sense
        :mod:`docverse_server.storage.edition_store` documents: one
        transaction repoints every edition the build matches, and the
        matches arrive in slug order. Under the database's
        ``en_US.UTF-8`` collation ``__main`` sorts after ordinary slugs,
        so a co-matching ``git_ref`` or ``lsst_doc`` edition is
        repointed first — locking ``editions`` and ``builds`` — and only
        the later ``__main`` repoint asks for the ``projects`` row it
        stamps. That is ``editions -> projects``, the reverse of the
        project soft-delete cascade, and the two deadlock.

        Asking for the project row here, before any edition row is
        locked, makes the order a property of the whole transaction
        rather than of each
        :meth:`~docverse_server.storage.edition_store.EditionStore.set_current_build`
        call. Nothing is taken for a build that reaches no default
        edition, which is what keeps unrelated projects' — and this
        project's non-default — tracking from serializing on one row.
        """
        default = next((e for e in editions if e.is_default), None)
        if default is None:
            return
        await self._deps.edition_store.lock_for_repoint(
            edition_id=default.id,
            project_id=default.project_id,
            is_default=True,
        )

    async def _refresh_kinds(
        self,
        editions: list[Edition],
        *,
        derivation: SlugDerivationResult,
        org_id: int,
        project_id: int,
    ) -> list[Edition]:
        """Converge matched editions on the current derivation.

        ``find_matching_editions`` only ever moves an edition's build
        pointer, so an edition created before the built-in version rules
        existed keeps whatever kind it was stamped with — a
        version-slugged edition stays ``draft`` forever, holding the
        short CDN cache profile and staying eligible for
        ``draft_inactivity`` soft-deletion, while the byte-identical
        edition on a keeper-sync-migrated project is corrected on every
        sync. Re-deriving here closes that gap.

        The sweep converges rather than promotes: a ``derived`` row is
        rewritten to whatever the rules say *today*, demotions included,
        so an org that adds ``{"type": "semver", "edition_kind":
        "draft"}`` sees its earlier promotions unwound instead of
        stranded. Convergence — not a re-asserting PATCH — is the
        supported way to undo an automatic kind.

        An edition in a release-only tracking mode (``eups_release``,
        ``eups_weekly_release``, ``eups_daily_release``) converges on
        ``release`` instead, whatever the build's ref derives: its
        matcher only ever accepts a release ref, so the mode certifies
        the kind. That is how the ``weekly`` / ``daily`` meta editions,
        whose own slugs match no version rule, stop being drafts.

        Returns the list with converged rows replaced by their
        post-write state, so downstream steps see the kind that is in
        the database.
        """
        refreshed: list[Edition] = []
        for edition in editions:
            converged = await self._converge_kind(
                edition,
                derivation=derivation,
                org_id=org_id,
                project_id=project_id,
            )
            refreshed.append(converged)
        return refreshed

    async def _converge_kind(
        self,
        edition: Edition,
        *,
        derivation: SlugDerivationResult,
        org_id: int,
        project_id: int,
    ) -> Edition:
        """Write the kind that governs *edition* when it differs.

        The target comes from :func:`_kind_target`: ``release`` for a
        release-only tracking mode, else *derivation*'s kind when it
        governs the edition. A ``declared`` kind is never rewritten.
        """
        target = _kind_target(edition, derivation)
        if target is None:
            return edition
        if edition.kind_source is EditionKindSource.declared:
            return edition
        if edition.kind is target.kind:
            return edition
        async with self._edition_update_lock(
            org_id=org_id, project_id=project_id, edition_id=edition.id
        ):
            await self._deps.edition_store.update_kind(
                edition_id=edition.id, kind=target.kind
            )
        self._deps.logger.info(
            "Converged edition kind",
            edition_slug=edition.slug,
            edition_id=edition.id,
            project_id=project_id,
            previous_kind=edition.kind.value,
            edition_kind=target.kind.value,
            kind_trigger="build_upload",
            tracking_mode=edition.tracking_mode.value,
            matched_rule_type=target.matched_rule_type,
        )
        return edition.model_copy(update={"kind": target.kind})

    async def _update_editions(
        self,
        editions: list[Edition],
        build: Build,
        *,
        created_ids: set[int],
        org_id: int,
        project_id: int,
        default_branch: str,
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
                default_branch=default_branch,
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
        default_branch: str,
    ) -> EditionTrackingOutcome:
        """Attempt to update a single edition's build pointer."""
        if not self._should_update(
            edition, build, default_branch=default_branch
        ):
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

        # Version modes order builds by version, not date — except an
        # ``lsst_doc`` edition moving between two builds of the default
        # branch, where the date guard is the only ordering there is.
        is_version_mode = edition.tracking_mode in _VERSION_MODES
        skip_date_guard = is_version_mode and not (
            edition.tracking_mode == TrackingMode.lsst_doc
            and build.git_ref == default_branch
            and edition.current_build_git_ref == default_branch
        )
        updated = await self._set_current_build_locked(
            org_id=org_id,
            project_id=project_id,
            edition_id=edition.id,
            build_id=build.id,
            skip_date_guard=skip_date_guard,
            is_default=edition.is_default,
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

    @asynccontextmanager
    async def _edition_update_lock(
        self, *, org_id: int, project_id: int, edition_id: int
    ) -> AsyncGenerator[None]:
        """Hold the ``EDITION_UPDATE`` lock for one edition, if wired.

        When ``self._deps.lock_service`` is configured (worker call
        paths), everything inside the block serializes against
        concurrent ``publish_edition`` jobs, keeper-sync's writes, and
        admin-triggered edition changes for the same edition. With no
        lock service (unit-test call paths), the body runs unwrapped —
        non-worker callers are explicitly out of scope per the design
        (SQR-112).
        """
        if self._deps.lock_service is None:
            yield
            return
        lock_key = LockKey.for_edition_update(
            org_id=org_id,
            project_id=project_id,
            edition_id=edition_id,
        )
        async with self._deps.lock_service.acquire(lock_key):
            yield

    async def _set_current_build_locked(
        self,
        *,
        org_id: int,
        project_id: int,
        edition_id: int,
        build_id: int,
        skip_date_guard: bool,
        is_default: bool,
    ) -> Edition | None:
        """Update the edition pointer under an EDITION_UPDATE lock.

        Flattens the store's three-valued outcome back to "the edition,
        or nothing": tracking has the same work to do whether the
        repoint moved the binding or found it already where it wanted
        it, and a guard's refusal is the only answer it stands down on.

        *project_id* and *is_default* are passed down to the store,
        which needs the ``projects`` row only for a ``__main`` repoint
        and would otherwise re-derive both from the edition it is about
        to lock. The matched edition is right here, and its slug and
        project cannot change under it, so answering saves the lock
        step a subquery on every repoint and the statement entirely on
        every edition that is not the default.
        """
        async with self._edition_update_lock(
            org_id=org_id, project_id=project_id, edition_id=edition_id
        ):
            repoint = await self._deps.edition_store.set_current_build(
                edition_id=edition_id,
                build_id=build_id,
                skip_date_guard=skip_date_guard,
                project_id=project_id,
                is_default=is_default,
            )
            return repoint.edition

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _should_update(
        self, edition: Edition, build: Build, *, default_branch: str
    ) -> bool:
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

        # Special handling for lsst_doc + the default branch
        if edition.tracking_mode == TrackingMode.lsst_doc:
            return self._should_update_lsst_doc(
                edition, build, default_branch=default_branch
            )

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

    def _should_update_lsst_doc(
        self, edition: Edition, build: Build, *, default_branch: str
    ) -> bool:
        """Version guard for ``lsst_doc`` tracking mode.

        *default_branch* is the project's pre-release ref — its
        ``github_default_branch``, or ``main`` until that is known:

        - branch→branch: accepted (fall through to date guard)
        - branch→version: always accepted (upgrade from the branch)
        - version→branch: rejected
        - version→version: compare parsed versions
        """
        current_ref = edition.current_build_git_ref

        # No current build → accept anything
        if edition.current_build_id is None or current_ref is None:
            return True

        candidate_is_branch = build.git_ref == default_branch
        current_is_branch = current_ref == default_branch

        if current_is_branch:
            # branch→branch or branch→version (always upgrade)
            return (
                candidate_is_branch
                or LsstDocVersion.parse(build.git_ref) is not None
            )
        if candidate_is_branch:
            # version→branch: reject
            return False

        # version → version: compare parsed versions
        candidate_v = LsstDocVersion.parse(build.git_ref)
        current_v = LsstDocVersion.parse(current_ref)
        if candidate_v is None:
            return False
        return current_v is None or candidate_v >= current_v

    async def _auto_create_edition(
        self,
        matched: list[Edition],
        *,
        project_id: int,
        derivation: SlugDerivationResult,
    ) -> _RefEdition | None:
        """Auto-create the edition a slug derivation names for the ref.

        Returns ``None`` without touching anything when one of the
        *matched* editions already owns the ref (see ``_owns_ref``).

        A live edition already holding the derived slug is reused, not
        duplicated, and comes back with ``created`` false. That is an
        expected path, not only a race with another worker: it is taken
        whenever no matched edition owns the ref but an unmatched one
        holds its slug — a ``git_ref`` edition named ``v30_0_13`` that
        tracks another ref, say — and ``track_build`` then points that
        edition at the build as the ref's own. A slug held by a
        lifecycle-deleted edition revives that edition through
        :meth:`~docverse_server.storage.edition_store.EditionStore.revive_or_create`.

        Also returns ``None`` when an operator deleted the edition
        holding the slug: their delete reserves it, so the build gets no
        edition at the slug, though the editions it matched still
        advance as usual. That is a decision, not a fault, so it is
        logged at warning level rather than raised to Sentry.
        """
        if any(_owns_ref(e, derivation) for e in matched):
            return None

        existing = await self._deps.edition_store.get_by_slug(
            project_id=project_id, slug=derivation.slug
        )
        if existing is not None:
            self._deps.logger.info(
                "Edition already exists, skipping auto-create",
                slug=derivation.slug,
                project_id=project_id,
            )
            return _RefEdition(edition=existing, created=False, revived=False)

        ensured = await self._deps.edition_store.revive_or_create(
            project_id=project_id,
            slug=derivation.slug,
            title=derivation.slug,
            kind=derivation.edition_kind,
            tracking_mode=derivation.tracking_mode,
            tracking_params=derivation.tracking_params,
            alternate_name=derivation.tracking_params.get("alternate_name"),
        )
        if isinstance(ensured, ReservedSlug):
            self._log_slug_reserved(
                slug=derivation.slug, project_id=project_id
            )
            return None
        if not ensured.revived:
            self._deps.logger.info(
                "Auto-created edition",
                slug=ensured.edition.slug,
                kind=ensured.edition.kind,
                tracking_mode=ensured.edition.tracking_mode,
                project_id=project_id,
            )
        return _RefEdition(
            edition=ensured.edition, created=True, revived=ensured.revived
        )

    def _log_slug_reserved(self, *, slug: str, project_id: int) -> None:
        """Warn that a manual delete keeps tracking from recreating *slug*.

        The editions the build matched still advance as usual; only the
        deleted edition at *slug* stays gone.
        """
        self._deps.logger.warning(
            "Edition slug reserved by manual delete; edition not recreated",
            slug=slug,
            project_id=project_id,
        )

    def _log_aggregate_slug_held(
        self, *, edition: Edition, project_id: int, deleted: bool
    ) -> None:
        """Say that *edition*, not an aggregate, holds an aggregate slug.

        *deleted* tells a soft-deleted holder, which stays deleted, from
        a live one, which stays as it is; either way the build leaves
        the aggregate alone.
        """
        self._deps.logger.info(
            "Semver aggregate slug held by a non-aggregate edition; leaving"
            " it untouched",
            project_id=project_id,
            edition_id=edition.id,
            edition_slug=edition.slug,
            edition_kind=edition.kind.value,
            tracking_mode=edition.tracking_mode.value,
            edition_deleted=deleted,
        )

    async def _auto_create_version_editions(
        self,
        *,
        project_id: int,
        build: Build,
        autocreation: EditionAutocreationConfig,
    ) -> _VersionEditions:
        """Auto-create ``semver_major`` / ``semver_minor`` editions.

        Only triggers for stable semver tags (no prerelease), and only
        when the resolved ``autocreation`` config enables
        ``semver_aggregates``.  Uses ``revive_or_create`` because
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

        An aggregate slug held by a lifecycle-deleted edition revives
        it, and one an operator deleted stays deleted, through the same
        :meth:`~docverse_server.storage.edition_store.EditionStore.revive_or_create`
        primitive :meth:`_auto_create_edition` uses — but only a deleted
        edition already on the aggregate's tracking mode is revived. One
        tracking anything else (an operator's ``2`` on git ref ``2``,
        reaped by ``draft_inactivity``) stays deleted and the aggregate
        is skipped for this build, exactly as a live edition of another
        tracking mode holding the slug is: neither is this build's to
        turn into an aggregate. Keeper-sync's
        ``_backfill_semver_aggregates`` settles the slug the same way.
        """
        version_editions = _VersionEditions()
        if not autocreation.semver_aggregates:
            return version_editions

        sv = parse_stable_semver(build.git_ref)
        if sv is None:
            return version_editions

        for spec in semver_aggregate_specs(sv):
            edition = await self._deps.edition_store.get_by_slug(
                project_id=project_id, slug=spec.slug
            )
            if edition is not None:
                if edition.tracking_mode == spec.tracking_mode:
                    version_editions.matched.append(edition)
                else:
                    self._log_aggregate_slug_held(
                        edition=edition, project_id=project_id, deleted=False
                    )
                continue
            ensured = await self._deps.edition_store.revive_or_create(
                project_id=project_id,
                slug=spec.slug,
                title=spec.title,
                kind=spec.kind,
                tracking_mode=spec.tracking_mode,
                tracking_params=spec.tracking_params,
                match_tracking_mode=True,
            )
            if isinstance(ensured, ReservedSlug):
                if ensured.reason is SlugReservation.manual_delete:
                    self._log_slug_reserved(
                        slug=spec.slug, project_id=project_id
                    )
                else:
                    self._log_aggregate_slug_held(
                        edition=ensured.edition,
                        project_id=project_id,
                        deleted=True,
                    )
                continue
            if not ensured.revived:
                self._deps.logger.info(
                    "Auto-created semver aggregate edition",
                    slug=spec.slug,
                    tracking_mode=spec.tracking_mode,
                    project_id=project_id,
                )
            version_editions.matched.append(ensured.edition)
            version_editions.created_ids.add(ensured.edition.id)
            if ensured.revived:
                version_editions.revived_ids.add(ensured.edition.id)

        return version_editions
