"""Pure mappers from LTD edition fields onto Docverse counterparts.

These are intentionally side-effect-free helpers so the
:class:`docverse_server.services.keeper_sync.service.KeeperSyncService`
orchestration can remain a thin wrapper over the existing
:class:`docverse_server.services.edition.EditionService` and friends. Every
:class:`LtdEditionMode` value has a documented Docverse counterpart;
``manual`` is special-cased because Docverse has no semantic ``manual``
mode (PRD #275 "Out of scope") and is collapsed onto a pinned
``git_ref`` instead.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from docverse.models import EditionKind, TrackingMode
from docverse_server.domain.slug import (
    AnySlugRewriteRule,
    derive_edition_kind_from_ref,
)
from docverse_server.storage.ltd import LtdBuild, LtdEdition, LtdEditionMode

__all__ = [
    "LTD_MAIN_SLUG",
    "EditionKindDerivation",
    "EditionKindSource",
    "derive_edition_kind",
    "derive_edition_slug",
    "derive_edition_source_prefix",
    "map_edition_tracking",
]

LTD_MAIN_SLUG = "main"
"""LTD slug that corresponds to Docverse's auto-created ``__main`` edition."""

DOCVERSE_MAIN_SLUG = "__main"
"""Docverse slug for a project's auto-created default edition."""

#: Path segment LTD writes build uploads under, between the product slug
#: and the build slug: ``<product>/builds/<build-slug>/``.
_LTD_BUILDS_SEGMENT = "builds"

#: Path segment LTD publishes edition copies under:
#: ``<product>/v/<edition-slug>/``.
_LTD_EDITIONS_SEGMENT = "v"


class EditionKindSource(StrEnum):
    """Where a derived Docverse edition kind came from.

    Carried on :class:`EditionKindDerivation` purely for observability:
    operators triaging a mis-kinded import need to know whether LTD's
    tracking mode or a slug-rewrite rule decided the kind.
    """

    ltd_main = "ltd_main"
    """LTD's ``main`` edition, which always maps to ``EditionKind.main``."""

    ltd_mode = "ltd_mode"
    """An LTD version tracking mode mapped directly onto a kind."""

    rule = "rule"
    """A slug-rewrite rule (user-configured or built-in) matched the ref."""

    fallback = "fallback"
    """No mode mapping and no rule matched; the draft fallback applied."""


@dataclass(frozen=True, slots=True)
class EditionKindDerivation:
    """A derived Docverse edition kind plus its provenance."""

    kind: EditionKind
    """The Docverse edition kind to import the LTD edition as."""

    source: EditionKindSource
    """Which derivation arm produced :attr:`kind`."""

    detail: str | None = None
    """LTD mode for ``ltd_mode``, matched rule ``type`` for ``rule``."""


#: LTD version tracking modes that imply a Docverse kind on their own,
#: with no ref inspection. LTD only ever points these editions at
#: releases of the matching grammar, so the mode *is* the classification.
#: ``eups_daily_release`` is deliberately ``draft`` — dailies should keep
#: aging out under the ``draft_inactivity`` lifecycle rule.
_MODE_KIND_TABLE: dict[LtdEditionMode, EditionKind] = {
    LtdEditionMode.lsst_doc: EditionKind.release,
    LtdEditionMode.eups_major_release: EditionKind.release,
    LtdEditionMode.eups_weekly_release: EditionKind.release,
    LtdEditionMode.eups_daily_release: EditionKind.draft,
}


def derive_edition_kind(
    ltd_edition: LtdEdition,
    *,
    git_ref: str | None = None,
    rules: Sequence[AnySlugRewriteRule] = (),
) -> EditionKindDerivation:
    """Pick the Docverse :class:`EditionKind` for an LTD edition.

    Mode-first, then rule-driven:

    1. LTD's ``main`` edition maps onto Docverse's auto-created
       ``__main`` edition (``EditionKind.main``).
    2. An LTD version tracking mode (``lsst_doc``, ``eups_*``) maps
       straight onto a kind via ``_MODE_KIND_TABLE``, without
       consulting the ref at all.
    3. Everything else (``git_refs``, ``manual``) runs the full
       slug-rewrite rule chain — org/project rules then the built-in
       version heuristics — against the tracked ref.

    Parameters
    ----------
    ltd_edition
        The LTD edition being imported.
    git_ref
        The ref the Docverse tracking pair pins, as returned by
        :func:`map_edition_tracking`. Falls back to the edition's first
        ``tracked_refs`` entry and finally to the LTD slug, so an
        edition with no ref at all still gets classified on its slug.
    rules
        Ordered org/project-configured slug rewrite rules. The built-in
        version rules are always appended by the domain layer.

    Returns
    -------
    EditionKindDerivation
        The derived kind and where it came from.
    """
    if ltd_edition.slug == LTD_MAIN_SLUG:
        return EditionKindDerivation(
            kind=EditionKind.main, source=EditionKindSource.ltd_main
        )

    ltd_mode = _parse_mode(ltd_edition.mode)
    if ltd_mode is not None:
        mode_kind = _MODE_KIND_TABLE.get(ltd_mode)
        if mode_kind is not None:
            return EditionKindDerivation(
                kind=mode_kind,
                source=EditionKindSource.ltd_mode,
                detail=ltd_mode.value,
            )

    tracked = ltd_edition.tracked_refs[0] if ltd_edition.tracked_refs else None
    ref = git_ref or tracked or ltd_edition.slug
    derivation = derive_edition_kind_from_ref(ref, rules)
    if derivation.matched_rule_type is None:
        return EditionKindDerivation(
            kind=derivation.edition_kind, source=EditionKindSource.fallback
        )
    return EditionKindDerivation(
        kind=derivation.edition_kind,
        source=EditionKindSource.rule,
        detail=derivation.matched_rule_type,
    )


def _parse_mode(mode: str) -> LtdEditionMode | None:
    """Parse LTD's ``mode`` string, tolerating schema drift.

    Unknown modes return ``None`` so kind derivation degrades to the
    rule chain. :func:`map_edition_tracking` — which every caller runs
    first — raises on the same input, so this is a defensive branch.
    """
    try:
        return LtdEditionMode(mode)
    except ValueError:
        return None


def derive_edition_slug(ltd_slug: str) -> str:
    """Derive the Docverse edition slug from the LTD edition slug.

    The LTD ``main`` edition is folded onto Docverse's ``__main`` slug
    so the auto-created default edition is updated rather than
    duplicated. Every other LTD slug is preserved verbatim — uppercase
    ticket-style slugs (e.g. ``DM-54112``) round-trip thanks to the
    relaxed edition-slug regex from #286.
    """
    if ltd_slug == LTD_MAIN_SLUG:
        return DOCVERSE_MAIN_SLUG
    return ltd_slug


def derive_edition_source_prefix(
    *, bucket_root_dir: str, ltd_edition_slug: str
) -> str | None:
    """Derive the LTD *edition* prefix that mirrors a build's content.

    LTD's publish step copies a build's objects from
    ``<product>/builds/<build-slug>/`` to
    ``<product>/v/<edition-slug>/`` and it is the latter copy Fastly
    serves. On LTD's earliest uploads only that published copy carries a
    public-read ACL, so it is the only prefix an anonymous reader can
    recover the content from (#516).

    The product root is read back off ``bucket_root_dir`` rather than
    taken from ``LtdProduct.slug`` so the two prefixes are guaranteed to
    be siblings in the bucket even if a product's layout ever diverges
    from its slug.

    Returns
    -------
    str | None
        The ``<product>/v/<edition-slug>/`` prefix, or ``None`` when the
        edition has no such prefix: LTD serves the default edition from
        the product root, not ``v/main/``, and a ``bucket_root_dir``
        that is not ``<product>/builds/<build-slug>`` does not locate a
        product root at all.
    """
    if derive_edition_slug(ltd_edition_slug) == DOCVERSE_MAIN_SLUG:
        return None
    segments = bucket_root_dir.strip("/").split("/")
    if len(segments) < 3 or segments[-2] != _LTD_BUILDS_SEGMENT:
        return None
    product_root = "/".join(segments[:-2])
    edition_segment = ltd_edition_slug.strip("/")
    return f"{product_root}/{_LTD_EDITIONS_SEGMENT}/{edition_segment}/"


_VERSION_MODE_TABLE: dict[LtdEditionMode, TrackingMode] = {
    LtdEditionMode.lsst_doc: TrackingMode.lsst_doc,
    LtdEditionMode.eups_major_release: TrackingMode.eups_major_release,
    LtdEditionMode.eups_weekly_release: TrackingMode.eups_weekly_release,
    LtdEditionMode.eups_daily_release: TrackingMode.eups_daily_release,
}


def map_edition_tracking(
    edition: LtdEdition,
    *,
    build: LtdBuild | None = None,
) -> tuple[TrackingMode, dict[str, Any]]:
    """Map an LTD edition's tracking mode onto Docverse's tracking pair.

    Returns a ``(tracking_mode, tracking_params)`` tuple matching the
    columns on Docverse's ``editions`` row.

    ``manual`` is the only mode that needs the currently-published
    build: LTD's ``manual`` editions do not auto-track at all, so the
    importer pins them to whichever ref the published build was built
    from (``LtdBuild.git_refs[0]``). The original LTD ``manual`` mode
    is preserved by the caller in ``keeper_sync_state.annotations`` for
    reversibility — this mapper just emits the tracking pair.

    Raises
    ------
    ValueError
        If ``mode == "git_refs"`` but ``tracked_refs`` is empty/None,
        if ``mode == "manual"`` but ``build`` is None or its
        ``git_refs`` is empty/None, or if ``mode`` is an unknown LTD
        string (schema drift).
    """
    try:
        ltd_mode = LtdEditionMode(edition.mode)
    except ValueError as exc:
        msg = (
            f"LTD edition {edition.slug!r} reports unknown mode"
            f" {edition.mode!r}; LTD schema drift not handled here"
        )
        raise ValueError(msg) from exc

    if ltd_mode is LtdEditionMode.git_refs:
        return _map_git_refs(edition)
    mapped = _VERSION_MODE_TABLE.get(ltd_mode)
    if mapped is not None:
        return mapped, {}
    if ltd_mode is LtdEditionMode.manual:
        return _map_manual(edition, build)

    msg = (
        f"LTD edition {edition.slug!r} declares mode {edition.mode!r} but no"
        " mapper rule is defined; this is a programming error in mappers.py"
    )
    raise ValueError(msg)


def _map_git_refs(edition: LtdEdition) -> tuple[TrackingMode, dict[str, Any]]:
    if not edition.tracked_refs:
        msg = (
            f"LTD edition {edition.slug!r} declares mode=git_refs but"
            " supplies no tracked_refs"
        )
        raise ValueError(msg)
    return TrackingMode.git_ref, {"git_ref": edition.tracked_refs[0]}


def _map_manual(
    edition: LtdEdition, build: LtdBuild | None
) -> tuple[TrackingMode, dict[str, Any]]:
    if build is None:
        msg = (
            f"LTD edition {edition.slug!r} declares mode=manual but no"
            " build was supplied; the published build's git_refs is"
            " required to pin a Docverse git_ref tracking pair"
        )
        raise ValueError(msg)
    if not build.git_refs:
        msg = (
            f"LTD edition {edition.slug!r} declares mode=manual and the"
            f" published build (id={build.ltd_id}) reports no git_refs;"
            " cannot derive a Docverse git_ref tracking pair"
        )
        raise ValueError(msg)
    return TrackingMode.git_ref, {"git_ref": build.git_refs[0]}
