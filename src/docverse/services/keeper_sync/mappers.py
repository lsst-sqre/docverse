"""Pure mappers from LTD edition fields onto Docverse counterparts.

These are intentionally side-effect-free helpers so the
:class:`docverse.services.keeper_sync.service.KeeperSyncService`
orchestration can remain a thin wrapper over the existing
:class:`docverse.services.edition.EditionService` and friends. Every
:class:`LtdEditionMode` value has a documented Docverse counterpart;
``manual`` is special-cased because Docverse has no semantic ``manual``
mode (PRD #275 "Out of scope") and is collapsed onto a pinned
``git_ref`` instead.
"""

from __future__ import annotations

from typing import Any

from docverse.client.models import EditionKind, TrackingMode
from docverse.storage.ltd import LtdBuild, LtdEdition, LtdEditionMode

__all__ = [
    "LTD_MAIN_SLUG",
    "derive_edition_kind",
    "derive_edition_slug",
    "map_edition_tracking",
]

LTD_MAIN_SLUG = "main"
"""LTD slug that corresponds to Docverse's auto-created ``__main`` edition."""


def derive_edition_kind(ltd_slug: str) -> EditionKind:
    """Pick the Docverse :class:`EditionKind` for an LTD edition slug.

    LTD's ``main`` edition maps onto Docverse's auto-created ``__main``
    edition (``EditionKind.main``). All other LTD editions are imported
    as ``EditionKind.draft``; the LTD slug is preserved verbatim.
    """
    if ltd_slug == LTD_MAIN_SLUG:
        return EditionKind.main
    return EditionKind.draft


def derive_edition_slug(ltd_slug: str) -> str:
    """Derive the Docverse edition slug from the LTD edition slug.

    The LTD ``main`` edition is folded onto Docverse's ``__main`` slug
    so the auto-created default edition is updated rather than
    duplicated. Every other LTD slug is preserved verbatim — uppercase
    ticket-style slugs (e.g. ``DM-54112``) round-trip thanks to the
    relaxed edition-slug regex from #286.
    """
    if ltd_slug == LTD_MAIN_SLUG:
        return "__main"
    return ltd_slug


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
