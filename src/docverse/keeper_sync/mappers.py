"""Pure mappers from LTD edition fields onto Docverse counterparts.

These are intentionally side-effect-free helpers so the
:class:`docverse.keeper_sync.service.KeeperSyncService` orchestration
can remain a thin wrapper over the existing
:class:`docverse.services.edition.EditionService` and friends. Only
``git_refs`` is fully mapped here; the remaining LTD modes raise
:class:`NotImplementedError` and are filled in by issue #289.
"""

from __future__ import annotations

from typing import Any

from docverse.client.models import EditionKind, TrackingMode

from .models import LtdEdition, LtdEditionMode

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


def map_edition_tracking(
    edition: LtdEdition,
) -> tuple[TrackingMode, dict[str, Any]]:
    """Map an LTD edition's tracking mode onto Docverse's tracking pair.

    Returns a ``(tracking_mode, tracking_params)`` tuple matching the
    columns on Docverse's ``editions`` row. Only ``git_refs`` is
    implemented in this slice; other modes raise
    :class:`NotImplementedError`.

    Raises
    ------
    NotImplementedError
        For any non-``git_refs`` LTD mode. Filled in by #289.
    ValueError
        If ``mode == "git_refs"`` but ``tracked_refs`` is empty/None.
    """
    if edition.mode == LtdEditionMode.git_refs:
        if not edition.tracked_refs:
            msg = (
                f"LTD edition {edition.slug!r} declares mode=git_refs but"
                " supplies no tracked_refs"
            )
            raise ValueError(msg)
        return TrackingMode.git_ref, {"git_ref": edition.tracked_refs[0]}

    msg = (
        f"LTD edition mode {edition.mode!r} is not yet supported by the"
        " sync engine; tracked in #289"
    )
    raise NotImplementedError(msg)
