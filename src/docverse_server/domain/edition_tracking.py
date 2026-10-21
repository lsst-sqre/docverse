"""Result types for edition tracking."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from docverse.models import EditionKind


@dataclass(slots=True)
class EditionTrackingOutcome:
    """Result of tracking a single edition."""

    edition_id: int
    """Internal edition ID."""

    slug: str
    """Edition slug."""

    build_id: int
    """Build ID that was tracked."""

    action: Literal["updated", "created", "skipped"]
    """What happened to this edition.

    - ``updated``: existing edition pointer moved to this build.
    - ``created``: new edition auto-created and pointed to this build.
    - ``skipped``: stale-build guard rejected the update.
    """


@dataclass(slots=True, frozen=True)
class RevivedEdition:
    """An edition tracking brought back from a lifecycle soft-delete.

    Carries what the worker needs, after the tracking transaction
    commits, to publish the edition's ``edition_lifecycle`` ``restore``
    event without re-reading it.
    """

    edition_id: int
    """Internal edition ID."""

    slug: str
    """Edition slug."""

    kind: EditionKind
    """The edition's kind once tracking re-derived it."""


@dataclass(slots=True)
class EditionTrackingResult:
    """Aggregate result from ``EditionTrackingService.track_build``."""

    derived_slug: str | None
    """The slug derived from the build's git ref.

    ``None`` when suppressed by an ignore rule or when slug derivation
    raised ``InvalidSlugError``.
    """

    suppressed: bool
    """True only when an ignore rule matched the git ref."""

    outcomes: list[EditionTrackingOutcome] = field(default_factory=list)
    """Per-edition outcomes."""

    revived: list[RevivedEdition] = field(default_factory=list)
    """Editions this tracking revived from a lifecycle soft-delete.

    Each also appears in :attr:`outcomes`, as ``created`` when its
    pointer moved to the build: to everything downstream a revived
    edition is a new one. Listed here too because a revive is announced
    whether or not the pointer moved.
    """

    @property
    def updated(self) -> list[EditionTrackingOutcome]:
        """Outcomes where the edition pointer was set (updated or created)."""
        return [o for o in self.outcomes if o.action in ("updated", "created")]

    @property
    def skipped(self) -> list[EditionTrackingOutcome]:
        """Outcomes where the stale-build guard rejected the update."""
        return [o for o in self.outcomes if o.action == "skipped"]
