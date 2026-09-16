"""Domain model for editions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from docverse.models import EditionKind, EditionKindSource, TrackingMode
from docverse.models.queue_enums import PublishStatus

from .base32id import Base32Id

DEFAULT_EDITION_SLUG = "__main"
"""Slug for the default edition auto-created with every project.

Lives in the domain layer because both the service that creates the
edition and the storage layer that repoints it need to recognize it:
``EditionStore.set_current_build`` treats a repoint of *this* edition
as a change to the project itself.
"""


class Edition(BaseModel):
    """Domain representation of an edition."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the edition.")

    slug: str = Field(description="URL-safe identifier for the edition.")

    title: str = Field(description="Display title for the edition.")

    project_id: int = Field(
        description="ID of the project this edition belongs to."
    )

    kind: EditionKind = Field(description="Kind of edition.")

    kind_source: EditionKindSource = Field(
        default=EditionKindSource.derived,
        description=(
            "Who owns ``kind``. ``derived`` hands it to the automated"
            " derivation paths (keeper-sync's per-sync refresh and the"
            " native build-upload heal), which converge the row on the"
            " current rules in both directions; ``declared`` records an"
            " operator's decision and is never rewritten."
        ),
    )

    tracking_mode: TrackingMode = Field(
        description="How this edition tracks builds for automatic updates."
    )

    tracking_params: dict[str, Any] | None = Field(
        default=None,
        description="Parameters for the tracking mode.",
    )

    alternate_name: str | None = Field(
        default=None,
        description=(
            "Deployment variant scope for the edition (e.g., 'usdf-dev'). "
            "Populated for deployment-scoped tracking rules; ``None`` for "
            "all other editions."
        ),
    )

    current_build_id: int | None = Field(
        default=None,
        description="ID of the currently published build.",
    )

    current_build_public_id: Base32Id | None = Field(
        default=None,
        description=(
            "Public Base32 ID of the currently published build. "
            "Populated via join."
        ),
    )

    current_build_git_ref: str | None = Field(
        default=None,
        description=(
            "Git ref of the currently published build. Populated via join."
        ),
    )

    lifecycle_exempt: bool = Field(
        default=False,
        description="Whether this edition is exempt from lifecycle rules.",
    )

    publish_status: PublishStatus | None = Field(
        default=None,
        description=(
            "CDN publish state for the edition's current build. ``None``"
            " indicates the edition has never been published."
        ),
    )

    date_created: datetime = Field(
        description="Timestamp when the edition was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )

    date_deleted: datetime | None = Field(
        default=None,
        description="Timestamp when the edition was soft-deleted.",
    )


class RepointOutcome(StrEnum):
    """What a repoint did to an edition's ``current_build_id``.

    The three answers
    :meth:`~docverse_server.storage.edition_store.EditionStore.set_current_build`
    can give, kept apart because callers act on them differently:

    - ``repointed`` — the binding moved, so everything that announces a
      change (the history row, the ``publish_status`` flip, the publish
      job, the project's clock) is owed.
    - ``unchanged`` — the edition already served the target, so nothing
      was written. Only reachable where the stale-build guard is waived:
      that guard refuses a build compared with itself, because a build's
      ``date_created`` is never newer than its own.
    - ``refused`` — a guard turned the repoint down, either because the
      target is soft-deleted or because the edition already serves a
      build that is equally new or newer.
    """

    repointed = "repointed"
    unchanged = "unchanged"
    refused = "refused"


@dataclass(frozen=True, slots=True)
class EditionRepoint:
    """The result of one repoint attempt.

    ``edition`` is the edition as it stands after the call — the
    repointed row for ``repointed``, the untouched row for
    ``unchanged`` — and is ``None`` exactly when the outcome is
    ``refused``, which is the case in which there is no meaningful "as
    it stands" to report and every caller stands down anyway.
    """

    outcome: RepointOutcome

    edition: Edition | None
