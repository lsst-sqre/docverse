"""Domain model for projects."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from docverse.models import EditionAutocreationConfig
from docverse.models.projects import InstallationStatus, build_github_url

from .lifecycle import LifecycleRuleSet


class Project(BaseModel):
    """Domain representation of a project."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the project.")

    public_id: int = Field(
        description=(
            "Time-ordered Crockford Base32 identifier for the project,"
            " stored as an integer and serialized on the wire as the"
            " ``id`` field."
        )
    )

    slug: str = Field(description="URL-safe identifier for the project.")

    title: str = Field(description="Display title for the project.")

    org_id: int = Field(
        description="ID of the organization this project belongs to."
    )

    source_url: str | None = Field(
        default=None,
        description=(
            "Stored non-GitHub URL of the documentation source"
            " repository, or ``None``. Never a ``github.com`` URL: a"
            " GitHub repo is tracked by the structured ``github_*``"
            " binding instead. Read"
            " :attr:`effective_source_url` for the value consumers"
            " should display."
        ),
    )

    github_owner: str | None = Field(
        default=None,
        description=(
            "Owner login of the GitHub repository backing this project."
            " Populated together with ``github_repo``."
        ),
    )

    github_repo: str | None = Field(
        default=None,
        description=(
            "Name of the GitHub repository backing this project."
            " Populated together with ``github_owner``."
        ),
    )

    github_owner_id: int | None = Field(
        default=None,
        description=(
            "GitHub numeric owner id, captured opportunistically once the"
            " GitHub App resolves the repository."
        ),
    )

    github_repo_id: int | None = Field(
        default=None,
        description=(
            "GitHub numeric repository id, captured opportunistically once"
            " the GitHub App resolves the repository."
        ),
    )

    github_installation_id: int | None = Field(
        default=None,
        description=(
            "GitHub App installation id for the repository, captured"
            " opportunistically once the App is installed."
        ),
    )

    github_default_branch: str | None = Field(
        default=None,
        description=(
            "The repository's default branch as GitHub last reported"
            " it, captured by the resolve worker. ``None`` until"
            " learned; consumers fall back to ``main``."
        ),
    )

    slug_rewrite_rules: list[dict[str, Any]] | None = Field(
        default=None,
        description="Rules for rewriting project slugs in URLs.",
    )

    lifecycle_rules: LifecycleRuleSet | None = Field(
        default=None,
        description="Rules governing build lifecycle.",
    )

    edition_autocreation: EditionAutocreationConfig | None = Field(
        default=None,
        description=(
            "Which editions are auto-created for this project. ``None``"
            " defers to the organization's config, then to the built-in"
            " defaults."
        ),
    )

    date_created: datetime = Field(
        description="Timestamp when the project was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )

    date_deleted: datetime | None = Field(
        default=None,
        description="Timestamp when the project was soft-deleted.",
    )

    @property
    def effective_source_url(self) -> str | None:
        """Derive the source-repository URL consumers should display.

        The structured ``github`` binding is the single source of truth
        for GitHub-backed projects, so it wins when present and yields
        the canonical ``https://github.com/{owner}/{repo}``. Otherwise
        the stored non-GitHub ``source_url`` is returned verbatim, and
        ``None`` when the project has no source coordinates at all. This
        lives here, once, so the API response and the dashboard context
        derive the same value.
        """
        if self.github_owner is not None and self.github_repo is not None:
            return build_github_url(self.github_owner, self.github_repo)
        return self.source_url

    @property
    def github_installation_status(self) -> InstallationStatus | None:
        """Derive the App-installation status for the github binding.

        Only meaningful with a binding present. ``installation_id`` set
        -> installed; else not_installed. Note: a not-yet-resolved or
        transiently-failed resolve also reads as not_installed until the
        worker or installation webhook backfills the id (acceptable for
        the derived-only status; persisting the resolve outcome is a
        later slice).
        """
        if self.github_owner is None or self.github_repo is None:
            return None
        if self.github_installation_id is not None:
            return InstallationStatus.installed
        return InstallationStatus.not_installed


@dataclass(frozen=True, slots=True)
class ProjectListingWatermark:
    """Validator material for one organization's project listing.

    Conditional GET on ``GET /orgs/{org}/projects`` needs a cheap value
    that changes whenever any row of the listing changes. The newest
    ``date_updated`` is not that value: every row is stamped with
    PostgreSQL's transaction *start* clock (``now()``), and commit order
    is not start order, so a slow writer can commit after a poller has
    read and land a ``date_updated`` below the maximum that poller
    already stored. The maximum would not move, and the poller would
    keep being told 304.

    These two aggregates have no such hole: a row that appears or
    disappears changes ``project_count``, and a row whose clock moves at
    all — up or down, above or below any maximum — changes
    ``clock_sum``. Neither names an instant, which is fine because the
    ``ETag`` they feed is opaque and Docverse publishes no
    ``Last-Modified``.
    """

    project_count: int
    """How many projects the org owns, deleted rows included."""

    clock_sum: int
    """Sum of every project's ``date_updated`` in whole microseconds
    since the POSIX epoch, or ``0`` when the org owns no projects.

    A plain sum rather than a hash: it is one aggregate PostgreSQL can
    compute over the same index scan as the other two, and any change
    to any row's clock moves it.
    """
