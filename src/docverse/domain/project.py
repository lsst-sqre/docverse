"""Domain model for projects."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from docverse.client.models.projects import build_github_url

from .lifecycle import LifecycleRuleSet


class Project(BaseModel):
    """Domain representation of a project."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the project.")

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

    slug_rewrite_rules: list[dict[str, Any]] | None = Field(
        default=None,
        description="Rules for rewriting project slugs in URLs.",
    )

    lifecycle_rules: LifecycleRuleSet | None = Field(
        default=None,
        description="Rules governing build lifecycle.",
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
