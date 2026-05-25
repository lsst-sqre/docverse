"""Pydantic models for project resources."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Self
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .editions import DefaultEditionConfig
from .editions import Edition as EditionResponse
from .lifecycle import LifecycleRuleSet

__all__ = [
    "Project",
    "ProjectCreate",
    "ProjectGitHubBinding",
    "ProjectGitHubBindingCreate",
    "ProjectUpdate",
    "parse_github_url",
]


def parse_github_url(url: str) -> tuple[str, str] | None:
    """Parse a ``github.com`` URL into ``(owner, repo)``.

    Returns ``None`` if the host is not ``github.com`` (case-insensitive)
    or if the path does not have at least two non-empty segments. The
    conventional ``.git`` suffix on the repo segment is trimmed so the
    parsed value matches what GitHub returns for the repository name.
    Deeper paths (``/tree/main/docs``) are accepted; only the first two
    segments are interpreted as ``owner``/``repo``.
    """
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host != "github.com":
        return None
    segments = [s for s in parsed.path.split("/") if s]
    min_segments = 2
    if len(segments) < min_segments:
        return None
    owner = segments[0]
    repo = segments[1].removesuffix(".git")
    return owner, repo


_GITHUB_OWNER_FIELD = Field(
    min_length=1,
    max_length=39,
    pattern=r"^[A-Za-z0-9](?:-?[A-Za-z0-9])*$",
    description="GitHub owner (user or organization login).",
)

_GITHUB_REPO_FIELD = Field(
    min_length=1,
    max_length=100,
    pattern=r"^[A-Za-z0-9._-]+$",
    description="GitHub repository name.",
)


class ProjectGitHubBindingCreate(BaseModel):
    """Structured GitHub coordinates supplied on project create / update."""

    model_config = ConfigDict(extra="forbid")

    owner: Annotated[str, _GITHUB_OWNER_FIELD]

    repo: Annotated[str, _GITHUB_REPO_FIELD]


class ProjectGitHubBinding(BaseModel):
    """Structured GitHub coordinates returned on a project resource."""

    model_config = ConfigDict(from_attributes=True)

    owner: Annotated[str, _GITHUB_OWNER_FIELD]

    repo: Annotated[str, _GITHUB_REPO_FIELD]

    installation_id: int | None = Field(
        default=None,
        description=(
            "GitHub App installation id for the repository. ``None`` when"
            " the App is not installed or has not yet been resolved."
        ),
    )


def _validate_github_source_url_agreement(
    *,
    github: ProjectGitHubBindingCreate | None,
    source_url: str | None,
) -> None:
    """Reject inputs whose ``github`` disagrees with a github.com URL.

    Raises a ``ValueError`` (which Pydantic surfaces as a 422 response)
    when both fields are explicitly set, the URL parses as a
    ``github.com`` repository URL, and the parsed ``(owner, repo)`` pair
    does not match the structured sub-object. Non-GitHub ``source_url``
    values and ``None`` on either side short-circuit without checking,
    so the agreement rule never blocks a project bound to a non-GitHub
    host or a project being cleared of one side.
    """
    if github is None or source_url is None:
        return
    parsed = parse_github_url(source_url)
    if parsed is None:
        return
    if (github.owner, github.repo) != parsed:
        msg = (
            f"github sub-object ({github.owner}/{github.repo}) disagrees"
            f" with source_url ({source_url})"
        )
        raise ValueError(msg)


class ProjectCreate(BaseModel):
    """Request model for creating a project."""

    model_config = ConfigDict(extra="forbid")

    slug: Annotated[
        str,
        Field(
            pattern=r"^[a-z0-9][a-z0-9-]*[a-z0-9]$",
            min_length=2,
            max_length=128,
            description="URL-safe identifier for the project.",
            examples=["pipelines"],
        ),
    ]

    title: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            description="Display title for the project.",
            examples=["LSST Science Pipelines"],
        ),
    ]

    source_url: Annotated[
        str | None,
        Field(
            default=None,
            min_length=1,
            max_length=512,
            description=(
                "URL of the documentation source repository. ``None``"
                " for projects whose source coordinates are tracked only"
                " by the structured ``github`` sub-object."
            ),
            examples=["https://github.com/lsst/pipelines_lsst_io"],
        ),
    ] = None

    github: ProjectGitHubBindingCreate | None = Field(
        default=None,
        description=(
            "Structured GitHub coordinates for the project's source"
            " repository. When omitted, the server populates this from"
            " ``source_url`` if the URL is on ``github.com``."
        ),
    )

    default_edition: DefaultEditionConfig | None = Field(
        default=None,
        description=(
            "Configuration for the default edition. If omitted, the"
            " organization default or hardcoded default is used."
        ),
    )

    lifecycle_rules: LifecycleRuleSet | None = Field(
        default=None,
        description="Rules governing build lifecycle.",
    )

    @model_validator(mode="after")
    def _check_github_source_url_agreement(self) -> Self:
        _validate_github_source_url_agreement(
            github=self.github, source_url=self.source_url
        )
        return self


class Project(BaseModel):
    """Response model for a project."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this project resource.")

    org_url: str = Field(description="URL to the parent organization.")

    editions_url: str = Field(
        description="URL to list editions for this project."
    )

    builds_url: str = Field(description="URL to list builds for this project.")

    dashboard_template_url: str = Field(
        description=(
            "URL to the project's dashboard-template binding override."
        ),
    )

    slug: str = Field(description="URL-safe identifier for the project.")

    title: str = Field(description="Display title for the project.")

    source_url: str | None = Field(
        default=None,
        description=(
            "URL of the documentation source repository. ``None`` for"
            " projects whose source coordinates are tracked only by the"
            " structured ``github`` sub-object."
        ),
    )

    github: ProjectGitHubBinding | None = Field(
        default=None,
        description=(
            "Structured GitHub coordinates for the project. ``None`` for"
            " projects whose source repository is not on GitHub."
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

    default_edition: EditionResponse | None = Field(
        default=None,
        description=(
            "The default (__main) edition for this project. Populated on"
            " single-project responses (GET, POST, PATCH) but omitted"
            " from list responses."
        ),
    )

    date_created: datetime = Field(
        description="Timestamp when the project was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )


class ProjectUpdate(BaseModel):
    """Request model for updating a project (PATCH)."""

    model_config = ConfigDict(extra="forbid")

    title: str | None = Field(
        default=None, description="Display title for the project."
    )

    source_url: str | None = Field(
        default=None,
        description=(
            "URL of the documentation source repository. Pass ``null``"
            " explicitly to clear the field."
        ),
    )

    github: ProjectGitHubBindingCreate | None = Field(
        default=None,
        description=(
            "Structured GitHub coordinates for the project. Pass ``null``"
            " explicitly to clear the binding (and the opportunistically-"
            "captured numeric ids that depend on it)."
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

    @model_validator(mode="after")
    def _check_github_source_url_agreement(self) -> Self:
        _validate_github_source_url_agreement(
            github=self.github, source_url=self.source_url
        )
        return self
