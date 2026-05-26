"""Pydantic models for project resources."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Self
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .editions import DefaultEditionConfig
from .editions import Edition as EditionResponse
from .lifecycle import LifecycleRuleSet

__all__ = [
    "InstallationStatus",
    "Project",
    "ProjectCreate",
    "ProjectGitHubBinding",
    "ProjectGitHubBindingCreate",
    "ProjectUpdate",
    "build_github_url",
    "parse_github_url",
]


def build_github_url(owner: str, repo: str) -> str:
    """Build the canonical ``github.com`` URL for ``owner``/``repo``.

    Inverse of :func:`parse_github_url`: returns
    ``https://github.com/{owner}/{repo}`` with no trailing slash, path
    tail, or ``.git`` suffix. Used to derive a project's effective
    source URL from its structured GitHub binding, which is the single
    source of truth for GitHub-backed projects.
    """
    return f"https://github.com/{owner}/{repo}"


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


class InstallationStatus(StrEnum):
    """Whether the Docverse GitHub App is installed on a repository.

    Derived per response from the project's GitHub binding. Today only
    ``not_installed`` and ``installed`` are ever returned;
    ``suspended`` and ``needs_permissions`` are reserved enum values
    for a later slice (projects currently have no suspended state — the
    suspend/unsuspend webhooks only touch dashboard-template bindings).
    """

    not_installed = "not_installed"
    installed = "installed"
    suspended = "suspended"
    needs_permissions = "needs_permissions"


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

    installation_status: InstallationStatus = Field(
        description=(
            "Whether the Docverse GitHub App is installed on this repo."
            " Today only ``not_installed``/``installed`` are returned;"
            " ``suspended`` and ``needs_permissions`` are reserved."
        ),
    )

    app_url: str | None = Field(
        default=None,
        description=(
            "Public URL of the Docverse GitHub App's install page (e.g."
            " ``https://github.com/apps/{slug}``), so an operator can"
            " install the App on the repository. ``None`` when the"
            " GitHub App feature is unconfigured or its credentials"
            " failed startup validation."
        ),
    )


def _validate_source_url_github_exclusivity(
    *,
    github: ProjectGitHubBindingCreate | None,
    source_url: str | None,
) -> None:
    """Keep ``source_url`` free of GitHub repos and apart from ``github``.

    The structured ``github`` binding is the single source of truth for
    GitHub-backed projects, so the free-form ``source_url`` column must
    never carry a ``github.com`` URL. Two rules, both surfaced as a 422:

    * Rule A — ``source_url`` parses as a ``github.com`` repository URL.
      Reject it: the caller must use the ``github`` field instead.
    * Rule B — both ``source_url`` (non-null) and ``github`` (non-null)
      are supplied. Reject it: the two are mutually exclusive, since a
      project is GitHub-bound *or* points at a non-GitHub URL, never
      both.

    ``None`` on either side short-circuits the corresponding rule, so a
    PATCH that clears one field while setting the other (e.g.
    ``github: null`` plus a GitLab ``source_url``) passes cleanly.
    """
    if source_url is not None and parse_github_url(source_url) is not None:
        msg = (
            "source_url must not be a github.com URL; use the github field"
            " for GitHub repositories"
        )
        raise ValueError(msg)
    if source_url is not None and github is not None:
        msg = "source_url and github are mutually exclusive"
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
                "Non-GitHub URL of the documentation source repository;"
                " use the ``github`` field for GitHub repositories. A"
                " ``github.com`` value is rejected with a 422. ``None``"
                " for GitHub-bound projects and for projects with no"
                " source coordinates."
            ),
            examples=["https://gitlab.com/lsst/pipelines"],
        ),
    ] = None

    github: ProjectGitHubBindingCreate | None = Field(
        default=None,
        description=(
            "Structured GitHub coordinates for the project's source"
            " repository. Mutually exclusive with ``source_url``."
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
    def _check_source_url_github_exclusivity(self) -> Self:
        _validate_source_url_github_exclusivity(
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
            "Effective URL of the documentation source repository."
            " Derived from the ``github`` binding when present"
            " (``https://github.com/{owner}/{repo}``); otherwise the"
            " stored non-GitHub URL. ``None`` when neither is set."
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
            "Non-GitHub URL of the documentation source repository; use"
            " the ``github`` field for GitHub repositories. A"
            " ``github.com`` value is rejected with a 422. Pass a"
            " non-GitHub URL to clear any existing GitHub binding, or"
            " ``null`` explicitly to clear the field."
        ),
    )

    github: ProjectGitHubBindingCreate | None = Field(
        default=None,
        description=(
            "Structured GitHub coordinates for the project. Setting this"
            " nulls any stored ``source_url``. Pass ``null`` explicitly"
            " to clear the binding (and the opportunistically-captured"
            " numeric ids that depend on it)."
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
    def _check_source_url_github_exclusivity(self) -> Self:
        _validate_source_url_github_exclusivity(
            github=self.github, source_url=self.source_url
        )
        return self
