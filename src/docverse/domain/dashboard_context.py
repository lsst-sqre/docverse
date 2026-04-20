"""Jinja template context dataclasses for the dashboard pipeline.

These mirror the SQR-112 specification with one MVP deviation:
``ProjectContext.surrogate_key`` is intentionally omitted and will be
re-introduced with the CDN cache-purging ticket.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from docverse.client.models import EditionKind

__all__ = [
    "AssetsContext",
    "BuildContext",
    "DashboardContext",
    "DocverseContext",
    "EditionContext",
    "EditionsContext",
    "OrgContext",
    "ProjectContext",
]


@dataclass(frozen=True)
class OrgContext:
    """Template context for the owning organization."""

    slug: str
    title: str
    base_domain: str


@dataclass(frozen=True)
class ProjectContext:
    """Template context for a project.

    ``surrogate_key`` is intentionally absent in the MVP — see SQR-112
    deferred-scope notes.
    """

    slug: str
    title: str
    source_repo_url: str
    published_url: str


@dataclass(frozen=True)
class BuildContext:
    """Template context for an edition's currently-published build."""

    slug: str
    git_ref: str
    date: datetime


@dataclass(frozen=True)
class EditionContext:
    """Template context for one edition."""

    slug: str
    title: str
    kind: EditionKind
    alternate_name: str | None
    date_updated: datetime
    published_url: str
    build: BuildContext | None


@dataclass(frozen=True)
class EditionsContext:
    """Pre-grouped, pre-sorted edition lists for templates."""

    main: EditionContext | None
    releases: list[EditionContext] = field(default_factory=list)
    drafts: list[EditionContext] = field(default_factory=list)
    major: list[EditionContext] = field(default_factory=list)
    minor: list[EditionContext] = field(default_factory=list)
    alternates: list[EditionContext] = field(default_factory=list)


@dataclass(frozen=True)
class AssetsContext:
    """Inlined CSS/JS/image assets surfaced to templates.

    ``css`` and ``js`` are the concatenated source-text of all declared
    files in declared order. ``images`` is keyed by the asset's basename
    with dots and hyphens replaced by underscores; SVG values hold the
    raw markup, and raster values hold a base64 ``data:`` URI.
    """

    css: str = ""
    js: str = ""
    images: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DocverseContext:
    """Metadata about the Docverse server itself."""

    api_url: str
    version: str


@dataclass(frozen=True)
class DashboardContext:
    """Full Jinja context for one dashboard render."""

    org: OrgContext
    project: ProjectContext
    editions: EditionsContext
    assets: AssetsContext
    docverse: DocverseContext
    rendered_at: datetime
