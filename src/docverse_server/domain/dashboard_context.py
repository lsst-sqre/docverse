"""Jinja template context dataclasses for the dashboard pipeline.

These mirror the SQR-112 specification with one MVP deviation:
``ProjectContext.surrogate_key`` is intentionally omitted and will be
re-introduced with the CDN cache-purging ticket.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime

from docverse.models import EditionKind
from docverse_server.domain.version import EupsReleaseVersion

__all__ = [
    "MAIN_SLUG",
    "AssetsContext",
    "BuildContext",
    "DashboardContext",
    "DocverseContext",
    "EditionContext",
    "EditionsContext",
    "OrgContext",
    "ProjectContext",
    "eups_release_sort_key",
    "version_sort_key",
]


MAIN_SLUG = "__main"
"""Reserved slug for a project's canonical main edition."""

_VERSION_PREFIX = re.compile(r"^v?(\d+(?:\.\d+){0,2})")

_EUPS_CONTINUATION = re.compile(r"[-_]\d")
"""An EUPS separator that carries a version on past its leading dotted run.

``v30_0_12`` and the legacy ``13-0`` continue this way; a semver
prerelease such as ``v1.2.3-rc1`` does not.
"""


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
    by_slug: dict[str, EditionContext] = field(default_factory=dict)
    """Every live edition keyed by slug, ``__main`` included.

    Values are the same objects the grouped lists hold, so a template can
    pick an edition by name (``editions.by_slug['weekly']``) and compare
    it against the lists by identity.
    """


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


def eups_release_sort_key(
    version: EupsReleaseVersion,
) -> tuple[int, int, int, int, int]:
    """Compute a descending sort key for a parsed EUPS release tag.

    The key is ``(major, minor, patch, is_final, rc)``, so a final release
    sorts above its candidates and candidates sort by their number. Both
    the grouped ``releases`` list and `version_sort_key` use it, so the
    two agree on how EUPS release tags order.
    """
    return (
        version.major,
        version.minor,
        version.patch,
        int(version.is_final),
        version.rc or 0,
    )


def version_sort_key(edition: EditionContext) -> tuple[int, ...]:
    """Compute a descending sort key from an edition slug.

    The key is parsed from the leading numeric run of ``edition.slug``,
    except that a slug whose version carries on past that run with an
    EUPS ``_`` or ``-`` separator (``v30_0_12``, ``v30_0_12_rc3``, the
    legacy ``13-0``) and parses as an EUPS release tag keys by
    `eups_release_sort_key`, so such tags order by their full version
    rather than tying on their major. Non-version slugs sort last
    (``(-1,)``).
    """
    slug = edition.slug
    match = _VERSION_PREFIX.match(slug)
    if match is None:
        return (-1,)
    if _EUPS_CONTINUATION.match(slug, match.end()):
        eups = EupsReleaseVersion.parse(slug)
        if eups is not None:
            return eups_release_sort_key(eups)
    return tuple(int(p) for p in match.group(1).split("."))
