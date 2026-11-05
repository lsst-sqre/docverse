"""Build a :class:`DashboardContext` for a project."""

from __future__ import annotations

from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version

import structlog
from rubin.repertoire import DiscoveryClient

from docverse.models import EditionKind
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.build import Build
from docverse_server.domain.dashboard_context import (
    MAIN_SLUG,
    AssetsContext,
    BuildContext,
    DashboardContext,
    DocverseContext,
    EditionContext,
    EditionsContext,
    OrgContext,
    ProjectContext,
    version_sort_key,
)
from docverse_server.domain.edition import Edition
from docverse_server.domain.published_url import (
    edition_published_url,
    project_published_url,
)
from docverse_server.domain.version import EupsReleaseVersion, SemverVersion
from docverse_server.exceptions import NotFoundError
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore

__all__ = ["DashboardContextBuilder"]


def _docverse_version() -> str:
    try:
        return version("docverse-server")
    except PackageNotFoundError:
        return "0.0.0"


def _build_context(build: Build | None) -> BuildContext | None:
    if build is None:
        return None
    return BuildContext(
        slug=serialize_base32_id(build.public_id),
        git_ref=build.git_ref,
        date=build.date_created,
    )


def _edition_context(
    edition: Edition,
    build: Build | None,
    project_url: str,
) -> EditionContext:
    return EditionContext(
        slug=edition.slug,
        title=edition.title,
        kind=edition.kind,
        alternate_name=edition.alternate_name,
        date_updated=edition.date_updated,
        published_url=edition_published_url(project_url, edition),
        build=_build_context(build),
    )


_UNPARSEABLE_RELEASE_KEY = (-1, 0, 0, 0, 0)
"""Release sort key for a slug no version grammar parses.

The ``-1`` major sorts it after every parseable slug under
``reverse=True``; ties among such slugs keep the store's slug order.
"""


def _release_sort_key(
    edition: EditionContext,
) -> tuple[int, int, int, int, int]:
    """Descending version sort key for ``release`` editions.

    The slug is read as semver first (``v1.2.3``), then as an EUPS
    release tag (``v30_0_12``, two-component ``v17_0``, legacy ``13-0``),
    so both kinds of project list their releases newest version first and
    the two grammars share one ``(major, minor, patch)`` space. The last
    two components rank a final release above its candidates: an EUPS
    ``rcN`` sorts below its final and by its number. A semver prerelease
    keeps its final's key, as it always has, so semver-slugged projects
    order exactly as they did before EUPS slugs were understood. Slugs
    that neither grammar parses sort last.
    """
    semver = SemverVersion.parse(edition.slug.lstrip("v"))
    if semver is not None:
        return (semver.major, semver.minor, semver.patch, 1, 0)
    eups = EupsReleaseVersion.parse(edition.slug)
    if eups is not None:
        return (
            eups.major,
            eups.minor,
            eups.patch,
            int(eups.is_final),
            eups.rc or 0,
        )
    return _UNPARSEABLE_RELEASE_KEY


def _group_editions(
    editions: list[EditionContext],
) -> EditionsContext:
    main: EditionContext | None = None
    releases: list[EditionContext] = []
    drafts: list[EditionContext] = []
    major: list[EditionContext] = []
    minor: list[EditionContext] = []
    alternates: list[EditionContext] = []

    for edition in editions:
        if edition.slug == MAIN_SLUG:
            main = edition
            continue
        if edition.kind == EditionKind.release:
            releases.append(edition)
        elif edition.kind == EditionKind.draft:
            drafts.append(edition)
        elif edition.kind == EditionKind.major:
            major.append(edition)
        elif edition.kind == EditionKind.minor:
            minor.append(edition)
        elif edition.kind == EditionKind.alternate:
            alternates.append(edition)

    releases.sort(key=_release_sort_key, reverse=True)
    drafts.sort(key=lambda e: e.date_updated, reverse=True)
    major.sort(key=version_sort_key, reverse=True)
    minor.sort(key=version_sort_key, reverse=True)
    alternates.sort(key=lambda e: e.title)

    return EditionsContext(
        main=main,
        releases=releases,
        drafts=drafts,
        major=major,
        minor=minor,
        alternates=alternates,
        by_slug={edition.slug: edition for edition in editions},
    )


class DashboardContextBuilder:
    """Assemble a :class:`DashboardContext` for one project render."""

    def __init__(
        self,
        *,
        org_store: OrganizationStore,
        project_store: ProjectStore,
        edition_store: EditionStore,
        build_store: BuildStore,
        discovery: DiscoveryClient,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._org_store = org_store
        self._project_store = project_store
        self._edition_store = edition_store
        self._build_store = build_store
        self._discovery = discovery
        self._logger = logger

    async def build(
        self,
        *,
        org_id: int,
        project_id: int,
        rendered_at: datetime | None = None,
    ) -> DashboardContext:
        """Build the full context for one render.

        Raises
        ------
        NotFoundError
            If the org or project is missing.
        """
        org = await self._org_store.get_by_id(org_id)
        if org is None:
            msg = f"Organization {org_id} not found"
            raise NotFoundError(msg)
        project = await self._project_store.get_by_id(project_id)
        if project is None:
            msg = f"Project {project_id} not found"
            raise NotFoundError(msg)

        api_url = await self._discovery.url_for_internal("docverse")
        if api_url is None:
            msg = "Docverse is not registered in Repertoire"
            raise RuntimeError(msg)

        editions = await self._edition_store.list_all_by_project(project_id)

        project_url = project_published_url(org, project)
        edition_contexts: list[EditionContext] = []
        for edition in editions:
            build: Build | None = None
            if edition.current_build_id is not None:
                build = await self._build_store.get_by_id(
                    edition.current_build_id
                )
            edition_contexts.append(
                _edition_context(edition, build, project_url)
            )

        editions_context = _group_editions(edition_contexts)

        return DashboardContext(
            org=OrgContext(
                slug=org.slug,
                title=org.title,
                base_domain=org.base_domain,
            ),
            project=ProjectContext(
                slug=project.slug,
                title=project.title,
                source_repo_url=project.effective_source_url or "",
                published_url=project_url,
            ),
            editions=editions_context,
            assets=AssetsContext(),
            docverse=DocverseContext(
                api_url=api_url,
                version=_docverse_version(),
            ),
            rendered_at=rendered_at or datetime.now(tz=UTC),
        )
