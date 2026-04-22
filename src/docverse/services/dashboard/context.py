"""Build a :class:`DashboardContext` for a project."""

from __future__ import annotations

from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version

import structlog
from rubin.repertoire import DiscoveryClient

from docverse.client.models import EditionKind
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.build import Build
from docverse.domain.dashboard_context import (
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
from docverse.domain.edition import Edition
from docverse.domain.published_url import (
    edition_published_url,
    project_published_url,
)
from docverse.domain.version import SemverVersion
from docverse.exceptions import NotFoundError
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

__all__ = ["DashboardContextBuilder"]


def _docverse_version() -> str:
    try:
        return version("docverse")
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


def _semver_release_sort_key(
    edition: EditionContext,
) -> tuple[int, int, int, int]:
    """Descending semver sort key for ``release`` editions.

    Editions whose slug doesn't parse as semver sort last.
    """
    candidate = edition.slug.lstrip("v")
    parsed = SemverVersion.parse(candidate)
    if parsed is None:
        # ``-1`` ensures unparseable slugs sort after parseable ones
        # under reverse=True.
        return (-1, 0, 0, 0)
    return (parsed.major, parsed.minor, parsed.patch, 0)


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

    releases.sort(key=_semver_release_sort_key, reverse=True)
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
    )


class DashboardContextBuilder:
    """Assemble a :class:`DashboardContext` for one project render."""

    def __init__(  # noqa: PLR0913
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
                source_repo_url=project.doc_repo,
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
