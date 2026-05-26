"""Tests for DashboardContextBuilder."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
import structlog
from rubin.repertoire import DiscoveryClient, register_mock_discovery
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    BuildCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
    UrlScheme,
)
from docverse.dbschema.edition import SqlEdition
from docverse.domain.organization import Organization
from docverse.domain.project import Project
from docverse.domain.published_url import project_published_url
from docverse.services.dashboard.context import DashboardContextBuilder
from docverse.storage.build_store import BuildStore
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

_HASH = "sha256:" + "a" * 64


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _make_builder(
    session: AsyncSession, discovery_client: DiscoveryClient
) -> DashboardContextBuilder:
    logger = _logger()
    return DashboardContextBuilder(
        org_store=OrganizationStore(session=session, logger=logger),
        project_store=ProjectStore(session=session, logger=logger),
        edition_store=EditionStore(session=session, logger=logger),
        build_store=BuildStore(session=session, logger=logger),
        discovery=discovery_client,
        logger=logger,
    )


async def _seed_org_and_project(
    session: AsyncSession,
    *,
    org_slug: str = "ctx-org",
    project_slug: str = "ctx-proj",
) -> tuple[Any, Any]:
    logger = _logger()
    org_store = OrganizationStore(session=session, logger=logger)
    proj_store = ProjectStore(session=session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title="Context Org",
            base_domain=f"{org_slug}.example.com",
        )
    )
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug=project_slug,
            title="Context Project",
            source_url="https://example.com/example/repo",
        ),
    )
    return org, project


async def _create_build(
    session: AsyncSession,
    project_id: int,
    *,
    git_ref: str,
    project_slug: str = "ctx-proj",
) -> Any:
    build_store = BuildStore(session=session, logger=_logger())
    return await build_store.create(
        project_id=project_id,
        data=BuildCreate(git_ref=git_ref, content_hash=_HASH),
        uploader="testuser",
        project_slug=project_slug,
    )


async def _create_edition(
    session: AsyncSession,
    project_id: int,
    *,
    slug: str,
    title: str,
    kind: EditionKind,
    tracking_params: dict[str, Any] | None = None,
    current_build_id: int | None = None,
) -> Any:
    store = EditionStore(session=session, logger=_logger())
    edition = await store.create_internal(
        project_id=project_id,
        slug=slug,
        title=title,
        kind=kind,
        tracking_mode=TrackingMode.git_ref,
        tracking_params=tracking_params or {"git_ref": slug},
    )
    if current_build_id is not None:
        await store.set_current_build(
            edition_id=edition.id,
            build_id=current_build_id,
            skip_date_guard=True,
        )
    return edition


@pytest.mark.asyncio
async def test_context_builder_groups_editions_by_kind(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    async with db_session.begin():
        org, project = await _seed_org_and_project(db_session)
        await _create_edition(
            db_session,
            project.id,
            slug="__main",
            title="Main",
            kind=EditionKind.main,
        )
        await _create_edition(
            db_session,
            project.id,
            slug="v3.0.0",
            title="v3.0.0",
            kind=EditionKind.release,
        )
        await _create_edition(
            db_session,
            project.id,
            slug="v1.5.0",
            title="v1.5.0",
            kind=EditionKind.release,
        )
        await _create_edition(
            db_session,
            project.id,
            slug="latest",
            title="Latest",
            kind=EditionKind.draft,
        )
        await _create_edition(
            db_session,
            project.id,
            slug="2",
            title="2.x",
            kind=EditionKind.major,
        )
        await _create_edition(
            db_session,
            project.id,
            slug="1.5",
            title="1.5.x",
            kind=EditionKind.minor,
        )
        await _create_edition(
            db_session,
            project.id,
            slug="staging",
            title="Staging",
            kind=EditionKind.alternate,
        )
        await db_session.commit()

    async with db_session.begin():
        builder = _make_builder(db_session, discovery_client)
        ctx = await builder.build(org_id=org.id, project_id=project.id)

    assert ctx.editions.main is not None
    assert ctx.editions.main.slug == "__main"
    assert [e.slug for e in ctx.editions.releases] == ["v3.0.0", "v1.5.0"]
    assert [e.slug for e in ctx.editions.major] == ["2"]
    assert [e.slug for e in ctx.editions.minor] == ["1.5"]
    assert [e.slug for e in ctx.editions.drafts] == ["latest"]
    assert [e.slug for e in ctx.editions.alternates] == ["staging"]


@pytest.mark.asyncio
async def test_releases_sorted_semver_descending(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    async with db_session.begin():
        org, project = await _seed_org_and_project(
            db_session, org_slug="rel-org", project_slug="rel-proj"
        )
        # Insert out of order to prove the sort.
        for slug in ("v1.0.0", "v10.0.0", "v2.5.3", "v2.5.10"):
            await _create_edition(
                db_session,
                project.id,
                slug=slug,
                title=slug,
                kind=EditionKind.release,
            )
        await db_session.commit()

    async with db_session.begin():
        builder = _make_builder(db_session, discovery_client)
        ctx = await builder.build(org_id=org.id, project_id=project.id)

    assert [e.slug for e in ctx.editions.releases] == [
        "v10.0.0",
        "v2.5.10",
        "v2.5.3",
        "v1.0.0",
    ]


@pytest.mark.asyncio
async def test_drafts_sorted_by_date_updated_descending(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    async with db_session.begin():
        org, project = await _seed_org_and_project(
            db_session, org_slug="draft-org", project_slug="draft-proj"
        )
        old = await _create_edition(
            db_session,
            project.id,
            slug="oldfeature",
            title="Old",
            kind=EditionKind.draft,
        )
        new = await _create_edition(
            db_session,
            project.id,
            slug="newfeature",
            title="New",
            kind=EditionKind.draft,
        )
        # Force date_updated values to differ.
        now = datetime.now(tz=UTC)
        await db_session.execute(
            update(SqlEdition)
            .where(SqlEdition.id == old.id)
            .values(date_updated=now - timedelta(days=2))
        )
        await db_session.execute(
            update(SqlEdition)
            .where(SqlEdition.id == new.id)
            .values(date_updated=now)
        )
        await db_session.commit()

    async with db_session.begin():
        builder = _make_builder(db_session, discovery_client)
        ctx = await builder.build(org_id=org.id, project_id=project.id)

    assert [e.slug for e in ctx.editions.drafts] == [
        "newfeature",
        "oldfeature",
    ]


@pytest.mark.asyncio
async def test_alternates_sorted_alphabetically_by_title(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    async with db_session.begin():
        org, project = await _seed_org_and_project(
            db_session, org_slug="alt-org", project_slug="alt-proj"
        )
        for slug, title in (("z-east", "East"), ("a-west", "West")):
            await _create_edition(
                db_session,
                project.id,
                slug=slug,
                title=title,
                kind=EditionKind.alternate,
            )
        await db_session.commit()

    async with db_session.begin():
        builder = _make_builder(db_session, discovery_client)
        ctx = await builder.build(org_id=org.id, project_id=project.id)

    assert [e.title for e in ctx.editions.alternates] == ["East", "West"]


@pytest.mark.asyncio
async def test_edition_without_current_build_has_no_build(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    async with db_session.begin():
        org, project = await _seed_org_and_project(
            db_session, org_slug="nob-org", project_slug="nob-proj"
        )
        await _create_edition(
            db_session,
            project.id,
            slug="__main",
            title="Main",
            kind=EditionKind.main,
        )
        await db_session.commit()

    async with db_session.begin():
        builder = _make_builder(db_session, discovery_client)
        ctx = await builder.build(org_id=org.id, project_id=project.id)

    assert ctx.editions.main is not None
    assert ctx.editions.main.build is None


@pytest.mark.asyncio
async def test_alternate_name_surfaces_into_edition_context(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    async with db_session.begin():
        org, project = await _seed_org_and_project(
            db_session, org_slug="an-org", project_slug="an-proj"
        )
        edition = await _create_edition(
            db_session,
            project.id,
            slug="dev-east",
            title="Dev East",
            kind=EditionKind.draft,
        )
        await db_session.execute(
            update(SqlEdition)
            .where(SqlEdition.id == edition.id)
            .values(alternate_name="east")
        )
        await db_session.commit()

    async with db_session.begin():
        builder = _make_builder(db_session, discovery_client)
        ctx = await builder.build(org_id=org.id, project_id=project.id)

    drafts = ctx.editions.drafts
    assert len(drafts) == 1
    assert drafts[0].alternate_name == "east"


@pytest.mark.asyncio
async def test_main_surfaced_separately_not_in_releases(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    async with db_session.begin():
        org, project = await _seed_org_and_project(
            db_session, org_slug="ms-org", project_slug="ms-proj"
        )
        await _create_edition(
            db_session,
            project.id,
            slug="__main",
            title="Main",
            kind=EditionKind.main,
        )
        await _create_edition(
            db_session,
            project.id,
            slug="v1.0.0",
            title="v1.0.0",
            kind=EditionKind.release,
        )
        await db_session.commit()

    async with db_session.begin():
        builder = _make_builder(db_session, discovery_client)
        ctx = await builder.build(org_id=org.id, project_id=project.id)

    assert ctx.editions.main is not None
    assert ctx.editions.main.slug == "__main"
    assert all(e.slug != "__main" for e in ctx.editions.releases)


@pytest.mark.asyncio
async def test_empty_project_yields_empty_groupings(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    async with db_session.begin():
        org, project = await _seed_org_and_project(
            db_session, org_slug="empty-org", project_slug="empty-proj"
        )
        await db_session.commit()

    async with db_session.begin():
        builder = _make_builder(db_session, discovery_client)
        ctx = await builder.build(org_id=org.id, project_id=project.id)

    assert ctx.editions.main is None
    assert ctx.editions.releases == []
    assert ctx.editions.drafts == []
    assert ctx.editions.major == []
    assert ctx.editions.minor == []
    assert ctx.editions.alternates == []


@pytest.mark.asyncio
async def test_rendered_at_defaults_to_now_utc(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    async with db_session.begin():
        org, project = await _seed_org_and_project(
            db_session, org_slug="ts-org", project_slug="ts-proj"
        )
        await db_session.commit()

    async with db_session.begin():
        builder = _make_builder(db_session, discovery_client)
        before = datetime.now(tz=UTC)
        ctx = await builder.build(org_id=org.id, project_id=project.id)
        after = datetime.now(tz=UTC)

    assert before <= ctx.rendered_at <= after
    assert ctx.rendered_at.tzinfo is UTC


@pytest.mark.asyncio
async def test_build_context_populated_when_current_build_set(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    async with db_session.begin():
        org, project = await _seed_org_and_project(
            db_session, org_slug="bc-org", project_slug="bc-proj"
        )
        build = await _create_build(
            db_session, project.id, git_ref="main", project_slug="bc-proj"
        )
        await _create_edition(
            db_session,
            project.id,
            slug="__main",
            title="Main",
            kind=EditionKind.main,
            current_build_id=build.id,
        )
        await db_session.commit()

    async with db_session.begin():
        builder = _make_builder(db_session, discovery_client)
        ctx = await builder.build(org_id=org.id, project_id=project.id)

    main = ctx.editions.main
    assert main is not None
    assert main.build is not None
    assert main.build.git_ref == "main"


@pytest.mark.asyncio
async def test_api_url_comes_from_repertoire_discovery(
    db_session: AsyncSession,
    discovery_client: DiscoveryClient,
) -> None:
    """The built context's ``docverse.api_url`` is the Repertoire URL."""
    async with db_session.begin():
        org, project = await _seed_org_and_project(
            db_session, org_slug="rep-org", project_slug="rep-proj"
        )
        await db_session.commit()

    async with db_session.begin():
        builder = _make_builder(db_session, discovery_client)
        ctx = await builder.build(org_id=org.id, project_id=project.id)

    assert ctx.docverse.api_url == "https://example.test/docverse/api"


@pytest.mark.asyncio
async def test_build_raises_when_docverse_not_registered(
    db_session: AsyncSession,
    mock_discovery: respx.Router,
) -> None:
    """``build()`` raises when Repertoire returns no ``docverse`` URL."""
    # Re-register discovery without a ``docverse`` internal service.
    mock_discovery.reset()
    register_mock_discovery(mock_discovery, {"services": {"internal": {}}})

    async with db_session.begin():
        org, project = await _seed_org_and_project(
            db_session, org_slug="norep-org", project_slug="norep-proj"
        )
        await db_session.commit()

    async with httpx.AsyncClient() as http_client:
        discovery = DiscoveryClient(http_client)
        async with db_session.begin():
            builder = _make_builder(db_session, discovery)
            with pytest.raises(
                RuntimeError, match="not registered in Repertoire"
            ):
                await builder.build(org_id=org.id, project_id=project.id)


def _make_org(base_domain: str, url_scheme: UrlScheme) -> Organization:
    now = datetime.now(tz=UTC)
    return Organization.model_construct(
        id=1,
        slug="org",
        title="Org",
        base_domain=base_domain,
        url_scheme=url_scheme,
        root_path_prefix="/",
        slug_rewrite_rules=None,
        lifecycle_rules=None,
        default_edition_config=None,
        publishing_store_label=None,
        staging_store_label=None,
        cdn_service_label=None,
        dns_service_label=None,
        purgatory_retention=timedelta(seconds=0),
        date_created=now,
        date_updated=now,
    )


def _make_project() -> Project:
    now = datetime.now(tz=UTC)
    return Project.model_construct(
        id=1,
        slug="proj",
        title="Project",
        org_id=1,
        source_url="https://example.com/repo",
        slug_rewrite_rules=None,
        lifecycle_rules=None,
        date_created=now,
        date_updated=now,
    )


@pytest.mark.parametrize(
    "raw_base_domain",
    [
        "lsst.io",
        "https://lsst.io",
        "https://lsst.io/",
        "http://lsst.io",
        "docverse-dev-jsc-test-20260409.jsickcodes.workers.dev",
        "https://docverse-dev-jsc-test-20260409.jsickcodes.workers.dev",
    ],
)
@pytest.mark.parametrize(
    "url_scheme", [UrlScheme.subdomain, UrlScheme.path_prefix]
)
def test_project_published_url_normalizes_base_domain(
    raw_base_domain: str, url_scheme: UrlScheme
) -> None:
    org = _make_org(raw_base_domain, url_scheme)
    project = _make_project()

    url = project_published_url(org, project)

    assert url.startswith("https://")
    assert url.endswith("/")
    # Exactly one scheme prefix.
    assert url.count("https://") == 1
    assert "http://" not in url[len("https://") :]
    # Single trailing slash.
    assert not url.endswith("//")
