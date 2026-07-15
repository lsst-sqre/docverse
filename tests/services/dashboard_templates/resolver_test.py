"""Tests for TemplateResolver."""

from __future__ import annotations

import pytest
import structlog
from docverse.client.models import OrganizationCreate, ProjectCreate
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.domain.project import Project
from docverse.services.dashboard_templates.resolver import (
    ResolvedTemplateOrigin,
    TemplateResolver,
)
from docverse.storage.dashboard_templates.builtin import BuiltInTemplateSource
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
    GitHubTemplateSource,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore

_TEMPLATE_TOML = b"""\
[dashboard]
template = "dashboard.html.jinja"
"""


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


async def _seed_org_and_project(
    session: AsyncSession,
    *,
    org_slug: str,
    project_slug: str = "resolver-proj",
) -> tuple[int, Project]:
    logger = _logger()
    org_store = OrganizationStore(session=session, logger=logger)
    proj_store = ProjectStore(session=session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title=f"Org {org_slug}",
            base_domain=f"{org_slug}.example.com",
        )
    )
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug=project_slug,
            title="Resolver Project",
            source_url="https://example.com/example/repo",
        ),
    )
    return org.id, project


async def _seed_template(
    session: AsyncSession,
    *,
    key: GitHubTemplateKey,
    template_toml: bytes = _TEMPLATE_TOML,
) -> int:
    template_store = DashboardGitHubTemplateStore(
        session=session, logger=_logger()
    )
    result = await template_store.upsert(
        key=key,
        commit_sha="deadbeef",
        etag="etag-1",
        template_toml=template_toml,
        files=[
            GitHubTemplateFileInput(
                relative_path="dashboard.html.jinja",
                is_text=True,
                data=b"<html>custom</html>",
            ),
        ],
    )
    return result.template.id


def _binding_create(
    *,
    org_id: int,
    project_id: int | None,
    github_owner: str = "acme",
    github_repo: str = "dashboard-templates",
    github_ref: str = "main",
    root_path: str = "/",
) -> DashboardGitHubTemplateBindingCreate:
    return DashboardGitHubTemplateBindingCreate(
        org_id=org_id,
        project_id=project_id,
        github_owner=github_owner,
        github_repo=github_repo,
        github_ref=github_ref,
        root_path=root_path,
    )


def _make_resolver(session: AsyncSession) -> TemplateResolver:
    logger = _logger()
    binding_store = DashboardGitHubTemplateBindingStore(
        session=session, logger=logger
    )
    template_store = DashboardGitHubTemplateStore(
        session=session, logger=logger
    )
    return TemplateResolver(
        binding_store=binding_store,
        template_store=template_store,
        logger=logger,
    )


@pytest.mark.asyncio
async def test_resolve_returns_builtin_when_no_bindings_exist(
    db_session: AsyncSession,
) -> None:
    """With zero binding rows, the built-in template serves every project."""
    async with db_session.begin():
        _, project = await _seed_org_and_project(
            db_session, org_slug="resolver-empty"
        )
        await db_session.commit()

    resolver = _make_resolver(db_session)
    async with db_session.begin():
        resolved = await resolver.resolve(
            org_id=project.org_id, project_id=project.id
        )
        await db_session.rollback()

    assert resolved.origin is ResolvedTemplateOrigin.builtin
    assert isinstance(resolved.source, BuiltInTemplateSource)


@pytest.mark.asyncio
async def test_resolve_returns_org_default_when_only_default_synced(
    db_session: AsyncSession,
) -> None:
    """With an org default binding that has a synced template, use it."""
    async with db_session.begin():
        org_id, project = await _seed_org_and_project(
            db_session, org_slug="resolver-org"
        )
        template_id = await _seed_template(
            db_session,
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="dashboard-templates",
                github_ref="main",
                root_path="/",
            ),
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await binding_store.create(
            _binding_create(org_id=org_id, project_id=None)
        )
        await binding_store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="succeeded",
            github_template_id=template_id,
        )
        await db_session.commit()

    resolver = _make_resolver(db_session)
    async with db_session.begin():
        resolved = await resolver.resolve(
            org_id=project.org_id, project_id=project.id
        )
        await db_session.rollback()

    assert resolved.origin is ResolvedTemplateOrigin.org_default
    assert isinstance(resolved.source, GitHubTemplateSource)


@pytest.mark.asyncio
async def test_resolve_returns_project_override_when_override_synced(
    db_session: AsyncSession,
) -> None:
    """A synced project override shadows any other binding layer."""
    async with db_session.begin():
        org_id, project = await _seed_org_and_project(
            db_session, org_slug="resolver-override"
        )
        # Seed an org default too, to prove the override wins.
        default_template_id = await _seed_template(
            db_session,
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="dashboard-templates",
                github_ref="main",
                root_path="/",
            ),
        )
        override_template_id = await _seed_template(
            db_session,
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="override-templates",
                github_ref="main",
                root_path="/",
            ),
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        default_binding = await binding_store.create(
            _binding_create(org_id=org_id, project_id=None)
        )
        await binding_store.update_sync_state(
            binding_id=default_binding.id,
            last_sync_status="succeeded",
            github_template_id=default_template_id,
        )
        override_binding = await binding_store.create(
            _binding_create(
                org_id=org_id,
                project_id=project.id,
                github_repo="override-templates",
            )
        )
        await binding_store.update_sync_state(
            binding_id=override_binding.id,
            last_sync_status="succeeded",
            github_template_id=override_template_id,
        )
        await db_session.commit()

    resolver = _make_resolver(db_session)
    async with db_session.begin():
        resolved = await resolver.resolve(
            org_id=project.org_id, project_id=project.id
        )
        await db_session.rollback()

    assert resolved.origin is ResolvedTemplateOrigin.project_override
    assert isinstance(resolved.source, GitHubTemplateSource)


@pytest.mark.asyncio
async def test_resolve_falls_through_when_override_template_id_is_null(
    db_session: AsyncSession,
) -> None:
    """A project override pending initial sync falls through to org default."""
    async with db_session.begin():
        org_id, project = await _seed_org_and_project(
            db_session, org_slug="resolver-override-null"
        )
        default_template_id = await _seed_template(
            db_session,
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="dashboard-templates",
                github_ref="main",
                root_path="/",
            ),
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        default_binding = await binding_store.create(
            _binding_create(org_id=org_id, project_id=None)
        )
        await binding_store.update_sync_state(
            binding_id=default_binding.id,
            last_sync_status="succeeded",
            github_template_id=default_template_id,
        )
        # Override exists but has NEVER completed a sync.
        await binding_store.create(
            _binding_create(
                org_id=org_id,
                project_id=project.id,
                github_repo="override-templates",
            )
        )
        await db_session.commit()

    resolver = _make_resolver(db_session)
    async with db_session.begin():
        resolved = await resolver.resolve(
            org_id=project.org_id, project_id=project.id
        )
        await db_session.rollback()

    assert resolved.origin is ResolvedTemplateOrigin.org_default
    assert isinstance(resolved.source, GitHubTemplateSource)


@pytest.mark.asyncio
async def test_resolve_falls_through_to_builtin_when_both_template_ids_null(
    db_session: AsyncSession,
) -> None:
    """Override AND default both pending-sync ⇒ built-in fallback."""
    async with db_session.begin():
        org_id, project = await _seed_org_and_project(
            db_session, org_slug="resolver-both-null"
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        await binding_store.create(
            _binding_create(org_id=org_id, project_id=None)
        )
        await binding_store.create(
            _binding_create(
                org_id=org_id,
                project_id=project.id,
                github_repo="override-templates",
            )
        )
        await db_session.commit()

    resolver = _make_resolver(db_session)
    async with db_session.begin():
        resolved = await resolver.resolve(
            org_id=project.org_id, project_id=project.id
        )
        await db_session.rollback()

    assert resolved.origin is ResolvedTemplateOrigin.builtin
    assert isinstance(resolved.source, BuiltInTemplateSource)


@pytest.mark.asyncio
async def test_resolve_returns_builtin_when_only_org_default_is_null(
    db_session: AsyncSession,
) -> None:
    """Org default pending-sync with no override ⇒ built-in fallback."""
    async with db_session.begin():
        org_id, project = await _seed_org_and_project(
            db_session, org_slug="resolver-default-null"
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        await binding_store.create(
            _binding_create(org_id=org_id, project_id=None)
        )
        await db_session.commit()

    resolver = _make_resolver(db_session)
    async with db_session.begin():
        resolved = await resolver.resolve(
            org_id=project.org_id, project_id=project.id
        )
        await db_session.rollback()

    assert resolved.origin is ResolvedTemplateOrigin.builtin
    assert isinstance(resolved.source, BuiltInTemplateSource)


@pytest.mark.asyncio
async def test_resolve_returns_builtin_when_only_override_is_null(
    db_session: AsyncSession,
) -> None:
    """Override pending-sync with no default ⇒ built-in fallback."""
    async with db_session.begin():
        org_id, project = await _seed_org_and_project(
            db_session, org_slug="resolver-override-only-null"
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        await binding_store.create(
            _binding_create(org_id=org_id, project_id=project.id)
        )
        await db_session.commit()

    resolver = _make_resolver(db_session)
    async with db_session.begin():
        resolved = await resolver.resolve(
            org_id=project.org_id, project_id=project.id
        )
        await db_session.rollback()

    assert resolved.origin is ResolvedTemplateOrigin.builtin
    assert isinstance(resolved.source, BuiltInTemplateSource)


@pytest.mark.asyncio
async def test_resolve_preloads_github_source_so_reads_are_cache_hits(
    db_session: AsyncSession,
) -> None:
    """Resolver returns a preloaded GitHubTemplateSource.

    Callers (renderers) invoke ``load_config`` / ``read_template`` /
    ``read_asset`` synchronously on the ``TemplateSource`` protocol, so
    the resolver must do the async ``preload`` itself — otherwise those
    sync reads would raise ``RuntimeError``.
    """
    async with db_session.begin():
        org_id, project = await _seed_org_and_project(
            db_session, org_slug="resolver-preload"
        )
        template_id = await _seed_template(
            db_session,
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="dashboard-templates",
                github_ref="main",
                root_path="/",
            ),
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await binding_store.create(
            _binding_create(org_id=org_id, project_id=None)
        )
        await binding_store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="succeeded",
            github_template_id=template_id,
        )
        await db_session.commit()

    resolver = _make_resolver(db_session)
    async with db_session.begin():
        resolved = await resolver.resolve(
            org_id=project.org_id, project_id=project.id
        )
        # Reading template contents should succeed synchronously.
        text = resolved.source.read_template("dashboard.html.jinja")
        await db_session.rollback()

    assert text == "<html>custom</html>"
