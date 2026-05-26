"""Integration tests for ProjectGitHubBindingResolver."""

from __future__ import annotations

import httpx
import pytest
import structlog
from safir.github import GitHubAppClientFactory
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate, ProjectCreate
from docverse.client.models.projects import ProjectGitHubBindingCreate
from docverse.dbschema.project import SqlProject
from docverse.services.project_github_binding import (
    ProjectGitHubBindingResolver,
    ResolvedProjectGitHubBinding,
)
from docverse.storage.github import GitHubAppClient, InstallationAuth
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from tests.support.github_mock import DEFAULT_APP_NAME, GitHubMock


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(session: AsyncSession, *, slug: str) -> int:
    org_store = OrganizationStore(session=session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"Org {slug}",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


async def _seed_project(
    session: AsyncSession,
    *,
    org_id: int,
    slug: str,
    github_owner: str | None = None,
    github_repo: str | None = None,
    source_url: str | None = None,
    github_installation_id: int | None = None,
) -> int:
    proj_store = ProjectStore(session=session, logger=_logger())
    binding = (
        ProjectGitHubBindingCreate(owner=github_owner, repo=github_repo)
        if github_owner is not None and github_repo is not None
        else None
    )
    project = await proj_store.create(
        org_id=org_id,
        data=ProjectCreate(
            slug=slug,
            title=f"Project {slug}",
            source_url=source_url,
            github=binding,
        ),
        github_owner=github_owner,
        github_repo=github_repo,
    )
    if github_installation_id is not None:
        # ``ProjectStore.create`` does not accept the opportunistic
        # numeric id columns; the worker / webhook backfill them post-
        # create. Directly populate ``github_installation_id`` here so
        # the resolver sees the installed shape under test.
        await session.execute(
            update(SqlProject)
            .where(SqlProject.id == project.id)
            .values(github_installation_id=github_installation_id)
        )
        await session.flush()
    return project.id


def _make_resolver(
    *,
    session: AsyncSession,
    http_client: httpx.AsyncClient,
    mock_github: GitHubMock,
) -> ProjectGitHubBindingResolver:
    project_store = ProjectStore(session=session, logger=_logger())
    safir_factory = GitHubAppClientFactory(
        id=mock_github.app_id,
        key=mock_github.private_key_pem,
        name=DEFAULT_APP_NAME,
        http_client=http_client,
    )
    app_client = GitHubAppClient(
        factory=safir_factory,
        http_client=http_client,
        logger=_logger(),
    )
    return ProjectGitHubBindingResolver(
        session=session,
        project_store=project_store,
        app_client=app_client,
        logger=_logger(),
    )


@pytest.mark.asyncio
async def test_resolve_mints_token_outside_open_db_transaction(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """The GitHub token-exchange must not run inside an open DB tx.

    Holding a DB transaction open across the
    ``exchange_installation_token`` round-trip leaves a connection
    idle-in-transaction for the duration of the network call. For an
    org with many GitHub-bound projects this would compound per
    project in the daily ``git_ref_audit`` worker. The resolver owns
    the short read transaction for the binding lookup and must mint
    the installation token only after that transaction has closed.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="resolver-tx-boundary")
        project_id = await _seed_project(
            db_session,
            org_id=org_id,
            slug="proj-tx-boundary",
            github_owner="acme",
            github_repo="docs",
            github_installation_id=77,
        )
        await db_session.commit()

    mock_github.seed_installation(
        "acme", "docs", installation_id=77, token="ghs_no_tx"
    )

    async with httpx.AsyncClient() as http_client:
        resolver = _make_resolver(
            session=db_session,
            http_client=http_client,
            mock_github=mock_github,
        )
        original_exchange = resolver._app_client.exchange_installation_token
        in_tx_during_mint: list[bool] = []

        async def _capturing_exchange(installation_id: int) -> str:
            in_tx_during_mint.append(db_session.in_transaction())
            return await original_exchange(installation_id)

        resolver._app_client.exchange_installation_token = (  # type: ignore[method-assign]
            _capturing_exchange
        )
        result = await resolver.resolve(project_id)

    assert result is not None
    assert result.auth is not None
    assert in_tx_during_mint == [False]


@pytest.mark.asyncio
async def test_resolve_returns_binding_with_minted_auth_for_installed_project(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """An installation-backed project gets a populated ``InstallationAuth``.

    Pins user story 12 for the audit caller: when a project's
    ``github_installation_id`` is populated, the resolver mints an
    installation token and returns a binding the audit worker can pass
    straight to :class:`GitHubRefSetFetcher`.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="resolver-installed")
        project_id = await _seed_project(
            db_session,
            org_id=org_id,
            slug="proj-installed",
            github_owner="acme",
            github_repo="docs",
            github_installation_id=42,
        )
        await db_session.commit()

    # The resolver calls ``exchange_installation_token``, which POSTs to
    # the access-tokens endpoint. ``seed_installation`` wires both that
    # and the unused ``/installation`` lookup — the latter does no harm.
    mock_github.seed_installation(
        "acme", "docs", installation_id=42, token="ghs_minted_token"
    )

    async with httpx.AsyncClient() as http_client:
        resolver = _make_resolver(
            session=db_session,
            http_client=http_client,
            mock_github=mock_github,
        )
        result = await resolver.resolve(project_id)

    assert isinstance(result, ResolvedProjectGitHubBinding)
    assert result.owner == "acme"
    assert result.repo == "docs"
    assert result.installation_id == 42
    assert result.auth is not None
    assert isinstance(result.auth, InstallationAuth)
    assert result.auth.token == "ghs_minted_token"  # noqa: S105
    assert result.auth.installation_id == 42


@pytest.mark.asyncio
async def test_resolve_returns_binding_without_auth_for_anonymous_project(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A project with structured GitHub but NULL installation_id is anonymous.

    Pins user story 13: a project whose GitHub App is not (yet)
    installed still benefits from the periodic audit by hitting the
    public API. The resolver returns the binding with ``auth=None`` so
    the fetcher knows to use the anonymous path.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="resolver-anon")
        project_id = await _seed_project(
            db_session,
            org_id=org_id,
            slug="proj-anon",
            github_owner="acme",
            github_repo="public-docs",
            github_installation_id=None,
        )
        await db_session.commit()

    async with httpx.AsyncClient() as http_client:
        resolver = _make_resolver(
            session=db_session,
            http_client=http_client,
            mock_github=mock_github,
        )
        result = await resolver.resolve(project_id)

    assert isinstance(result, ResolvedProjectGitHubBinding)
    assert result.owner == "acme"
    assert result.repo == "public-docs"
    assert result.installation_id is None
    assert result.auth is None
    # The anonymous path must not have hit GitHub at all (no token
    # exchange, no installation lookup).
    assert mock_github.router.calls.call_count == 0


@pytest.mark.asyncio
async def test_resolve_returns_none_for_non_github_project(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A project whose ``source_url`` is non-GitHub returns ``None``.

    Pins user story 14: a GitLab / Codeberg / on-prem project has
    ``github_owner`` and ``github_repo`` both NULL, so it is out of
    scope for ``ref_deleted`` and the audit walks past it cleanly.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="resolver-nongithub")
        project_id = await _seed_project(
            db_session,
            org_id=org_id,
            slug="proj-gitlab",
            source_url="https://gitlab.example.com/acme/docs",
        )
        await db_session.commit()

    async with httpx.AsyncClient() as http_client:
        resolver = _make_resolver(
            session=db_session,
            http_client=http_client,
            mock_github=mock_github,
        )
        result = await resolver.resolve(project_id)

    assert result is None
    assert mock_github.router.calls.call_count == 0


@pytest.mark.asyncio
async def test_resolve_returns_none_for_missing_project(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """An unknown ``project_id`` returns ``None`` without raising.

    The audit walks a snapshot of project ids; a project can be
    soft-deleted between snapshot and per-project pass. The resolver
    must not raise so a single deleted project does not abort the
    per-org tick.
    """
    async with httpx.AsyncClient() as http_client:
        resolver = _make_resolver(
            session=db_session,
            http_client=http_client,
            mock_github=mock_github,
        )
        result = await resolver.resolve(999999)

    assert result is None
    assert mock_github.router.calls.call_count == 0
