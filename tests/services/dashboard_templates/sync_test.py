"""Tests for DashboardTemplateSyncer."""

from __future__ import annotations

import httpx
import pytest
import structlog
from safir.github import GitHubAppClientFactory
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate
from docverse.exceptions import NotFoundError
from docverse.services.dashboard_templates.sync import (
    DashboardSyncStatus,
    DashboardTemplateSyncer,
)
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
)
from docverse.storage.github import GitHubAppClient
from docverse.storage.organization_store import OrganizationStore
from tests.support.github_mock import DEFAULT_APP_NAME, GitHubMock

_VALID_TEMPLATE_TOML = b"""\
[dashboard]
template = "dashboard.html.jinja"
"""


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_binding(
    session: AsyncSession,
    *,
    org_slug: str,
    github_owner: str = "acme",
    github_repo: str = "templates",
    github_ref: str = "main",
    root_path: str = "/",
) -> int:
    logger = _logger()
    org_store = OrganizationStore(session=session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title=f"Org {org_slug}",
            base_domain=f"{org_slug}.example.com",
        )
    )
    binding_store = DashboardGitHubTemplateBindingStore(
        session=session, logger=logger
    )
    binding = await binding_store.create(
        DashboardGitHubTemplateBindingCreate(
            org_id=org.id,
            project_id=None,
            github_owner=github_owner,
            github_repo=github_repo,
            github_ref=github_ref,
            root_path=root_path,
        )
    )
    return binding.id


def _make_syncer(
    session: AsyncSession,
    *,
    http_client: httpx.AsyncClient,
    mock_github: GitHubMock,
) -> DashboardTemplateSyncer:
    logger = _logger()
    factory = GitHubAppClientFactory(
        id=mock_github.app_id,
        key=mock_github.private_key_pem,
        name=DEFAULT_APP_NAME,
        http_client=http_client,
    )
    app_client = GitHubAppClient(
        factory=factory, http_client=http_client, logger=logger
    )
    binding_store = DashboardGitHubTemplateBindingStore(
        session=session, logger=logger
    )
    template_store = DashboardGitHubTemplateStore(
        session=session, logger=logger
    )
    return DashboardTemplateSyncer(
        binding_store=binding_store,
        template_store=template_store,
        app_client=app_client,
        http_client=http_client,
        logger=logger,
    )


@pytest.mark.asyncio
async def test_sync_writes_content_and_files_on_first_sync(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Happy path: first sync stores template + files and marks succeeded."""
    async with db_session.begin():
        binding_id = await _seed_binding(db_session, org_slug="sync-first")
        await db_session.commit()

    mock_github.seed_installation("acme", "templates", installation_id=42)
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            "template.toml": _VALID_TEMPLATE_TOML,
            "dashboard.html.jinja": b"<html>dash</html>",
            "logo.png": b"\x89PNG\r\n\x1a\npng-bytes",
        },
        etag='W/"tree-etag-1"',
    )

    async with httpx.AsyncClient() as http_client:
        syncer = _make_syncer(
            db_session, http_client=http_client, mock_github=mock_github
        )
        async with db_session.begin():
            result = await syncer.sync(binding_id)
            await db_session.commit()

    assert result.changed is True
    assert result.github_template_id is not None
    assert result.binding.last_sync_status == "succeeded"
    assert result.binding.last_sync_error is None

    async with db_session.begin():
        template_store = DashboardGitHubTemplateStore(
            session=db_session, logger=_logger()
        )
        files = await template_store.list_files(result.github_template_id)
    file_paths = {f.relative_path for f in files}
    assert file_paths == {"dashboard.html.jinja", "logo.png"}
    logo_file = next(f for f in files if f.relative_path == "logo.png")
    assert logo_file.is_text is False
    jinja_file = next(
        f for f in files if f.relative_path == "dashboard.html.jinja"
    )
    assert jinja_file.is_text is True


@pytest.mark.asyncio
async def test_sync_etag_unchanged_does_not_rewrite_content(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Re-sync with same ETag returns ``changed=False`` and no new row."""
    async with db_session.begin():
        binding_id = await _seed_binding(db_session, org_slug="sync-etag")
        await db_session.commit()

    mock_github.seed_installation("acme", "templates", installation_id=42)
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            "template.toml": _VALID_TEMPLATE_TOML,
            "dashboard.html.jinja": b"<html>etag</html>",
        },
        commit_sha="sha-1",
        tree_sha="tree-1",
        etag='W/"tree-etag-1"',
    )

    async with httpx.AsyncClient() as http_client:
        syncer = _make_syncer(
            db_session, http_client=http_client, mock_github=mock_github
        )
        async with db_session.begin():
            first = await syncer.sync(binding_id)
            await db_session.commit()
        async with db_session.begin():
            second = await syncer.sync(binding_id)
            await db_session.commit()

    assert first.changed is True
    assert second.changed is False
    assert first.github_template_id == second.github_template_id
    assert second.binding.last_sync_status == "succeeded"


@pytest.mark.asyncio
async def test_sync_invalid_template_toml_marks_failed_and_keeps_prior_content(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A second sync with bad TOML keeps the binding's github_template_id."""
    async with db_session.begin():
        binding_id = await _seed_binding(db_session, org_slug="sync-bad-toml")
        await db_session.commit()

    mock_github.seed_installation("acme", "templates", installation_id=42)
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            "template.toml": _VALID_TEMPLATE_TOML,
            "dashboard.html.jinja": b"<html>good</html>",
        },
        commit_sha="sha-good",
        tree_sha="tree-good",
        etag='W/"tree-etag-good"',
    )
    async with httpx.AsyncClient() as http_client:
        syncer = _make_syncer(
            db_session, http_client=http_client, mock_github=mock_github
        )
        async with db_session.begin():
            first = await syncer.sync(binding_id)
            await db_session.commit()

    good_template_id = first.github_template_id
    assert good_template_id is not None

    # Re-seed the same (owner/repo/ref) tuple with broken TOML and a new
    # tree SHA so the ETag no longer matches — the syncer must attempt
    # to parse and then record the failure.
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            "template.toml": b"[dashboard\n= NOT TOML",
            "dashboard.html.jinja": b"<html>broken</html>",
        },
        commit_sha="sha-bad",
        tree_sha="tree-bad",
        etag='W/"tree-etag-bad"',
    )
    async with httpx.AsyncClient() as http_client:
        syncer = _make_syncer(
            db_session, http_client=http_client, mock_github=mock_github
        )
        async with db_session.begin():
            bad = await syncer.sync(binding_id)
            await db_session.commit()

    assert bad.status is DashboardSyncStatus.failed
    assert bad.error
    assert "template.toml" in bad.error.lower()

    async with db_session.begin():
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await binding_store.get_by_id(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "failed"
    assert binding.last_sync_error
    assert "template.toml" in binding.last_sync_error.lower()
    # The last-good template row is still pointed at.
    assert binding.github_template_id == good_template_id


@pytest.mark.asyncio
async def test_sync_missing_template_toml_marks_failed(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A tree that lacks ``template.toml`` fails with a clear reason."""
    async with db_session.begin():
        binding_id = await _seed_binding(
            db_session, org_slug="sync-no-template"
        )
        await db_session.commit()

    mock_github.seed_installation("acme", "templates", installation_id=42)
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={"dashboard.html.jinja": b"<html>no toml</html>"},
    )

    async with httpx.AsyncClient() as http_client:
        syncer = _make_syncer(
            db_session, http_client=http_client, mock_github=mock_github
        )
        async with db_session.begin():
            result = await syncer.sync(binding_id)
            await db_session.commit()

    assert result.status is DashboardSyncStatus.failed
    assert result.error
    assert "template.toml" in result.error.lower()

    async with db_session.begin():
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await binding_store.get_by_id(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "failed"
    assert binding.last_sync_error
    assert "template.toml" in binding.last_sync_error.lower()
    assert binding.github_template_id is None


@pytest.mark.asyncio
async def test_sync_missing_binding_raises_not_found(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """An unknown binding id raises NotFoundError without a GitHub call."""
    async with httpx.AsyncClient() as http_client:
        syncer = _make_syncer(
            db_session, http_client=http_client, mock_github=mock_github
        )
        with pytest.raises(NotFoundError):
            async with db_session.begin():
                await syncer.sync(9999)


@pytest.mark.asyncio
async def test_sync_github_error_records_failure_without_clearing_content(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """GitHub fetch errors land as a recorded failure, not a raise."""
    async with db_session.begin():
        binding_id = await _seed_binding(
            db_session,
            org_slug="sync-gh-error",
            github_repo="broken",
        )
        await db_session.commit()

    # Do not seed any tree for owner=acme repo=broken — the fetcher's
    # HTTP call will 404 because respx is configured with
    # assert_all_mocked=False and the underlying httpx.AsyncClient
    # cannot reach the network. Seed only the installation lookup so
    # we exercise the tree-fetch failure branch.
    mock_github.seed_installation("acme", "broken", installation_id=77)

    async with httpx.AsyncClient() as http_client:
        syncer = _make_syncer(
            db_session, http_client=http_client, mock_github=mock_github
        )
        async with db_session.begin():
            result = await syncer.sync(binding_id)
            await db_session.commit()

    assert result.status is DashboardSyncStatus.failed
    assert result.error

    async with db_session.begin():
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await binding_store.get_by_id(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "failed"
    assert binding.last_sync_error
    assert binding.github_template_id is None
