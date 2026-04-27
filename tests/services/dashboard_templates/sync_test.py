"""Tests for DashboardTemplateSyncer."""

from __future__ import annotations

from typing import Any, cast

import httpx
import pytest
import structlog
from safir.github import GitHubAppClientFactory
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate
from docverse.exceptions import NotFoundError
from docverse.services.dashboard_templates import sync as sync_module
from docverse.services.dashboard_templates.sync import (
    DashboardSyncStatus,
    DashboardTemplateSyncer,
)
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
)
from docverse.storage.github import GitHubAppClient, InstallationAuth
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
async def test_sync_captures_github_numeric_ids_on_first_sync(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """First sync writes repo/owner/installation IDs to binding + content."""
    async with db_session.begin():
        binding_id = await _seed_binding(db_session, org_slug="sync-ids")
        await db_session.commit()

    mock_github.seed_installation(
        "acme", "templates", installation_id=4242, owner_id=111
    )
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            "template.toml": _VALID_TEMPLATE_TOML,
            "dashboard.html.jinja": b"<html>ids</html>",
        },
        repo_id=999,
        owner_id=111,
    )

    async with httpx.AsyncClient() as http_client:
        syncer = _make_syncer(
            db_session, http_client=http_client, mock_github=mock_github
        )
        async with db_session.begin():
            result = await syncer.sync(binding_id)
            await db_session.commit()

    assert result.binding.github_owner_id == 111
    assert result.binding.github_repo_id == 999
    assert result.binding.github_installation_id == 4242
    assert result.github_template_id is not None

    async with db_session.begin():
        template_store = DashboardGitHubTemplateStore(
            session=db_session, logger=_logger()
        )
        template = await template_store.get_by_id(result.github_template_id)
    assert template is not None
    assert template.github_owner_id == 111
    assert template.github_repo_id == 999


@pytest.mark.asyncio
async def test_sync_does_not_overwrite_populated_ids_on_resync(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Re-sync of an already-populated binding leaves IDs intact.

    The store-layer guard (``update_sync_state`` / ``upsert`` only
    assigning IDs when the kwarg is non-``None``) is the load-bearing
    invariant, but pin it at the syncer seam so the contract holds
    end-to-end. Passing the same IDs twice is a no-op overwrite, but
    the test asserts that the second sync does not zero them — which
    is the realistic regression to guard against.
    """
    async with db_session.begin():
        binding_id = await _seed_binding(
            db_session, org_slug="sync-ids-resync"
        )
        await db_session.commit()

    mock_github.seed_installation(
        "acme", "templates", installation_id=42, owner_id=11
    )
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={"template.toml": _VALID_TEMPLATE_TOML},
        commit_sha="sha-1",
        tree_sha="tree-1",
        etag='W/"tree-etag-1"',
        repo_id=900,
        owner_id=11,
    )

    async with httpx.AsyncClient() as http_client:
        syncer = _make_syncer(
            db_session, http_client=http_client, mock_github=mock_github
        )
        async with db_session.begin():
            await syncer.sync(binding_id)
            await db_session.commit()
        async with db_session.begin():
            second = await syncer.sync(binding_id)
            await db_session.commit()

    assert second.binding.github_owner_id == 11
    assert second.binding.github_repo_id == 900
    assert second.binding.github_installation_id == 42


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

    # Seed the installation lookup + repo metadata so auth and the
    # initial repo fetch succeed, then wire an explicit 404 on the
    # tree's commit lookup so the fetcher raises
    # ``httpx.HTTPStatusError`` (a subclass of ``httpx.HTTPError``) and
    # the syncer records the failure via its narrow catch.
    mock_github.seed_installation("acme", "broken", installation_id=77)
    mock_github.seed_repo("acme", "broken")
    mock_github.router.get(
        "https://api.github.com/repos/acme/broken/commits/main"
    ).mock(return_value=httpx.Response(404, json={"message": "Not Found"}))

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


@pytest.mark.asyncio
async def test_sync_unexpected_auth_error_propagates(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Unexpected errors from the app-client lookup propagate.

    The syncer's failure-recording catches only the known-bad cases
    (``httpx.HTTPError``, gidgethub exceptions). A programming bug that
    surfaces as ``RuntimeError`` must bubble up so the worker's outer
    ``except Exception`` records it with a full traceback, rather than
    being hidden as a user-visible sync failure.
    """
    async with db_session.begin():
        binding_id = await _seed_binding(
            db_session, org_slug="sync-unexpected-auth"
        )
        await db_session.commit()

    class _ExplodingAppClient:
        async def get_installation_auth(
            self,
            *,
            owner: str,  # noqa: ARG002
            repo: str,  # noqa: ARG002
        ) -> InstallationAuth:
            msg = "unexpected bug during auth lookup"
            raise RuntimeError(msg)

    logger = _logger()
    async with httpx.AsyncClient() as http_client:
        syncer = DashboardTemplateSyncer(
            binding_store=DashboardGitHubTemplateBindingStore(
                session=db_session, logger=logger
            ),
            template_store=DashboardGitHubTemplateStore(
                session=db_session, logger=logger
            ),
            app_client=cast("Any", _ExplodingAppClient()),
            http_client=http_client,
            logger=logger,
        )
        with pytest.raises(RuntimeError, match="unexpected bug"):
            async with db_session.begin():
                await syncer.sync(binding_id)


@pytest.mark.asyncio
async def test_sync_unexpected_fetch_error_propagates(
    db_session: AsyncSession,
    mock_github: GitHubMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unexpected error raised during tree fetch propagates.

    The fetch block's catch is narrowed to ``httpx.HTTPError``; a
    programming bug that surfaces as ``RuntimeError`` must reach the
    worker's outer handler so it's logged with a traceback instead of
    being recorded as a generic user-visible sync failure.
    """
    async with db_session.begin():
        binding_id = await _seed_binding(
            db_session, org_slug="sync-unexpected-fetch"
        )
        await db_session.commit()

    mock_github.seed_installation("acme", "templates", installation_id=42)

    class _ExplodingFetcher:
        def __init__(
            self,
            *args: Any,
            **kwargs: Any,
        ) -> None:
            pass

        async def fetch(self, **kwargs: Any) -> Any:  # noqa: ARG002
            msg = "unexpected bug during tree fetch"
            raise RuntimeError(msg)

    monkeypatch.setattr(sync_module, "GitHubTreeFetcher", _ExplodingFetcher)

    async with httpx.AsyncClient() as http_client:
        syncer = _make_syncer(
            db_session, http_client=http_client, mock_github=mock_github
        )
        with pytest.raises(RuntimeError, match="unexpected bug"):
            async with db_session.begin():
                await syncer.sync(binding_id)


@pytest.mark.asyncio
async def test_sync_invalid_app_private_key_records_failure(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A misconfigured GitHub App key lands as a recorded failure.

    A bad PEM passed to ``GitHubAppClientFactory`` surfaces as
    ``jwt.exceptions.InvalidKeyError`` from ``get_app_jwt`` deep inside
    ``get_installation_auth``. The syncer must catch that alongside its
    other expected GitHub-auth errors and write a friendly
    ``last_sync_error`` instead of letting the exception propagate to
    the worker's outer ``except Exception`` (which would leave the
    binding stuck at ``last_sync_status="pending"``).
    """
    async with db_session.begin():
        binding_id = await _seed_binding(db_session, org_slug="sync-bad-key")
        await db_session.commit()

    logger = _logger()
    async with httpx.AsyncClient() as http_client:
        bad_factory = GitHubAppClientFactory(
            id=mock_github.app_id,
            key="not-a-real-pem",
            name=DEFAULT_APP_NAME,
            http_client=http_client,
        )
        app_client = GitHubAppClient(
            factory=bad_factory, http_client=http_client, logger=logger
        )
        syncer = DashboardTemplateSyncer(
            binding_store=DashboardGitHubTemplateBindingStore(
                session=db_session, logger=logger
            ),
            template_store=DashboardGitHubTemplateStore(
                session=db_session, logger=logger
            ),
            app_client=app_client,
            http_client=http_client,
            logger=logger,
        )
        async with db_session.begin():
            result = await syncer.sync(binding_id)
            await db_session.commit()

    assert result.status is DashboardSyncStatus.failed
    assert result.error
    assert "github app" in result.error.lower()

    async with db_session.begin():
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await binding_store.get_by_id(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "failed"
    assert binding.last_sync_error
    assert "github app" in binding.last_sync_error.lower()
    assert binding.github_template_id is None


@pytest.mark.asyncio
async def test_sync_unexpected_parse_error_propagates(
    db_session: AsyncSession,
    mock_github: GitHubMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unexpected error raised during TOML parse propagates.

    The parse block's catch is narrowed to ``tomllib.TOMLDecodeError``
    and ``UnicodeDecodeError``; programming bugs that surface as
    ``RuntimeError`` must bubble up so the worker's outer handler
    records them with a full traceback.
    """
    async with db_session.begin():
        binding_id = await _seed_binding(
            db_session, org_slug="sync-unexpected-parse"
        )
        await db_session.commit()

    mock_github.seed_installation("acme", "templates", installation_id=42)
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            "template.toml": _VALID_TEMPLATE_TOML,
            "dashboard.html.jinja": b"<html>ok</html>",
        },
    )

    def _exploding_parse(data: bytes) -> Any:
        msg = "unexpected bug during template parse"
        raise RuntimeError(msg)

    monkeypatch.setattr(sync_module, "parse_template_toml", _exploding_parse)

    async with httpx.AsyncClient() as http_client:
        syncer = _make_syncer(
            db_session, http_client=http_client, mock_github=mock_github
        )
        with pytest.raises(RuntimeError, match="unexpected bug"):
            async with db_session.begin():
                await syncer.sync(binding_id)
