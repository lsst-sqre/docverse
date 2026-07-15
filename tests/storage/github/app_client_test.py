"""Tests for the GitHubAppClient wrapper and the Factory helper."""

from __future__ import annotations

from typing import Literal

import gidgethub
import httpx
import jwt as pyjwt
import pytest
import structlog
from pydantic import SecretStr
from safir.github import GitHubAppClientFactory
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.exceptions import DocverseSlackException
from docverse.factory import Factory
from docverse.storage.github import (
    GITHUB_API_BASE_URL,
    GitHubAppClient,
    GitHubAppNotConfiguredError,
    GitHubAppNotInstalledError,
    InstallationAuth,
    RepositoryMetadata,
)
from tests.support.github_mock import DEFAULT_APP_NAME, GitHubMock


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


@pytest.mark.asyncio
async def test_get_installation_id_uses_app_jwt(
    mock_github: GitHubMock,
) -> None:
    """``get_installation_id`` returns the mocked installation ID."""
    mock_github.seed_installation("acme", "templates", installation_id=42)

    async with httpx.AsyncClient() as http_client:
        factory = GitHubAppClientFactory(
            id=mock_github.app_id,
            key=mock_github.private_key_pem,
            name=DEFAULT_APP_NAME,
            http_client=http_client,
        )
        client = GitHubAppClient(
            factory=factory, http_client=http_client, logger=_logger()
        )
        installation_id = await client.get_installation_id("acme", "templates")

    assert installation_id == 42


@pytest.mark.asyncio
async def test_get_installation_id_raises_not_installed_on_404(
    mock_github: GitHubMock,
) -> None:
    """A 404 maps to ``GitHubAppNotInstalledError`` carrying owner/repo.

    A 404 from ``/repos/{owner}/{repo}/installation`` means no
    installation grants the App access to this repo — an expected,
    operator-recoverable state, not a credential bug. The wrapper
    translates it to a dedicated non-Slack exception so the worker can
    leave the ids NULL without paging Sentry.
    """
    mock_github.router.get(
        "https://api.github.com/repos/acme/templates/installation"
    ).mock(return_value=httpx.Response(404, json={"message": "Not Found"}))

    async with httpx.AsyncClient() as http_client:
        factory = GitHubAppClientFactory(
            id=mock_github.app_id,
            key=mock_github.private_key_pem,
            name=DEFAULT_APP_NAME,
            http_client=http_client,
        )
        client = GitHubAppClient(
            factory=factory, http_client=http_client, logger=_logger()
        )
        with pytest.raises(GitHubAppNotInstalledError) as excinfo:
            await client.get_installation_id("acme", "templates")

    assert excinfo.value.owner == "acme"
    assert excinfo.value.repo == "templates"
    assert not isinstance(excinfo.value, DocverseSlackException)


@pytest.mark.asyncio
async def test_get_installation_id_reraises_non_404_bad_request(
    mock_github: GitHubMock,
) -> None:
    """A non-404 ``BadRequest`` (e.g. 403) propagates unchanged.

    Only 404 means "App not connected to this repo". Other 4xx — a 403
    forbidden, say — is a genuine error the worker must still capture to
    Sentry, so the wrapper re-raises the original ``gidgethub`` error
    rather than masking it as not-installed.
    """
    mock_github.router.get(
        "https://api.github.com/repos/acme/templates/installation"
    ).mock(return_value=httpx.Response(403, json={"message": "Forbidden"}))

    async with httpx.AsyncClient() as http_client:
        factory = GitHubAppClientFactory(
            id=mock_github.app_id,
            key=mock_github.private_key_pem,
            name=DEFAULT_APP_NAME,
            http_client=http_client,
        )
        client = GitHubAppClient(
            factory=factory, http_client=http_client, logger=_logger()
        )
        with pytest.raises(gidgethub.BadRequest):
            await client.get_installation_id("acme", "templates")


@pytest.mark.asyncio
async def test_exchange_installation_token(mock_github: GitHubMock) -> None:
    """``exchange_installation_token`` returns the mocked bearer token."""
    mock_github.seed_installation(
        "acme", "templates", installation_id=42, token="ghs_test_abc"
    )

    async with httpx.AsyncClient() as http_client:
        factory = GitHubAppClientFactory(
            id=mock_github.app_id,
            key=mock_github.private_key_pem,
            name=DEFAULT_APP_NAME,
            http_client=http_client,
        )
        client = GitHubAppClient(
            factory=factory, http_client=http_client, logger=_logger()
        )
        token = await client.exchange_installation_token(42)

    assert token == "ghs_test_abc"


@pytest.mark.asyncio
async def test_get_installation_auth_returns_token_record(
    mock_github: GitHubMock,
) -> None:
    """``get_installation_auth`` round-trips through both endpoints.

    Pins that the wrapper resolves the installation ID then exchanges
    it for a token, returning an :class:`InstallationAuth` with the
    seeded token and the default GitHub API base URL — without minting
    any per-call ``httpx.AsyncClient``.
    """
    mock_github.seed_installation(
        "acme", "templates", installation_id=42, token="ghs_installtok"
    )

    async with httpx.AsyncClient() as http_client:
        factory = GitHubAppClientFactory(
            id=mock_github.app_id,
            key=mock_github.private_key_pem,
            name=DEFAULT_APP_NAME,
            http_client=http_client,
        )
        client = GitHubAppClient(
            factory=factory, http_client=http_client, logger=_logger()
        )
        auth = await client.get_installation_auth(
            owner="acme", repo="templates"
        )

    assert isinstance(auth, InstallationAuth)
    assert auth.token == "ghs_installtok"
    assert auth.base_url == GITHUB_API_BASE_URL
    assert auth.installation_id == 42


@pytest.mark.asyncio
async def test_resolve_repository_metadata_returns_all_three_ids(
    mock_github: GitHubMock,
) -> None:
    """``resolve_repository_metadata`` returns installation, owner, repo IDs.

    Combines the existing ``/repos/{owner}/{repo}/installation`` and
    ``/repos/{owner}/{repo}`` lookups into one call so the ``project_
    github_resolve`` worker has a single deep entry point. Asserts the
    return record carries all three IDs without depending on which
    endpoint surfaced each — the caller just needs the bundle.
    """
    mock_github.seed_installation(
        "acme", "templates", installation_id=42, owner_id=111
    )
    mock_github.seed_repo("acme", "templates", repo_id=12345, owner_id=111)

    async with httpx.AsyncClient() as http_client:
        factory = GitHubAppClientFactory(
            id=mock_github.app_id,
            key=mock_github.private_key_pem,
            name=DEFAULT_APP_NAME,
            http_client=http_client,
        )
        client = GitHubAppClient(
            factory=factory, http_client=http_client, logger=_logger()
        )
        metadata = await client.resolve_repository_metadata(
            owner="acme", repo="templates"
        )

    assert isinstance(metadata, RepositoryMetadata)
    assert metadata.installation_id == 42
    assert metadata.owner_id == 111
    assert metadata.repo_id == 12345


@pytest.mark.asyncio
async def test_validate_succeeds_on_2xx_app_response(
    mock_github: GitHubMock,
) -> None:
    """``validate`` returns the App ``html_url`` when ``GET /app`` is 200.

    The captured ``html_url`` is the App's public install page, threaded
    through startup so the API can surface it as ``github.app_url``.
    """
    mock_github.seed_app()

    async with httpx.AsyncClient() as http_client:
        factory = GitHubAppClientFactory(
            id=mock_github.app_id,
            key=mock_github.private_key_pem,
            name=DEFAULT_APP_NAME,
            http_client=http_client,
        )
        client = GitHubAppClient(
            factory=factory, http_client=http_client, logger=_logger()
        )
        html_url = await client.validate()

    assert html_url == "https://github.com/apps/docverse"


@pytest.mark.asyncio
async def test_validate_raises_on_malformed_private_key() -> None:
    """A malformed PEM raises before any network call.

    The pyjwt → safir.github → gidgethub stack surfaces an
    ``InvalidKeyError`` from ``get_app_jwt``; the validator must not
    swallow it. Asserting on the real propagation path (rather than
    mocking ``get_app_jwt`` to raise) catches the case where a future
    safir update changes the exception type.
    """
    async with httpx.AsyncClient() as http_client:
        factory = GitHubAppClientFactory(
            id=12345,
            key="not-a-real-pem",
            name=DEFAULT_APP_NAME,
            http_client=http_client,
        )
        client = GitHubAppClient(
            factory=factory, http_client=http_client, logger=_logger()
        )
        with pytest.raises(pyjwt.exceptions.InvalidKeyError):
            await client.validate()


@pytest.mark.asyncio
async def test_validate_raises_on_unauthorized_app_response(
    mock_github: GitHubMock,
) -> None:
    """A 401 from ``GET /app`` raises a ``gidgethub`` error."""
    mock_github.seed_app(
        status_code=401,
        body={"message": "Bad credentials"},
    )

    async with httpx.AsyncClient() as http_client:
        factory = GitHubAppClientFactory(
            id=mock_github.app_id,
            key=mock_github.private_key_pem,
            name=DEFAULT_APP_NAME,
            http_client=http_client,
        )
        client = GitHubAppClient(
            factory=factory, http_client=http_client, logger=_logger()
        )
        with pytest.raises(gidgethub.GitHubException):
            await client.validate()


@pytest.mark.asyncio
async def test_factory_create_github_app_client_all_set(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """With all three secrets set, the helper returns a usable client.

    Exercises an actual installation-token round-trip through the shared
    http_client to prove the returned client is wired end-to-end, not
    just type-checks. Asserting on ``InstallationAuth.token`` (rather
    than ``isinstance``) catches the case where the shared client is
    closed or otherwise unusable by the time the helper runs.
    """
    mock_github.seed_installation(
        "acme", "templates", installation_id=42, token="ghs_factory_test"
    )

    async with httpx.AsyncClient() as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            http_client=http_client,
            github_app_id=mock_github.app_id,
            github_app_private_key=SecretStr(mock_github.private_key_pem),
            github_webhook_secret=SecretStr("webhook-secret"),
            default_queue_name="docverse:queue",
        )
        client = factory.create_github_app_client()
        assert isinstance(client, GitHubAppClient)
        auth = await client.get_installation_auth(
            owner="acme", repo="templates"
        )

    assert auth.token == "ghs_factory_test"


@pytest.mark.parametrize(
    ("app_id", "private_key", "webhook_secret"),
    [
        (None, SecretStr("key"), SecretStr("wh")),
        (12345, None, SecretStr("wh")),
        (12345, SecretStr("key"), None),
    ],
    ids=["missing_app_id", "missing_private_key", "missing_webhook_secret"],
)
@pytest.mark.asyncio
async def test_factory_create_github_app_client_one_missing_raises(
    db_session: AsyncSession,
    app_id: int | None,
    private_key: SecretStr | None,
    webhook_secret: SecretStr | None,
) -> None:
    """Any single missing secret disables the feature."""
    async with httpx.AsyncClient() as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            http_client=http_client,
            github_app_id=app_id,
            github_app_private_key=private_key,
            github_webhook_secret=webhook_secret,
            default_queue_name="docverse:queue",
        )
        with pytest.raises(GitHubAppNotConfiguredError):
            factory.create_github_app_client()


@pytest.mark.asyncio
async def test_factory_create_github_app_client_all_missing_raises(
    db_session: AsyncSession,
) -> None:
    """Default (no GitHub config) raises ``GitHubAppNotConfiguredError``."""
    async with httpx.AsyncClient() as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            http_client=http_client,
            default_queue_name="docverse:queue",
        )
        with pytest.raises(GitHubAppNotConfiguredError):
            factory.create_github_app_client()


def test_github_app_not_configured_is_docverse_slack_exception() -> None:
    """The migrated error derives from the shared ``DocverseSlackException``.

    Pins the slice #340 / #344 contract: every non-``ClientRequestError``
    server-side exception inherits from the shared base so the
    ``before_send_handler`` in :mod:`safir.sentry` merges its tags and
    contexts onto the captured event.
    """
    exc = GitHubAppNotConfiguredError(missing_secret="app_id")
    assert isinstance(exc, DocverseSlackException)


@pytest.mark.parametrize(
    ("missing_secret", "org_slug", "installation_id"),
    [
        ("app_id", "rubin", None),
        ("private_key", "rubin", 42),
        ("webhook_secret", "rubin", 42),
        ("app_id", None, None),
    ],
    ids=[
        "missing_app_id",
        "missing_private_key",
        "missing_webhook_secret",
        "no_org_context",
    ],
)
def test_github_app_not_configured_to_sentry_tags(
    missing_secret: Literal["app_id", "private_key", "webhook_secret"],
    org_slug: str | None,
    installation_id: int | None,
) -> None:
    """``to_sentry`` surfaces ``missing_secret`` and ``org_slug`` as tags.

    Tags are low cardinality (the three known secret names and the org
    slug) so they can be aggregated in the Sentry UI for on-call
    routing — operators can filter to one tenant's events or to a
    specific class of credential misconfiguration without paging
    through every event. ``org_slug`` is omitted from the tag set when
    the raise site has no org context (e.g. the global factory gate)
    rather than emitting a literal ``"None"``.
    """
    exc = GitHubAppNotConfiguredError(
        missing_secret=missing_secret,
        org_slug=org_slug,
        installation_id=installation_id,
    )
    info = exc.to_sentry()
    assert info.tags["missing_secret"] == missing_secret
    if org_slug is None:
        assert "org_slug" not in info.tags
    else:
        assert info.tags["org_slug"] == org_slug


@pytest.mark.parametrize(
    ("missing_secret", "org_slug", "installation_id"),
    [
        ("app_id", "rubin", None),
        ("private_key", "rubin", 42),
        ("webhook_secret", "rubin", 42),
        ("app_id", None, None),
    ],
    ids=[
        "missing_app_id",
        "missing_private_key",
        "missing_webhook_secret",
        "no_org_context",
    ],
)
def test_github_app_not_configured_to_sentry_context(
    missing_secret: Literal["app_id", "private_key", "webhook_secret"],
    org_slug: str | None,
    installation_id: int | None,
) -> None:
    """``to_sentry`` exposes the non-secret ``github_app`` context.

    The ``github_app`` context carries the GitHub App ``installation_id``
    (when known — ``None`` when the missing secret is the app id
    itself, since no installation can have been minted yet) and the
    static app name. The installation id is non-secret and gives a
    triager the one identifier they need to look the tenant up in the
    GitHub App admin UI without leaking any credential into Sentry.
    """
    exc = GitHubAppNotConfiguredError(
        missing_secret=missing_secret,
        org_slug=org_slug,
        installation_id=installation_id,
    )
    info = exc.to_sentry()
    context = info.contexts["github_app"]
    assert context["installation_id"] == installation_id
    assert context["app_name"] == "lsst-sqre/docverse"


def test_github_app_not_configured_default_message_is_useful() -> None:
    """The default message names the missing secret for log readers.

    Without an explicit ``message`` the rendered string carries the
    name of the missing secret so a log line or Slack alert is
    actionable without unpacking the structured fields.
    """
    exc = GitHubAppNotConfiguredError(missing_secret="webhook_secret")
    rendered = str(exc)
    assert "webhook_secret" in rendered


def test_github_app_not_configured_explicit_message_wins() -> None:
    """Passing ``message`` overrides the auto-generated default.

    Used by the factory's validation-failed gate to keep the existing
    "credentials failed startup validation" wording in pod logs while
    still routing the structured ``missing_secret`` field into Sentry
    tags.
    """
    exc = GitHubAppNotConfiguredError(
        missing_secret="private_key",
        message="GitHub App credentials failed startup validation",
    )
    assert str(exc) == "GitHub App credentials failed startup validation"


@pytest.mark.parametrize(
    ("app_id", "private_key", "webhook_secret", "expected_missing"),
    [
        (None, SecretStr("key"), SecretStr("wh"), "app_id"),
        (12345, None, SecretStr("wh"), "private_key"),
        (12345, SecretStr("key"), None, "webhook_secret"),
    ],
    ids=["missing_app_id", "missing_private_key", "missing_webhook_secret"],
)
@pytest.mark.asyncio
async def test_factory_raises_with_specific_missing_secret(
    db_session: AsyncSession,
    app_id: int | None,
    private_key: SecretStr | None,
    webhook_secret: SecretStr | None,
    expected_missing: str,
) -> None:
    """The factory tags the raise with which specific secret is missing.

    The factory's ``_require_github_app_config`` gate sees each of the
    three secrets directly, so it must pass the exact missing-secret
    value to the exception — generic "something is missing" would lose
    the routing signal the Sentry tag is for.
    """
    async with httpx.AsyncClient() as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            http_client=http_client,
            github_app_id=app_id,
            github_app_private_key=private_key,
            github_webhook_secret=webhook_secret,
            default_queue_name="docverse:queue",
        )
        with pytest.raises(GitHubAppNotConfiguredError) as excinfo:
            factory.create_github_app_client()
    assert excinfo.value.missing_secret == expected_missing


@pytest.mark.asyncio
async def test_factory_validation_failed_raises_private_key(
    db_session: AsyncSession,
) -> None:
    """A failed startup validation tags the event with ``private_key``.

    All three secrets are set but ``github_app_validated=False`` — the
    canonical failure mode is a malformed PEM or a key that doesn't
    match the registered app, both of which surface through the
    private key. Tagging the raise as ``private_key`` keeps the Sentry
    routing aligned with the most common operator-actionable cause
    while the explicit message preserves the "failed startup
    validation" log wording for ops grepping the pod logs.
    """
    async with httpx.AsyncClient() as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            http_client=http_client,
            github_app_id=12345,
            github_app_private_key=SecretStr("key"),
            github_webhook_secret=SecretStr("wh"),
            github_app_validated=False,
            default_queue_name="docverse:queue",
        )
        with pytest.raises(GitHubAppNotConfiguredError) as excinfo:
            factory.create_github_app_client()
    assert excinfo.value.missing_secret == "private_key"
    assert "validation" in str(excinfo.value)
