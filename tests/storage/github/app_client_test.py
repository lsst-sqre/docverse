"""Tests for the GitHubAppClient wrapper and the Factory helper."""

from __future__ import annotations

import gidgethub
import httpx
import jwt as pyjwt
import pytest
import structlog
from pydantic import SecretStr
from safir.github import GitHubAppClientFactory
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.factory import Factory
from docverse.storage.github import (
    GITHUB_API_BASE_URL,
    GitHubAppClient,
    GitHubAppNotConfiguredError,
    InstallationAuth,
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

    assert token == "ghs_test_abc"  # noqa: S105


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
    assert auth.token == "ghs_installtok"  # noqa: S105
    assert auth.base_url == GITHUB_API_BASE_URL


@pytest.mark.asyncio
async def test_validate_succeeds_on_2xx_app_response(
    mock_github: GitHubMock,
) -> None:
    """``validate`` returns cleanly when ``GET /app`` returns 200."""
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
        await client.validate()


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

    assert auth.token == "ghs_factory_test"  # noqa: S105


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
