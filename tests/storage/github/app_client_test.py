"""Tests for the GitHubAppClient wrapper and the Factory helper."""

from __future__ import annotations

import httpx
import pytest
import structlog
from pydantic import SecretStr
from safir.github import GitHubAppClientFactory
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.factory import Factory
from docverse.storage.github import (
    GitHubAppClient,
    GitHubAppNotConfiguredError,
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
async def test_create_installation_http_client_sets_auth_header(
    mock_github: GitHubMock,
) -> None:
    """Returned client carries ``Authorization: Bearer <token>``."""
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
        installation_client = await client.create_installation_http_client(
            owner="acme", repo="templates"
        )

    try:
        assert (
            installation_client.headers["authorization"]
            == "Bearer ghs_installtok"
        )
        assert str(installation_client.base_url) == "https://api.github.com"
    finally:
        await installation_client.aclose()


@pytest.mark.asyncio
async def test_factory_create_github_app_client_all_set(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """With all three secrets set, the helper returns a GitHubAppClient."""
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
