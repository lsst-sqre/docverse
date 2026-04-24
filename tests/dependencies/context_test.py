"""Tests for the ContextDependency plumbing of GitHub App secrets."""

from __future__ import annotations

from unittest.mock import Mock

import httpx
import pytest
import structlog
from fastapi import Request, Response
from pydantic import SecretStr
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dependencies.context import ContextDependency
from docverse.storage.github import (
    GitHubAppClient,
    GitHubAppNotConfiguredError,
)
from tests.support.github_mock import GitHubMock


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


@pytest.mark.asyncio
async def test_request_context_creates_github_app_client_when_configured(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """With all three secrets set, the factory builds a GitHubAppClient."""
    dep = ContextDependency()
    async with httpx.AsyncClient() as http_client:
        await dep.initialize(
            http_client=http_client,
            github_app_id=mock_github.app_id,
            github_app_private_key=SecretStr(mock_github.private_key_pem),
            github_webhook_secret=SecretStr("webhook-secret"),
        )
        context = await dep(
            request=Mock(spec=Request),
            response=Mock(spec=Response),
            session=db_session,
            logger=_logger(),
            arq_queue=MockArqQueue(default_queue_name="docverse:queue"),
        )
        client = context.factory.create_github_app_client()

    assert isinstance(client, GitHubAppClient)


@pytest.mark.asyncio
async def test_request_context_raises_when_github_secret_missing(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A single missing secret makes ``create_github_app_client`` raise."""
    dep = ContextDependency()
    async with httpx.AsyncClient() as http_client:
        await dep.initialize(
            http_client=http_client,
            github_app_id=mock_github.app_id,
            github_app_private_key=SecretStr(mock_github.private_key_pem),
            # github_webhook_secret intentionally unset
        )
        context = await dep(
            request=Mock(spec=Request),
            response=Mock(spec=Response),
            session=db_session,
            logger=_logger(),
            arq_queue=MockArqQueue(default_queue_name="docverse:queue"),
        )
        with pytest.raises(GitHubAppNotConfiguredError):
            context.factory.create_github_app_client()
