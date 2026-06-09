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
from docverse.metrics.events import DocverseEvents
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
    mock_events: DocverseEvents,
) -> None:
    """With all three secrets set, the factory builds a GitHubAppClient."""
    dep = ContextDependency()
    async with httpx.AsyncClient() as http_client:
        await dep.initialize(
            http_client=http_client,
            github_app_id=mock_github.app_id,
            github_app_private_key=SecretStr(mock_github.private_key_pem),
            github_webhook_secret=SecretStr("webhook-secret"),
            events=mock_events,
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
    mock_events: DocverseEvents,
) -> None:
    """A single missing secret makes ``create_github_app_client`` raise."""
    dep = ContextDependency()
    async with httpx.AsyncClient() as http_client:
        await dep.initialize(
            http_client=http_client,
            github_app_id=mock_github.app_id,
            github_app_private_key=SecretStr(mock_github.private_key_pem),
            # github_webhook_secret intentionally unset
            events=mock_events,
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


@pytest.mark.asyncio
async def test_set_github_app_validated_disables_feature(
    db_session: AsyncSession,
    mock_github: GitHubMock,
    mock_events: DocverseEvents,
) -> None:
    """``set_github_app_validated(False)`` makes the gate raise.

    Even with all three secrets set, flipping the validation flag off
    routes the binding endpoints + webhook to their feature-disabled
    response — same shape as if the secrets had been unset.
    """
    dep = ContextDependency()
    async with httpx.AsyncClient() as http_client:
        await dep.initialize(
            http_client=http_client,
            github_app_id=mock_github.app_id,
            github_app_private_key=SecretStr(mock_github.private_key_pem),
            github_webhook_secret=SecretStr("webhook-secret"),
            events=mock_events,
        )
        # Sanity check: the feature is enabled out of the gate.
        context = await dep(
            request=Mock(spec=Request),
            response=Mock(spec=Response),
            session=db_session,
            logger=_logger(),
            arq_queue=MockArqQueue(default_queue_name="docverse:queue"),
        )
        assert isinstance(
            context.factory.create_github_app_client(), GitHubAppClient
        )

        dep.set_github_app_validated(value=False)
        context = await dep(
            request=Mock(spec=Request),
            response=Mock(spec=Response),
            session=db_session,
            logger=_logger(),
            arq_queue=MockArqQueue(default_queue_name="docverse:queue"),
        )
        with pytest.raises(GitHubAppNotConfiguredError):
            context.factory.create_github_app_client()


@pytest.mark.asyncio
async def test_github_app_enabled_property_tracks_secret_presence(
    mock_github: GitHubMock,
) -> None:
    """``github_app_enabled`` is True iff all three secrets are set."""
    dep = ContextDependency()
    assert dep.github_app_enabled is False
    dep.set_github_secrets(
        app_id=mock_github.app_id,
        private_key=SecretStr(mock_github.private_key_pem),
        webhook_secret=SecretStr("webhook-secret"),
    )
    assert dep.github_app_enabled is True
    dep.set_github_secrets(app_id=None, private_key=None, webhook_secret=None)
    assert dep.github_app_enabled is False


@pytest.mark.asyncio
async def test_set_github_secrets_resets_validated_flag(
    db_session: AsyncSession,
    mock_github: GitHubMock,
    mock_events: DocverseEvents,
) -> None:
    """A new secret bundle clears the prior ``validated=False`` decision.

    The startup validator owns the flag; if a deployment's secrets are
    rotated and re-applied, the rotated bundle should not inherit the
    rejection of the previous one.
    """
    dep = ContextDependency()
    async with httpx.AsyncClient() as http_client:
        await dep.initialize(
            http_client=http_client,
            github_app_id=mock_github.app_id,
            github_app_private_key=SecretStr(mock_github.private_key_pem),
            github_webhook_secret=SecretStr("webhook-secret"),
            events=mock_events,
        )
        dep.set_github_app_validated(value=False)
        dep.set_github_secrets(
            app_id=mock_github.app_id,
            private_key=SecretStr(mock_github.private_key_pem),
            webhook_secret=SecretStr("webhook-secret-rotated"),
        )
        context = await dep(
            request=Mock(spec=Request),
            response=Mock(spec=Response),
            session=db_session,
            logger=_logger(),
            arq_queue=MockArqQueue(default_queue_name="docverse:queue"),
        )
        assert isinstance(
            context.factory.create_github_app_client(), GitHubAppClient
        )


@pytest.mark.asyncio
async def test_set_github_secrets_overrides_three_fields(
    db_session: AsyncSession,
    mock_github: GitHubMock,
    mock_events: DocverseEvents,
) -> None:
    """``set_github_secrets`` updates the three GitHub-App secret slots."""
    dep = ContextDependency()
    async with httpx.AsyncClient() as http_client:
        await dep.initialize(http_client=http_client, events=mock_events)
        # No secrets yet.
        context = await dep(
            request=Mock(spec=Request),
            response=Mock(spec=Response),
            session=db_session,
            logger=_logger(),
            arq_queue=MockArqQueue(default_queue_name="docverse:queue"),
        )
        with pytest.raises(GitHubAppNotConfiguredError):
            context.factory.create_github_app_client()

        # Toggle on via the public setter.
        dep.set_github_secrets(
            app_id=mock_github.app_id,
            private_key=SecretStr(mock_github.private_key_pem),
            webhook_secret=SecretStr("webhook-secret"),
        )
        context = await dep(
            request=Mock(spec=Request),
            response=Mock(spec=Response),
            session=db_session,
            logger=_logger(),
            arq_queue=MockArqQueue(default_queue_name="docverse:queue"),
        )
        assert isinstance(
            context.factory.create_github_app_client(), GitHubAppClient
        )

        # Toggle back off in a single call.
        dep.set_github_secrets(
            app_id=None, private_key=None, webhook_secret=None
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
