"""Tests for the startup-time GitHub App validator."""

from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any

import httpx
import pytest
import structlog
from pydantic import SecretStr
from structlog.testing import capture_logs

from docverse.storage.github import validate_github_app
from tests.support.github_mock import DEFAULT_APP_NAME, GitHubMock


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


class _FakeState:
    """Stand-in for ``ContextDependency`` / ``WorkerFactoryBuilder``.

    Tests assert directly on the recorded ``set_github_app_validated``
    calls rather than instantiating either real state holder, so this
    one helper covers both.
    """

    def __init__(
        self,
        *,
        enabled: bool,
        app_id: int | None = None,
    ) -> None:
        self._enabled = enabled
        self._app_id = app_id
        self.validated_calls: list[bool] = []

    @property
    def github_app_enabled(self) -> bool:
        return self._enabled

    @property
    def github_app_id(self) -> int | None:
        return self._app_id

    def set_github_app_validated(self, *, value: bool) -> None:
        self.validated_calls.append(value)


def _log_events(captured: list[MutableMapping[str, Any]]) -> list[str]:
    return [str(entry.get("event", "")) for entry in captured]


@pytest.mark.asyncio
async def test_skips_when_secrets_unset() -> None:
    """Disabled feature → no network call, no log, no state mutation."""
    state = _FakeState(enabled=False)

    async with httpx.AsyncClient() as http_client:
        with capture_logs() as captured:
            await validate_github_app(
                state=state,
                app_id=None,
                private_key=None,
                app_name=DEFAULT_APP_NAME,
                http_client=http_client,
                logger=_logger(),
            )

    assert state.validated_calls == []
    assert _log_events(captured) == []


@pytest.mark.asyncio
async def test_logs_info_when_validation_succeeds(
    mock_github: GitHubMock,
) -> None:
    """All secrets set + valid ``GET /app`` → INFO log, no state mutation."""
    mock_github.seed_app()
    state = _FakeState(enabled=True, app_id=mock_github.app_id)

    async with httpx.AsyncClient() as http_client:
        with capture_logs() as captured:
            await validate_github_app(
                state=state,
                app_id=mock_github.app_id,
                private_key=SecretStr(mock_github.private_key_pem),
                app_name=DEFAULT_APP_NAME,
                http_client=http_client,
                logger=_logger(),
            )

    assert state.validated_calls == []
    info_events = [
        entry
        for entry in captured
        if entry.get("event") == "GitHub App config validated"
    ]
    assert len(info_events) == 1
    assert info_events[0]["app_id"] == mock_github.app_id
    assert info_events[0]["log_level"] == "info"


@pytest.mark.asyncio
async def test_disables_feature_on_malformed_key() -> None:
    """Malformed PEM → ERROR log, ``set_github_app_validated(False)``."""
    state = _FakeState(enabled=True, app_id=12345)

    async with httpx.AsyncClient() as http_client:
        with capture_logs() as captured:
            await validate_github_app(
                state=state,
                app_id=12345,
                private_key=SecretStr("not-a-real-pem"),
                app_name=DEFAULT_APP_NAME,
                http_client=http_client,
                logger=_logger(),
            )

    assert state.validated_calls == [False]
    error_events = [
        entry
        for entry in captured
        if entry.get("event") == "GitHub App config invalid; disabling feature"
    ]
    assert len(error_events) == 1
    assert error_events[0]["log_level"] == "error"
    assert error_events[0]["error_type"] == "InvalidKeyError"


@pytest.mark.asyncio
async def test_disables_feature_on_unauthorized_app_response(
    mock_github: GitHubMock,
) -> None:
    """A 401 from ``GET /app`` flips the feature off and logs ERROR."""
    mock_github.seed_app(status_code=401, body={"message": "Bad credentials"})
    state = _FakeState(enabled=True, app_id=mock_github.app_id)

    async with httpx.AsyncClient() as http_client:
        with capture_logs() as captured:
            await validate_github_app(
                state=state,
                app_id=mock_github.app_id,
                private_key=SecretStr(mock_github.private_key_pem),
                app_name=DEFAULT_APP_NAME,
                http_client=http_client,
                logger=_logger(),
            )

    assert state.validated_calls == [False]
    error_events = [
        entry
        for entry in captured
        if entry.get("event") == "GitHub App config invalid; disabling feature"
    ]
    assert len(error_events) == 1
    assert error_events[0]["log_level"] == "error"
