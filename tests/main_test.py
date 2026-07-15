"""Lifespan-level tests for the API service ``main.py``.

These exercise the integration of the GitHub App startup validator
(see :func:`docverse.storage.github.validate_github_app`) with the
real FastAPI lifespan. The validator itself is unit-tested in
``tests/storage/github/startup_test.py`` — this file confirms that the
lifespan calls it and that the resulting ``context_dependency`` state
makes the webhook endpoint behave correctly.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
import structlog
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from safir.arq import MockArqQueue
from safir.database import (
    create_database_engine,
    initialize_database,
    stamp_database_async,
)
from safir.dependencies.arq import arq_dependency
from sqlalchemy import text
from structlog.testing import capture_logs

from docverse.config import config
from docverse.dbschema import Base
from docverse.dependencies.context import context_dependency
from docverse.main import app as docverse_app
from docverse.storage.user_info_store import StubUserInfoStore
from tests.support.github_mock import GitHubMock


@pytest_asyncio.fixture(autouse=True)
async def _reset_context_dependency() -> AsyncGenerator[None]:
    """Snapshot and restore the GitHub-App-related singleton state.

    The ``context_dependency`` singleton survives between tests in this
    module because the lifespan ``aclose`` flips ``_initialized`` but
    leaves the secrets and ``_github_app_validated`` flag untouched.
    Without this guard, a passing-secrets test could pollute a later
    ``test_..._skips_validation_when_secrets_unset`` run.
    """
    saved_secrets = (
        context_dependency._github_app_id,
        context_dependency._github_app_private_key,
        context_dependency._github_webhook_secret,
    )
    saved_validated = context_dependency._github_app_validated
    try:
        yield
    finally:
        context_dependency.set_github_secrets(
            app_id=saved_secrets[0],
            private_key=saved_secrets[1],
            webhook_secret=saved_secrets[2],
        )
        context_dependency.set_github_app_validated(value=saved_validated)


@pytest_asyncio.fixture
async def database_initialized() -> AsyncGenerator[None]:
    """Stamp the test database so ``is_database_current`` passes.

    Mirrors the DB setup the ``app`` fixture in ``tests/conftest.py``
    performs, but without entering the application's
    ``LifespanManager`` — these tests run the lifespan themselves to
    exercise the GitHub-App startup validator path.
    """
    logger = structlog.get_logger("docverse")
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    await initialize_database(engine, logger, schema=Base.metadata, reset=True)
    await stamp_database_async(engine)
    await engine.dispose()
    yield


def _patch_github_secrets(
    monkeypatch: pytest.MonkeyPatch,
    *,
    app_id: int,
    private_key: str,
    webhook_secret: str = "wh-secret",
) -> None:
    monkeypatch.setattr(config, "github_app_id", app_id)
    monkeypatch.setattr(
        config, "github_app_private_key", SecretStr(private_key)
    )
    monkeypatch.setattr(
        config, "github_webhook_secret", SecretStr(webhook_secret)
    )


def _override_arq_and_user_info() -> None:
    """Replace process-global mocks the existing test suite assumes.

    The conftest ``app`` fixture sets these inside its own
    ``LifespanManager`` block; tests in this file run the lifespan
    themselves, so they must repeat the same overrides post-startup.
    """
    arq_dependency._arq_queue = MockArqQueue(
        default_queue_name=config.arq_queue_name
    )
    context_dependency._user_info_store = StubUserInfoStore()


@pytest.mark.asyncio
async def test_lifespan_validates_github_app_when_secrets_set(
    database_initialized: None,
    mock_github: GitHubMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Valid mocked credentials → feature stays enabled, INFO log."""
    mock_github.seed_app()
    _patch_github_secrets(
        monkeypatch,
        app_id=mock_github.app_id,
        private_key=mock_github.private_key_pem,
    )

    with capture_logs() as captured:
        async with LifespanManager(docverse_app):
            _override_arq_and_user_info()
            assert context_dependency._github_app_validated is True

    info_events = [
        entry
        for entry in captured
        if entry.get("event") == "GitHub App config validated"
    ]
    assert len(info_events) == 1
    assert info_events[0]["app_id"] == mock_github.app_id


@pytest.mark.asyncio
async def test_lifespan_disables_github_app_on_invalid_key(
    database_initialized: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed PEM → feature disabled, ERROR log, service still starts.

    Acceptance #2: subsequent webhook POSTs return 404 (the same shape
    the endpoint produces when the three secrets are unset).
    """
    _patch_github_secrets(
        monkeypatch,
        app_id=999_999,
        private_key="not-a-real-pem",
    )

    with capture_logs() as captured:
        async with LifespanManager(docverse_app):
            _override_arq_and_user_info()
            assert context_dependency._github_app_validated is False
            async with AsyncClient(
                base_url="https://example.com/",
                transport=ASGITransport(app=docverse_app),
            ) as client:
                response = await client.post(
                    "/docverse/webhooks/github",
                    content=b"{}",
                    headers={
                        "Content-Type": "application/json",
                        "X-GitHub-Event": "push",
                        "X-GitHub-Delivery": (
                            "00000000-0000-0000-0000-000000000000"
                        ),
                    },
                )
            assert response.status_code == 404

    error_events = [
        entry
        for entry in captured
        if entry.get("event") == "GitHub App config invalid; disabling feature"
    ]
    assert len(error_events) == 1
    assert error_events[0]["log_level"] == "error"


@pytest.mark.asyncio
async def test_lifespan_disables_github_app_on_unauthorized_app(
    database_initialized: None,
    mock_github: GitHubMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``GET /app`` returns 401 → feature disabled, ERROR log."""
    mock_github.seed_app(status_code=401, body={"message": "Bad credentials"})
    _patch_github_secrets(
        monkeypatch,
        app_id=mock_github.app_id,
        private_key=mock_github.private_key_pem,
    )

    with capture_logs() as captured:
        async with LifespanManager(docverse_app):
            _override_arq_and_user_info()
            assert context_dependency._github_app_validated is False

    error_events = [
        entry
        for entry in captured
        if entry.get("event") == "GitHub App config invalid; disabling feature"
    ]
    assert len(error_events) == 1


@pytest.mark.asyncio
async def test_lifespan_skips_validation_when_secrets_unset(
    database_initialized: None,
    mock_github: GitHubMock,
) -> None:
    """No GitHub App secrets configured → no log, no GitHub call."""
    with capture_logs() as captured:
        async with LifespanManager(docverse_app):
            _override_arq_and_user_info()
            assert context_dependency.github_app_enabled is False
            assert context_dependency._github_app_validated is True

    validator_events = [
        entry
        for entry in captured
        if entry.get("event")
        in {
            "GitHub App config validated",
            "GitHub App config invalid; disabling feature",
        }
    ]
    assert validator_events == []
    app_calls = [
        call
        for call in mock_github.router.calls
        if call.request.url.path == "/app"
    ]
    assert app_calls == []
