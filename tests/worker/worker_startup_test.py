"""Tests for the GitHub App startup-validation path in the arq worker.

The full ``docverse_server.worker.main.startup`` function requires a real
Redis instance to call ``RedisArqQueue.initialize``; these tests
exercise the GitHub-App validation seam by driving the validator
against a real :class:`docverse_server.worker.main.WorkerFactoryBuilder`,
mirroring how the worker's ``startup`` wires the two together.
"""

from __future__ import annotations

import httpx
import pytest
import structlog
from cryptography.fernet import Fernet
from pydantic import SecretStr
from rubin.repertoire import DiscoveryClient
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse_server.config import Configuration
from docverse_server.services.credential_encryptor import CredentialEncryptor
from docverse_server.storage.github import (
    GitHubAppNotConfiguredError,
    validate_github_app,
)
from docverse_server.worker.main import WorkerFactoryBuilder
from tests.support.github_mock import DEFAULT_APP_NAME, GitHubMock

_config = Configuration()


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _make_builder(
    *,
    http_client: httpx.AsyncClient,
    copy_http_client: httpx.AsyncClient | None = None,
    github_app_id: int | None = None,
    github_app_private_key: SecretStr | None = None,
    github_webhook_secret: SecretStr | None = None,
    keeper_sync_copy_concurrency: int | None = None,
    keeper_sync_upload_max_attempts: int | None = None,
    keeper_sync_upload_max_backoff_seconds: float | None = None,
) -> WorkerFactoryBuilder:
    return WorkerFactoryBuilder(
        encryptor=CredentialEncryptor(
            current_key=Fernet.generate_key().decode()
        ),
        http_client=http_client,
        copy_http_client=copy_http_client,
        arq_queue=MockArqQueue(default_queue_name=_config.arq_queue_name),
        discovery=DiscoveryClient(http_client),
        github_app_id=github_app_id,
        github_app_private_key=github_app_private_key,
        github_webhook_secret=github_webhook_secret,
        default_queue_name=_config.arq_queue_name,
        keeper_sync_copy_concurrency=(
            keeper_sync_copy_concurrency
            if keeper_sync_copy_concurrency is not None
            else _config.keeper_sync_copy_concurrency
        ),
        keeper_sync_upload_max_attempts=(
            keeper_sync_upload_max_attempts
            if keeper_sync_upload_max_attempts is not None
            else _config.keeper_sync_upload_max_attempts
        ),
        keeper_sync_upload_max_backoff_seconds=(
            keeper_sync_upload_max_backoff_seconds
            if keeper_sync_upload_max_backoff_seconds is not None
            else _config.keeper_sync_upload_max_backoff_seconds
        ),
    )


@pytest.mark.asyncio
async def test_builder_threads_copy_concurrency_to_per_job_factory(
    db_session: AsyncSession,
) -> None:
    """The worker's copier fan-out bound is process-config driven.

    ``keeper_sync_project`` is the only job kind that copies build
    content, so the sync worker's peak resident size is this bound
    times the pool's ``max_jobs``. The builder must carry the operator
    knob onto every per-job factory rather than letting the factory
    fall back to the copier's own default (#517).
    """
    async with httpx.AsyncClient() as http_client:
        builder = _make_builder(
            http_client=http_client, keeper_sync_copy_concurrency=3
        )
        factory = builder(session=db_session, logger=_logger())
        assert factory.keeper_sync_copy_concurrency == 3


@pytest.mark.asyncio
async def test_builder_threads_upload_budget_to_per_job_factory(
    db_session: AsyncSession,
) -> None:
    """The keeper-sync presigned-upload budget is process-config driven.

    The sync worker's copier is the only caller that needs more than
    the shared retry budget to ride out an R2 connect outage (PRD #685),
    so the builder must carry both operator knobs onto every per-job
    factory rather than letting it fall back to the shared defaults.
    """
    async with httpx.AsyncClient() as http_client:
        builder = _make_builder(
            http_client=http_client,
            keeper_sync_upload_max_attempts=9,
            keeper_sync_upload_max_backoff_seconds=42.0,
        )
        factory = builder(session=db_session, logger=_logger())
        assert factory.keeper_sync_upload_max_attempts == 9
        assert factory.keeper_sync_upload_max_backoff_seconds == 42.0


@pytest.mark.asyncio
async def test_builder_threads_copy_client_to_per_job_factory(
    db_session: AsyncSession,
) -> None:
    """Every per-job factory copies build content over the copy client.

    ``_startup`` builds the copy client once per process, sized for the
    keeper-sync pool's full copy concurrency (PRD #685); a per-job
    factory that fell back to the shared client would put every copy
    burst back on the pool the discovery, LTD API and GitHub calls use.
    """
    async with (
        httpx.AsyncClient() as http_client,
        httpx.AsyncClient() as copy_http_client,
    ):
        builder = _make_builder(
            http_client=http_client, copy_http_client=copy_http_client
        )
        factory = builder(session=db_session, logger=_logger())
        assert factory.copy_http_client is copy_http_client


@pytest.mark.asyncio
async def test_builder_without_copy_client_copies_over_shared_client(
    db_session: AsyncSession,
) -> None:
    """A builder given no copy client leaves copies on the shared one."""
    async with httpx.AsyncClient() as http_client:
        builder = _make_builder(http_client=http_client)
        factory = builder(session=db_session, logger=_logger())
        assert factory.copy_http_client is http_client


@pytest.mark.asyncio
async def test_worker_startup_validates_when_secrets_set(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """All secrets set + valid ``GET /app`` → builder stays validated."""
    mock_github.seed_app()

    async with httpx.AsyncClient() as http_client:
        builder = _make_builder(
            http_client=http_client,
            github_app_id=mock_github.app_id,
            github_app_private_key=SecretStr(mock_github.private_key_pem),
            github_webhook_secret=SecretStr("wh-secret"),
        )
        with capture_logs() as captured:
            await validate_github_app(
                state=builder,
                app_id=mock_github.app_id,
                private_key=SecretStr(mock_github.private_key_pem),
                app_name=DEFAULT_APP_NAME,
                http_client=http_client,
                logger=_logger(),
            )
        # Per-job factory should still allow GitHub App client creation.
        factory = builder(session=db_session, logger=_logger())
        client = factory.create_github_app_client()
    assert client is not None

    info_events = [
        entry
        for entry in captured
        if entry.get("event") == "GitHub App config validated"
    ]
    assert len(info_events) == 1


@pytest.mark.asyncio
async def test_worker_startup_disables_feature_on_invalid_key(
    db_session: AsyncSession,
) -> None:
    """Malformed PEM → builder marked invalid; per-job factory raises.

    Acceptance #6 (worker mirror of the API service): subsequent
    ``dashboard_sync`` / webhook codepaths reach the same
    ``GitHubAppNotConfiguredError`` they would hit on a deployment with
    the secrets unset.
    """
    async with httpx.AsyncClient() as http_client:
        builder = _make_builder(
            http_client=http_client,
            github_app_id=999_999,
            github_app_private_key=SecretStr("not-a-real-pem"),
            github_webhook_secret=SecretStr("wh-secret"),
        )
        with capture_logs() as captured:
            await validate_github_app(
                state=builder,
                app_id=999_999,
                private_key=SecretStr("not-a-real-pem"),
                app_name=DEFAULT_APP_NAME,
                http_client=http_client,
                logger=_logger(),
            )
        factory = builder(session=db_session, logger=_logger())
        with pytest.raises(GitHubAppNotConfiguredError):
            factory.create_github_app_client()

    error_events = [
        entry
        for entry in captured
        if entry.get("event") == "GitHub App config invalid; disabling feature"
    ]
    assert len(error_events) == 1
    assert error_events[0]["error_type"] == "InvalidKeyError"


@pytest.mark.asyncio
async def test_worker_startup_disables_feature_on_unauthorized_app(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A 401 from ``GET /app`` flips the per-job factory off."""
    mock_github.seed_app(status_code=401, body={"message": "Bad credentials"})

    async with httpx.AsyncClient() as http_client:
        builder = _make_builder(
            http_client=http_client,
            github_app_id=mock_github.app_id,
            github_app_private_key=SecretStr(mock_github.private_key_pem),
            github_webhook_secret=SecretStr("wh-secret"),
        )
        with capture_logs() as captured:
            await validate_github_app(
                state=builder,
                app_id=mock_github.app_id,
                private_key=SecretStr(mock_github.private_key_pem),
                app_name=DEFAULT_APP_NAME,
                http_client=http_client,
                logger=_logger(),
            )
        factory = builder(session=db_session, logger=_logger())
        with pytest.raises(GitHubAppNotConfiguredError):
            factory.create_github_app_client()

    error_events = [
        entry
        for entry in captured
        if entry.get("event") == "GitHub App config invalid; disabling feature"
    ]
    assert len(error_events) == 1


@pytest.mark.asyncio
async def test_worker_startup_skips_validation_when_secrets_unset(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Disabled feature on the worker → no GitHub call, no log."""
    async with httpx.AsyncClient() as http_client:
        builder = _make_builder(http_client=http_client)
        with capture_logs() as captured:
            await validate_github_app(
                state=builder,
                app_id=None,
                private_key=None,
                app_name=DEFAULT_APP_NAME,
                http_client=http_client,
                logger=_logger(),
            )
        factory = builder(session=db_session, logger=_logger())
        # Per-job factory still raises — but on the secrets-unset gate,
        # not the validated-False gate (validated stays True).
        with pytest.raises(GitHubAppNotConfiguredError):
            factory.create_github_app_client()

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


@pytest.mark.asyncio
async def test_worker_factory_builder_shares_one_purge_coalescer(
    db_session: AsyncSession,
) -> None:
    """Per-job factories share the builder's process-lifetime coalescer.

    Coalescing CDN purges only reduces call volume if the state outlives
    a single job: keeper-sync enqueues one ``publish_edition`` job per
    synced edition, so a publish burst is many jobs rather than one
    in-process loop.
    """
    async with httpx.AsyncClient() as http_client:
        builder = _make_builder(http_client=http_client)
        first = builder(session=db_session, logger=_logger())
        second = builder(session=db_session, logger=_logger())

    assert first.purge_coalescer is second.purge_coalescer
