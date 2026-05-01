"""Startup-time validation of GitHub App credentials.

The API service lifespan and the arq worker startup hook both call
:func:`validate_github_app` once after process initialization. Failure
(parse error or non-2xx response from ``GET /app``) flips a flag on
the state holder so the feature is disabled for the lifetime of the
process — same behaviour binding endpoints + webhook already exhibit
when the three secrets are unset.
"""

from __future__ import annotations

from typing import Protocol

import httpx
import structlog
from pydantic import SecretStr
from safir.github import GitHubAppClientFactory

from .app_client import GitHubAppClient

__all__ = ["GitHubAppValidationState", "validate_github_app"]


class GitHubAppValidationState(Protocol):
    """Duck-type contract shared by the API and worker state holders.

    :class:`docverse.dependencies.context.ContextDependency` and
    :class:`docverse.worker.main.WorkerFactoryBuilder` each implement
    this protocol so the same validator helper can flip either one.
    """

    @property
    def github_app_enabled(self) -> bool: ...

    @property
    def github_app_id(self) -> int | None: ...

    def set_github_app_validated(self, *, value: bool) -> None: ...


async def validate_github_app(
    *,
    state: GitHubAppValidationState,
    app_id: int | None,
    private_key: SecretStr | None,
    app_name: str,
    http_client: httpx.AsyncClient,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    """Validate GitHub App credentials and update ``state`` on failure.

    No-op when ``state.github_app_enabled`` is ``False`` — the
    secrets-unset gate on
    :meth:`docverse.factory.Factory._require_github_app_config` already
    keeps the feature disabled, so issuing a network call would be
    redundant and would fail anyway.
    """
    if not state.github_app_enabled:
        return
    # ``github_app_enabled`` guarantees both are set; the explicit
    # check narrows the types for the GitHubAppClientFactory call.
    if app_id is None or private_key is None:  # pragma: no cover
        return
    factory = GitHubAppClientFactory(
        id=app_id,
        key=private_key.get_secret_value(),
        name=app_name,
        http_client=http_client,
    )
    client = GitHubAppClient(
        factory=factory, http_client=http_client, logger=logger
    )
    try:
        await client.validate()
    except Exception as exc:
        logger.error(
            "GitHub App config invalid; disabling feature",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        state.set_github_app_validated(value=False)
    else:
        logger.info(
            "GitHub App config validated", app_id=state.github_app_id
        )
