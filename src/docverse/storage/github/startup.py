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
    def github_app_enabled(self) -> bool:
        """Whether all three GitHub App secrets are set."""

    @property
    def github_app_id(self) -> int | None:
        """The configured GitHub App numeric ID, or ``None``."""

    def set_github_app_validated(self, *, value: bool) -> None:
        """Record the outcome of the startup-time validation."""


async def validate_github_app(  # noqa: PLR0913
    *,
    state: GitHubAppValidationState,
    app_id: int | None,
    private_key: SecretStr | None,
    app_name: str,
    http_client: httpx.AsyncClient,
    logger: structlog.stdlib.BoundLogger,
) -> str | None:
    """Validate GitHub App credentials and update ``state`` on failure.

    No-op when ``state.github_app_enabled`` is ``False`` — the
    secrets-unset gate on
    :meth:`docverse.factory.Factory._require_github_app_config` already
    keeps the feature disabled, so issuing a network call would be
    redundant and would fail anyway.

    Returns
    -------
    str or None
        On a successful validation, the App's public ``html_url`` from
        the ``GET /app`` response (the install page the API surfaces so
        the UI can prompt operators to install the App). ``None`` when
        the feature is disabled, when GitHub omits the field, or when
        validation failed (the failure also flips ``state`` off). The
        API lifespan stores this value; the worker startup path
        discards it.
    """
    if not state.github_app_enabled:
        return None
    # ``github_app_enabled`` guarantees both are set; the explicit
    # check narrows the types for the GitHubAppClientFactory call.
    if app_id is None or private_key is None:  # pragma: no cover
        return None
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
        html_url = await client.validate()
    except Exception as exc:  # noqa: BLE001
        # Validator must not crash the service. Known failure types
        # are ``jwt.exceptions.InvalidKeyError`` (PEM parse) and
        # ``gidgethub.GitHubException`` (non-2xx response), but
        # underlying ``httpx`` errors and any future safir/pyjwt
        # update can surface different types — narrowing the catch
        # would trade a clear behaviour for a brittle list.
        logger.error(  # noqa: TRY400
            "GitHub App config invalid; disabling feature",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        state.set_github_app_validated(value=False)
        return None
    logger.info("GitHub App config validated", app_id=state.github_app_id)
    return html_url
