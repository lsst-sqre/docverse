"""Factory for creating EditionPublisher instances from service config."""

from __future__ import annotations

from typing import Any

import httpx
import structlog

from ._cloudflare_kv import CloudflareKvEditionPublisher
from ._protocol import EditionPublisher

__all__ = ["create_edition_publisher"]


_CLOUDFLARE_WORKERS_PROVIDER = "cloudflare_workers"

_REQUIRED_CONFIG_KEYS: dict[str, set[str]] = {
    _CLOUDFLARE_WORKERS_PROVIDER: {"account_id", "kv_namespace_id"},
}
_REQUIRED_CREDENTIAL_KEYS: dict[str, set[str]] = {
    _CLOUDFLARE_WORKERS_PROVIDER: {"api_token"},
}


def _validate_keys(
    provider: str,
    config: dict[str, Any],
    credentials: dict[str, Any],
) -> None:
    """Raise ``ValueError`` if required keys are missing or empty."""
    required_config = _REQUIRED_CONFIG_KEYS[provider]
    required_creds = _REQUIRED_CREDENTIAL_KEYS[provider]
    missing_config = {k for k in required_config if not config.get(k)}
    missing_creds = {k for k in required_creds if not credentials.get(k)}
    errors: list[str] = []
    if missing_config:
        errors.append(f"config: {', '.join(sorted(missing_config))}")
    if missing_creds:
        errors.append(f"credentials: {', '.join(sorted(missing_creds))}")
    if errors:
        msg = (
            f"Missing required keys for {provider!r} edition publisher — "
            + "; ".join(errors)
        )
        raise ValueError(msg)


def create_edition_publisher(
    *,
    provider: str,
    config: dict[str, Any],
    credentials: dict[str, Any],
    logger: structlog.stdlib.BoundLogger,
    http_client: httpx.AsyncClient,
) -> EditionPublisher:
    """Create an EditionPublisher from service config and credentials.

    Parameters
    ----------
    provider
        The service provider (currently only ``cloudflare_workers``).
    config
        Non-secret service configuration (account_id, kv_namespace_id, …).
    credentials
        Decrypted credential payload (api_token, …).
    logger
        Bound logger for contextual logging.
    http_client
        Shared ``httpx.AsyncClient`` used to issue requests.

    Returns
    -------
    EditionPublisher
        An unopened EditionPublisher. The caller must use it as an async
        context manager before calling ``publish``.

    Raises
    ------
    ValueError
        If the provider is not supported or required config/credential
        keys are missing.
    """
    if provider == _CLOUDFLARE_WORKERS_PROVIDER:
        _validate_keys(provider, config, credentials)
        return CloudflareKvEditionPublisher(
            account_id=config["account_id"],
            namespace_id=config["kv_namespace_id"],
            api_token=credentials["api_token"],
            http_client=http_client,
            logger=logger,
        )
    msg = f"Unsupported edition publisher provider: {provider!r}"
    raise ValueError(msg)
