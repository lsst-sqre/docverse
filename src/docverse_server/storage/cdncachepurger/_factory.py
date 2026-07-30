"""Factory for creating CdnCachePurger instances from service config."""

from __future__ import annotations

from typing import Any

import httpx
import structlog

from ._cloudflare import CloudflareCachePurger
from ._noop import NoopCdnCachePurger
from ._protocol import CdnCachePurger

__all__ = ["create_cdn_cache_purger"]


_CLOUDFLARE_WORKERS_PROVIDER = "cloudflare_workers"

_REQUIRED_CONFIG_KEYS: dict[str, set[str]] = {
    _CLOUDFLARE_WORKERS_PROVIDER: set(),
}
"""Config keys a purger cannot be built without.

``zone_id`` is deliberately **not** required: organizations served from
a ``workers.dev`` subdomain have no zone, and they get a
`NoopCdnCachePurger` rather than a configuration error.
"""

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
            f"Missing required keys for {provider!r} CDN cache purger — "
            + "; ".join(errors)
        )
        raise ValueError(msg)


def create_cdn_cache_purger(
    *,
    provider: str,
    config: dict[str, Any],
    credentials: dict[str, Any],
    logger: structlog.stdlib.BoundLogger,
    http_client: httpx.AsyncClient,
) -> CdnCachePurger:
    """Create a CdnCachePurger from service config and credentials.

    Parameters
    ----------
    provider
        The service provider (currently only ``cloudflare_workers``).
    config
        Non-secret service configuration. ``zone_id`` selects a real
        purger; without it the organization has no purgeable zone.
    credentials
        Decrypted credential payload (api_token, …). The same
        ``api_token`` as the edition publisher is reused.
    logger
        Bound logger for contextual logging.
    http_client
        Shared ``httpx.AsyncClient`` used to issue requests.

    Returns
    -------
    CdnCachePurger
        An unopened purger. The caller must use it as an async context
        manager before calling ``purge_hostname``. A `NoopCdnCachePurger`
        is returned when the provider is supported but ``zone_id`` is
        absent, so callers never have to branch on configuration.

    Raises
    ------
    ValueError
        If the provider is not supported or required credential keys are
        missing.
    """
    if provider == _CLOUDFLARE_WORKERS_PROVIDER:
        _validate_keys(provider, config, credentials)
        zone_id = config.get("zone_id")
        if not zone_id:
            return NoopCdnCachePurger(logger=logger)
        return CloudflareCachePurger(
            zone_id=zone_id,
            api_token=credentials["api_token"],
            http_client=http_client,
            logger=logger,
        )
    msg = f"Unsupported CDN cache purger provider: {provider!r}"
    raise ValueError(msg)
