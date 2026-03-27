"""Factory for creating ObjectStore instances from service config."""

from __future__ import annotations

from typing import Any

import httpx
import structlog

from ._protocol import ObjectStore
from ._s3 import S3ObjectStore

__all__ = ["create_objectstore"]

# Service providers that use the S3-compatible implementation.
_S3_COMPATIBLE_PROVIDERS = {"aws_s3", "cloudflare_r2", "minio"}

# Mapping from service provider to the endpoint URL template.
# Providers not listed here must include endpoint_url in config.
_R2_ENDPOINT_TEMPLATE = "https://{account_id}.r2.cloudflarestorage.com"

_REQUIRED_CONFIG_KEYS: dict[str, set[str]] = {
    "aws_s3": {"bucket"},
    "cloudflare_r2": {"account_id", "bucket"},
    "minio": {"endpoint_url", "bucket"},
}
_REQUIRED_CREDENTIAL_KEYS: set[str] = {"access_key_id", "secret_access_key"}


def _validate_s3_keys(
    provider: str,
    config: dict[str, Any],
    credentials: dict[str, Any],
) -> None:
    """Raise ``ValueError`` if required keys are missing."""
    missing_config = _REQUIRED_CONFIG_KEYS[provider] - config.keys()
    missing_creds = _REQUIRED_CREDENTIAL_KEYS - credentials.keys()
    errors: list[str] = []
    if missing_config:
        errors.append(f"config: {', '.join(sorted(missing_config))}")
    if missing_creds:
        errors.append(f"credentials: {', '.join(sorted(missing_creds))}")
    if errors:
        msg = (
            f"Missing required keys for {provider!r} object store — "
            + "; ".join(errors)
        )
        raise ValueError(msg)


def create_objectstore(
    *,
    provider: str,
    config: dict[str, Any],
    credentials: dict[str, Any],
    logger: structlog.stdlib.BoundLogger,
    http_client: httpx.AsyncClient | None = None,
) -> ObjectStore:
    """Create an ObjectStore from service config and decrypted credentials.

    Parameters
    ----------
    provider
        The service provider (e.g. ``aws_s3``, ``cloudflare_r2``,
        ``minio``).
    config
        Non-secret service configuration (bucket, region, account_id, etc.).
    credentials
        Decrypted credential payload (access keys, tokens, etc.).
    logger
        Bound logger for contextual logging.

    Returns
    -------
    ObjectStore
        An unopened ObjectStore. The caller must use it as an async
        context manager or call ``open()`` before use.

    Raises
    ------
    ValueError
        If the provider is not supported or required configuration/credential
        keys are missing.
    """
    if provider in _S3_COMPATIBLE_PROVIDERS:
        _validate_s3_keys(provider, config, credentials)
        # Derive endpoint_url based on provider
        if provider == "cloudflare_r2":
            endpoint_url = _R2_ENDPOINT_TEMPLATE.format(
                account_id=config["account_id"]
            )
        elif provider == "minio":
            endpoint_url = config["endpoint_url"]
        else:
            # aws_s3: use the default AWS endpoint (no custom endpoint needed)
            endpoint_url = None

        return S3ObjectStore(
            endpoint_url=endpoint_url,
            bucket=config["bucket"],
            access_key_id=credentials["access_key_id"],
            secret_access_key=credentials["secret_access_key"],
            region=config.get("region", ""),
            logger=logger,
            http_client=http_client,
        )
    msg = f"Unsupported object store provider: {provider!r}"
    raise ValueError(msg)
