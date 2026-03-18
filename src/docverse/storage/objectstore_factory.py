"""Factory for creating ObjectStore instances from credentials."""

from __future__ import annotations

from typing import Any

from .s3_objectstore import S3ObjectStore

__all__ = ["create_objectstore"]

# Service types that use the S3-compatible implementation.
_S3_COMPATIBLE_TYPES = {"s3", "r2", "minio"}


def create_objectstore(
    *, service_type: str, credential: dict[str, Any]
) -> S3ObjectStore:
    """Create an ObjectStore from a service type and decrypted credential.

    Parameters
    ----------
    service_type
        The object store service type (e.g. ``s3``, ``r2``, ``minio``).
    credential
        Decrypted credential payload. For S3-compatible stores, must
        contain ``endpoint_url``, ``bucket``, ``access_key_id``, and
        ``secret_access_key``. May also contain ``region``.

    Returns
    -------
    S3ObjectStore
        An unopened S3ObjectStore. The caller must use it as an async
        context manager or call ``open()`` before use.

    Raises
    ------
    ValueError
        If the service type is not supported.
    """
    if service_type in _S3_COMPATIBLE_TYPES:
        return S3ObjectStore(
            endpoint_url=credential["endpoint_url"],
            bucket=credential["bucket"],
            access_key_id=credential["access_key_id"],
            secret_access_key=credential["secret_access_key"],
            region=credential.get("region", ""),
        )
    msg = f"Unsupported object store service type: {service_type!r}"
    raise ValueError(msg)
