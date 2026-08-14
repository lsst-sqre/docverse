"""S3-compatible object store implementation using aiobotocore."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import httpx
import structlog
from aiobotocore.client import AioBaseClient
from aiobotocore.session import AioSession, ClientCreatorContext, get_session
from botocore.config import Config

from .._http_retry import (
    DEFAULT_BASE_BACKOFF_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    RETRYABLE_TRANSPORT_ERRORS,
    retry_request,
)

__all__ = ["S3ObjectStore"]

#: Lifetime of a presigned upload signature. Every attempt mints a fresh
#: one, so this only has to outlive a single PUT rather than the whole
#: retry sequence.
_UPLOAD_URL_EXPIRES_SECONDS = 900


class S3ObjectStore:
    """Object store backed by an S3-compatible service.

    Works with AWS S3, Cloudflare R2, MinIO, and other S3-compatible
    services.

    Use as an async context manager to manage the underlying client
    session, or call ``open`` / ``close`` explicitly.

    Parameters
    ----------
    endpoint_url
        S3-compatible endpoint URL, or ``None`` to use the default
        AWS endpoint.
    bucket
        Bucket name.
    access_key_id
        AWS access key ID (or equivalent).
    secret_access_key
        AWS secret access key (or equivalent).
    region
        AWS region name (optional for non-AWS services).
    logger
        Bound logger for contextual logging.
    http_client
        Shared HTTP client. When set, ``upload_object`` PUTs to a
        presigned URL over this client instead of going through
        aiobotocore.
    max_attempts
        Attempts allowed for a presigned upload, including the first.
        Clamped to at least 1 so a misconfigured budget degrades to
        "upload once, no retries" rather than to a silent no-op.
    base_backoff_seconds
        Delay after a presigned upload's first failure; doubles each
        subsequent attempt.
    """

    def __init__(
        self,
        *,
        endpoint_url: str | None,
        bucket: str,
        access_key_id: str,
        secret_access_key: str,
        region: str = "",
        logger: structlog.stdlib.BoundLogger,
        http_client: httpx.AsyncClient | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS,
    ) -> None:
        self._endpoint_url = endpoint_url
        self._bucket = bucket
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._region = region
        self._logger = logger
        self._http_client = http_client
        # Clamped here as well as inside ``retry_request`` so the stored
        # value is the one actually spent, and the exhausted-transport
        # log below can report it without re-deriving the policy.
        self._max_attempts = max(1, max_attempts)
        self._base_backoff_seconds = base_backoff_seconds
        self._session: AioSession = get_session()
        self._client_cm: ClientCreatorContext | None = None
        self._client: AioBaseClient | None = None

    async def __aenter__(self) -> Self:
        """Open the S3 client session."""
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the S3 client session."""
        await self.close()

    async def open(self) -> None:
        """Create the underlying S3 client."""
        self._client_cm = self._session.create_client(
            "s3",
            endpoint_url=self._endpoint_url or None,
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
            region_name=self._region or None,
            config=Config(
                signature_version="s3v4",
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )
        self._client = await self._client_cm.__aenter__()

    async def close(self) -> None:
        """Close the underlying S3 client."""
        if self._client_cm is not None:
            await self._client_cm.__aexit__(None, None, None)
            self._client_cm = None
            self._client = None

    def _get_client(self) -> AioBaseClient:
        if self._client is None:
            msg = "S3ObjectStore is not open; use as async context manager"
            raise RuntimeError(msg)
        return self._client

    async def generate_presigned_upload_url(
        self, *, key: str, content_type: str, expires_in: int = 3600
    ) -> str:
        """Generate a pre-signed URL for uploading an object."""
        client = self._get_client()
        url: str = await client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": self._bucket,
                "Key": key,
                "ContentType": content_type,
            },
            ExpiresIn=expires_in,
        )
        return url

    async def generate_presigned_download_url(
        self, *, key: str, expires_in: int = 3600
    ) -> str:
        """Generate a pre-signed URL for downloading an object."""
        client = self._get_client()
        url: str = await client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self._bucket,
                "Key": key,
            },
            ExpiresIn=expires_in,
        )
        return url

    async def download_object(self, *, key: str) -> bytes:
        """Download an object from S3."""
        client = self._get_client()
        response = await client.get_object(Bucket=self._bucket, Key=key)
        async with response["Body"] as stream:
            data: bytes = await stream.read()
        return data

    async def delete_object(self, *, key: str) -> None:
        """Delete an object from S3."""
        client = self._get_client()
        await client.delete_object(Bucket=self._bucket, Key=key)

    async def list_objects(self, *, prefix: str) -> list[str]:
        """List objects with the given prefix."""
        client = self._get_client()
        keys: list[str] = []
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(
            Bucket=self._bucket, Prefix=prefix
        ):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> None:
        """Upload an object via presigned URL if http_client is available.

        Falls back to direct put_object when no http_client is set. The
        fallback needs no retry logic of its own: aiobotocore inherits
        botocore's standard retry mode.
        """
        if self._http_client is not None:
            await self._upload_via_presigned_url(
                http_client=self._http_client,
                key=key,
                data=data,
                content_type=content_type,
            )
        else:
            client = self._get_client()
            await client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )

    async def _upload_via_presigned_url(
        self,
        *,
        http_client: httpx.AsyncClient,
        key: str,
        data: bytes,
        content_type: str,
    ) -> None:
        """PUT an object to a presigned URL, retrying transient failures.

        Cloudflare R2 answers a bulk copy with occasional ``500``s and
        dropped connections, and this path — not the aiobotocore
        fallback — is the one production takes, so the retry lives here
        rather than in every caller. Retrying is safe because a PUT of
        the same key with the same bytes is idempotent.

        Raises
        ------
        httpx.HTTPStatusError
            If the destination returns any non-2xx status that is not
            worth retrying (including a 3xx redirect, which this client
            does not follow), or keeps returning a retryable one until
            the attempt budget is exhausted.
        httpx.TransportError
            If the transport keeps failing until the attempt budget is
            exhausted, or fails in a way a retry cannot fix.
        """
        logger = self._logger.bind(key=key)

        async def send() -> httpx.Response:
            # Sign afresh on every attempt. The signature is only valid
            # for _UPLOAD_URL_EXPIRES_SECONDS, and a retry sequence that
            # honours a long Retry-After can outlive that window; signing
            # is a local HMAC, so re-minting costs nothing but rules out
            # ever replaying an expired URL.
            url = await self._generate_upload_url(key)
            return await http_client.put(
                url,
                content=data,
                headers={"Content-Type": content_type},
            )

        try:
            outcome = await retry_request(
                send,
                operation="presigned upload",
                logger=logger,
                max_attempts=self._max_attempts,
                base_backoff_seconds=self._base_backoff_seconds,
            )
        except RETRYABLE_TRANSPORT_ERRORS as exc:
            logger.exception(
                "Presigned upload failed",
                error=str(exc),
                error_type=type(exc).__name__,
                attempts=self._max_attempts,
                retryable=True,
            )
            raise

        # Only a 2xx means the bytes landed. Anything else — a 3xx
        # region/endpoint redirect from S3 or R2 (this client does not
        # follow redirects, so the body went nowhere) as much as a 4xx or
        # 5xx — has to fail loudly rather than report a build as copied
        # when the object was never stored.
        if outcome.response.is_success:
            return

        logger.error(
            "Presigned upload failed",
            status_code=outcome.response.status_code,
            response_body=outcome.response.text,
            attempts=outcome.attempts,
            retryable=outcome.retryable,
        )
        outcome.response.raise_for_status()

    async def _generate_upload_url(self, key: str) -> str:
        """Mint a short-lived presigned PUT URL for ``key``."""
        client = self._get_client()
        url: str = await client.generate_presigned_url(
            "put_object",
            Params={"Bucket": self._bucket, "Key": key},
            ExpiresIn=_UPLOAD_URL_EXPIRES_SECONDS,
        )
        return url
