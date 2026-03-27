"""S3-compatible object store implementation using aiobotocore."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import httpx
import structlog
from aiobotocore.client import AioBaseClient
from aiobotocore.session import AioSession, ClientCreatorContext, get_session
from botocore.config import Config

__all__ = ["S3ObjectStore"]


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
    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        endpoint_url: str | None,
        bucket: str,
        access_key_id: str,
        secret_access_key: str,
        region: str = "",
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._endpoint_url = endpoint_url
        self._bucket = bucket
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._region = region
        self._http_client = http_client
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

        Falls back to direct put_object when no http_client is set.
        """
        if self._http_client is not None:
            client = self._get_client()
            url: str = await client.generate_presigned_url(
                "put_object",
                Params={"Bucket": self._bucket, "Key": key},
                ExpiresIn=900,
            )
            response = await self._http_client.put(
                url,
                content=data,
                headers={"Content-Type": content_type},
            )
            if response.is_error:
                logger = structlog.get_logger("docverse.storage.objectstore")
                logger.error(
                    "Presigned upload failed",
                    status_code=response.status_code,
                    response_body=response.text,
                    key=key,
                )
            response.raise_for_status()
        else:
            client = self._get_client()
            await client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )
