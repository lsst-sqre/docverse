"""Anonymous async S3 client for the public ``lsst-the-docs`` bucket.

The legacy LTD Keeper service uploaded build artefacts to a public-read
S3 bucket; the sync engine needs anonymous (unsigned) access to list
keys under a build prefix and stream object bodies into Docverse R2.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from types import TracebackType
from typing import Protocol, Self, runtime_checkable

import structlog
from aiobotocore.client import AioBaseClient
from aiobotocore.session import AioSession, ClientCreatorContext, get_session
from botocore import UNSIGNED
from botocore.config import Config

__all__ = [
    "LtdS3Source",
    "LtdSourceProtocol",
]

#: Default region for the legacy ``lsst-the-docs`` bucket.
_DEFAULT_REGION = "us-east-1"


@runtime_checkable
class LtdSourceProtocol(Protocol):
    """Read interface that :class:`BuildContentCopier` consumes.

    Defined as a Protocol so the copier accepts both
    :class:`LtdS3Source` and the in-memory test double in
    ``tests/keeper_sync`` without coupling.
    """

    async def list_keys(self, *, prefix: str) -> list[str]:
        """Return every key under ``prefix``."""
        ...

    async def download_object(self, *, key: str) -> bytes:
        """Return the bytes of the object at ``key``."""
        ...


class LtdS3Source:
    """Async S3 source for the public-read ``lsst-the-docs`` bucket.

    Use as an async context manager; the underlying aiobotocore client
    is created on ``__aenter__`` with ``botocore.UNSIGNED`` credentials
    so the source never needs to wire AWS secrets through Docverse.
    """

    def __init__(
        self,
        *,
        bucket: str = "lsst-the-docs",
        region: str = _DEFAULT_REGION,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._bucket = bucket
        self._region = region
        self._logger = logger
        self._session: AioSession = get_session()
        self._client_cm: ClientCreatorContext | None = None
        self._client: AioBaseClient | None = None

    async def __aenter__(self) -> Self:
        """Open the underlying anonymous S3 client on context entry."""
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the underlying S3 client on context exit."""
        await self.close()

    async def open(self) -> None:
        """Open the underlying anonymous S3 client."""
        self._client_cm = self._session.create_client(
            "s3",
            region_name=self._region,
            config=Config(signature_version=UNSIGNED),
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
            msg = "LtdS3Source is not open; use as async context manager"
            raise RuntimeError(msg)
        return self._client

    async def list_keys(self, *, prefix: str) -> list[str]:
        """List every object key under ``prefix`` (paginated)."""
        return [key async for key in self._iter_keys(prefix)]

    async def _iter_keys(self, prefix: str) -> AsyncIterator[str]:
        client = self._get_client()
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(
            Bucket=self._bucket, Prefix=prefix
        ):
            for obj in page.get("Contents", []):
                yield obj["Key"]

    async def download_object(self, *, key: str) -> bytes:
        """Download an object body as bytes."""
        client = self._get_client()
        response = await client.get_object(Bucket=self._bucket, Key=key)
        async with response["Body"] as stream:
            data: bytes = await stream.read()
        return data
