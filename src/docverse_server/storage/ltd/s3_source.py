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
from botocore.exceptions import ClientError

from docverse_server.exceptions import DocverseSlackException

__all__ = [
    "LtdS3Source",
    "LtdSourceAccessDeniedError",
    "LtdSourceProtocol",
]

#: Default region for the legacy ``lsst-the-docs`` bucket.
_DEFAULT_REGION = "us-east-1"

#: S3 error codes that mean "the anonymous principal may not read this",
#: as opposed to "this key does not exist". LTD's earliest uploads carry
#: no public-read object ACL, so every ``GetObject`` under those build
#: prefixes answers with one of these.
_DENIAL_ERROR_CODES = frozenset(
    {"AccessDenied", "AccessDeniedException", "AllAccessDisabled", "403"}
)


class LtdSourceAccessDeniedError(DocverseSlackException):
    """The anonymous LTD S3 principal may not read a bucket key.

    LTD's oldest uploads were written without a public-read object ACL,
    so :class:`LtdS3Source` — which is deliberately ``UNSIGNED`` and has
    no AWS credentials to fall back on — is denied every object under
    those build prefixes. keeper-sync recovers by re-reading the
    edition's published copy (``<product>/v/<edition-slug>/``, which
    *is* public), and it keys that recovery off this exception type: the
    denial has to be distinguishable from an empty prefix or a missing
    key, and botocore's raw ``ClientError`` carries that distinction
    only inside a response dict.

    No ``to_sentry`` override: the rendered message already names the
    bucket, key, and S3 operation, which is the whole triage story.
    Mirrors :class:`~docverse_server.domain.slug.InvalidSlugError`.
    """

    def __init__(
        self,
        *,
        bucket: str | None = None,
        key: str | None = None,
        operation: str | None = None,
        message: str | None = None,
    ) -> None:
        self.bucket = bucket
        self.key = key
        self.operation = operation
        super().__init__(
            message
            if message is not None
            else self._format_message(
                bucket=bucket, key=key, operation=operation
            )
        )

    @staticmethod
    def _format_message(
        *, bucket: str | None, key: str | None, operation: str | None
    ) -> str:
        target = f"s3://{bucket}/{key}" if bucket and key else (key or bucket)
        located = f" for {target}" if target else ""
        called = f" {operation}" if operation else ""
        return (
            f"Anonymous LTD S3 access denied{called}{located};"
            " the object carries no public-read ACL"
        )


def _is_access_denied(exc: ClientError) -> bool:
    """Report whether ``exc`` is a denial rather than another S3 fault."""
    response = exc.response if isinstance(exc.response, dict) else {}
    error = response.get("Error") or {}
    if str(error.get("Code", "")) in _DENIAL_ERROR_CODES:
        return True
    metadata = response.get("ResponseMetadata") or {}
    return metadata.get("HTTPStatusCode") == 403


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
        """List every object key under ``prefix`` (paginated).

        Raises
        ------
        LtdSourceAccessDeniedError
            If the anonymous principal may not list ``prefix``.
        """
        try:
            return [key async for key in self._iter_keys(prefix)]
        except ClientError as exc:
            denial = _denial_for(
                exc, bucket=self._bucket, key=prefix, operation="ListObjectsV2"
            )
            if denial is None:
                raise
            raise denial from exc

    async def _iter_keys(self, prefix: str) -> AsyncIterator[str]:
        client = self._get_client()
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(
            Bucket=self._bucket, Prefix=prefix
        ):
            for obj in page.get("Contents", []):
                yield obj["Key"]

    async def download_object(self, *, key: str) -> bytes:
        """Download an object body as bytes.

        Raises
        ------
        LtdSourceAccessDeniedError
            If the anonymous principal may not read ``key``.
        """
        client = self._get_client()
        try:
            response = await client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            denial = _denial_for(
                exc, bucket=self._bucket, key=key, operation="GetObject"
            )
            if denial is None:
                raise
            raise denial from exc
        async with response["Body"] as stream:
            data: bytes = await stream.read()
        return data


def _denial_for(
    exc: ClientError, *, bucket: str, key: str, operation: str
) -> LtdSourceAccessDeniedError | None:
    """Return the denial exception for ``exc``, or ``None`` if it is not one.

    Only denials are translated: a ``NoSuchKey`` or a throttling error
    has to keep its botocore identity so the keeper-sync fallback does
    not mistake "this prefix is empty" or "S3 is unhappy right now" for
    "this content is unreadable and lives at the edition prefix".
    """
    if not _is_access_denied(exc):
        return None
    return LtdSourceAccessDeniedError(
        bucket=bucket, key=key, operation=operation
    )
