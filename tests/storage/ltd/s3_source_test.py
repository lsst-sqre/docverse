"""Smoke tests for ``LtdS3Source``.

The full S3 path is tested end-to-end through ``BuildContentCopier``
with an in-memory fake source. These tests just assert that the real
``LtdS3Source`` honours the open/close contract, rejects pre-open
calls so a copier wired with one cannot silently hang, and translates
botocore's denial errors into the Docverse-side
``LtdSourceAccessDeniedError`` the keeper-sync edition-prefix fallback
matches on, while leaving the botocore transport errors in
``RETRYABLE_SOURCE_TRANSPORT_ERRORS`` (the ones the keeper-sync
build-level retry re-runs a copy on) as they are. They also pin that an
optional ``max_pool_connections`` sizes the opened client's connection
pool, so the sync worker's one shared source is not throttled at
botocore's default of ten connections.
"""

from __future__ import annotations

from typing import Any

import pytest
import structlog
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    HTTPClientError,
    ProxyConnectionError,
    ReadTimeoutError,
    ResponseStreamingError,
    SSLError,
)

from docverse_server.storage.ltd import (
    RETRYABLE_SOURCE_TRANSPORT_ERRORS,
    LtdS3Source,
    LtdSourceAccessDeniedError,
    LtdSourceProtocol,
)

_ENDPOINT = "https://lsst-the-docs.s3.amazonaws.com/"


def _client_error(code: str, *, operation: str = "GetObject") -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": "boom"},
            "ResponseMetadata": {
                "HTTPStatusCode": 403 if code == "AccessDenied" else 404
            },
        },
        operation,
    )


class _RaisingClient:
    """Stand-in S3 client whose ``get_object`` always raises."""

    def __init__(self, error: BaseException) -> None:
        self._error = error

    async def get_object(self, **_: Any) -> Any:
        raise self._error


def test_implements_source_protocol() -> None:
    source = LtdS3Source(logger=structlog.get_logger("test"))
    assert isinstance(source, LtdSourceProtocol)


@pytest.mark.asyncio
async def test_download_translates_access_denied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A denied ``GetObject`` surfaces as ``LtdSourceAccessDeniedError``.

    The keeper-sync edition-prefix fallback keys off this type, so the
    raw botocore ``ClientError`` must not escape the storage layer.
    """
    source = LtdS3Source(logger=structlog.get_logger("test"))
    client = _RaisingClient(_client_error("AccessDenied"))
    monkeypatch.setattr(source, "_get_client", lambda: client)

    with pytest.raises(LtdSourceAccessDeniedError) as excinfo:
        await source.download_object(key="documenteer/builds/33/index.html")

    assert "documenteer/builds/33/index.html" in str(excinfo.value)
    assert excinfo.value.key == "documenteer/builds/33/index.html"
    assert excinfo.value.bucket == "lsst-the-docs"


@pytest.mark.asyncio
async def test_download_leaves_other_client_errors_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only denials are translated; a missing key stays a ``ClientError``."""
    source = LtdS3Source(logger=structlog.get_logger("test"))
    client = _RaisingClient(_client_error("NoSuchKey"))
    monkeypatch.setattr(source, "_get_client", lambda: client)

    with pytest.raises(ClientError) as excinfo:
        await source.download_object(key="documenteer/builds/33/missing.html")

    assert not isinstance(excinfo.value, LtdSourceAccessDeniedError)


@pytest.mark.parametrize(
    "error",
    [
        EndpointConnectionError(endpoint_url=_ENDPOINT),
        ConnectTimeoutError(endpoint_url=_ENDPOINT),
        ProxyConnectionError(proxy_url="http://proxy.example:3128/"),
        SSLError(endpoint_url=_ENDPOINT, error="handshake failed"),
        ReadTimeoutError(endpoint_url=_ENDPOINT),
        ConnectionClosedError(endpoint_url=_ENDPOINT),
        ResponseStreamingError(error="connection reset by peer"),
        HTTPClientError(error="aiohttp client error"),
    ],
    ids=lambda error: type(error).__name__,
)
def test_retryable_source_transport_errors_cover_transport_failures(
    error: Exception,
) -> None:
    """Every way aiobotocore reports S3 being unreachable is retryable.

    A connect failure or timeout, a read timeout, a connection dropped
    before or during the response body, and aiobotocore's generic
    wrapper for any other aiohttp client error: each says "try again
    later", which is what the keeper-sync build-level retry acts on.
    """
    assert isinstance(error, RETRYABLE_SOURCE_TRANSPORT_ERRORS)


@pytest.mark.parametrize(
    "error",
    [
        _client_error("AccessDenied"),
        _client_error("NoSuchKey"),
        _client_error("SlowDown"),
        LtdSourceAccessDeniedError(bucket="lsst-the-docs", key="a/b.html"),
    ],
    ids=["AccessDenied", "NoSuchKey", "SlowDown", "denied"],
)
def test_retryable_source_transport_errors_exclude_s3_answers(
    error: Exception,
) -> None:
    """An S3 error *response* is not a transport failure.

    ``AccessDenied`` and ``NoSuchKey`` are permanent, and a throttling
    status such as ``SlowDown`` is the source-side analogue of an R2
    ``429``/``5xx``, which the build-level retry deliberately leaves
    alone.
    """
    assert not isinstance(error, RETRYABLE_SOURCE_TRANSPORT_ERRORS)


@pytest.mark.asyncio
async def test_download_leaves_transport_errors_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transport failure escapes ``download_object`` as itself.

    Only ``ClientError`` denials are translated, so the keeper-sync
    build-level retry sees the botocore transport error it matches on.
    """
    source = LtdS3Source(logger=structlog.get_logger("test"))
    error = ReadTimeoutError(endpoint_url=_ENDPOINT)
    client = _RaisingClient(error)
    monkeypatch.setattr(source, "_get_client", lambda: client)

    with pytest.raises(ReadTimeoutError) as excinfo:
        await source.download_object(key="documenteer/builds/33/index.html")

    assert excinfo.value is error


@pytest.mark.asyncio
async def test_pre_open_calls_raise_runtime_error() -> None:
    """Calling ``list_keys`` before ``__aenter__`` is a programmer error."""
    source = LtdS3Source(logger=structlog.get_logger("test"))
    with pytest.raises(RuntimeError, match="async context manager"):
        await source.list_keys(prefix="anything")


@pytest.mark.asyncio
async def test_open_close_roundtrip_does_not_raise() -> None:
    """The async-context lifecycle wires aiobotocore without needing creds."""
    async with LtdS3Source(logger=structlog.get_logger("test")):
        pass


@pytest.mark.asyncio
async def test_max_pool_connections_sizes_the_client_pool() -> None:
    """``max_pool_connections`` reaches the opened client's botocore config.

    The sync worker shares one source across every copier in the
    process, so botocore's default pool of ten connections would
    throttle downloads well below the worker's upload cap. The client
    must stay anonymous while the pool is resized.
    """
    source = LtdS3Source(
        max_pool_connections=3, logger=structlog.get_logger("test")
    )
    async with source:
        client_config = source._get_client().meta.config

    assert client_config.max_pool_connections == 3
    assert client_config.signature_version is UNSIGNED


@pytest.mark.asyncio
async def test_omitted_max_pool_connections_keeps_the_botocore_default() -> (
    None
):
    """Without ``max_pool_connections`` botocore's own pool size stands."""
    source = LtdS3Source(logger=structlog.get_logger("test"))
    async with source:
        client_config = source._get_client().meta.config

    assert client_config.max_pool_connections == (
        Config().max_pool_connections
    )
    assert client_config.signature_version is UNSIGNED
