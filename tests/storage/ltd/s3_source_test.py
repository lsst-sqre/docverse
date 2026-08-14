"""Smoke tests for ``LtdS3Source``.

The full S3 path is tested end-to-end through ``BuildContentCopier``
with an in-memory fake source. These tests just assert that the real
``LtdS3Source`` honours the open/close contract, rejects pre-open
calls so a copier wired with one cannot silently hang, and translates
botocore's denial errors into the Docverse-side
``LtdSourceAccessDeniedError`` the keeper-sync edition-prefix fallback
matches on.
"""

from __future__ import annotations

from typing import Any

import pytest
import structlog
from botocore.exceptions import ClientError

from docverse_server.storage.ltd import (
    LtdS3Source,
    LtdSourceAccessDeniedError,
    LtdSourceProtocol,
)


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
