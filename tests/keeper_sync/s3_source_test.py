"""Smoke tests for ``LtdS3Source``.

The full S3 path is tested end-to-end through ``BuildContentCopier``
with an in-memory fake source. These tests just assert that the real
``LtdS3Source`` honours the open/close contract and rejects pre-open
calls so a copier wired with one cannot silently hang.
"""

from __future__ import annotations

import pytest
import structlog

from docverse.keeper_sync.s3_source import LtdS3Source, LtdSourceProtocol


def test_implements_source_protocol() -> None:
    source = LtdS3Source(logger=structlog.get_logger("test"))
    assert isinstance(source, LtdSourceProtocol)


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
