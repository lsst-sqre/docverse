"""Tests for ``create_objectstore``'s upload retry budget.

The keeper-sync worker is the one caller that asks for a larger
presigned-upload budget than the shared ``_http_retry`` defaults, and
it asks through this factory, so the factory has to hand both knobs to
the store it builds and leave every other caller's store exactly as it
was. The tests drive a real upload through an ``httpx.MockTransport``
that keeps answering ``503`` with a ``Retry-After`` longer than the
shared ceiling: the attempt count shows ``max_attempts`` arrived and the
recorded sleeps show ``max_backoff_seconds`` did.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
import structlog

from docverse_server.storage._http_retry import (
    DEFAULT_MAX_ATTEMPTS,
    MAX_BACKOFF_SECONDS,
)
from docverse_server.storage.objectstore import create_objectstore

_CONFIG = {"endpoint_url": "https://minio.example.com", "bucket": "docs"}
_CREDENTIALS = {"access_key_id": "key-id", "secret_access_key": "secret"}


def _record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replace ``asyncio.sleep`` with a recorder and return the log."""
    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    return delays


def _always_throttled(attempts: list[int]) -> httpx.MockTransport:
    """Answer every PUT with a ``503`` asking for a 30 s wait."""

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(503, headers={"Retry-After": "30"})

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_create_objectstore_threads_the_upload_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both budget knobs reach the store's presigned upload."""
    delays = _record_sleeps(monkeypatch)
    attempts: list[int] = []

    async with httpx.AsyncClient(
        transport=_always_throttled(attempts)
    ) as http_client:
        store = create_objectstore(
            provider="minio",
            config=_CONFIG,
            credentials=_CREDENTIALS,
            logger=structlog.get_logger("test"),
            http_client=http_client,
            max_attempts=3,
            max_backoff_seconds=25.0,
        )
        async with store:
            with pytest.raises(httpx.HTTPStatusError):
                await store.upload_object(
                    key="build/index.html",
                    data=b"<html></html>",
                    content_type="text/html",
                )

    assert attempts == [1, 2, 3]
    assert delays == [25.0, 25.0]


@pytest.mark.asyncio
async def test_create_objectstore_defaults_to_the_shared_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller that passes no budget gets the store it always got.

    The API process and every worker path but the keeper-sync copier
    build their stores this way, and they keep the shared four attempts
    and 10 s ceiling.
    """
    delays = _record_sleeps(monkeypatch)
    attempts: list[int] = []

    async with httpx.AsyncClient(
        transport=_always_throttled(attempts)
    ) as http_client:
        store = create_objectstore(
            provider="minio",
            config=_CONFIG,
            credentials=_CREDENTIALS,
            logger=structlog.get_logger("test"),
            http_client=http_client,
        )
        async with store:
            with pytest.raises(httpx.HTTPStatusError):
                await store.upload_object(
                    key="build/index.html",
                    data=b"<html></html>",
                    content_type="text/html",
                )

    assert len(attempts) == DEFAULT_MAX_ATTEMPTS
    assert delays == [MAX_BACKOFF_SECONDS] * (DEFAULT_MAX_ATTEMPTS - 1)
