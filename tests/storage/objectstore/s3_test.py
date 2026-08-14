"""Tests for ``S3ObjectStore``'s presigned-upload retry policy.

The presigned PUT path is the one production takes (the factory always
hands the store the worker's shared ``httpx.AsyncClient``), so it is the
path that has to survive Cloudflare R2's transient `500`s and dropped
connections. These tests drive it through an ``httpx.MockTransport``;
the aiobotocore client is real but only ever asked to sign URLs, which
is a local computation.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

from docverse_server.storage._http_retry import MAX_BACKOFF_SECONDS
from docverse_server.storage.objectstore import S3ObjectStore


def _make_store(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    max_attempts: int = 4,
) -> tuple[S3ObjectStore, httpx.AsyncClient]:
    """Build a store whose presigned PUTs land in ``handler``."""
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    store = S3ObjectStore(
        endpoint_url="https://account.r2.cloudflarestorage.com",
        bucket="docs",
        access_key_id="key-id",
        secret_access_key="secret-key",
        region="auto",
        logger=structlog.get_logger("test"),
        http_client=client,
        max_attempts=max_attempts,
        base_backoff_seconds=0.0,
    )
    return store, client


def _record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replace ``asyncio.sleep`` with a recorder and return the log."""
    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    return delays


@pytest.mark.asyncio
async def test_upload_object_retries_500_then_succeeds() -> None:
    """An R2 `500` is retried rather than killing the upload."""
    statuses = [500, 500, 200]
    seen: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.content)
        return httpx.Response(statuses[len(seen) - 1])

    store, client = _make_store(handler)
    async with client, store as s:
        await s.upload_object(
            key="build/index.html",
            data=b"<html></html>",
            content_type="text/html",
        )

    assert seen == [b"<html></html>"] * 3


@pytest.mark.asyncio
async def test_upload_object_retries_read_timeout_then_succeeds() -> None:
    """A bare ``ReadTimeout`` is retried, not propagated."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(200)

    store, client = _make_store(handler)
    async with client, store as s:
        await s.upload_object(
            key="build/index.html",
            data=b"<html></html>",
            content_type="text/html",
        )

    assert attempts == [1, 2]


@pytest.mark.asyncio
async def test_upload_object_retries_write_error_then_succeeds() -> None:
    """A connection reset while sending the body is retried.

    A TCP reset mid-PUT arrives as ``WriteError``, not ``ConnectError``,
    so this is the failure mode the upload retry mainly exists for.
    """
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1:
            raise httpx.WriteError("broken pipe")
        return httpx.Response(200)

    store, client = _make_store(handler)
    async with client, store as s:
        await s.upload_object(
            key="build/index.html",
            data=b"<html></html>",
            content_type="text/html",
        )

    assert attempts == [1, 2]


@pytest.mark.asyncio
async def test_upload_object_retries_read_error_then_succeeds() -> None:
    """A reset while reading the response is retried, not swallowed."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1:
            raise httpx.ReadError("connection reset by peer")
        return httpx.Response(200)

    store, client = _make_store(handler)
    async with client, store as s:
        await s.upload_object(
            key="build/index.html",
            data=b"<html></html>",
            content_type="text/html",
        )

    assert attempts == [1, 2]


@pytest.mark.asyncio
async def test_upload_object_raises_on_redirect_response() -> None:
    """A 3xx is a failed upload, not a quiet success.

    R2 and S3 answer a wrong-region or wrong-endpoint PUT with a 301
    ``PermanentRedirect``, and a proxy in front of them can answer with
    a 307. Redirects are not followed on this path, so the bytes were
    never stored — treating the response as "not an error" would report
    a build as copied while the object is missing.
    """
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(
            301,
            headers={"Location": "https://elsewhere.example.com/"},
            text="PermanentRedirect",
        )

    store, client = _make_store(handler)
    async with client, store as s:
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            await s.upload_object(
                key="build/index.html",
                data=b"<html></html>",
                content_type="text/html",
            )

    assert attempts == [1]
    assert excinfo.value.response.status_code == 301


@pytest.mark.asyncio
async def test_upload_object_does_not_retry_403() -> None:
    """An expired or malformed signature fails without burning attempts."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(403, text="SignatureDoesNotMatch")

    store, client = _make_store(handler)
    async with client, store as s:
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            await s.upload_object(
                key="build/index.html",
                data=b"<html></html>",
                content_type="text/html",
            )

    assert attempts == [1]
    assert excinfo.value.response.status_code == 403


@pytest.mark.asyncio
async def test_upload_object_raises_when_retries_are_exhausted() -> None:
    """A never-clearing `500` still raises, so ``copy_build`` still fails."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(500, text="internal error")

    with capture_logs() as logs:
        store, client = _make_store(handler, max_attempts=3)
        async with client, store as s:
            with pytest.raises(httpx.HTTPStatusError) as excinfo:
                await s.upload_object(
                    key="build/index.html",
                    data=b"<html></html>",
                    content_type="text/html",
                )

    assert attempts == [1, 2, 3]
    assert excinfo.value.response.status_code == 500

    errors = [entry for entry in logs if entry["log_level"] == "error"]
    assert len(errors) == 1
    assert errors[0]["key"] == "build/index.html"
    assert errors[0]["attempts"] == 3
    assert errors[0]["retryable"] is True


@pytest.mark.asyncio
async def test_upload_object_raises_when_transport_retries_exhausted() -> None:
    """Transport failures share the status path's attempt budget."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        raise httpx.ConnectError("connection refused", request=request)

    store, client = _make_store(handler, max_attempts=3)
    async with client, store as s:
        with pytest.raises(httpx.ConnectError):
            await s.upload_object(
                key="build/index.html",
                data=b"<html></html>",
                content_type="text/html",
            )

    assert attempts == [1, 2, 3]


@pytest.mark.asyncio
async def test_upload_object_honours_numeric_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A numeric ``Retry-After`` wins over the computed backoff."""
    delays = _record_sleeps(monkeypatch)
    responses = [
        httpx.Response(503, headers={"Retry-After": "5"}),
        httpx.Response(200),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    store, client = _make_store(handler)
    async with client, store as s:
        await s.upload_object(
            key="build/index.html",
            data=b"<html></html>",
            content_type="text/html",
        )

    assert delays == [5.0]


@pytest.mark.asyncio
async def test_upload_object_caps_long_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The store keeps the tight shared ceiling on ``Retry-After``.

    Uploads run inside a build-copy job whose progress the rest of the
    publish waits on, so a five-minute obedient sleep is worse than
    another attempt — only ``LtdClient`` raises its ceiling.
    """
    delays = _record_sleeps(monkeypatch)
    responses = [
        httpx.Response(503, headers={"Retry-After": "300"}),
        httpx.Response(200),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    store, client = _make_store(handler)
    async with client, store as s:
        await s.upload_object(
            key="build/index.html",
            data=b"<html></html>",
            content_type="text/html",
        )

    assert delays == [MAX_BACKOFF_SECONDS]


@pytest.mark.asyncio
async def test_upload_object_resigns_url_for_every_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retries never replay a signature that may have expired."""
    urls: list[str] = []
    statuses = [500, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        urls.append(str(request.url))
        return httpx.Response(statuses[len(urls) - 1])

    store, client = _make_store(handler)
    signed_keys: list[str] = []
    sign = store._generate_upload_url

    async def _counting_sign(key: str) -> str:
        signed_keys.append(key)
        return await sign(key)

    monkeypatch.setattr(store, "_generate_upload_url", _counting_sign)

    async with client, store as s:
        await s.upload_object(
            key="build/index.html",
            data=b"<html></html>",
            content_type="text/html",
        )

    # Two URLs signed within the same second are byte-identical, so the
    # signing call count — not the URL text — is what proves the retry
    # minted a fresh signature instead of replaying a stale one.
    assert signed_keys == ["build/index.html", "build/index.html"]
    assert len(urls) == 2
    assert all("X-Amz-Signature=" in url for url in urls)
