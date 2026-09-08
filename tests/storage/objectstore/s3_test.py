"""Tests for ``S3ObjectStore``'s retry policy and bulk prefix delete.

The presigned PUT path is the one production takes (the factory always
hands the store the worker's shared ``httpx.AsyncClient``), so it is the
path that has to survive Cloudflare R2's transient `500`s and dropped
connections. These tests drive it through an ``httpx.MockTransport``;
the aiobotocore client is real but only ever asked to sign URLs, which
is a local computation.

``delete_prefix`` goes the other way — it never touches httpx and does
nothing locally — so its tests swap the aiobotocore client for
:class:`_StubS3Client` and assert on the ``DeleteObjects`` calls the
store issues.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from typing import Any, cast

import httpx
import pytest
import structlog
from aiobotocore.client import AioBaseClient
from structlog.testing import capture_logs

from docverse_server.storage._http_retry import MAX_BACKOFF_SECONDS
from docverse_server.storage.objectstore import ObjectStoreError, S3ObjectStore


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


class _StubS3Client:
    """Stand-in for the aiobotocore S3 client, for ``delete_prefix``.

    Serves ``pages`` from its ``list_objects_v2`` paginator and records
    every ``delete_objects`` call so a test can assert on the batching.
    ``delete_responses`` is consumed one entry per call, so a test can
    make a chosen batch report per-key ``Errors``; once it runs out,
    every further call succeeds silently the way S3 does in quiet mode.
    """

    def __init__(
        self,
        *,
        pages: list[dict[str, Any]],
        delete_responses: list[dict[str, Any]] | None = None,
    ) -> None:
        self.pages = pages
        self.paginator_names: list[str] = []
        self.paginate_kwargs: dict[str, Any] | None = None
        self.delete_calls: list[dict[str, Any]] = []
        self._delete_responses = list(delete_responses or [])

    def get_paginator(self, operation_name: str) -> _StubS3Client:
        self.paginator_names.append(operation_name)
        return self

    def paginate(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        self.paginate_kwargs = kwargs
        return self._iter_pages()

    async def _iter_pages(self) -> AsyncIterator[dict[str, Any]]:
        for page in self.pages:
            yield page

    async def delete_objects(self, **kwargs: Any) -> dict[str, Any]:
        self.delete_calls.append(kwargs)
        if self._delete_responses:
            return self._delete_responses.pop(0)
        return {}

    @property
    def deleted_keys(self) -> list[str]:
        """Every key handed to ``delete_objects``, call order preserved."""
        return [
            obj["Key"]
            for call in self.delete_calls
            for obj in call["Delete"]["Objects"]
        ]

    @property
    def batch_sizes(self) -> list[int]:
        """How many keys each ``delete_objects`` call carried."""
        return [len(call["Delete"]["Objects"]) for call in self.delete_calls]


def _make_store_with_client(client: _StubS3Client) -> S3ObjectStore:
    """Build a store whose S3 calls land in ``client``.

    The store is never opened: ``delete_prefix`` reaches the client
    through ``_get_client``, so injecting one is the whole setup.
    """
    store = S3ObjectStore(
        endpoint_url="https://account.r2.cloudflarestorage.com",
        bucket="docs",
        access_key_id="key-id",
        secret_access_key="secret-key",
        region="auto",
        logger=structlog.get_logger("test"),
    )
    store._client = cast("AioBaseClient", client)
    return store


def _page(keys: Sequence[str]) -> dict[str, Any]:
    """Render one ``list_objects_v2`` page holding ``keys``."""
    return {"Contents": [{"Key": key} for key in keys]}


_PREFIX = "orgs/rubin/projects/docs/builds/01ABCDEF/"


def _keys(count: int) -> list[str]:
    """Build ``count`` distinct keys under the test prefix."""
    return [f"{_PREFIX}file-{index:05d}.html" for index in range(count)]


@pytest.mark.asyncio
async def test_delete_prefix_deletes_in_batches_of_a_thousand() -> None:
    """2500 keys over three pages become 1000 + 1000 + 500 deletes."""
    keys = _keys(2500)
    client = _StubS3Client(
        pages=[
            _page(keys[:1000]),
            _page(keys[1000:2000]),
            _page(keys[2000:]),
        ]
    )
    store = _make_store_with_client(client)

    deleted = await store.delete_prefix(prefix=_PREFIX)

    assert deleted == 2500
    assert client.batch_sizes == [1000, 1000, 500]
    assert client.deleted_keys == keys


@pytest.mark.asyncio
async def test_delete_prefix_batches_across_page_boundaries() -> None:
    """Batches are filled from the key stream, not reset per page.

    S3 chooses its own page size, and R2's need not be 1000. Flushing
    once per page would issue three undersized requests here instead of
    the two full ones the API allows.
    """
    keys = _keys(1800)
    client = _StubS3Client(
        pages=[_page(keys[:600]), _page(keys[600:1200]), _page(keys[1200:])]
    )
    store = _make_store_with_client(client)

    deleted = await store.delete_prefix(prefix=_PREFIX)

    assert deleted == 1800
    assert client.batch_sizes == [1000, 800]


@pytest.mark.asyncio
async def test_delete_prefix_scopes_the_listing_to_bucket_and_prefix() -> None:
    """Only the named bucket and prefix are ever listed."""
    client = _StubS3Client(pages=[_page(_keys(1))])
    store = _make_store_with_client(client)

    await store.delete_prefix(prefix=_PREFIX)

    assert client.paginator_names == ["list_objects_v2"]
    assert client.paginate_kwargs == {"Bucket": "docs", "Prefix": _PREFIX}


@pytest.mark.asyncio
async def test_delete_prefix_returns_zero_when_nothing_matches() -> None:
    """An empty prefix deletes nothing and issues no delete call.

    A build whose staging tarball was already dropped at completion
    reaches the sweep this way, so this is the ordinary path, not an
    error.
    """
    client = _StubS3Client(pages=[{"KeyCount": 0}])
    store = _make_store_with_client(client)

    assert await store.delete_prefix(prefix=_PREFIX) == 0
    assert client.delete_calls == []


@pytest.mark.asyncio
async def test_delete_prefix_raises_when_a_key_fails() -> None:
    """A per-key error is never returned as a shortened success count."""
    keys = _keys(2)
    client = _StubS3Client(
        pages=[_page(keys)],
        delete_responses=[
            {
                "Errors": [
                    {
                        "Key": keys[1],
                        "Code": "AccessDenied",
                        "Message": "Access Denied",
                    }
                ]
            }
        ],
    )
    store = _make_store_with_client(client)

    with pytest.raises(ObjectStoreError) as excinfo:
        await store.delete_prefix(prefix=_PREFIX)

    assert excinfo.value.failure_count == 1
    message = str(excinfo.value)
    assert f"{keys[1]} (AccessDenied)" in message
    assert "DeleteObjects" in message
    assert f"s3://docs/{_PREFIX}" in message


@pytest.mark.asyncio
async def test_delete_prefix_stops_at_the_first_failing_batch() -> None:
    """The raise aborts the sweep rather than grinding on half-deleted."""
    keys = _keys(1500)
    client = _StubS3Client(
        pages=[_page(keys)],
        delete_responses=[
            {"Errors": [{"Key": keys[0], "Code": "InternalError"}]}
        ],
    )
    store = _make_store_with_client(client)

    with pytest.raises(ObjectStoreError):
        await store.delete_prefix(prefix=_PREFIX)

    assert client.batch_sizes == [1000]


@pytest.mark.asyncio
async def test_delete_prefix_refuses_a_blank_prefix() -> None:
    """A blank prefix never even reaches the listing."""
    client = _StubS3Client(pages=[_page(_keys(3))])
    store = _make_store_with_client(client)

    with pytest.raises(ValueError, match="empty prefix"):
        await store.delete_prefix(prefix="  ")

    assert client.paginate_kwargs is None
    assert client.delete_calls == []
