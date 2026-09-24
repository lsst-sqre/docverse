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
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from typing import Any, cast

import httpx
import pytest
import structlog
from aiobotocore.client import AioBaseClient
from structlog.testing import capture_logs

from docverse_server.storage import _http_retry
from docverse_server.storage._http_retry import MAX_BACKOFF_SECONDS
from docverse_server.storage.objectstore import (
    ObjectStoreError,
    S3ObjectStore,
    _s3,
)

#: A ``MockTransport`` handler: plain, or ``async`` when a test needs the
#: PUT to stay in flight across an ``await`` (to observe concurrency).
_Handler = (
    Callable[[httpx.Request], httpx.Response]
    | Callable[[httpx.Request], Coroutine[None, None, httpx.Response]]
)


def _make_store(
    handler: _Handler,
    *,
    max_attempts: int = 4,
    base_backoff_seconds: float = 0.0,
    max_backoff_seconds: float | None = None,
    upload_limiter: asyncio.Semaphore | None = None,
) -> tuple[S3ObjectStore, httpx.AsyncClient]:
    """Build a store whose presigned PUTs land in ``handler``.

    ``max_backoff_seconds`` is passed on only when given, so a test that
    leaves it out exercises a store built exactly as callers that never
    heard of the knob build one.
    """
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    ceiling: dict[str, float] = (
        {}
        if max_backoff_seconds is None
        else {"max_backoff_seconds": max_backoff_seconds}
    )
    store = S3ObjectStore(
        endpoint_url="https://account.r2.cloudflarestorage.com",
        bucket="docs",
        access_key_id="key-id",
        secret_access_key="secret-key",
        region="auto",
        logger=structlog.get_logger("test"),
        http_client=client,
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
        upload_limiter=upload_limiter,
        **ceiling,
    )
    return store, client


def _record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replace ``asyncio.sleep`` with a recorder and return the log."""
    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    return delays


def _record_sleeps_on_a_clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record sleeps and advance the upload's clocks by each one.

    With ``asyncio.sleep`` stubbed out no real time passes, so both the
    retry loop's and the store's ``elapsed_seconds`` would read zero.
    Driving their clocks from the recorded delays makes the elapsed time
    exactly the backoff spent so far, which a test can assert.
    """
    delays: list[float] = []
    now = [1000.0]

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)
        now[0] += delay

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    monkeypatch.setattr(_http_retry, "monotonic", lambda: now[0])
    monkeypatch.setattr(_s3, "monotonic", lambda: now[0])
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
async def test_upload_object_reports_the_attempts_it_spent() -> None:
    """The store says how many attempts each upload took to land.

    A retry that eventually succeeds leaves no trace but a warning log,
    so this count is what lets the keeper-sync copier tally retried
    objects for its metrics event. It counts the first attempt, so an
    upload that landed first time reports 1.
    """
    responses = [
        httpx.Response(200),
        httpx.Response(503),
        httpx.Response(200),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    store, client = _make_store(handler)
    async with client, store as s:
        first = await s.upload_object(
            key="build/index.html",
            data=b"<html></html>",
            content_type="text/html",
        )
        second = await s.upload_object(
            key="build/app.css",
            data=b"body{}",
            content_type="text/css",
        )

    assert (first, second) == (1, 2)


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
async def test_upload_object_raises_when_retries_are_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A never-clearing `500` still raises, so ``copy_build`` still fails.

    The failure line says how long the object was tried, backoff
    included, as well as how many attempts that took.
    """
    _record_sleeps_on_a_clock(monkeypatch)
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(500, text="internal error")

    with capture_logs() as logs:
        store, client = _make_store(
            handler, max_attempts=3, base_backoff_seconds=0.5
        )
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
    assert errors[0]["elapsed_seconds"] == 1.5
    assert errors[0]["retryable"] is True


@pytest.mark.asyncio
async def test_upload_object_raises_when_transport_retries_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Transport failures share the status path's attempt budget.

    An R2 connect outage surfaces as an ``httpx.ConnectTimeout`` whose
    ``str`` is empty, so every line on the way to giving up has to name
    the failure by its repr and say how long the object has been tried:
    the retry warnings are the only record of a retry that recovers, and
    the "Presigned upload failed" line is the Sentry event for one that
    does not.
    """
    delays = _record_sleeps_on_a_clock(monkeypatch)
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        raise httpx.ConnectTimeout("", request=request)

    with capture_logs() as logs:
        store, client = _make_store(
            handler, max_attempts=3, base_backoff_seconds=0.5
        )
        async with client, store as s:
            with pytest.raises(httpx.ConnectTimeout):
                await s.upload_object(
                    key="build/index.html",
                    data=b"<html></html>",
                    content_type="text/html",
                )

    assert attempts == [1, 2, 3]
    assert delays == [0.5, 1.0]

    retries = [
        entry
        for entry in logs
        if entry["event"] == "Retrying presigned upload after transport error"
    ]
    assert [
        (
            entry["attempt"],
            entry["max_attempts"],
            entry["retry_delay"],
            entry["elapsed_seconds"],
        )
        for entry in retries
    ] == [(1, 3, 0.5, 0.0), (2, 3, 1.0, 0.5)]
    for entry in retries:
        assert entry["key"] == "build/index.html"
        assert entry["error"] == "ConnectTimeout('')"
        assert entry["error_type"] == "ConnectTimeout"

    failures = [
        entry for entry in logs if entry["event"] == "Presigned upload failed"
    ]
    assert len(failures) == 1
    failure = failures[0]
    assert failure["key"] == "build/index.html"
    assert failure["error"] == "ConnectTimeout('')"
    assert failure["error_type"] == "ConnectTimeout"
    assert failure["attempts"] == 3
    assert failure["elapsed_seconds"] == 1.5
    assert failure["retryable"] is True


@pytest.mark.asyncio
async def test_upload_object_rides_out_five_connect_timeouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The keeper-sync budget turns five connect timeouts into a success.

    Six attempts at the shared 0.5 s base backoff is the keeper-sync
    copy path's default budget, which is what lets an object outlast an
    R2 connect outage the shared four-attempt budget gave up inside.
    """
    delays = _record_sleeps(monkeypatch)
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        if len(attempts) <= 5:
            raise httpx.ConnectTimeout("", request=request)
        return httpx.Response(200)

    store, client = _make_store(
        handler,
        max_attempts=6,
        base_backoff_seconds=0.5,
        max_backoff_seconds=30.0,
    )
    async with client, store as s:
        await s.upload_object(
            key="build/index.html",
            data=b"<html></html>",
            content_type="text/html",
        )

    assert attempts == [1, 2, 3, 4, 5, 6]
    assert delays == [0.5, 1.0, 2.0, 4.0, 8.0]


@pytest.mark.asyncio
async def test_upload_object_raises_after_six_connect_timeouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sixth connect timeout exhausts the budget and says so.

    The "Presigned upload failed" line is the Sentry event for an object
    that outlasted its budget, so it has to report the budget actually
    spent — six attempts over the 15.5 s of backoff between them — and
    name the failure even though ``str(httpx.ConnectTimeout())`` is empty.
    """
    delays = _record_sleeps_on_a_clock(monkeypatch)
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        raise httpx.ConnectTimeout("", request=request)

    with capture_logs() as logs:
        store, client = _make_store(
            handler,
            max_attempts=6,
            base_backoff_seconds=0.5,
            max_backoff_seconds=30.0,
        )
        async with client, store as s:
            with pytest.raises(httpx.ConnectTimeout):
                await s.upload_object(
                    key="build/index.html",
                    data=b"<html></html>",
                    content_type="text/html",
                )

    assert attempts == [1, 2, 3, 4, 5, 6]
    assert delays == [0.5, 1.0, 2.0, 4.0, 8.0]

    failures = [
        entry for entry in logs if entry["event"] == "Presigned upload failed"
    ]
    assert len(failures) == 1
    failure = failures[0]
    assert failure["attempts"] == 6
    assert failure["elapsed_seconds"] == 15.5
    assert failure["error"] == "ConnectTimeout('')"


@pytest.mark.asyncio
async def test_upload_object_caps_backoff_at_its_own_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A store given a ceiling clamps its exponential backoff to it.

    Eight attempts at a 0.5 s base would sleep 32 s before the last
    one; a 30 s ceiling caps that sleep at 30 s, and every earlier
    sleep — including the 16 s one the shared 10 s ceiling would have
    clamped — goes through untouched.
    """
    delays = _record_sleeps(monkeypatch)

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("", request=request)

    store, client = _make_store(
        handler,
        max_attempts=8,
        base_backoff_seconds=0.5,
        max_backoff_seconds=30.0,
    )
    async with client, store as s:
        with pytest.raises(httpx.ConnectTimeout):
            await s.upload_object(
                key="build/index.html",
                data=b"<html></html>",
                content_type="text/html",
            )

    assert delays == [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0]


@pytest.mark.asyncio
async def test_upload_object_honours_retry_after_up_to_its_own_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raised ceiling lets a longer ``Retry-After`` through, still capped.

    The ceiling is the one knob that decides how long R2 may ask the
    copy to wait, so a 20 s request is honoured whole under a 30 s
    ceiling, while a pathological 300 s one is still clamped to it.
    """
    delays = _record_sleeps(monkeypatch)
    responses = [
        httpx.Response(503, headers={"Retry-After": "20"}),
        httpx.Response(503, headers={"Retry-After": "300"}),
        httpx.Response(200),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    store, client = _make_store(handler, max_backoff_seconds=30.0)
    async with client, store as s:
        await s.upload_object(
            key="build/index.html",
            data=b"<html></html>",
            content_type="text/html",
        )

    assert delays == [20.0, 30.0]


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
    """A store built without a ceiling keeps the shared one.

    Every object store but the keeper-sync copier's runs on the shared
    ceiling, and those uploads (dashboard renders, build processing) run
    inside jobs whose progress a publish waits on, so a five-minute
    obedient sleep is worse than another attempt. Only a caller that
    asks for a longer ceiling gets one.
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


@pytest.mark.asyncio
async def test_upload_object_never_exceeds_the_upload_limiter() -> None:
    """Concurrent uploads share the limiter's slots for their PUTs.

    The keeper-sync worker hands every copier's store one process-wide
    semaphore so the whole worker never has more presigned PUTs — and so
    connections — in flight than the cap, however many builds copy at
    once. The handler holds each PUT open across a real ``await`` so
    uploads that are not held back by the limiter pile up and show in
    the peak.
    """
    limiter = asyncio.Semaphore(2)
    in_flight = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return httpx.Response(200)

    store, client = _make_store(handler, upload_limiter=limiter)
    async with client, store as s:
        attempts = await asyncio.gather(
            *(
                s.upload_object(
                    key=f"build/page-{index}.html",
                    data=b"<html></html>",
                    content_type="text/html",
                )
                for index in range(6)
            )
        )

    assert attempts == [1] * 6
    assert peak <= 2


@pytest.mark.asyncio
async def test_upload_object_releases_the_slot_while_backing_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retrying upload waits out its backoff without holding a slot.

    A slot held across a backoff sleep would idle one of the worker's
    capped upload connections for as long as R2 is struggling, so a few
    slow objects could starve every other copy in the process. The PUTs
    themselves do hold it, which is what makes the sleep's release the
    thing under test rather than a limiter that was never taken.
    """
    limiter = asyncio.Semaphore(1)
    locked_while_sleeping: list[bool] = []

    async def _fake_sleep(delay: float) -> None:
        locked_while_sleeping.append(limiter.locked())

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    statuses = [503, 200]
    locked_while_putting: list[bool] = []

    def handler(request: httpx.Request) -> httpx.Response:
        locked_while_putting.append(limiter.locked())
        return httpx.Response(statuses[len(locked_while_putting) - 1])

    store, client = _make_store(handler, upload_limiter=limiter)
    async with client, store as s:
        attempts = await s.upload_object(
            key="build/index.html",
            data=b"<html></html>",
            content_type="text/html",
        )

    assert attempts == 2
    assert locked_while_putting == [True, True]
    assert locked_while_sleeping == [False]
    assert not limiter.locked()


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
