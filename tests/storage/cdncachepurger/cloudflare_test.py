"""Tests for the CloudflareCachePurger."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

from docverse_server.storage.cdncachepurger import (
    CloudflareCachePurgeError,
    CloudflareCachePurger,
)


def _make_purger(
    handler: httpx.MockTransport,
    *,
    zone_id: str = "zone-123",
    api_token: str = "token-789",
    max_attempts: int = 4,
) -> tuple[CloudflareCachePurger, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=handler)
    purger = CloudflareCachePurger(
        zone_id=zone_id,
        api_token=api_token,
        http_client=client,
        logger=structlog.get_logger("test"),
        max_attempts=max_attempts,
        base_backoff_seconds=0.0,
    )
    return purger, client


def _record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replace ``asyncio.sleep`` with a recorder and return the log.

    The purger's backoff is the behaviour under test, so the delays it
    asks for are asserted directly rather than waited out.
    """
    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    return delays


@pytest.mark.asyncio
async def test_purge_hostname_posts_to_zone_purge_endpoint() -> None:
    seen: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["request"] = request
        return httpx.Response(200, json={"success": True})

    purger, client = _make_purger(httpx.MockTransport(handler))
    async with client, purger as p:
        await p.purge_hostname("myproject.example.org")

    request = seen["request"]
    assert request.method == "POST"
    assert str(request.url) == (
        "https://api.cloudflare.com/client/v4/zones/zone-123/purge_cache"
    )
    assert request.headers["Authorization"] == "Bearer token-789"
    assert json.loads(request.content) == {"hosts": ["myproject.example.org"]}


@pytest.mark.asyncio
async def test_purge_hostname_retries_429_then_succeeds() -> None:
    """A rate-limited purge is retried and reported as a success."""
    statuses = [429, 429, 200]
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        status = statuses[len(attempts) - 1]
        if status == 200:
            return httpx.Response(200, json={"success": True})
        return httpx.Response(
            status,
            json={
                "success": False,
                "errors": [
                    {"code": 1134, "message": "Unable to purge, rate limit"}
                ],
            },
        )

    purger, client = _make_purger(httpx.MockTransport(handler))
    async with client, purger as p:
        await p.purge_hostname("myproject.example.org")

    assert attempts == [1, 2, 3]


@pytest.mark.asyncio
async def test_purge_hostname_exhausts_retries_on_persistent_429() -> None:
    """A never-clearing 429 stops at the attempt budget and reports 1134."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(
            429,
            json={
                "success": False,
                "errors": [
                    {
                        "code": 1134,
                        "message": "Unable to purge, rate limit reached",
                    }
                ],
            },
        )

    with capture_logs() as logs:
        purger, client = _make_purger(
            httpx.MockTransport(handler), max_attempts=3
        )
        async with client, purger as p:
            with pytest.raises(CloudflareCachePurgeError) as excinfo:
                await p.purge_hostname("myproject.example.org")

    assert attempts == [1, 2, 3]
    assert excinfo.value.status_code == 429
    assert excinfo.value.error_codes == [1134]
    assert excinfo.value.attempts == 3

    errors = [entry for entry in logs if entry["log_level"] == "error"]
    assert len(errors) == 1
    assert errors[0]["cloudflare_error_codes"] == [1134]
    assert errors[0]["attempts"] == 3


@pytest.mark.asyncio
async def test_purge_hostname_raises_on_4xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"errors": ["forbidden"]})

    purger, client = _make_purger(httpx.MockTransport(handler))
    async with client, purger as p:
        with pytest.raises(CloudflareCachePurgeError):
            await p.purge_hostname("myproject.example.org")


@pytest.mark.asyncio
async def test_purge_hostname_honours_numeric_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A numeric ``Retry-After`` wins over the computed backoff."""
    delays = _record_sleeps(monkeypatch)
    responses = [
        httpx.Response(429, headers={"Retry-After": "7"}),
        httpx.Response(200, json={"success": True}),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    purger, client = _make_purger(httpx.MockTransport(handler))
    async with client, purger as p:
        await p.purge_hostname("myproject.example.org")

    assert delays == [7.0]


@pytest.mark.asyncio
async def test_purge_hostname_warns_on_non_numeric_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-numeric ``Retry-After`` warns and falls back to backoff."""
    delays = _record_sleeps(monkeypatch)
    responses = [
        httpx.Response(
            429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}
        ),
        httpx.Response(200, json={"success": True}),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    with capture_logs() as logs:
        purger, client = _make_purger(httpx.MockTransport(handler))
        async with client, purger as p:
            await p.purge_hostname("myproject.example.org")

    # base_backoff_seconds is 0.0 in tests, so the fallback is the
    # computed backoff rather than the seven-day-in-the-future date.
    assert delays == [0.0]
    warnings = [
        entry
        for entry in logs
        if entry["event"] == "Ignoring non-numeric Retry-After"
    ]
    assert len(warnings) == 1
    assert warnings[0]["log_level"] == "warning"


@pytest.mark.asyncio
async def test_purge_hostname_does_not_retry_non_retryable_status() -> None:
    """A 403 fails on the first attempt — a retry cannot fix a bad token."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(
            403,
            json={
                "success": False,
                "errors": [{"code": 9109, "message": "Invalid access token"}],
            },
        )

    purger, client = _make_purger(httpx.MockTransport(handler))
    async with client, purger as p:
        with pytest.raises(CloudflareCachePurgeError) as excinfo:
            await p.purge_hostname("myproject.example.org")

    assert attempts == [1]
    assert excinfo.value.attempts == 1
    assert excinfo.value.error_codes == [9109]


@pytest.mark.asyncio
async def test_purge_hostname_raises_on_5xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"errors": ["boom"]})

    purger, client = _make_purger(httpx.MockTransport(handler))
    async with client, purger as p:
        with pytest.raises(CloudflareCachePurgeError):
            await p.purge_hostname("myproject.example.org")


@pytest.mark.asyncio
async def test_purge_hostname_logs_error_with_context() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"errors": ["boom"]})

    with capture_logs() as logs:
        purger, client = _make_purger(httpx.MockTransport(handler))
        async with client, purger as p:
            with pytest.raises(CloudflareCachePurgeError):
                await p.purge_hostname("myproject.example.org")

    errors = [entry for entry in logs if entry["log_level"] == "error"]
    assert len(errors) == 1
    assert errors[0]["hostname"] == "myproject.example.org"
    assert errors[0]["zone_id"] == "zone-123"
    assert errors[0]["status_code"] == 500
    assert "boom" in errors[0]["response_body"]


def test_purge_error_to_sentry_tags_status_and_error_code() -> None:
    """The override tags the status and Cloudflare's own error code."""
    exc = CloudflareCachePurgeError(
        hostname="myproject.example.org",
        zone_id="zone-123",
        status_code=429,
        error_codes=[1134],
        error_messages=["Unable to purge, rate limit reached"],
        attempts=4,
    )

    info = exc.to_sentry()

    assert info.tags == {
        "cdn_status_code": "429",
        "cloudflare_error_code": "1134",
    }
    assert info.contexts["cloudflare_purge"] == {
        "hostname": "myproject.example.org",
        "zone_id": "zone-123",
        "status_code": 429,
        "error_codes": [1134],
        "error_messages": ["Unable to purge, rate limit reached"],
        "attempts": 4,
    }


def test_purge_error_to_sentry_omits_unknown_error_code() -> None:
    """An unparseable body leaves the error-code tag off entirely.

    A literal ``"None"`` (or an empty string) tag value is worse than an
    absent one: it would collect every Cloudflare failure whose body was
    HTML into a single meaningless Sentry facet.
    """
    exc = CloudflareCachePurgeError(
        hostname="myproject.example.org",
        zone_id="zone-123",
        status_code=502,
    )

    info = exc.to_sentry()

    assert info.tags == {"cdn_status_code": "502"}
    assert info.contexts["cloudflare_purge"]["error_codes"] == []
