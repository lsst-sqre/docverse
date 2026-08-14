"""Tests for the CloudflareKvEditionPublisher."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

from docverse_server.storage._http_retry import MAX_BACKOFF_SECONDS
from docverse_server.storage.editionpublisher import (
    CloudflareKvEditionPublisher,
)


def _make_publisher(
    handler: httpx.MockTransport,
    *,
    account_id: str = "acct-123",
    namespace_id: str = "ns-456",
    api_token: str = "token-789",
    max_attempts: int = 4,
) -> tuple[CloudflareKvEditionPublisher, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=handler)
    publisher = CloudflareKvEditionPublisher(
        account_id=account_id,
        namespace_id=namespace_id,
        api_token=api_token,
        http_client=client,
        logger=structlog.get_logger("test"),
        max_attempts=max_attempts,
        base_backoff_seconds=0.0,
    )
    return publisher, client


def _record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replace ``asyncio.sleep`` with a recorder and return the log."""
    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    return delays


@pytest.mark.asyncio
async def test_publish_issues_put_to_kv_endpoint() -> None:
    seen: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["request"] = request
        return httpx.Response(200, json={"success": True})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )

    request = seen["request"]
    assert request.method == "PUT"
    assert str(request.url) == (
        "https://api.cloudflare.com/client/v4"
        "/accounts/acct-123"
        "/storage/kv/namespaces/ns-456"
        "/values/myproject/main"
    )
    assert request.headers["Authorization"] == "Bearer token-789"
    assert json.loads(request.content) == {
        "build_id": "ABC123",
        "r2_prefix": "myproject/__builds/ABC123/",
        "cache_profile": "long",
    }


@pytest.mark.asyncio
async def test_publish_writes_short_cache_profile() -> None:
    seen: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["request"] = request
        return httpx.Response(200, json={"success": True})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="tickets-dm-1",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="short",
        )

    assert json.loads(seen["request"].content) == {
        "build_id": "ABC123",
        "r2_prefix": "myproject/__builds/ABC123/",
        "cache_profile": "short",
    }


@pytest.mark.asyncio
async def test_publish_retries_429_then_succeeds() -> None:
    """A rate-limited pointer write is retried, not failed.

    The KV write is on the critical path of every publish and shares
    Cloudflare's API rate limits with the zone purge, so a single 429
    used to abandon an otherwise healthy publish.
    """
    statuses = [429, 429, 200]
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(
            statuses[len(attempts) - 1], json={"success": True}
        )

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )

    assert attempts == [1, 2, 3]


@pytest.mark.asyncio
async def test_publish_honours_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cloudflare's own ``Retry-After`` sets the wait, up to the cap."""
    delays = _record_sleeps(monkeypatch)
    responses = [
        httpx.Response(429, headers={"Retry-After": "4"}),
        httpx.Response(200, json={"success": True}),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )

    assert delays == [4.0]


@pytest.mark.asyncio
async def test_publish_caps_long_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The publisher keeps the tight shared ceiling on ``Retry-After``.

    The pointer write happens inside the publish job, which holds an
    open transaction and the CDN purge coalescer's per-hostname lock, so
    an obedient five-minute sleep would serialize a publish burst.
    """
    delays = _record_sleeps(monkeypatch)
    responses = [
        httpx.Response(429, headers={"Retry-After": "300"}),
        httpx.Response(200, json={"success": True}),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )

    assert delays == [MAX_BACKOFF_SECONDS]


@pytest.mark.asyncio
async def test_publish_exhausts_retries_on_persistent_5xx() -> None:
    """A never-clearing 5xx still fails the publish once the budget ends."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(503, json={"errors": ["boom"]})

    with capture_logs() as logs:
        publisher, client = _make_publisher(
            httpx.MockTransport(handler), max_attempts=3
        )
        async with client, publisher as pub:
            with pytest.raises(httpx.HTTPStatusError) as excinfo:
                await pub.publish(
                    project_slug="p",
                    edition_slug="e",
                    build_public_id="B",
                    object_key_prefix="p/__builds/B/",
                    cache_profile="long",
                )

    assert attempts == [1, 2, 3]
    assert excinfo.value.response.status_code == 503

    errors = [entry for entry in logs if entry["log_level"] == "error"]
    assert len(errors) == 1
    assert errors[0]["attempts"] == 3


@pytest.mark.asyncio
async def test_publish_does_not_retry_4xx() -> None:
    """A 404 namespace or a bad token fails on the first attempt."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(404, json={"errors": ["not found"]})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(httpx.HTTPStatusError):
            await pub.publish(
                project_slug="p",
                edition_slug="e",
                build_public_id="B",
                object_key_prefix="p/__builds/B/",
                cache_profile="long",
            )

    assert attempts == [1]


@pytest.mark.asyncio
async def test_publish_raises_on_4xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"errors": ["not found"]})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(httpx.HTTPStatusError):
            await pub.publish(
                project_slug="p",
                edition_slug="e",
                build_public_id="B",
                object_key_prefix="p/__builds/B/",
                cache_profile="long",
            )


@pytest.mark.asyncio
async def test_publish_raises_on_5xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"errors": ["boom"]})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(httpx.HTTPStatusError):
            await pub.publish(
                project_slug="p",
                edition_slug="e",
                build_public_id="B",
                object_key_prefix="p/__builds/B/",
                cache_profile="long",
            )


@pytest.mark.asyncio
async def test_publish_reports_redirect_response() -> None:
    """A 3xx publish failure reaches the log with its response context.

    The pointer write is not followed on this client, so a 302 from a
    proxy or a moved Cloudflare endpoint means the KV entry was never
    written. Gating the diagnostic on ``is_error`` (4xx/5xx only) left
    that failure with a bare ``HTTPStatusError`` and no status or body
    to triage from.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            302,
            headers={"Location": "https://login.example.com/"},
            text="<html>login</html>",
        )

    with capture_logs() as logs:
        publisher, client = _make_publisher(httpx.MockTransport(handler))
        async with client, publisher as pub:
            with pytest.raises(httpx.HTTPStatusError):
                await pub.publish(
                    project_slug="p",
                    edition_slug="e",
                    build_public_id="B",
                    object_key_prefix="p/__builds/B/",
                    cache_profile="long",
                )

    errors = [
        entry
        for entry in logs
        if entry["event"] == "Cloudflare KV publish failed"
    ]
    assert len(errors) == 1
    assert errors[0]["status_code"] == 302
    assert errors[0]["response_body"] == "<html>login</html>"


@pytest.mark.asyncio
async def test_unpublish_issues_delete_to_kv_endpoint() -> None:
    seen: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["request"] = request
        return httpx.Response(200, json={"success": True})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.unpublish(
            project_slug="myproject",
            edition_slug="main",
        )

    request = seen["request"]
    assert request.method == "DELETE"
    assert str(request.url) == (
        "https://api.cloudflare.com/client/v4"
        "/accounts/acct-123"
        "/storage/kv/namespaces/ns-456"
        "/values/myproject/main"
    )
    assert request.headers["Authorization"] == "Bearer token-789"


@pytest.mark.asyncio
async def test_unpublish_treats_404_as_success() -> None:
    """A missing KV key must not raise — unpublish is idempotent."""
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(404, json={"errors": ["not found"]})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        # Should not raise.
        await pub.unpublish(project_slug="p", edition_slug="e")
    assert call_count["n"] == 1


@pytest.mark.asyncio
async def test_unpublish_reports_redirect_response() -> None:
    """A 3xx unpublish failure reaches the log with its response context.

    Only the 404 above is a successful no-op; a redirect means the
    delete never reached the namespace, so it has to be as loud as a
    5xx rather than an unexplained ``HTTPStatusError``.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            307,
            headers={"Location": "https://login.example.com/"},
            text="<html>login</html>",
        )

    with capture_logs() as logs:
        publisher, client = _make_publisher(httpx.MockTransport(handler))
        async with client, publisher as pub:
            with pytest.raises(httpx.HTTPStatusError):
                await pub.unpublish(project_slug="p", edition_slug="e")

    errors = [
        entry
        for entry in logs
        if entry["event"] == "Cloudflare KV unpublish failed"
    ]
    assert len(errors) == 1
    assert errors[0]["status_code"] == 307
    assert errors[0]["response_body"] == "<html>login</html>"


@pytest.mark.asyncio
async def test_unpublish_raises_on_5xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"errors": ["boom"]})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(httpx.HTTPStatusError):
            await pub.unpublish(project_slug="p", edition_slug="e")
