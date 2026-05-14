"""Tests for the CloudflareKvEditionPublisher."""

from __future__ import annotations

import json

import httpx
import pytest
import structlog

from docverse.storage.editionpublisher import CloudflareKvEditionPublisher


def _make_publisher(
    handler: httpx.MockTransport,
    *,
    account_id: str = "acct-123",
    namespace_id: str = "ns-456",
    api_token: str = "token-789",  # noqa: S107
) -> tuple[CloudflareKvEditionPublisher, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=handler)
    publisher = CloudflareKvEditionPublisher(
        account_id=account_id,
        namespace_id=namespace_id,
        api_token=api_token,
        http_client=client,
        logger=structlog.get_logger("test"),
    )
    return publisher, client


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
    }


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
            )


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
async def test_unpublish_raises_on_5xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"errors": ["boom"]})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(httpx.HTTPStatusError):
            await pub.unpublish(project_slug="p", edition_slug="e")
