"""Tests for the CloudflareCachePurger."""

from __future__ import annotations

import json

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

from docverse_server.storage.cdncachepurger import CloudflareCachePurger


def _make_purger(
    handler: httpx.MockTransport,
    *,
    zone_id: str = "zone-123",
    api_token: str = "token-789",
) -> tuple[CloudflareCachePurger, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=handler)
    purger = CloudflareCachePurger(
        zone_id=zone_id,
        api_token=api_token,
        http_client=client,
        logger=structlog.get_logger("test"),
    )
    return purger, client


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
async def test_purge_hostname_raises_on_4xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"errors": ["forbidden"]})

    purger, client = _make_purger(httpx.MockTransport(handler))
    async with client, purger as p:
        with pytest.raises(httpx.HTTPStatusError):
            await p.purge_hostname("myproject.example.org")


@pytest.mark.asyncio
async def test_purge_hostname_raises_on_5xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"errors": ["boom"]})

    purger, client = _make_purger(httpx.MockTransport(handler))
    async with client, purger as p:
        with pytest.raises(httpx.HTTPStatusError):
            await p.purge_hostname("myproject.example.org")


@pytest.mark.asyncio
async def test_purge_hostname_logs_error_with_context() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"errors": ["boom"]})

    with capture_logs() as logs:
        purger, client = _make_purger(httpx.MockTransport(handler))
        async with client, purger as p:
            with pytest.raises(httpx.HTTPStatusError):
                await p.purge_hostname("myproject.example.org")

    errors = [entry for entry in logs if entry["log_level"] == "error"]
    assert len(errors) == 1
    assert errors[0]["hostname"] == "myproject.example.org"
    assert errors[0]["zone_id"] == "zone-123"
    assert errors[0]["status_code"] == 500
    assert "boom" in errors[0]["response_body"]
