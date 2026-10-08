"""Tests for the worker's dedicated, connection-capped LTD API client.

``LtdClient.list_editions_for_product`` keeps up to eight edition GETs in
flight per call, but that semaphore bounds one call, not the process.
Every LTD client was built over the worker's shared
``httpx.AsyncClient()`` (100 connections, 20 kept alive), so the three
tier crons plus up to ten fanned-out ``keeper_sync_project`` jobs could
hold about a hundred concurrent GETs, and about a hundred fresh TCP
handshakes, against the one LTD Keeper deployment. On roundtable-prod
on 2026-10-08 every hourly ``tier_other`` tick then logged hundreds of
LTD ``ConnectTimeout`` retries and failed a few sync jobs outright
(#801). LTD API calls now get a client of their own, capped at a few
connections that are all kept alive, which queues callers past the cap
for a connection rather than failing them; these tests pin that sizing.
"""

from __future__ import annotations

import httpx
import pytest

from docverse_server.worker.main import (
    LTD_HTTP_KEEPALIVE_EXPIRY_SECONDS,
    LTD_HTTP_MAX_CONNECTIONS,
    LTD_HTTP_TIMEOUT,
    create_ltd_http_client,
)


@pytest.mark.asyncio
async def test_ltd_client_caps_and_keeps_alive_every_connection() -> None:
    """The LTD client's pool is a small cap, every connection kept alive.

    The cap bounds the process's concurrent GETs to LTD Keeper however
    many jobs and crons list editions at once, and keeping every pooled
    connection alive is what removes the handshake storm: httpcore
    closes an idle connection whenever the pool holds more than
    ``max_keepalive_connections``, so a lower value would re-dial LTD
    after almost every GET of a burst.
    """
    async with create_ltd_http_client() as client:
        assert isinstance(client, httpx.AsyncClient)
        pool = client._transport._pool  # type: ignore[attr-defined]
        max_connections = pool._max_connections
        max_keepalive = pool._max_keepalive_connections
        keepalive_expiry = pool._keepalive_expiry

    assert 8 <= LTD_HTTP_MAX_CONNECTIONS <= 16
    assert max_connections == LTD_HTTP_MAX_CONNECTIONS
    assert max_keepalive == LTD_HTTP_MAX_CONNECTIONS
    assert keepalive_expiry == LTD_HTTP_KEEPALIVE_EXPIRY_SECONDS


@pytest.mark.asyncio
async def test_ltd_client_queues_for_a_connection_past_the_cap() -> None:
    """A GET past the cap waits for a connection instead of failing.

    ``pool=None`` disables httpx's pool timeout, so the GETs that the
    cap holds back queue rather than raising ``PoolTimeout``, which
    ``LtdClient``'s retry loop would otherwise spend attempts on. The
    connect, read and write timeouts stay at the shared client's 5 s.
    """
    async with create_ltd_http_client() as client:
        assert client.timeout == LTD_HTTP_TIMEOUT

    assert LTD_HTTP_TIMEOUT.pool is None
    assert httpx.Timeout(5.0, pool=None) == LTD_HTTP_TIMEOUT
