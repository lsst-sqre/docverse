"""Tests for the keeper-sync worker's dedicated build-copy HTTP client.

Every outbound call from a worker used to share one bare
``httpx.AsyncClient()``: 5 s connect/read/write/pool timeouts and a
100-connection pool. At the stock ``keeper_sync_max_jobs`` (10) x
``keeper_sync_copy_concurrency`` (8) = 80 in-flight presigned PUTs that
pool ran near its ceiling, so a pool wait surfaced as a timeout too, and
the 5 s connect timeout left each upload attempt little room to ride out
an R2 connect outage (PRD #685). Build-content copies now get a client
of their own, sized from the two concurrency settings; these tests pin
that sizing and its teardown.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from safir.dependencies.db_session import db_session_dependency

from docverse_server.worker.main import (
    COPY_HTTP_CONNECTION_HEADROOM,
    COPY_HTTP_TIMEOUT,
    config,
    create_copy_http_client,
    initialize_worker_http_clients,
    shutdown,
)


@pytest.mark.asyncio
async def test_copy_client_has_documented_timeout_and_derived_limits() -> None:
    """The copy client's pool fits every in-flight copy plus headroom.

    One connection per concurrent object transfer across every
    concurrent sync job, so no presigned PUT waits on the pool, plus
    headroom; two copiers' worth of connections stay warm between
    builds.
    """
    async with create_copy_http_client(
        max_jobs=3, copy_concurrency=5
    ) as client:
        assert isinstance(client, httpx.AsyncClient)
        assert client.timeout == COPY_HTTP_TIMEOUT
        pool = client._transport._pool  # type: ignore[attr-defined]
        max_connections = pool._max_connections
        max_keepalive = pool._max_keepalive_connections

    assert (
        httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=30.0)
        == COPY_HTTP_TIMEOUT
    )
    assert COPY_HTTP_CONNECTION_HEADROOM == 10
    assert max_connections == 3 * 5 + COPY_HTTP_CONNECTION_HEADROOM
    assert max_keepalive == 5 * 2


@pytest.mark.asyncio
async def test_startup_records_a_distinct_copy_client_sized_from_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker startup puts a copy client beside the shared one in ctx.

    The copy client is sized from the keeper-sync pool's
    ``keeper_sync_max_jobs`` x ``keeper_sync_copy_concurrency`` whichever
    pool starts, because only keeper-sync jobs copy; the shared client
    keeps httpx's defaults for everything else. ``shutdown`` then closes
    both.
    """

    async def _noop_aclose() -> None:
        return None

    # The session dependency is process-global; leave it to the fixtures
    # that own it rather than disposing of their engine here.
    monkeypatch.setattr(db_session_dependency, "aclose", _noop_aclose)
    ctx: dict[str, Any] = {}

    initialize_worker_http_clients(ctx)

    http_client = ctx["http_client"]
    copy_http_client = ctx["copy_http_client"]
    assert isinstance(http_client, httpx.AsyncClient)
    assert isinstance(copy_http_client, httpx.AsyncClient)
    assert copy_http_client is not http_client
    assert copy_http_client.timeout == COPY_HTTP_TIMEOUT
    assert http_client.timeout == httpx.Timeout(5.0)
    pool = copy_http_client._transport._pool  # type: ignore[attr-defined]
    assert pool._max_connections == (
        config.keeper_sync_max_jobs * config.keeper_sync_copy_concurrency
        + COPY_HTTP_CONNECTION_HEADROOM
    )
    assert pool._max_keepalive_connections == (
        config.keeper_sync_copy_concurrency * 2
    )

    await shutdown(ctx)

    assert http_client.is_closed
    assert copy_http_client.is_closed
