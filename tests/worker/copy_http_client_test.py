"""Tests for the keeper-sync worker's dedicated build-copy HTTP client.

Every outbound call from a worker used to share one bare
``httpx.AsyncClient()``: 5 s connect/read/write/pool timeouts and a
100-connection pool. At the stock ``keeper_sync_max_jobs`` (10) x
``keeper_sync_copy_concurrency`` (8) = 80 in-flight presigned PUTs that
pool ran near its ceiling, so a pool wait surfaced as a timeout too, and
the 5 s connect timeout left each upload attempt little room to ride out
an R2 connect outage (PRD #685). Build-content copies now get a client
of their own, sized from the process-wide upload cap and keeping every
connection alive (PRD #698); these tests pin that sizing and its
teardown. The LTD side of a copy gets the same treatment: one anonymous
S3 source per worker process, its pool sized from the same cap, opened
at startup and closed at shutdown.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse_server.storage.ltd import LtdS3Source
from docverse_server.worker.main import (
    COPY_HTTP_CONNECTION_HEADROOM,
    COPY_HTTP_KEEPALIVE_EXPIRY_SECONDS,
    COPY_HTTP_TIMEOUT,
    config,
    create_copy_http_client,
    initialize_worker_http_clients,
    initialize_worker_ltd_s3_source,
    shutdown,
)


@pytest.mark.asyncio
async def test_copy_client_has_documented_timeout_and_derived_limits() -> None:
    """The copy client's pool fits every capped upload plus headroom.

    One connection per upload the worker-wide cap lets through at once,
    so no presigned PUT waits on the pool, plus headroom. Every one of
    them stays alive for a minute between uploads: httpcore closes an
    idle connection as soon as the pool holds more than
    ``max_keepalive_connections``, and during a burst that churn re-dialed
    R2 per object and pinned a Cloud NAT port per closed connection.
    """
    async with create_copy_http_client(upload_concurrency=5) as client:
        assert isinstance(client, httpx.AsyncClient)
        assert client.timeout == COPY_HTTP_TIMEOUT
        pool = client._transport._pool  # type: ignore[attr-defined]
        max_connections = pool._max_connections
        max_keepalive = pool._max_keepalive_connections
        keepalive_expiry = pool._keepalive_expiry

    assert (
        httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=30.0)
        == COPY_HTTP_TIMEOUT
    )
    assert COPY_HTTP_CONNECTION_HEADROOM == 10
    assert max_connections == 5 + COPY_HTTP_CONNECTION_HEADROOM
    assert max_keepalive == max_connections
    assert COPY_HTTP_KEEPALIVE_EXPIRY_SECONDS == 60.0
    assert keepalive_expiry == COPY_HTTP_KEEPALIVE_EXPIRY_SECONDS


@pytest.mark.asyncio
async def test_startup_records_a_distinct_copy_client_sized_from_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker startup puts a copy client beside the shared one in ctx.

    The copy client is sized from ``keeper_sync_upload_concurrency``
    whichever pool starts, because only keeper-sync jobs copy; the
    shared client keeps httpx's defaults for everything else.
    ``shutdown`` then closes both.
    """

    async def _noop_aclose() -> None:
        return None

    # The session dependency is process-global; leave it to the fixtures
    # that own it rather than disposing of their engine here.
    monkeypatch.setattr(db_session_dependency, "aclose", _noop_aclose)
    # Off the default, so the pool can only have come from this setting.
    monkeypatch.setattr(config, "keeper_sync_upload_concurrency", 7)
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
    assert pool._max_connections == 7 + COPY_HTTP_CONNECTION_HEADROOM
    assert pool._max_keepalive_connections == pool._max_connections
    assert pool._keepalive_expiry == COPY_HTTP_KEEPALIVE_EXPIRY_SECONDS

    await shutdown(ctx)

    assert http_client.is_closed
    assert copy_http_client.is_closed


@pytest.mark.asyncio
async def test_startup_opens_one_ltd_source_sized_from_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker startup opens the process's one LTD source; shutdown closes it.

    Every copier in the process reads LTD through this source, so its
    pool is sized from ``keeper_sync_upload_concurrency`` rather than
    botocore's default of ten, which would throttle the copiers'
    downloads. It is open before any job runs, since copiers never open
    the shared source themselves, and ``shutdown`` owns closing it.
    """

    async def _noop_aclose() -> None:
        return None

    monkeypatch.setattr(db_session_dependency, "aclose", _noop_aclose)
    # Off the default, so the pool can only have come from this setting.
    monkeypatch.setattr(config, "keeper_sync_upload_concurrency", 7)
    ctx: dict[str, Any] = {}
    initialize_worker_http_clients(ctx)

    source = await initialize_worker_ltd_s3_source(
        ctx, logger=structlog.get_logger("test")
    )

    assert ctx["ltd_s3_source"] is source
    assert isinstance(source, LtdS3Source)
    assert source._get_client().meta.config.max_pool_connections == 7

    await shutdown(ctx)

    assert source._client is None
    with pytest.raises(RuntimeError, match="not open"):
        source._get_client()


@pytest.mark.asyncio
async def test_shutdown_without_an_ltd_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shutdown tolerates a ctx whose startup never opened an LTD source.

    Startup can fail before the source is opened, and arq still runs
    ``shutdown``; the HTTP clients it did open must still be closed.
    """

    async def _noop_aclose() -> None:
        return None

    monkeypatch.setattr(db_session_dependency, "aclose", _noop_aclose)
    ctx: dict[str, Any] = {}
    initialize_worker_http_clients(ctx)

    await shutdown(ctx)

    assert ctx["http_client"].is_closed
    assert ctx["copy_http_client"].is_closed
