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
at startup and closed at shutdown. Both sides' S3 clients come from one
aiobotocore session per process, so a backfill's thousands of copies do
not each re-parse botocore's S3 service model (PRD #753), and the
destination clients themselves come from one cache per process, so those
copies do not each build an aiohttp connector and SSL context (#751).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import structlog
from aiobotocore.session import get_session
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy.ext.asyncio import AsyncSession

from docverse_server.services.keeper_sync import CopyTally
from docverse_server.storage.ltd import LtdS3Source
from docverse_server.storage.objectstore import (
    ObjectStore,
    ObjectStoreCache,
    ObjectStoreKey,
    S3ObjectStore,
)
from docverse_server.worker.main import (
    COPY_HTTP_CONNECTION_HEADROOM,
    COPY_HTTP_KEEPALIVE_EXPIRY_SECONDS,
    COPY_HTTP_TIMEOUT,
    config,
    create_copy_http_client,
    initialize_worker_http_clients,
    initialize_worker_ltd_s3_source,
    initialize_worker_objectstore_cache,
    shutdown,
)
from tests.support.botocore_sessions import record_aiobotocore_sessions
from tests.support.objectstore import (
    record_s3_store_lifecycle,
    seed_minio_service,
)

from .conftest import make_worker_ctx


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
        ctx, session=get_session(), logger=structlog.get_logger("test")
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


def _serve_from_memory(
    monkeypatch: pytest.MonkeyPatch,
    source: LtdS3Source,
    objects: dict[str, bytes],
) -> None:
    """Answer ``source``'s reads from ``objects`` instead of S3.

    The source stays real, and open on its real client, so a test can
    still ask which session it was opened from; only its two reads are
    swapped out, so no request leaves the test.
    """

    async def _list_keys(*, prefix: str) -> list[str]:
        return [key for key in objects if key.startswith(prefix)]

    async def _download_object(*, key: str) -> bytes:
        return objects[key]

    monkeypatch.setattr(source, "list_keys", _list_keys)
    monkeypatch.setattr(source, "download_object", _download_object)


@pytest.mark.asyncio
async def test_keeper_sync_copies_share_one_aiobotocore_session(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sync's copies and manifest hashes create one ``AioSession`` in all.

    ``create_keeper_sync_service`` builds a destination store per build
    copy and per manifest hash, and each store used to create its own
    session, parsing botocore's S3 service model and endpoint ruleset
    afresh: thousands of times per backfill, the churn behind the sync
    worker's unbounded growth (PRD #753). Wired as ``_startup`` wires
    the worker, three copies and a manifest hash must create only the
    process's one session, which the shared LTD source is opened from
    too. Uploads land in an ``httpx.MockTransport`` and the source's
    reads are served from memory, so every S3 client is real but no
    request leaves the test.
    """
    sessions = record_aiobotocore_sessions(monkeypatch)
    objects = {
        f"proj/builds/{build}/index.html": b"<html></html>"
        for build in range(3)
    }
    puts: list[str] = []

    def _record_put(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            puts.append(request.url.path)
        return httpx.Response(200)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_record_put)
    ) as http_client:
        # The two steps ``_startup`` takes before building the factory
        # builder: one session per process, and the shared LTD source
        # opened from it.
        aiobotocore_session = get_session()
        source = await initialize_worker_ltd_s3_source(
            {},
            session=aiobotocore_session,
            logger=structlog.get_logger("test"),
        )
        _serve_from_memory(monkeypatch, source, objects)
        ctx = make_worker_ctx(
            http_client=http_client,
            ltd_s3_source=source,
            aiobotocore_session=aiobotocore_session,
        )
        factory = ctx["factory_builder"](
            session=db_session, logger=structlog.get_logger("test")
        )
        org_id = await seed_minio_service(db_session, factory)
        service = factory.create_keeper_sync_service(
            org_id=org_id, service_label="minio"
        )

        for build in range(3):
            await service._copy_callable(
                f"proj/builds/{build}/",
                f"proj/__builds/{build}/",
                CopyTally(),
            )
        await service._manifest_callable("proj/builds/0/")
        await source.close()

    assert sessions == [aiobotocore_session]
    assert source._session is aiobotocore_session
    assert sorted(puts) == [
        f"/docs/proj/__builds/{build}/index.html" for build in range(3)
    ]


@pytest.mark.asyncio
async def test_startup_creates_one_objectstore_cache_and_shutdown_closes_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker startup records the process's store cache; shutdown closes it.

    The cache keeps destination clients open between jobs, so nothing
    but ``shutdown`` ever closes them: every client it holds has to be
    closed there, once the jobs have stopped.
    """

    async def _noop_aclose() -> None:
        return None

    monkeypatch.setattr(db_session_dependency, "aclose", _noop_aclose)
    ctx: dict[str, Any] = {}
    initialize_worker_http_clients(ctx)
    cache = initialize_worker_objectstore_cache(
        ctx, logger=structlog.get_logger("test")
    )
    lifecycle = record_s3_store_lifecycle(monkeypatch)
    session = get_session()

    def _build(logger: structlog.stdlib.BoundLogger) -> ObjectStore:
        return S3ObjectStore(
            endpoint_url="https://minio.example.com",
            bucket="docs",
            access_key_id="key-id",
            secret_access_key="secret",
            logger=logger,
            session=session,
        )

    for org_id in (1, 2):
        key = ObjectStoreKey.create(
            org_id=org_id,
            service_label="minio",
            provider="minio",
            config={"bucket": "docs"},
            max_attempts=4,
            max_backoff_seconds=10.0,
            http_client=None,
            upload_limiter=None,
        )
        fingerprint = (datetime.now(tz=UTC), datetime.now(tz=UTC))
        async with cache.acquire(key, fingerprint, _build):
            pass

    assert ctx["objectstore_cache"] is cache
    assert isinstance(cache, ObjectStoreCache)
    assert len(lifecycle.opened) == 2
    assert lifecycle.closed == []

    await shutdown(ctx)

    assert lifecycle.closed == lifecycle.opened
    assert cache.open_clients == 0


@pytest.mark.asyncio
async def test_keeper_sync_and_job_stores_open_two_clients_in_all(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sync's copies and another job's store open two clients between them.

    Before the cache, every build copy, manifest hash and job opened a
    destination client of its own, and each one left about 2.7 MB of
    RSS that tracemalloc never saw: the sync worker grew by a gigabyte
    over a few hundred copies on roundtable-dev (#751). Wired as
    ``_startup`` wires the worker, three copies and two manifest hashes
    share the copier's client (the keeper-sync upload budget, copy
    client and upload limiter), and a ``dashboard_build``-style job's
    store shares one more, built with the shared budget and client.
    Neither is closed until the cache is.
    """
    lifecycle = record_s3_store_lifecycle(monkeypatch)
    objects = {
        f"proj/builds/{build}/index.html": b"<html></html>"
        for build in range(3)
    }
    puts: list[str] = []

    def _record_put(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            puts.append(request.url.path)
        return httpx.Response(200)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_record_put)
    ) as http_client:
        aiobotocore_session = get_session()
        source = await initialize_worker_ltd_s3_source(
            {},
            session=aiobotocore_session,
            logger=structlog.get_logger("test"),
        )
        _serve_from_memory(monkeypatch, source, objects)
        ctx = make_worker_ctx(
            http_client=http_client,
            ltd_s3_source=source,
            aiobotocore_session=aiobotocore_session,
            objectstore_cache=ObjectStoreCache(
                logger=structlog.get_logger("test")
            ),
        )

        # One keeper_sync_project job.
        sync_factory = ctx["factory_builder"](
            session=db_session, logger=structlog.get_logger("test")
        )
        org_id = await seed_minio_service(db_session, sync_factory)
        service = sync_factory.create_keeper_sync_service(
            org_id=org_id, service_label="minio"
        )
        for build in range(3):
            await service._copy_callable(
                f"proj/builds/{build}/",
                f"proj/__builds/{build}/",
                CopyTally(),
            )
        for build in range(2):
            await service._manifest_callable(f"proj/builds/{build}/")

        # One dashboard_build job, with a per-job factory of its own.
        job_factory = ctx["factory_builder"](
            session=db_session, logger=structlog.get_logger("test")
        )
        async with db_session.begin():
            store = await job_factory.create_objectstore_for_org(
                org_id=org_id, service_label="minio"
            )
        async with store:
            await store.upload_object(
                key="proj/index.html",
                data=b"<html></html>",
                content_type="text/html",
            )

        assert len(lifecycle.opened) == 2
        assert lifecycle.closed == []

        await ctx["objectstore_cache"].aclose()
        await source.close()

    assert lifecycle.closed == lifecycle.opened
    assert sorted(puts) == [
        *(f"/docs/proj/__builds/{build}/index.html" for build in range(3)),
        "/docs/proj/index.html",
    ]
