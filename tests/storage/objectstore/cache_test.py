"""Tests for the process-lifetime object-store client cache.

Every keeper-sync build copy and manifest hash used to open its own R2
destination client, and each client built a new aiohttp connector and an
``ssl.SSLContext`` loaded with the full CA store: about 2.7 MB of RSS per
store that tracemalloc never saw (#751). ``ObjectStoreCache`` keeps one
open store per identity for the life of a worker process; these tests pin
its sharing, replacement and teardown rules against an in-memory store
that counts its opens and closes.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import TracebackType
from typing import Self

import pytest
import structlog
from structlog.testing import capture_logs

from docverse_server.storage.objectstore import (
    MockObjectStore,
    ObjectStore,
    ObjectStoreCache,
    ObjectStoreKey,
)

_FINGERPRINT = (
    datetime(2026, 9, 30, 12, tzinfo=UTC),
    datetime(2026, 9, 30, 12, tzinfo=UTC),
)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _key(*, org_id: int = 1, service_label: str = "r2") -> ObjectStoreKey:
    return ObjectStoreKey.create(
        org_id=org_id,
        service_label=service_label,
        provider="minio",
        config={"endpoint_url": "https://minio.example.com", "bucket": "docs"},
        max_attempts=4,
        max_backoff_seconds=10.0,
        http_client=None,
        upload_limiter=None,
    )


class _CountingStore(MockObjectStore):
    """In-memory store that counts how often it is opened and closed."""

    def __init__(self) -> None:
        super().__init__()
        self.opens = 0
        self.closes = 0

    async def __aenter__(self) -> Self:
        self.opens += 1
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.closes += 1


class _Builds:
    """``build`` callable that records every store it makes."""

    def __init__(self) -> None:
        self.stores: list[_CountingStore] = []

    def __call__(self, logger: structlog.stdlib.BoundLogger) -> ObjectStore:
        store = _CountingStore()
        self.stores.append(store)
        return store


@pytest.mark.asyncio
async def test_acquires_for_one_key_share_one_open_store() -> None:
    """A second acquire reuses the first one's store, and exits close none.

    The cache exists so that the thousands of copies a backfill makes
    open one destination client between them rather than one each.
    """
    cache = ObjectStoreCache(logger=_logger())
    builds = _Builds()

    async with cache.acquire(_key(), _FINGERPRINT, builds) as first:
        pass
    async with cache.acquire(_key(), _FINGERPRINT, builds) as second:
        pass

    assert len(builds.stores) == 1
    assert first is second is builds.stores[0]
    assert builds.stores[0].opens == 1
    assert builds.stores[0].closes == 0


@pytest.mark.asyncio
async def test_fingerprint_change_replaces_the_store() -> None:
    """A newer fingerprint closes the old store and opens exactly one more.

    The fingerprint is the service row's and the credential's
    ``date_updated``, so a service edit or a credential rotation reaches
    the next copy instead of waiting for the worker to restart.
    """
    cache = ObjectStoreCache(logger=_logger())
    builds = _Builds()
    rotated = (_FINGERPRINT[0], datetime(2026, 10, 1, tzinfo=UTC))

    async with cache.acquire(_key(), _FINGERPRINT, builds):
        pass
    async with cache.acquire(_key(), rotated, builds) as current:
        pass
    async with cache.acquire(_key(), rotated, builds):
        pass

    old, new = builds.stores
    assert (old.opens, old.closes) == (1, 1)
    assert (new.opens, new.closes) == (1, 0)
    assert current is new
    assert cache.open_clients == 1


@pytest.mark.asyncio
async def test_replaced_store_closes_only_when_its_last_user_leaves() -> None:
    """A store retired while in use stays open until its holder releases.

    Closing it at once would fail the copy still uploading through it.
    """
    cache = ObjectStoreCache(logger=_logger())
    builds = _Builds()
    rotated = (datetime(2026, 10, 1, tzinfo=UTC), _FINGERPRINT[1])

    async with cache.acquire(_key(), _FINGERPRINT, builds) as held:
        async with cache.acquire(_key(), rotated, builds):
            pass
        assert builds.stores[0].closes == 0
        assert cache.open_clients == 2
        await held.upload_object(key="a", data=b"a", content_type="text/plain")

    assert builds.stores[0].closes == 1
    assert builds.stores[1].closes == 0
    assert cache.open_clients == 1


@pytest.mark.asyncio
async def test_concurrent_first_acquires_share_one_open() -> None:
    """Two jobs reaching a cold key together open one store, not two."""
    cache = ObjectStoreCache(logger=_logger())
    stores: list[_SlowOpenStore] = []

    def _build(logger: structlog.stdlib.BoundLogger) -> ObjectStore:
        store = _SlowOpenStore()
        stores.append(store)
        return store

    async def _use() -> ObjectStore:
        async with cache.acquire(_key(), _FINGERPRINT, _build) as store:
            return store

    first, second = await asyncio.gather(_use(), _use())

    assert len(stores) == 1
    assert first is second is stores[0]
    assert stores[0].opens == 1


@pytest.mark.asyncio
async def test_aclose_closes_every_store() -> None:
    """Shutdown closes the store of every key, including a retired one."""
    cache = ObjectStoreCache(logger=_logger())
    builds = _Builds()
    rotated = (datetime(2026, 10, 1, tzinfo=UTC), _FINGERPRINT[1])

    async with cache.acquire(_key(org_id=1), _FINGERPRINT, builds):
        pass
    async with cache.acquire(_key(org_id=2), _FINGERPRINT, builds) as held:
        # Retired while held, and still held when the process stops.
        async with cache.acquire(_key(org_id=2), rotated, builds):
            pass
        await cache.aclose()

    assert [store.closes for store in builds.stores] == [1, 1, 1]
    assert cache.open_clients == 0
    assert held is builds.stores[1]


@pytest.mark.asyncio
async def test_failed_open_is_not_cached() -> None:
    """An open that raises leaves no entry, so the next acquire retries."""
    cache = ObjectStoreCache(logger=_logger())
    builds = _Builds()
    attempts = 0

    def _build(logger: structlog.stdlib.BoundLogger) -> ObjectStore:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return _FailingOpenStore()
        return builds(logger)

    with pytest.raises(OSError, match="cannot open"):
        async with cache.acquire(_key(), _FINGERPRINT, _build):
            pass
    async with cache.acquire(_key(), _FINGERPRINT, _build) as store:
        pass

    assert store is builds.stores[0]
    assert cache.open_clients == 1


@pytest.mark.asyncio
async def test_logs_one_line_per_open_and_per_replacement() -> None:
    """Opens and replacements are logged with the live client count.

    A dev run reads how many destination clients a worker holds from
    these lines; a reuse logs nothing.
    """
    cache = ObjectStoreCache(logger=_logger())
    builds = _Builds()
    rotated = (datetime(2026, 10, 1, tzinfo=UTC), _FINGERPRINT[1])

    with capture_logs() as captured:
        async with cache.acquire(_key(org_id=1), _FINGERPRINT, builds):
            pass
        async with cache.acquire(_key(org_id=1), _FINGERPRINT, builds):
            pass
        async with cache.acquire(_key(org_id=2), _FINGERPRINT, builds):
            pass
        async with cache.acquire(_key(org_id=2), rotated, builds):
            pass

    assert [
        (
            line["event"],
            line["log_level"],
            line["org_id"],
            line["service_label"],
            line["open_clients"],
        )
        for line in captured
    ] == [
        ("Opened shared object store client", "info", 1, "r2", 1),
        ("Opened shared object store client", "info", 2, "r2", 2),
        ("Replaced shared object store client", "info", 2, "r2", 2),
    ]


@pytest.mark.asyncio
async def test_handle_borrows_the_shared_store_while_entered() -> None:
    """A handle calls through to the cached store and never closes it.

    Every existing ``async with store:`` call site gets a handle from the
    worker's factory, so it has to behave as the store it stands in for.
    """
    cache = ObjectStoreCache(logger=_logger())
    builds = _Builds()
    first = cache.handle(_key(), _FINGERPRINT, builds)
    second = cache.handle(_key(), _FINGERPRINT, builds)

    async with first:
        await first.upload_object(
            key="proj/a.html", data=b"a", content_type="text/html"
        )
    async with second:
        assert await second.list_objects(prefix="proj/") == ["proj/a.html"]
        assert await second.download_object(key="proj/a.html") == b"a"
        assert await second.delete_prefix(prefix="proj/") == 1

    (store,) = builds.stores
    assert (store.opens, store.closes) == (1, 0)
    with pytest.raises(RuntimeError, match="not open"):
        await first.list_objects(prefix="proj/")


class _SlowOpenStore(_CountingStore):
    """Store whose open yields to the loop, so a racing acquire can run."""

    async def __aenter__(self) -> Self:
        await asyncio.sleep(0)
        return await super().__aenter__()


class _FailingOpenStore(_CountingStore):
    """Store whose open always fails."""

    async def __aenter__(self) -> Self:
        msg = "cannot open"
        raise OSError(msg)
