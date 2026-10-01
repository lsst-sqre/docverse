"""Process-lifetime cache of open object-store clients.

A worker process opens an organization's object store many times: the
keeper-sync copier once per build copy and once per manifest hash, and
``build_processing``, ``dashboard_build`` and ``purgatory_cleanup`` once
per job. Opening an `S3ObjectStore` creates an aiobotocore client, and
with it an aiohttp connector and an ``ssl.SSLContext`` loaded with the
full CA store. On roundtable-dev each of those left about 2.7 MB of RSS
behind that tracemalloc never saw, so a backfill of a few hundred builds
grew the sync worker by a gigabyte (#751). `ObjectStoreCache` keeps one
open store per `ObjectStoreKey` for the life of the process instead, the
way the worker already shares one LTD source and one aiobotocore
session.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Hashable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

import httpx
import structlog

from ._protocol import ObjectStore

__all__ = ["ObjectStoreBuild", "ObjectStoreCache", "ObjectStoreKey"]

type ObjectStoreBuild = Callable[[structlog.stdlib.BoundLogger], ObjectStore]
"""Builds an unopened store for a cache entry, given the logger it keeps.

The cache passes a logger of its own rather than letting the caller's
through: a cached store outlives the job that first opened it, so a
job-bound logger would stamp later jobs' upload failures with the first
job's context.
"""


@dataclass(frozen=True, slots=True)
class ObjectStoreKey:
    """Identity of one cached object store.

    Two `Factory.create_objectstore_for_org
    <docverse_server.factory.Factory.create_objectstore_for_org>` calls
    share a client only when every field matches. The upload budget,
    client and limiter are part of the identity because they are baked
    into the store: the keeper-sync copier's store, with its larger
    budget, dedicated copy client and process-wide upload limiter, is
    therefore a separate entry from the store every other worker job
    uses for the same organization and service.

    Build keys with `create`, which freezes the parts that are not
    hashable as they come.
    """

    org_id: int
    """Organization the service belongs to."""

    service_label: str
    """Label of the organization's object-store service."""

    provider: str
    """Service provider, such as ``cloudflare_r2``."""

    config: str
    """The service's non-secret config, as canonical JSON."""

    max_attempts: int
    """Presigned-upload attempt budget the store is built with."""

    max_backoff_seconds: float
    """Presigned-upload backoff ceiling the store is built with."""

    http_client_id: int | None
    """``id()`` of the client the store PUTs presigned uploads over."""

    upload_limiter_id: int | None
    """``id()`` of the semaphore bounding the store's uploads."""

    @classmethod
    def create(
        cls,
        *,
        org_id: int,
        service_label: str,
        provider: str,
        config: dict[str, Any],
        max_attempts: int,
        max_backoff_seconds: float,
        http_client: httpx.AsyncClient | None,
        upload_limiter: asyncio.Semaphore | None,
    ) -> Self:
        """Build the key for a store made from these parts.

        ``http_client`` and ``upload_limiter`` are keyed by identity.
        That is sound because both are process-lifetime objects, and a
        cached store holds a reference to each, so neither can be
        collected and its ``id()`` reused while an entry keyed on it is
        alive.
        """
        return cls(
            org_id=org_id,
            service_label=service_label,
            provider=provider,
            config=json.dumps(config, sort_keys=True, default=str),
            max_attempts=max_attempts,
            max_backoff_seconds=max_backoff_seconds,
            http_client_id=None if http_client is None else id(http_client),
            upload_limiter_id=(
                None if upload_limiter is None else id(upload_limiter)
            ),
        )


@dataclass(eq=False)
class _Entry:
    """One open store and the bookkeeping that decides when to close it."""

    store: ObjectStore
    fingerprint: Hashable
    users: int = 0
    retired: bool = False
    closed: bool = False


class ObjectStoreCache:
    """One open object store per `ObjectStoreKey`, for a process lifetime.

    `acquire` hands out the open store for a key, opening it on first
    use, and leaves it open on exit; `aclose` closes every store the
    cache holds. Concurrent first acquires of a key wait on a per-key
    lock, so they share one open rather than racing to open two.

    Each acquire carries a fingerprint of the configuration the caller
    resolved the store from: the service row's and the credential's
    ``date_updated``. When it differs from the cached entry's, the entry
    is retired and a store is opened afresh from the caller's build, so
    a service edit or a credential rotation takes effect on the next use
    of the store. A retired store still in use by an earlier acquire is
    closed when that last user releases it, never underneath it.

    Parameters
    ----------
    logger
        Process-lifetime logger. The cache logs one line when it opens a
        store for a key and one when a fingerprint change replaces one,
        so a worker's log shows how many clients it holds, and passes a
        child of it to every store it builds.
    """

    def __init__(self, *, logger: structlog.stdlib.BoundLogger) -> None:
        self._logger = logger
        self._entries: dict[ObjectStoreKey, _Entry] = {}
        self._retired: list[_Entry] = []
        self._locks: dict[ObjectStoreKey, asyncio.Lock] = {}

    @property
    def open_clients(self) -> int:
        """Stores the cache holds open, including retired ones in use."""
        return len(self._entries) + len(self._retired)

    def acquire(
        self,
        key: ObjectStoreKey,
        fingerprint: Hashable,
        build: ObjectStoreBuild,
    ) -> AbstractAsyncContextManager[ObjectStore]:
        """Yield the open store for ``key``, opening it on first use.

        Parameters
        ----------
        key
            Identity of the store.
        fingerprint
            Version of the configuration ``build`` makes the store from.
            A cached entry with a different fingerprint is replaced.
        build
            Makes the unopened store when the key has no entry, or its
            entry's fingerprint is stale. Called at most once per
            acquire, and not at all when the cached entry is current.

        Returns
        -------
        contextlib.AbstractAsyncContextManager
            Yields the shared, open store. Leaving it releases the store
            without closing it.
        """
        return self._acquire(key, fingerprint, build)

    def handle(
        self,
        key: ObjectStoreKey,
        fingerprint: Hashable,
        build: ObjectStoreBuild,
    ) -> ObjectStore:
        """Return an unopened store that borrows the cached one when entered.

        The handle satisfies `ObjectStore`, so callers written against a
        store they open and close with ``async with`` keep working:
        entering the handle runs `acquire`, its methods call through to
        the shared store, and exiting releases it without closing.
        """
        return _CachedObjectStore(
            cache=self, key=key, fingerprint=fingerprint, build=build
        )

    async def aclose(self) -> None:
        """Close every store the cache holds and forget them.

        Called by the worker's ``shutdown`` once its jobs have stopped.
        A store that fails to close is logged and the rest are still
        closed.
        """
        entries = [*self._entries.values(), *self._retired]
        self._entries.clear()
        self._retired.clear()
        for entry in entries:
            await self._close(entry)

    @asynccontextmanager
    async def _acquire(
        self,
        key: ObjectStoreKey,
        fingerprint: Hashable,
        build: ObjectStoreBuild,
    ) -> AsyncIterator[ObjectStore]:
        entry = await self._checkout(key, fingerprint, build)
        try:
            yield entry.store
        finally:
            await self._release(entry)

    async def _checkout(
        self,
        key: ObjectStoreKey,
        fingerprint: Hashable,
        build: ObjectStoreBuild,
    ) -> _Entry:
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            entry = self._entries.get(key)
            replaced = False
            if entry is not None and entry.fingerprint != fingerprint:
                del self._entries[key]
                await self._retire(entry)
                entry = None
                replaced = True
            if entry is None:
                entry = await self._open(key, fingerprint, build)
                self._entries[key] = entry
                self._log_open(key, replaced=replaced)
            entry.users += 1
            return entry

    def _log_open(self, key: ObjectStoreKey, *, replaced: bool) -> None:
        # Two literal messages rather than one chosen at run time, so the
        # docs test can read both off this module's syntax tree.
        if replaced:
            self._logger.info(
                "Replaced shared object store client",
                org_id=key.org_id,
                service_label=key.service_label,
                provider=key.provider,
                open_clients=self.open_clients,
            )
        else:
            self._logger.info(
                "Opened shared object store client",
                org_id=key.org_id,
                service_label=key.service_label,
                provider=key.provider,
                open_clients=self.open_clients,
            )

    async def _open(
        self,
        key: ObjectStoreKey,
        fingerprint: Hashable,
        build: ObjectStoreBuild,
    ) -> _Entry:
        logger = self._logger.bind(
            org_id=key.org_id, service_label=key.service_label
        )
        store = build(logger)
        opened = await store.__aenter__()
        return _Entry(store=opened, fingerprint=fingerprint)

    async def _retire(self, entry: _Entry) -> None:
        entry.retired = True
        if entry.users == 0:
            await self._close(entry)
        else:
            self._retired.append(entry)

    async def _release(self, entry: _Entry) -> None:
        entry.users -= 1
        if entry.retired and entry.users == 0 and entry in self._retired:
            self._retired.remove(entry)
            await self._close(entry)

    async def _close(self, entry: _Entry) -> None:
        if entry.closed:
            return
        entry.closed = True
        try:
            await entry.store.__aexit__(None, None, None)
        except Exception:
            self._logger.warning(
                "Failed to close shared object store client", exc_info=True
            )


class _CachedObjectStore:
    """`ObjectStore` that borrows a cached store while it is entered."""

    def __init__(
        self,
        *,
        cache: ObjectStoreCache,
        key: ObjectStoreKey,
        fingerprint: Hashable,
        build: ObjectStoreBuild,
    ) -> None:
        self._cache = cache
        self._key = key
        self._fingerprint = fingerprint
        self._build = build
        self._lease: AbstractAsyncContextManager[ObjectStore] | None = None
        self._store: ObjectStore | None = None

    async def __aenter__(self) -> Self:
        if self._lease is not None:
            msg = "Cached object store handle is already entered"
            raise RuntimeError(msg)
        lease = self._cache.acquire(self._key, self._fingerprint, self._build)
        self._store = await lease.__aenter__()
        self._lease = lease
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        lease = self._lease
        self._lease = None
        self._store = None
        if lease is not None:
            await lease.__aexit__(exc_type, exc_val, exc_tb)

    def _require_store(self) -> ObjectStore:
        if self._store is None:
            msg = "Cached object store is not open; use async with"
            raise RuntimeError(msg)
        return self._store

    async def generate_presigned_upload_url(
        self, *, key: str, content_type: str, expires_in: int = 3600
    ) -> str:
        """Generate a pre-signed upload URL through the shared store."""
        return await self._require_store().generate_presigned_upload_url(
            key=key, content_type=content_type, expires_in=expires_in
        )

    async def generate_presigned_download_url(
        self, *, key: str, expires_in: int = 3600
    ) -> str:
        """Generate a pre-signed download URL through the shared store."""
        return await self._require_store().generate_presigned_download_url(
            key=key, expires_in=expires_in
        )

    async def delete_object(self, *, key: str) -> None:
        """Delete an object through the shared store."""
        await self._require_store().delete_object(key=key)

    async def delete_prefix(self, *, prefix: str) -> int:
        """Delete every object under ``prefix`` through the shared store."""
        return await self._require_store().delete_prefix(prefix=prefix)

    async def list_objects(self, *, prefix: str) -> list[str]:
        """List objects under ``prefix`` through the shared store."""
        return await self._require_store().list_objects(prefix=prefix)

    async def download_object(self, *, key: str) -> bytes:
        """Download an object through the shared store."""
        return await self._require_store().download_object(key=key)

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> int:
        """Upload an object through the shared store."""
        return await self._require_store().upload_object(
            key=key, data=data, content_type=content_type
        )
