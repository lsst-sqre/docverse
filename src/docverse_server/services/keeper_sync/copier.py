"""Copy build content from an LTD S3 source into a Docverse object store.

The copier feeds every key under ``source_prefix`` through a bounded
pool of worker tasks that write into the destination object store under
``dest_prefix``, so a single sync slot can never starve other work on
the worker — and, critically, so neither the number of live tasks nor
the number of buffered object bodies scales with the number of keys
under the prefix. It computes a deterministic manifest hash —
``sha256`` over a sorted ``(relative_key, sha256(data))`` table — so
re-runs against unchanged input produce byte-identical hashes that
double as the ``content_hash`` on the resulting Docverse build row.

Object bodies are still buffered whole rather than streamed:
:meth:`~docverse_server.storage.objectstore.ObjectStore.upload_object`
takes ``bytes``, so streaming the copy path would mean reworking the
destination protocol. With the pool bound in place peak resident bytes
are ``max_concurrent`` times the largest object under the prefix, which
for LTD's HTML/CSS/JS/image payloads is a small constant — not the term
that OOM-killed the sync worker.
"""

from __future__ import annotations

import asyncio
import hashlib
import mimetypes
from collections.abc import Awaitable, Callable, Iterator, Sequence
from dataclasses import dataclass

import structlog

from docverse_server.storage.ltd import LtdSourceProtocol
from docverse_server.storage.objectstore import ObjectStore

__all__ = ["EMPTY_MANIFEST_HASH", "BuildContentCopier", "CopyResult"]

#: Default number of worker tasks used for parallel object copies.
_DEFAULT_MAX_CONCURRENT = 8

#: Manifest hash of a prefix with no keys under it. Public because a
#: caller cannot otherwise tell "hashed an empty prefix" apart from
#: "hashed real content" by looking at the returned hash, and keeper-sync
#: has to make exactly that distinction when deciding whether its
#: edition-prefix fallback actually recovered anything (#516).
EMPTY_MANIFEST_HASH = f"sha256:{hashlib.sha256(b'').hexdigest()}"


@dataclass(frozen=True)
class CopyResult:
    """Outcome of one ``BuildContentCopier.copy_build`` call."""

    object_count: int
    total_size_bytes: int
    content_hash: str
    """``sha256:<64 hex chars>`` over the deterministic manifest."""


class BuildContentCopier:
    """Stream LTD build content into a Docverse R2 object store."""

    def __init__(
        self,
        *,
        source: LtdSourceProtocol,
        destination: ObjectStore,
        logger: structlog.stdlib.BoundLogger,
        max_concurrent: int = _DEFAULT_MAX_CONCURRENT,
    ) -> None:
        self._source = source
        self._destination = destination
        self._logger = logger
        self._max_concurrent = max_concurrent

    async def compute_manifest_hash(self, *, source_prefix: str) -> str:
        """Compute the manifest hash for ``source_prefix`` without copying.

        Performs the download-and-hash phase of :meth:`copy_build`
        without writing anything to the destination, so the keeper-sync
        engine can decide whether an existing Docverse build already
        matches the inbound LTD content (dual-upload convergence) before
        committing to an upload that would just duplicate it. Returns
        the same ``sha256:<hex>`` shape :meth:`copy_build` produces.
        """
        normalized_source_prefix = _ensure_trailing_slash(source_prefix)
        keys = sorted(
            await self._source.list_keys(prefix=normalized_source_prefix)
        )
        if not keys:
            return EMPTY_MANIFEST_HASH
        _reject_escaping_keys(keys, normalized_source_prefix, verb="hash")

        manifest_entries: list[tuple[str, str]] = []

        async def _hash_one(source_key: str) -> None:
            relative = source_key.removeprefix(normalized_source_prefix)
            # The body is hashed in the same expression that downloads
            # it and is never bound to a local, so the buffer is
            # released as soon as sha256 has consumed it.
            digest = hashlib.sha256(
                await self._source.download_object(key=source_key)
            ).hexdigest()
            manifest_entries.append((relative, digest))

        await self._run_bounded(keys, _hash_one)
        manifest_entries.sort(key=lambda e: e[0])
        return _hash_manifest_pairs(manifest_entries)

    async def copy_build(
        self, *, source_prefix: str, dest_prefix: str
    ) -> CopyResult:
        """Copy every key under ``source_prefix`` to ``dest_prefix``.

        ``source_prefix`` is the LTD bucket key prefix (e.g.
        ``pipelines/builds/42/``); ``dest_prefix`` is the Docverse
        ``storage_prefix`` for the build row. Both are treated as
        terminating in ``/`` for the purpose of computing relative
        keys.

        Returns
        -------
        CopyResult
            Object count, total bytes, and the deterministic manifest
            hash suitable for ``Build.content_hash``.
        """
        normalized_source_prefix = _ensure_trailing_slash(source_prefix)
        normalized_dest_prefix = _ensure_trailing_slash(dest_prefix)
        keys = sorted(
            await self._source.list_keys(prefix=normalized_source_prefix)
        )
        if not keys:
            self._logger.warning(
                "Empty source prefix; nothing to copy",
                source_prefix=normalized_source_prefix,
            )
            return CopyResult(
                object_count=0,
                total_size_bytes=0,
                content_hash=EMPTY_MANIFEST_HASH,
            )

        _reject_escaping_keys(keys, normalized_source_prefix, verb="copy")

        in_flight = _ConcurrencyTracker()
        manifest_entries: list[tuple[str, str, int]] = []

        async def _copy_one(source_key: str) -> None:
            relative = source_key.removeprefix(normalized_source_prefix)
            dest_key = f"{normalized_dest_prefix}{relative}"
            content_type = (
                mimetypes.guess_type(relative)[0] or "application/octet-stream"
            )
            async with in_flight:
                data = await self._source.download_object(key=source_key)
                digest = hashlib.sha256(data).hexdigest()
                size = len(data)
                await self._destination.upload_object(
                    key=dest_key,
                    data=data,
                    content_type=content_type,
                )
            # ``data`` dies with this coroutine's frame, before the
            # worker that ran it allocates the next object's buffer.
            manifest_entries.append((relative, digest, size))

        await self._run_bounded(keys, _copy_one)

        manifest_entries.sort(key=lambda e: e[0])
        manifest_hash = _hash_manifest(manifest_entries)
        total_bytes = sum(size for _, _, size in manifest_entries)

        self._logger.info(
            "Copied build content",
            object_count=len(manifest_entries),
            total_size_bytes=total_bytes,
            content_hash=manifest_hash,
            peak_concurrent_copies=in_flight.peak,
        )

        return CopyResult(
            object_count=len(manifest_entries),
            total_size_bytes=total_bytes,
            content_hash=manifest_hash,
        )

    async def _run_bounded(
        self,
        keys: Sequence[str],
        handler: Callable[[str], Awaitable[None]],
    ) -> None:
        """Run ``handler`` over ``keys`` with a bounded worker pool.

        A fixed pool of at most ``max_concurrent`` worker tasks pulls
        from one shared iterator over ``keys``, so both the number of
        live task objects and the number of simultaneously buffered
        object bodies are a function of the pool size rather than of
        the key count. (Feeding every key to ``asyncio.gather`` up front
        instead — throttling only the downloads with a semaphore — is
        what let a prefix with thousands of keys OOM the sync worker.)

        The pool runs inside a :class:`asyncio.TaskGroup`, so the first
        failing key cancels its siblings — no orphaned download survives
        the raise still holding its buffer — and the first real error is
        re-raised so the caller keeps today's contract that a failed
        copy propagates. That cancellation is the load-bearing half of
        the fix: ``gather`` left every sibling running after it raised,
        so a project whose oldest builds fail on every pass accumulated
        one orphaned fan-out per failed build.
        """
        # One shared iterator, drained cooperatively. ``next()`` never
        # awaits, so no two workers can observe the same key even though
        # they pull from the same object.
        pending = iter(keys)

        async def _worker() -> None:
            for key in pending:
                await handler(key)

        # Never spawn more workers than there is work for, and never
        # zero — a misconfigured bound must not silently drop keys.
        worker_count = max(1, min(self._max_concurrent, len(keys)))
        try:
            async with asyncio.TaskGroup() as task_group:
                for _ in range(worker_count):
                    task_group.create_task(_worker())
        except BaseExceptionGroup as exc_group:
            raise _first_real_error(exc_group) from None


class _ConcurrencyTracker:
    """Track the peak number of in-flight ``async with`` regions."""

    def __init__(self) -> None:
        self._current = 0
        self.peak = 0

    async def __aenter__(self) -> None:
        self._current += 1
        self.peak = max(self.peak, self._current)

    async def __aexit__(self, *_: object) -> None:
        self._current -= 1


def _ensure_trailing_slash(prefix: str) -> str:
    return prefix if prefix.endswith("/") else f"{prefix}/"


def _reject_escaping_keys(
    keys: Sequence[str], source_prefix: str, *, verb: str
) -> None:
    """Raise if any key's path relative to ``source_prefix`` escapes it.

    Checked up front, before any worker starts, so a malicious key can
    never be reached after some of its siblings have already been
    downloaded or uploaded.
    """
    for source_key in keys:
        relative = source_key.removeprefix(source_prefix)
        if ".." in relative.split("/"):
            msg = (
                f"Refusing to {verb} source key {source_key!r}:"
                " relative path contains '..' segment"
            )
            raise RuntimeError(msg)


def _iter_leaf_exceptions(
    group: BaseExceptionGroup[BaseException],
) -> Iterator[BaseException]:
    """Yield every non-group leaf exception in ``group``, depth-first."""
    for exc in group.exceptions:
        if isinstance(exc, BaseExceptionGroup):
            yield from _iter_leaf_exceptions(exc)
        else:
            yield exc


def _first_real_error(
    group: BaseExceptionGroup[BaseException],
) -> BaseException:
    """Return the first non-cancellation leaf exception in ``group``.

    ``asyncio.TaskGroup`` reports failures as an exception group, but the
    copier's callers (``sync_edition`` and the keeper-sync worker's
    ``except Exception`` isolation blocks) expect the underlying error
    itself — an S3 ``AccessDenied``, say — exactly as the old
    ``asyncio.gather`` call raised it. So the group is flattened back
    down to its first real failure. Cancellations are skipped: they are
    the pool tearing down siblings, not the cause.

    If two keys fail close enough together that both land before
    cancellation does, only the first is raised. That matches the
    ``gather`` contract this replaces, which likewise surfaced one
    error; the group itself is returned only when *every* leaf is a
    cancellation, which should not happen for a group the pool raised.
    """
    for exc in _iter_leaf_exceptions(group):
        if not isinstance(exc, asyncio.CancelledError):
            return exc
    return group


def _hash_manifest(entries: list[tuple[str, str, int]]) -> str:
    r"""Compute ``sha256:`` over sorted ``relative\tdigest\n`` lines.

    Size is deliberately excluded from the manifest line because it is
    derivable from the data; including it would couple the hash to a
    second representation of the same fact and risk drift.
    """
    return _hash_manifest_pairs(
        [(relative, digest) for relative, digest, _ in entries]
    )


def _hash_manifest_pairs(entries: list[tuple[str, str]]) -> str:
    r"""Compute ``sha256:`` over sorted ``relative\tdigest\n`` lines."""
    hasher = hashlib.sha256()
    for relative, digest in entries:
        hasher.update(f"{relative}\t{digest}\n".encode())
    return f"sha256:{hasher.hexdigest()}"
