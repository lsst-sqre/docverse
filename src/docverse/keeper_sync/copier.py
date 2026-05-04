"""Copy build content from an LTD S3 source into a Docverse object store.

The copier streams every key under ``source_prefix`` into the
destination object store under ``dest_prefix`` with a bounded
``asyncio.Semaphore`` so a single sync slot can never starve other
work on the worker. It computes a deterministic manifest hash —
``sha256`` over a sorted ``(relative_key, sha256(data))`` table — so
re-runs against unchanged input produce byte-identical hashes that
double as the ``content_hash`` on the resulting Docverse build row.
"""

from __future__ import annotations

import asyncio
import hashlib
import mimetypes
from dataclasses import dataclass

import structlog

from docverse.storage.objectstore import ObjectStore

from .s3_source import LtdSourceProtocol

__all__ = ["BuildContentCopier", "CopyResult"]

#: Default semaphore bound for parallel object copies.
_DEFAULT_MAX_CONCURRENT = 8


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
                content_hash=_empty_manifest_hash(),
            )

        semaphore = asyncio.Semaphore(self._max_concurrent)
        in_flight = _ConcurrencyTracker()
        manifest_entries: list[tuple[str, str, int]] = []
        manifest_lock = asyncio.Lock()

        async def _copy_one(source_key: str) -> None:
            relative = source_key.removeprefix(normalized_source_prefix)
            dest_key = f"{normalized_dest_prefix}{relative}"
            content_type = (
                mimetypes.guess_type(relative)[0] or "application/octet-stream"
            )
            async with semaphore, in_flight:
                data = await self._source.download_object(key=source_key)
                await self._destination.upload_object(
                    key=dest_key,
                    data=data,
                    content_type=content_type,
                )
            digest = hashlib.sha256(data).hexdigest()
            async with manifest_lock:
                manifest_entries.append((relative, digest, len(data)))

        await asyncio.gather(*(_copy_one(k) for k in keys))

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


def _hash_manifest(entries: list[tuple[str, str, int]]) -> str:
    r"""Compute ``sha256:`` over sorted ``relative\tdigest\n`` lines.

    Size is deliberately excluded from the manifest line because it is
    derivable from the data; including it would couple the hash to a
    second representation of the same fact and risk drift.
    """
    hasher = hashlib.sha256()
    for relative, digest, _ in entries:
        hasher.update(f"{relative}\t{digest}\n".encode())
    return f"sha256:{hasher.hexdigest()}"


def _empty_manifest_hash() -> str:
    return f"sha256:{hashlib.sha256(b'').hexdigest()}"
