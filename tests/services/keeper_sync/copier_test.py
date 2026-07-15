"""Tests for ``BuildContentCopier``.

Drives the copier with an in-memory fake LTD source and the in-tree
``MockObjectStore`` so concurrency, ordering, and replay-after-failure
can be asserted without S3 or R2.
"""

from __future__ import annotations

import asyncio
import hashlib

import pytest
import structlog

from docverse.services.keeper_sync.copier import BuildContentCopier
from docverse.storage.ltd import LtdSourceProtocol
from docverse.storage.objectstore import MockObjectStore


class _FakeSource(LtdSourceProtocol):
    """In-memory LTD source for copier tests."""

    def __init__(
        self,
        objects: dict[str, bytes],
        *,
        download_delay: float = 0.0,
        fail_once_for: set[str] | None = None,
    ) -> None:
        self._objects = objects
        self._download_delay = download_delay
        self._failed: set[str] = set()
        self._fail_once_for = fail_once_for or set()

    async def list_keys(self, *, prefix: str) -> list[str]:
        return [k for k in self._objects if k.startswith(prefix)]

    async def download_object(self, *, key: str) -> bytes:
        if key in self._fail_once_for and key not in self._failed:
            self._failed.add(key)
            msg = f"Simulated transient download error for {key}"
            raise RuntimeError(msg)
        if self._download_delay:
            await asyncio.sleep(self._download_delay)
        return self._objects[key]


def _logger() -> structlog.stdlib.BoundLogger:
    logger: structlog.stdlib.BoundLogger = structlog.get_logger("test")
    return logger


def _expected_manifest_hash(entries: list[tuple[str, bytes]]) -> str:
    """Mirror the production hash function for the assertion."""
    hasher = hashlib.sha256()
    for relative, data in sorted(entries, key=lambda e: e[0]):
        digest = hashlib.sha256(data).hexdigest()
        hasher.update(f"{relative}\t{digest}\n".encode())
    return f"sha256:{hasher.hexdigest()}"


@pytest.mark.asyncio
async def test_copy_writes_keys_under_dest_prefix() -> None:
    source_objects = {
        "pipelines/builds/42/index.html": b"<html>1</html>",
        "pipelines/builds/42/assets/app.css": b"body{}",
    }
    source = _FakeSource(source_objects)
    dest = MockObjectStore()
    copier = BuildContentCopier(
        source=source, destination=dest, logger=_logger()
    )

    result = await copier.copy_build(
        source_prefix="pipelines/builds/42/",
        dest_prefix="pipelines/__builds/AAAA/",
    )

    assert result.object_count == 2
    assert result.total_size_bytes == sum(
        len(v) for v in source_objects.values()
    )
    assert "pipelines/__builds/AAAA/index.html" in dest.objects
    assert "pipelines/__builds/AAAA/assets/app.css" in dest.objects
    assert dest.objects["pipelines/__builds/AAAA/index.html"].data == (
        b"<html>1</html>"
    )


@pytest.mark.asyncio
async def test_manifest_hash_is_deterministic_across_runs() -> None:
    objects = {
        "src/builds/1/a.html": b"A",
        "src/builds/1/b/c.css": b"BC",
    }
    expected = _expected_manifest_hash(
        [
            ("a.html", b"A"),
            ("b/c.css", b"BC"),
        ]
    )

    first = await BuildContentCopier(
        source=_FakeSource(objects),
        destination=MockObjectStore(),
        logger=_logger(),
    ).copy_build(source_prefix="src/builds/1/", dest_prefix="dst/")
    second = await BuildContentCopier(
        source=_FakeSource(objects),
        destination=MockObjectStore(),
        logger=_logger(),
    ).copy_build(source_prefix="src/builds/1/", dest_prefix="dst/")

    assert first.content_hash == second.content_hash == expected


@pytest.mark.asyncio
async def test_manifest_hash_independent_of_dest_prefix() -> None:
    """Hash is over relative key + content, so the dest is irrelevant."""
    objects = {"src/builds/1/a.html": b"A"}
    a = await BuildContentCopier(
        source=_FakeSource(objects),
        destination=MockObjectStore(),
        logger=_logger(),
    ).copy_build(source_prefix="src/builds/1/", dest_prefix="dst-x/")
    b = await BuildContentCopier(
        source=_FakeSource(objects),
        destination=MockObjectStore(),
        logger=_logger(),
    ).copy_build(source_prefix="src/builds/1/", dest_prefix="dst-y/")
    assert a.content_hash == b.content_hash


@pytest.mark.asyncio
async def test_replay_after_partial_failure_succeeds() -> None:
    """A flaky source recovers cleanly on replay (idempotent destination)."""
    objects = {
        "src/builds/1/a.html": b"A",
        "src/builds/1/b.html": b"B",
    }
    source = _FakeSource(objects, fail_once_for={"src/builds/1/b.html"})
    dest = MockObjectStore()
    copier = BuildContentCopier(
        source=source, destination=dest, logger=_logger()
    )

    with pytest.raises(RuntimeError, match="Simulated transient"):
        await copier.copy_build(
            source_prefix="src/builds/1/", dest_prefix="dst/"
        )
    # Replay: the previously-failing key now succeeds.
    result = await copier.copy_build(
        source_prefix="src/builds/1/", dest_prefix="dst/"
    )
    assert result.object_count == 2
    assert result.content_hash == _expected_manifest_hash(
        [("a.html", b"A"), ("b.html", b"B")]
    )
    assert dest.objects["dst/a.html"].data == b"A"
    assert dest.objects["dst/b.html"].data == b"B"


@pytest.mark.asyncio
async def test_empty_source_prefix_produces_empty_manifest_hash() -> None:
    source = _FakeSource({})
    result = await BuildContentCopier(
        source=source, destination=MockObjectStore(), logger=_logger()
    ).copy_build(source_prefix="src/builds/1/", dest_prefix="dst/")
    assert result.object_count == 0
    assert result.total_size_bytes == 0
    assert result.content_hash == (f"sha256:{hashlib.sha256(b'').hexdigest()}")


@pytest.mark.asyncio
async def test_relative_key_with_dotdot_segment_raises() -> None:
    """A source key whose relative path contains ``..`` must be rejected."""
    objects = {
        "src/builds/1/ok.html": b"OK",
        "src/builds/1/../escape.html": b"BAD",
    }
    dest = MockObjectStore()
    copier = BuildContentCopier(
        source=_FakeSource(objects),
        destination=dest,
        logger=_logger(),
    )

    with pytest.raises(RuntimeError, match=r"\.\."):
        await copier.copy_build(
            source_prefix="src/builds/1/", dest_prefix="dst/"
        )

    # The malicious key never reached the destination.
    assert "dst/../escape.html" not in dest.objects
    assert "../escape.html" not in dest.objects


@pytest.mark.asyncio
async def test_concurrency_observed_peak_does_not_exceed_limit() -> None:
    """Observe the peak via a custom source that records concurrency."""
    in_flight = 0
    peak = 0
    cond = asyncio.Lock()

    objects = {f"src/1/k{i}": b"x" for i in range(15)}

    class _Recorder(LtdSourceProtocol):
        async def list_keys(self, *, prefix: str) -> list[str]:
            return list(objects.keys())

        async def download_object(self, *, key: str) -> bytes:
            nonlocal in_flight, peak
            async with cond:
                in_flight += 1
                peak = max(peak, in_flight)
            await asyncio.sleep(0.01)
            async with cond:
                in_flight -= 1
            return objects[key]

    await BuildContentCopier(
        source=_Recorder(),
        destination=MockObjectStore(),
        logger=_logger(),
        max_concurrent=4,
    ).copy_build(source_prefix="src/1/", dest_prefix="dst/")

    assert peak <= 4
