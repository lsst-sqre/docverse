"""Tests for ``BuildContentCopier``.

Drives the copier with an in-memory fake LTD source and the in-tree
``MockObjectStore`` so concurrency, ordering, and replay-after-failure
can be asserted without S3 or R2.
"""

from __future__ import annotations

import asyncio
import hashlib
import traceback

import pytest
import structlog
from botocore.exceptions import ClientError

from docverse_server.services.keeper_sync.copier import BuildContentCopier
from docverse_server.storage.ltd import (
    LtdSourceAccessDeniedError,
    LtdSourceProtocol,
)
from docverse_server.storage.objectstore import MockObjectStore


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


class _TaskCountingSource(LtdSourceProtocol):
    """Source that records the peak live-task count during a download.

    ``len(asyncio.all_tasks())`` is sampled inside ``download_object``,
    which is the only point at which the copier's fan-out is guaranteed
    to be fully spun up. The recorded peak is compared against a
    baseline captured before the copy starts, so the assertion measures
    only the tasks the copier itself created.
    """

    def __init__(self, keys: list[str]) -> None:
        self._keys = keys
        self.peak_tasks = 0

    async def list_keys(self, *, prefix: str) -> list[str]:
        return list(self._keys)

    async def download_object(self, *, key: str) -> bytes:
        self.peak_tasks = max(self.peak_tasks, len(asyncio.all_tasks()))
        # Yield so a fan-out that spawned one task per key has every one
        # of them alive and countable at the same time.
        await asyncio.sleep(0)
        return b"x"


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
async def test_large_prefix_bounds_live_tasks() -> None:
    """Fan-out is bounded by ``max_concurrent``, not by the key count.

    The pre-worker-pool implementation built one coroutine per key and
    handed the whole batch to ``asyncio.gather``, so a prefix with
    thousands of keys allocated thousands of pending tasks (and, on
    failure, thousands of orphaned buffers) before any download ran.
    """
    key_count = 2000
    max_concurrent = 4
    source = _TaskCountingSource([f"src/1/k{i:05d}" for i in range(key_count)])
    baseline_tasks = len(asyncio.all_tasks())

    result = await BuildContentCopier(
        source=source,
        destination=MockObjectStore(),
        logger=_logger(),
        max_concurrent=max_concurrent,
    ).copy_build(source_prefix="src/1/", dest_prefix="dst/")

    assert result.object_count == key_count
    assert source.peak_tasks <= baseline_tasks + max_concurrent


@pytest.mark.asyncio
async def test_manifest_hash_bounds_live_tasks() -> None:
    """``compute_manifest_hash`` bounds its fan-out the same way."""
    key_count = 2000
    max_concurrent = 4
    source = _TaskCountingSource([f"src/1/k{i:05d}" for i in range(key_count)])
    baseline_tasks = len(asyncio.all_tasks())

    await BuildContentCopier(
        source=source,
        destination=MockObjectStore(),
        logger=_logger(),
        max_concurrent=max_concurrent,
    ).compute_manifest_hash(source_prefix="src/1/")

    assert source.peak_tasks <= baseline_tasks + max_concurrent


@pytest.mark.asyncio
async def test_manifest_hash_bounds_concurrent_downloads() -> None:
    """``compute_manifest_hash`` holds at most ``max_concurrent`` bodies."""
    in_flight = 0
    peak = 0
    objects = {f"src/1/k{i}": b"x" for i in range(15)}

    class _Recorder(LtdSourceProtocol):
        async def list_keys(self, *, prefix: str) -> list[str]:
            return list(objects)

        async def download_object(self, *, key: str) -> bytes:
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.01)
            in_flight -= 1
            return objects[key]

    await BuildContentCopier(
        source=_Recorder(),
        destination=MockObjectStore(),
        logger=_logger(),
        max_concurrent=4,
    ).compute_manifest_hash(source_prefix="src/1/")

    assert peak <= 4


@pytest.mark.asyncio
async def test_failing_key_stops_sibling_downloads() -> None:
    """A mid-iteration failure drains siblings instead of orphaning them.

    Under ``asyncio.gather`` (no ``return_exceptions``) the first error
    propagated to the caller while every sibling task kept running,
    kept downloading, and kept holding its buffer — the shape that
    OOM-killed the sync worker on projects whose oldest builds fail
    with ``AccessDenied`` on every pass. The load-bearing assertion is
    the last one: nothing starts after the copy has raised.
    """
    keys = [f"src/1/k{i:03d}" for i in range(200)]
    max_concurrent = 4
    started: list[str] = []
    completed: list[str] = []

    class _DenyingSource(LtdSourceProtocol):
        async def list_keys(self, *, prefix: str) -> list[str]:
            return list(keys)

        async def download_object(self, *, key: str) -> bytes:
            started.append(key)
            if key == "src/1/k005":
                msg = f"AccessDenied for {key}"
                raise RuntimeError(msg)
            await asyncio.sleep(0.05)
            completed.append(key)
            return b"x"

    copier = BuildContentCopier(
        source=_DenyingSource(),
        destination=MockObjectStore(),
        logger=_logger(),
        max_concurrent=max_concurrent,
    )

    with pytest.raises(RuntimeError, match="AccessDenied"):
        await copier.copy_build(source_prefix="src/1/", dest_prefix="dst/")

    # The pool only ever got a couple of generations of work in before
    # the failing key surfaced — nowhere near all 200 keys — and the
    # siblings that were mid-download when it failed were cancelled
    # rather than allowed to run to completion.
    started_at_failure = list(started)
    assert len(started_at_failure) <= 3 * max_concurrent
    assert len(completed) < len(started_at_failure)

    # No orphaned task survives the raise to pull further keys. This is
    # what ``gather`` got wrong: it raised on the first failure and left
    # every sibling running, so each failed build leaked a whole fan-out.
    await asyncio.sleep(0.2)
    assert started == started_at_failure


@pytest.mark.asyncio
async def test_pool_never_drops_keys_for_a_degenerate_bound() -> None:
    """A bound below 1, or above the key count, still copies every key."""
    objects = {f"src/1/k{i}": b"x" for i in range(5)}
    for max_concurrent in (0, 1, 500):
        dest = MockObjectStore()
        result = await BuildContentCopier(
            source=_FakeSource(objects),
            destination=dest,
            logger=_logger(),
            max_concurrent=max_concurrent,
        ).copy_build(source_prefix="src/1/", dest_prefix="dst/")
        assert result.object_count == len(objects)
        assert len(dest.objects) == len(objects)


@pytest.mark.asyncio
async def test_manifest_hash_matches_copy_build_hash() -> None:
    """Both entry points agree on the manifest hash for one prefix."""
    objects = {
        "src/builds/1/a.html": b"A",
        "src/builds/1/b/c.css": b"BC",
        "src/builds/1/d.js": b"DDD",
    }
    copied = await BuildContentCopier(
        source=_FakeSource(objects),
        destination=MockObjectStore(),
        logger=_logger(),
    ).copy_build(source_prefix="src/builds/1/", dest_prefix="dst/")
    hashed = await BuildContentCopier(
        source=_FakeSource(objects),
        destination=MockObjectStore(),
        logger=_logger(),
    ).compute_manifest_hash(source_prefix="src/builds/1/")

    assert copied.content_hash == hashed


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


class _ChainedDenialSource(LtdSourceProtocol):
    """Source whose downloads raise a botocore-chained Docverse denial.

    Mirrors ``LtdS3Source.download_object``, which raises
    ``LtdSourceAccessDeniedError`` ``from`` the underlying botocore
    ``ClientError`` — the chain that carries the S3 error code and HTTP
    status into Sentry and ``queue_jobs.errors``.
    """

    def __init__(self) -> None:
        self.client_error = ClientError(
            {
                "Error": {"Code": "AccessDenied", "Message": "boom"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            "GetObject",
        )

    async def list_keys(self, *, prefix: str) -> list[str]:
        return [f"{prefix}index.html"]

    async def download_object(self, *, key: str) -> bytes:
        try:
            raise self.client_error
        except ClientError as exc:
            raise LtdSourceAccessDeniedError(
                bucket="lsst-the-docs", key=key, operation="GetObject"
            ) from exc


@pytest.mark.asyncio
async def test_leaf_error_keeps_its_cause_through_the_pool() -> None:
    """The re-raised leaf keeps the ``__cause__`` its raiser gave it.

    ``raise leaf from None`` clobbers the leaf's own chain, so Sentry and
    ``queue_jobs.errors`` saw only the Docverse wrapper with no S3 error
    code or HTTP status underneath it.
    """
    source = _ChainedDenialSource()
    copier = BuildContentCopier(
        source=source, destination=MockObjectStore(), logger=_logger()
    )

    with pytest.raises(LtdSourceAccessDeniedError) as excinfo:
        await copier.copy_build(source_prefix="src/1/", dest_prefix="dst/")

    assert excinfo.value.__cause__ is source.client_error


@pytest.mark.asyncio
async def test_leaf_error_renders_its_cause_not_the_group() -> None:
    """The rendered chain shows the S3 fault, not the TaskGroup wrapper.

    This is the shape Sentry and ``queue_jobs.errors`` store: the
    botocore cause has to survive, and the pool's ``ExceptionGroup``
    must not be tacked on as noise.
    """
    copier = BuildContentCopier(
        source=_ChainedDenialSource(),
        destination=MockObjectStore(),
        logger=_logger(),
    )

    with pytest.raises(LtdSourceAccessDeniedError) as excinfo:
        await copier.copy_build(source_prefix="src/1/", dest_prefix="dst/")

    rendered = "".join(traceback.format_exception(excinfo.value))
    assert "ClientError" in rendered
    assert "AccessDenied" in rendered
    assert "direct cause" in rendered
    assert "unhandled errors in a TaskGroup" not in rendered
    assert "During handling of the above exception" not in rendered


@pytest.mark.asyncio
async def test_uncaused_leaf_error_does_not_render_the_group() -> None:
    """A leaf with no cause of its own still sheds the group context.

    Without an explicit ``__suppress_context__``, a bare ``raise leaf``
    inside the ``except BaseExceptionGroup`` block would install the
    group as ``__context__`` and render it under "During handling of the
    above exception" — the noise the old ``from None`` suppressed.
    """

    class _FailingSource(LtdSourceProtocol):
        async def list_keys(self, *, prefix: str) -> list[str]:
            return [f"{prefix}index.html"]

        async def download_object(self, *, key: str) -> bytes:
            msg = f"Simulated download failure for {key}"
            raise RuntimeError(msg)

    copier = BuildContentCopier(
        source=_FailingSource(),
        destination=MockObjectStore(),
        logger=_logger(),
    )

    with pytest.raises(RuntimeError) as excinfo:
        await copier.copy_build(source_prefix="src/1/", dest_prefix="dst/")

    rendered = "".join(traceback.format_exception(excinfo.value))
    assert "Simulated download failure" in rendered
    assert "unhandled errors in a TaskGroup" not in rendered
    assert "During handling of the above exception" not in rendered
