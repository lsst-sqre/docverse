"""Tests for CdnPurgeCoalescer."""

from __future__ import annotations

import asyncio

import pytest

from docverse_server.services.cdn_purge_coalescer import CdnPurgeCoalescer

_HOSTNAME = "proj.example.org"


class _RecordingPurge:
    """Records each purge invocation and optionally stalls in it."""

    def __init__(self, *, delay: float = 0.0) -> None:
        self.calls = 0
        self.started = asyncio.Event()
        self._delay = delay

    async def __call__(self) -> None:
        self.calls += 1
        self.started.set()
        if self._delay:
            await asyncio.sleep(self._delay)


class _FailingPurge:
    """Records each invocation and always raises."""

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> None:
        self.calls += 1
        msg = "purge exploded"
        raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_single_request_purges_immediately() -> None:
    """One request with no recent purge runs the purge without waiting."""
    coalescer = CdnPurgeCoalescer(min_interval=10.0)
    purge = _RecordingPurge()

    loop = asyncio.get_running_loop()
    started = loop.time()
    purged = await coalescer.purge(_HOSTNAME, purge)
    elapsed = loop.time() - started

    assert purged is True
    assert purge.calls == 1
    assert elapsed < 1.0


@pytest.mark.asyncio
async def test_concurrent_requests_coalesce_to_a_bounded_purge_count() -> None:
    """A burst of N requests for one hostname purges far fewer times."""
    coalescer = CdnPurgeCoalescer(min_interval=0.05)
    purge = _RecordingPurge(delay=0.01)
    burst = 20

    results = await asyncio.gather(
        *(coalescer.purge(_HOSTNAME, purge) for _ in range(burst))
    )

    # The first request finds an idle hostname and purges straight away,
    # covering only itself; the rest pile into a single throttled purge.
    assert purge.calls <= 2
    assert sum(results) == purge.calls
    assert results[0] is True


@pytest.mark.asyncio
async def test_request_arriving_mid_purge_gets_a_later_purge() -> None:
    """A request registered after the snapshot is not absorbed by it."""
    coalescer = CdnPurgeCoalescer(min_interval=0.05)
    purge = _RecordingPurge(delay=0.05)

    first = asyncio.create_task(coalescer.purge(_HOSTNAME, purge))
    # Let the first request register and enter its purge before the
    # second one arrives, so the second cannot be covered by the
    # snapshot the first took.
    await purge.started.wait()
    second = await coalescer.purge(_HOSTNAME, purge)

    assert await first is True
    assert second is True
    assert purge.calls == 2


@pytest.mark.asyncio
async def test_distinct_hostnames_do_not_coalesce() -> None:
    """Hostnames are independent: neither throttles nor absorbs the other."""
    coalescer = CdnPurgeCoalescer(min_interval=10.0)
    purge = _RecordingPurge()

    loop = asyncio.get_running_loop()
    started = loop.time()
    first = await coalescer.purge(_HOSTNAME, purge)
    second = await coalescer.purge("other.example.org", purge)
    elapsed = loop.time() - started

    assert first is True
    assert second is True
    assert purge.calls == 2
    assert elapsed < 1.0


@pytest.mark.asyncio
async def test_second_purge_waits_out_the_throttle_interval() -> None:
    """A sequential repeat purge is spaced by the minimum interval."""
    coalescer = CdnPurgeCoalescer(min_interval=0.2)
    purge = _RecordingPurge()

    loop = asyncio.get_running_loop()
    await coalescer.purge(_HOSTNAME, purge)
    started = loop.time()
    purged = await coalescer.purge(_HOSTNAME, purge)
    elapsed = loop.time() - started

    assert purged is True
    assert purge.calls == 2
    assert elapsed >= 0.1


@pytest.mark.asyncio
async def test_failed_purge_does_not_mark_waiters_served() -> None:
    """A raising purge leaves the burst uncovered, so a waiter retries."""
    coalescer = CdnPurgeCoalescer(min_interval=0.0)
    purge = _FailingPurge()

    results = await asyncio.gather(
        *(coalescer.purge(_HOSTNAME, purge) for _ in range(3)),
        return_exceptions=True,
    )

    assert purge.calls == 3
    assert all(isinstance(result, RuntimeError) for result in results)
