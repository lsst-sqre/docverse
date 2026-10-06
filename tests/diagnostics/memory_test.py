"""Tests for the opt-in memory sampler (PRD #753).

``read_memory_sample`` is exercised against injected ``/proc/self/status``
text so the RSS parsing runs identically on a macOS laptop and a Linux
CI runner. The tracemalloc cases start real tracing for the duration of
one test; an autouse fixture stops it again so a failing test cannot
leave the rest of the xdist worker running traced.
"""

from __future__ import annotations

import asyncio
import gc
import inspect
import resource
import sys
import tracemalloc
from collections.abc import Iterator, MutableMapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import structlog
from structlog.testing import capture_logs

from docverse_server.config import Configuration
from docverse_server.diagnostics import memory
from docverse_server.diagnostics.memory import (
    MemorySampler,
    read_memory_sample,
    start_memory_sampler,
    stop_memory_sampler,
)

#: A trimmed ``/proc/self/status`` as a Linux kernel prints it.
PROC_STATUS_TEXT = """\
Name:\tpython
Umask:\t0022
State:\tS (sleeping)
VmPeak:\t 1203456 kB
VmSize:\t 1103456 kB
VmHWM:\t  936448 kB
VmRSS:\t  591872 kB
RssAnon:\t  560000 kB
Threads:\t4
"""


@pytest.fixture(autouse=True)
def _stop_tracemalloc() -> Iterator[None]:
    """Leave the worker process untraced whatever the test did."""
    yield
    if tracemalloc.is_tracing():
        tracemalloc.stop()


def test_reads_rss_and_high_water_mark_from_proc_status() -> None:
    """``VmRSS`` and ``VmHWM`` are read in kB and reported in bytes."""
    sample, snapshot = read_memory_sample(
        None, top_n=5, proc_status_text=PROC_STATUS_TEXT
    )

    assert sample.rss_bytes == 591872 * 1024
    assert sample.rss_peak_bytes == 936448 * 1024
    assert len(sample.gc_counts) == len(gc.get_count())
    assert all(isinstance(count, int) for count in sample.gc_counts)
    # Untraced: nothing tracemalloc-derived is sampled, and the O(heap)
    # object count is skipped with it.
    assert sample.gc_objects is None
    assert sample.tracemalloc_current_bytes is None
    assert sample.tracemalloc_peak_bytes is None
    assert sample.top_sites == ()
    assert snapshot is None


def test_proc_status_amount_in_other_units_is_refused() -> None:
    """A ``VmRSS`` not in kB raises rather than logging a wrong size."""
    with pytest.raises(ValueError, match="VmRSS"):
        read_memory_sample(None, top_n=5, proc_status_text="VmRSS:\t  2 MB\n")


def _fake_getrusage(
    monkeypatch: pytest.MonkeyPatch, ru_maxrss: int
) -> list[int]:
    """Make ``resource.getrusage`` report ``ru_maxrss``; record its args."""
    calls: list[int] = []

    def getrusage(who: int) -> SimpleNamespace:
        calls.append(who)
        return SimpleNamespace(ru_maxrss=ru_maxrss)

    monkeypatch.setattr(resource, "getrusage", getrusage)
    return calls


def test_falls_back_to_getrusage_without_proc_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without ``/proc`` (macOS) the peak is ``ru_maxrss``; RSS is unknown.

    ``getrusage`` has no current-RSS figure, so ``rss_bytes`` is reported
    as ``None`` rather than guessed.
    """
    calls = _fake_getrusage(monkeypatch, 4096)

    sample, _ = read_memory_sample(None, top_n=5, proc_status_text=None)

    assert calls == [resource.RUSAGE_SELF]
    assert sample.rss_bytes is None
    unit = 1 if sys.platform == "darwin" else 1024
    assert sample.rss_peak_bytes == 4096 * unit


@pytest.mark.parametrize(
    ("platform", "expected"), [("darwin", 4096), ("linux", 4096 * 1024)]
)
def test_ru_maxrss_units_follow_the_platform(
    monkeypatch: pytest.MonkeyPatch, platform: str, expected: int
) -> None:
    """``ru_maxrss`` is bytes on macOS and kilobytes everywhere else."""
    _fake_getrusage(monkeypatch, 4096)

    assert memory._rusage_peak_bytes(platform) == expected


def test_proc_status_without_high_water_mark_uses_getrusage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``/proc`` text lacking ``VmHWM`` still yields a peak."""
    _fake_getrusage(monkeypatch, 4096)

    sample, _ = read_memory_sample(
        None, top_n=5, proc_status_text="VmRSS:\t  2048 kB\n"
    )

    assert sample.rss_bytes == 2048 * 1024
    assert sample.rss_peak_bytes == memory._rusage_peak_bytes()


def _allocate(blocks: int) -> tuple[list[bytearray], str, str]:
    """Allocate ``blocks`` KiB and say where, as tracemalloc renders it.

    Returns the buffers (the caller keeps them alive across the sample),
    this function's allocating line, and the caller's calling line. Two
    calls from different lines share the first frame and differ in the
    second, which is what tells a frame depth of one from two.
    """
    frame = inspect.currentframe()
    assert frame is not None
    assert frame.f_back is not None
    caller = f"{__file__}:{frame.f_back.f_lineno}"
    site = f"{__file__}:{frame.f_lineno + 2}"
    del frame
    buffers = [bytearray(1024) for _ in range(blocks)]
    return buffers, site, caller


def _split_site(site: str) -> tuple[str, int, int]:
    """Split a rendered site into its frames, size diff, and count diff."""
    frames, size_diff, count_diff = site.rsplit(" ", 2)
    assert size_diff[0] in "+-"
    assert count_diff[0] in "+-"
    return frames, int(size_diff), int(count_diff)


def test_top_sites_rank_growth_since_the_previous_snapshot() -> None:
    """Sites are ranked by size diff and rendered at the traced depth.

    Each site reads ``file:line <- file:line size_diff count_diff``,
    allocating frame first, with as many frames as tracemalloc records.
    """
    tracemalloc.start(2)
    baseline = tracemalloc.take_snapshot()
    small, small_site, small_caller = _allocate(300)
    big, big_site, big_caller = _allocate(3000)

    sample, snapshot = read_memory_sample(
        baseline, top_n=2, proc_status_text=PROC_STATUS_TEXT
    )

    assert snapshot is not None
    assert len(sample.top_sites) == 2
    big_frames, big_size, big_count = _split_site(sample.top_sites[0])
    small_frames, small_size, small_count = _split_site(sample.top_sites[1])
    assert big_frames == f"{big_site} <- {big_caller}"
    assert small_frames == f"{small_site} <- {small_caller}"
    assert big_size > small_size >= 300 * 1024
    assert big_count > small_count >= 300
    assert len(big) + len(small) == 3300


def test_top_sites_group_at_the_traced_frame_depth() -> None:
    """At a depth of one, two callers of one allocating line are one site."""
    tracemalloc.start(1)
    baseline = tracemalloc.take_snapshot()
    small, site, _ = _allocate(300)
    big, _, _ = _allocate(3000)

    sample, _ = read_memory_sample(
        baseline, top_n=1, proc_status_text=PROC_STATUS_TEXT
    )

    frames, size_diff, count_diff = _split_site(sample.top_sites[0])
    assert frames == site
    assert size_diff >= 3300 * 1024
    assert count_diff >= 3300
    assert len(big) + len(small) == 3300


def test_first_sample_counts_every_traced_allocation_as_growth() -> None:
    """Without a previous snapshot the diff runs against an empty heap.

    The first tick therefore reports the largest traced sites outright,
    and hands back the snapshot the next tick diffs against.
    """
    tracemalloc.start(1)
    buffers, site, _ = _allocate(3000)

    sample, snapshot = read_memory_sample(
        None, top_n=3, proc_status_text=PROC_STATUS_TEXT
    )

    assert snapshot is not None
    assert 0 < len(sample.top_sites) <= 3
    frames, size_diff, _ = _split_site(sample.top_sites[0])
    assert frames == site
    assert size_diff >= 3000 * 1024
    assert all(_split_site(s)[1] > 0 for s in sample.top_sites)
    assert len(buffers) == 3000


def test_traced_sample_carries_heap_totals_and_object_count() -> None:
    """Tracing adds traced current/peak bytes and the live object count."""
    tracemalloc.start(1)
    buffers, _, _ = _allocate(3000)

    sample, _ = read_memory_sample(
        None, top_n=3, proc_status_text=PROC_STATUS_TEXT
    )

    assert sample.tracemalloc_current_bytes is not None
    assert sample.tracemalloc_peak_bytes is not None
    assert sample.tracemalloc_current_bytes >= 3000 * 1024
    assert sample.tracemalloc_peak_bytes >= sample.tracemalloc_current_bytes
    assert sample.gc_objects is not None
    assert sample.gc_objects > 3000
    assert len(buffers) == 3000


def test_top_sites_exclude_the_sampler_s_own_snapshots() -> None:
    """The snapshot kept for the next diff is not reported as growth."""
    tracemalloc.start(1)
    _, first = read_memory_sample(
        None, top_n=50, proc_status_text=PROC_STATUS_TEXT
    )

    sample, _ = read_memory_sample(
        first, top_n=50, proc_status_text=PROC_STATUS_TEXT
    )

    assert not [s for s in sample.top_sites if tracemalloc.__file__ in s]
    assert all(_split_site(s)[1] != 0 for s in sample.top_sites)


def test_read_proc_status_returns_the_file_text(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """On Linux the sampler hands ``/proc/self/status`` to the parser."""
    status = tmp_path / "status"
    status.write_text(PROC_STATUS_TEXT)
    monkeypatch.setattr(memory, "PROC_STATUS_PATH", status)

    assert memory.read_proc_status() == PROC_STATUS_TEXT


def test_read_proc_status_is_none_without_procfs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A missing file (macOS) selects the ``getrusage`` fallback."""
    monkeypatch.setattr(memory, "PROC_STATUS_PATH", tmp_path / "missing")

    assert memory.read_proc_status() is None


def test_read_proc_status_raises_when_unreadable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Any other read failure is an error the sampler's tick reports.

    Only absence means "not Linux"; a ``/proc`` that exists but cannot be
    read must not silently turn into a ``getrusage`` peak.
    """
    monkeypatch.setattr(memory, "PROC_STATUS_PATH", tmp_path)

    with pytest.raises(OSError, match="directory"):
        memory.read_proc_status()


_TRACED_ONLY_FIELDS = (
    "gc_objects",
    "tracemalloc_current_bytes",
    "tracemalloc_peak_bytes",
    "top_sites",
)
"""Sample fields logged only while tracemalloc is tracing."""


def _events(
    captured: list[MutableMapping[str, Any]], event: str
) -> list[MutableMapping[str, Any]]:
    return [entry for entry in captured if entry["event"] == event]


async def _wait_for_samples(
    captured: list[MutableMapping[str, Any]], count: int
) -> list[MutableMapping[str, Any]]:
    """Wait until ``count`` sample lines are logged; fail after 5 s.

    Polls, because ``capture_logs`` appends to a plain list and offers
    nothing to wait on.
    """
    async with asyncio.timeout(5):
        while True:
            samples = _events(captured, "Memory sample")
            if len(samples) >= count:
                return samples
            await asyncio.sleep(0.01)


def _sampler(
    *,
    interval_seconds: float = 0.01,
    tracemalloc_enabled: bool = False,
    tracemalloc_frames: int = 2,
    top_n: int = 3,
) -> MemorySampler:
    # Fetched inside ``capture_logs`` so the logger binds the capturing
    # processors rather than any configured before the test.
    return MemorySampler(
        interval_seconds=interval_seconds,
        tracemalloc_enabled=tracemalloc_enabled,
        tracemalloc_frames=tracemalloc_frames,
        top_n=top_n,
        logger=structlog.get_logger("docverse_server.diagnostics.test"),
        component="worker-keeper-sync",
    )


@pytest.mark.asyncio
async def test_sampler_logs_a_sample_every_interval() -> None:
    """Each tick logs one INFO line tagged with the process's component."""
    with capture_logs() as captured:
        sampler = _sampler()
        await sampler.start()
        try:
            samples = await _wait_for_samples(captured, 2)
        finally:
            await sampler.stop()

    for sample in samples:
        assert sample["log_level"] == "info"
        assert sample["component"] == "worker-keeper-sync"
        assert "rss_bytes" in sample
        assert sample["rss_peak_bytes"] > 0
        assert len(sample["gc_counts"]) == len(gc.get_count())
        assert not [key for key in _TRACED_ONLY_FIELDS if key in sample]
    assert not tracemalloc.is_tracing()


@pytest.mark.asyncio
async def test_sampler_announces_its_config_once() -> None:
    """``start()`` logs the effective configuration exactly once."""
    with capture_logs() as captured:
        sampler = _sampler(interval_seconds=0.01, top_n=7)
        await sampler.start()
        try:
            await _wait_for_samples(captured, 2)
        finally:
            await sampler.stop()

    [enabled] = _events(captured, "Memory diagnostics enabled")
    assert enabled["log_level"] == "info"
    assert enabled["component"] == "worker-keeper-sync"
    assert enabled["interval_seconds"] == 0.01
    assert enabled["tracemalloc_enabled"] is False
    assert enabled["tracemalloc_frames"] == 2
    assert enabled["top_n"] == 7


@pytest.mark.asyncio
async def test_traced_sampler_logs_heap_totals_and_top_sites() -> None:
    """With tracemalloc on, each line also carries the traced fields."""
    with capture_logs() as captured:
        sampler = _sampler(tracemalloc_enabled=True, tracemalloc_frames=2)
        await sampler.start()
        try:
            assert tracemalloc.is_tracing()
            assert tracemalloc.get_traceback_limit() == 2
            samples = await _wait_for_samples(captured, 2)
        finally:
            await sampler.stop()

    for sample in samples:
        assert sample["component"] == "worker-keeper-sync"
        assert sample["tracemalloc_current_bytes"] > 0
        assert (
            sample["tracemalloc_peak_bytes"]
            >= sample["tracemalloc_current_bytes"]
        )
        assert sample["gc_objects"] > 0
        assert len(sample["top_sites"]) <= 3
    assert not tracemalloc.is_tracing()


@pytest.mark.asyncio
async def test_failed_tick_warns_once_and_the_next_tick_fires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tick that raises is logged as a warning; sampling carries on."""
    real_read_proc_status = memory.read_proc_status
    calls = 0

    def unreadable_once() -> str | None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise PermissionError("/proc/self/status")
        return real_read_proc_status()

    monkeypatch.setattr(memory, "read_proc_status", unreadable_once)

    with capture_logs() as captured:
        sampler = _sampler()
        await sampler.start()
        try:
            await _wait_for_samples(captured, 1)
        finally:
            await sampler.stop()

    [warning] = _events(captured, "Memory sample failed")
    assert warning["log_level"] == "warning"
    assert warning["exc_info"] is True
    assert warning["component"] == "worker-keeper-sync"
    assert captured.index(warning) < captured.index(
        _events(captured, "Memory sample")[0]
    )


@pytest.mark.asyncio
async def test_stop_returns_promptly_and_stops_tracing() -> None:
    """``stop()`` cancels a sleeping sampler instead of waiting it out."""
    with capture_logs() as captured:
        sampler = _sampler(interval_seconds=3600, tracemalloc_enabled=True)
        await sampler.start()
        await _wait_for_samples(captured, 1)

        async with asyncio.timeout(1):
            await sampler.stop()

    assert not tracemalloc.is_tracing()


@pytest.mark.asyncio
async def test_stop_leaves_tracing_it_did_not_start() -> None:
    """Tracing already on (``PYTHONTRACEMALLOC``) survives ``stop()``."""
    tracemalloc.start(1)
    with capture_logs():
        sampler = _sampler(tracemalloc_enabled=True, tracemalloc_frames=4)
        await sampler.start()
        await sampler.stop()

    assert tracemalloc.is_tracing()
    assert tracemalloc.get_traceback_limit() == 1


@pytest.mark.asyncio
async def test_announcement_reports_tracing_already_on() -> None:
    """The start line reports the tracing in effect, not just the setting.

    With tracing already on, every sample carries the traced fields at
    the existing depth whatever the settings say, so that is what the
    announcement says too.
    """
    tracemalloc.start(1)
    with capture_logs() as captured:
        sampler = _sampler(tracemalloc_enabled=False, tracemalloc_frames=4)
        await sampler.start()
        await sampler.stop()

    [enabled] = _events(captured, "Memory diagnostics enabled")
    assert enabled["tracemalloc_enabled"] is True
    assert enabled["tracemalloc_frames"] == 1


def _config(**settings: Any) -> Configuration:
    """Build a configuration with the given memory-diagnostics settings."""
    return Configuration(
        **{
            f"memory_diagnostics_{key}": value
            for key, value in settings.items()
        }
    )


@pytest.mark.asyncio
async def test_start_memory_sampler_applies_the_configuration() -> None:
    """Each ``memory_diagnostics_*`` setting reaches the running sampler."""
    config = _config(
        enabled=True,
        interval_seconds=3600,
        tracemalloc_enabled=True,
        tracemalloc_frames=3,
        top_n=4,
    )
    with capture_logs() as captured:
        sampler = await start_memory_sampler(
            config,
            component="api",
            logger=structlog.get_logger("docverse_server.diagnostics.test"),
        )
        assert sampler is not None
        try:
            assert tracemalloc.is_tracing()
            assert tracemalloc.get_traceback_limit() == 3
            [sample] = await _wait_for_samples(captured, 1)
        finally:
            await stop_memory_sampler(
                sampler,
                logger=structlog.get_logger(
                    "docverse_server.diagnostics.test"
                ),
            )

    [enabled] = _events(captured, "Memory diagnostics enabled")
    assert enabled["component"] == "api"
    assert enabled["interval_seconds"] == 3600
    assert enabled["tracemalloc_enabled"] is True
    assert enabled["tracemalloc_frames"] == 3
    assert enabled["top_n"] == 4
    assert sample["component"] == "api"
    assert len(sample["top_sites"]) <= 4
    assert not tracemalloc.is_tracing()


@pytest.mark.asyncio
async def test_start_memory_sampler_is_off_by_default() -> None:
    """With the settings at their defaults nothing starts or is logged."""
    with capture_logs() as captured:
        sampler = await start_memory_sampler(
            _config(),
            component="api",
            logger=structlog.get_logger("docverse_server.diagnostics.test"),
        )

    assert sampler is None
    assert captured == []
    assert not tracemalloc.is_tracing()


class _InfoFailsLogger:
    """A logger whose ``info`` raises, as a broken log sink would."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def info(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("log sink down")

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self._inner.warning(*args, **kwargs)


@pytest.mark.asyncio
async def test_failed_start_is_logged_and_undone() -> None:
    """A sampler that cannot start warns, returns None, and stops tracing.

    Here the announcement fails after ``start()`` has turned tracing on;
    the process must neither fail its startup nor be left traced.
    """
    with capture_logs() as captured:
        logger = _InfoFailsLogger(
            structlog.get_logger("docverse_server.diagnostics.test")
        )
        sampler = await start_memory_sampler(
            _config(enabled=True, tracemalloc_enabled=True),
            component="worker",
            logger=logger,  # type: ignore[arg-type]
        )
        await asyncio.sleep(0.05)

    assert sampler is None
    [warning] = _events(captured, "Memory diagnostics failed to start")
    assert warning["log_level"] == "warning"
    assert warning["component"] == "worker"
    assert warning["exc_info"] is True
    assert _events(captured, "Memory sample") == []
    assert not tracemalloc.is_tracing()


@pytest.mark.asyncio
async def test_failed_stop_is_logged_not_raised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sampler that fails to stop does not fail the process's shutdown."""

    async def failing_stop(self: MemorySampler) -> None:
        raise RuntimeError("stop failed")

    monkeypatch.setattr(MemorySampler, "stop", failing_stop)
    with capture_logs() as captured:
        await stop_memory_sampler(
            _sampler(),
            logger=structlog.get_logger("docverse_server.diagnostics.test"),
        )

    [warning] = _events(captured, "Memory diagnostics failed to stop")
    assert warning["log_level"] == "warning"
    assert warning["component"] == "worker-keeper-sync"
    assert warning["exc_info"] is True
