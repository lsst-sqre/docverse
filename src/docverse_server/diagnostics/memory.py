"""Opt-in process memory sampler (PRD #753).

The Docverse pods run with a read-only root filesystem, as non-root,
with every capability dropped, so no profiler can attach to a running
process and nothing can be exec'd in to take a heap snapshot. To find
where a worker's memory goes, the process has to report on itself:
:class:`MemorySampler` logs one ``"Memory sample"`` line per interval
carrying the fields of a :class:`MemorySample`, and those lines are read
back from the pod logs.

:func:`read_memory_sample` does the reading and nothing else — no
logging, no timers — so every field can be tested against injected
``/proc/self/status`` text and real tracemalloc snapshots. The sampler
is the thin loop around it. Nothing here starts on import; the
``memory_diagnostics_*`` settings decide whether a process runs a
sampler at all.
"""

from __future__ import annotations

import asyncio
import gc
import itertools
import resource
import sys
import tracemalloc
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from structlog.stdlib import BoundLogger

__all__ = [
    "PROC_STATUS_PATH",
    "MemorySample",
    "MemorySampler",
    "read_memory_sample",
    "read_proc_status",
]

PROC_STATUS_PATH = Path("/proc/self/status")
"""Where Linux reports this process's resident size and high-water mark."""

_SNAPSHOT_FILTERS = (tracemalloc.Filter(False, tracemalloc.__file__),)
"""Leave out tracemalloc's own allocations, above all the snapshots.

The sampler keeps each snapshot until the next tick diffs against it,
and a snapshot's traces are allocated inside ``tracemalloc.py``; without
this filter every tick would report the previous tick's snapshot as a
growing site in the process.
"""


@dataclass(frozen=True, slots=True)
class MemorySample:
    """One reading of the process's memory state.

    The tracemalloc-derived fields are ``None`` (``top_sites`` empty)
    whenever tracemalloc is not tracing; :meth:`log_fields` then leaves
    them off the log line rather than logging a row of nulls.
    """

    rss_bytes: int | None
    """Resident set size (``VmRSS``), or ``None`` without ``/proc``.

    ``getrusage`` reports only a high-water mark, so on a platform with
    no ``/proc/self/status`` (macOS) the current size is unknown rather
    than guessed.
    """

    rss_peak_bytes: int
    """Resident high-water mark: ``VmHWM``, else ``getrusage`` ``ru_maxrss``.

    It never falls, so after a burst it records how high the process
    went even once ``rss_bytes`` has settled.
    """

    gc_counts: tuple[int, ...]
    """The garbage collector's per-generation counts (``gc.get_count()``)."""

    gc_objects: int | None
    """Objects the collector tracks (``len(gc.get_objects())``).

    Counted only while tracemalloc is tracing, since building the list
    walks the whole heap.
    """

    tracemalloc_current_bytes: int | None
    """Size of the traced Python heap now."""

    tracemalloc_peak_bytes: int | None
    """Largest the traced Python heap has been since tracing started."""

    top_sites: tuple[str, ...]
    """Allocation sites whose traced size changed most since the last sample.

    Each reads ``file:line <- file:line ... size_diff count_diff``: the
    allocating frame first, then its callers, as many frames as
    tracemalloc records; then the signed change in bytes and in live
    blocks since the previous snapshot (or since nothing, on the first
    sample). Largest absolute size change first; unchanged sites are
    omitted.
    """

    def log_fields(self) -> dict[str, Any]:
        """Render the sample as structlog key-value pairs.

        The tracemalloc-only fields appear only when this sample was
        taken while tracing.
        """
        fields: dict[str, Any] = {
            "rss_bytes": self.rss_bytes,
            "rss_peak_bytes": self.rss_peak_bytes,
            "gc_counts": self.gc_counts,
        }
        if self.tracemalloc_current_bytes is not None:
            fields.update(
                gc_objects=self.gc_objects,
                tracemalloc_current_bytes=self.tracemalloc_current_bytes,
                tracemalloc_peak_bytes=self.tracemalloc_peak_bytes,
                top_sites=self.top_sites,
            )
        return fields


def read_proc_status() -> str | None:
    """Read ``/proc/self/status``, or return ``None`` where it does not exist.

    Only absence means "not Linux" and selects the ``getrusage``
    fallback. Any other failure to read it is raised, so a sampler tick
    reports it instead of quietly logging a different kind of peak.
    """
    try:
        return PROC_STATUS_PATH.read_text()
    except FileNotFoundError:
        return None


def read_memory_sample(
    previous_snapshot: tracemalloc.Snapshot | None,
    *,
    top_n: int,
    proc_status_text: str | None = None,
) -> tuple[MemorySample, tracemalloc.Snapshot | None]:
    """Read the process's current memory state.

    Pure apart from reading process state: it logs nothing and changes
    nothing, including tracemalloc's own peak.

    Parameters
    ----------
    previous_snapshot
        The snapshot returned by the previous call, which ``top_sites``
        is diffed against. ``None`` diffs against an empty heap, so the
        first sample reports the largest traced sites outright.
    top_n
        Most allocation sites to report in ``top_sites``.
    proc_status_text
        Contents of ``/proc/self/status`` (see `read_proc_status`), or
        ``None`` where the platform has none, in which case the peak
        comes from ``getrusage`` and ``rss_bytes`` is ``None``.

    Returns
    -------
    tuple of (MemorySample, tracemalloc.Snapshot or None)
        The sample, and the snapshot to pass as ``previous_snapshot``
        next time — ``None`` when tracemalloc is not tracing, in which
        case every tracemalloc-derived field of the sample is empty.

    Raises
    ------
    ValueError
        Raised if ``proc_status_text`` has a ``VmRSS`` or ``VmHWM`` line
        that is not a ``kB`` amount.
    """
    proc_fields = _parse_proc_status(proc_status_text or "")
    rss_bytes = proc_fields.get("VmRSS")
    rss_peak_bytes = proc_fields.get("VmHWM")
    if rss_peak_bytes is None:
        rss_peak_bytes = _rusage_peak_bytes()
    gc_counts = tuple(gc.get_count())

    if not tracemalloc.is_tracing():
        untraced = MemorySample(
            rss_bytes=rss_bytes,
            rss_peak_bytes=rss_peak_bytes,
            gc_counts=gc_counts,
            gc_objects=None,
            tracemalloc_current_bytes=None,
            tracemalloc_peak_bytes=None,
            top_sites=(),
        )
        return untraced, None

    # Counted before the snapshot so the object list is already freed
    # and cannot show up as a growing site.
    gc_objects = len(gc.get_objects())
    current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    snapshot = tracemalloc.take_snapshot().filter_traces(_SNAPSHOT_FILTERS)
    if previous_snapshot is None:
        previous_snapshot = tracemalloc.Snapshot((), snapshot.traceback_limit)
    changed = (
        diff
        for diff in snapshot.compare_to(previous_snapshot, "traceback")
        if diff.size_diff != 0
    )
    top_sites = tuple(
        _format_site(diff) for diff in itertools.islice(changed, top_n)
    )
    sample = MemorySample(
        rss_bytes=rss_bytes,
        rss_peak_bytes=rss_peak_bytes,
        gc_counts=gc_counts,
        gc_objects=gc_objects,
        tracemalloc_current_bytes=current_bytes,
        tracemalloc_peak_bytes=peak_bytes,
        top_sites=top_sites,
    )
    return sample, snapshot


class MemorySampler:
    """Log a `MemorySample` every interval from a background task.

    Parameters
    ----------
    interval_seconds
        Seconds between samples. The first is taken as soon as the task
        runs, so the log has a baseline from process start.
    tracemalloc_enabled
        Whether `start` turns tracemalloc on, adding the traced heap
        totals, object count, and top sites to each sample.
    tracemalloc_frames
        Frames tracemalloc records per allocation, and so the depth at
        which top sites are told apart.
    top_n
        Most allocation sites to log per sample.
    logger
        Logger for the sample lines.
    component
        Which process this is (``api``, ``worker``,
        ``worker-keeper-sync``, ``worker-maintenance``), logged on every
        line so the pools can be told apart.
    """

    def __init__(
        self,
        *,
        interval_seconds: float,
        tracemalloc_enabled: bool,
        tracemalloc_frames: int,
        top_n: int,
        logger: BoundLogger,
        component: str,
    ) -> None:
        self._interval_seconds = interval_seconds
        self._tracemalloc_enabled = tracemalloc_enabled
        self._tracemalloc_frames = tracemalloc_frames
        self._top_n = top_n
        self._logger = logger
        self._component = component
        self._task: asyncio.Task[None] | None = None
        self._started_tracing = False

    async def start(self) -> None:
        """Start tracing if configured, and the sampling task.

        Returns at once: the first sample is taken by the task, not
        here. Tracing that is already on (``PYTHONTRACEMALLOC``) is left
        as it is, at its own depth, and `stop` then leaves it running.
        The ``"Memory diagnostics enabled"`` line reports the tracing in
        effect, since that is what the samples will carry.
        """
        if self._task is not None:
            return
        if self._tracemalloc_enabled and not tracemalloc.is_tracing():
            tracemalloc.start(self._tracemalloc_frames)
            self._started_tracing = True
        tracing = tracemalloc.is_tracing()
        self._logger.info(
            "Memory diagnostics enabled",
            component=self._component,
            interval_seconds=self._interval_seconds,
            tracemalloc_enabled=tracing,
            tracemalloc_frames=(
                tracemalloc.get_traceback_limit()
                if tracing
                else self._tracemalloc_frames
            ),
            top_n=self._top_n,
        )
        self._task = asyncio.create_task(
            self._run(), name=f"memory-sampler-{self._component}"
        )

    async def stop(self) -> None:
        """Cancel the sampling task, and stop tracing if `start` began it."""
        task, self._task = self._task, None
        try:
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    # The task's own cancellation is expected; only a
                    # cancellation of this stop() itself propagates.
                    current = asyncio.current_task()
                    if current is not None and current.cancelling():
                        raise
        finally:
            if self._started_tracing:
                tracemalloc.stop()
                self._started_tracing = False

    async def _run(self) -> None:
        """Sample, log, and sleep until cancelled; never raise."""
        previous: tracemalloc.Snapshot | None = None
        while True:
            try:
                # Off the event loop: with tracing on, a snapshot and its
                # diff walk the whole heap, which on a large one takes
                # long enough to stall request handling or arq jobs.
                sample, previous = await asyncio.to_thread(
                    self._read, previous
                )
                self._logger.info(
                    "Memory sample",
                    component=self._component,
                    **sample.log_fields(),
                )
            except Exception:
                self._logger.warning(
                    "Memory sample failed",
                    component=self._component,
                    exc_info=True,
                )
            await asyncio.sleep(self._interval_seconds)

    def _read(
        self, previous: tracemalloc.Snapshot | None
    ) -> tuple[MemorySample, tracemalloc.Snapshot | None]:
        return read_memory_sample(
            previous, top_n=self._top_n, proc_status_text=read_proc_status()
        )


def _format_site(diff: tracemalloc.StatisticDiff) -> str:
    """Render one site as ``file:line <- ... size_diff count_diff``."""
    # A traceback iterates oldest frame first; lead with the allocation.
    frames = " <- ".join(
        f"{frame.filename}:{frame.lineno}"
        for frame in reversed(diff.traceback)
    )
    return f"{frames} {diff.size_diff:+d} {diff.count_diff:+d}"


def _parse_proc_status(text: str) -> dict[str, int]:
    """Read ``VmRSS`` and ``VmHWM`` from ``/proc`` text, in bytes."""
    values: dict[str, int] = {}
    for line in text.splitlines():
        key, _, value = line.partition(":")
        if key in {"VmRSS", "VmHWM"}:
            amount, unit = value.split()
            if unit != "kB":
                raise ValueError(f"Unexpected unit in /proc status: {line!r}")
            values[key] = int(amount) * 1024
    return values


def _rusage_peak_bytes(platform: str = sys.platform) -> int:
    """Read the resident high-water mark from ``getrusage``, in bytes.

    ``ru_maxrss`` is in bytes on macOS and in kilobytes on Linux and the
    BSDs.
    """
    ru_maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return ru_maxrss if platform == "darwin" else ru_maxrss * 1024
