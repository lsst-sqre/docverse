"""Coalescing of per-hostname CDN cache purges across publish jobs."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

__all__ = [
    "DEFAULT_PURGE_MIN_INTERVAL_SECONDS",
    "CdnPurgeCoalescer",
]


DEFAULT_PURGE_MIN_INTERVAL_SECONDS = 2.0
"""Default minimum spacing, in seconds, between purges of one hostname."""


_IDLE_RETENTION_FACTOR = 10
"""Multiple of the min interval an idle hostname's state is retained for."""


@dataclass(slots=True)
class _HostnameState:
    """Coalescing state for a single hostname."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    """Serializes the purge slot so only one caller purges at a time."""

    requested: int = 0
    """Number of purge requests seen for this hostname so far."""

    served: int = 0
    """Highest request sequence covered by a completed purge."""

    last_started: float | None = None
    """``time.monotonic()`` when the most recent purge attempt started."""

    users: int = 0
    """Callers currently inside :meth:`CdnPurgeCoalescer.purge`."""


class CdnPurgeCoalescer:
    """Fold repeated purges of one hostname into a throttled sequence.

    Every edition of a project purges the same project hostname, because
    hostname purging is the one mechanism available on every Cloudflare
    plan tier (see
    `~docverse_server.storage.cdncachepurger.CdnCachePurger`). Publishing
    a single release therefore emits several byte-identical purge calls
    (the release plus its semver aggregate editions), and a bulk
    keeper-sync backfill turns that into hundreds — enough to trip
    Cloudflare's purge rate limit.

    This coalescer sits in front of the purger and enforces two rules per
    hostname:

    * at most one purge runs at a time, and
    * successive purges are spaced by at least ``min_interval`` seconds.

    Requests that arrive while a purge is pending are absorbed by it
    rather than queuing another one.

    The invariant that makes the absorption safe is **happens-after**:
    a request is only ever marked served by a purge that *started after
    that request was registered*. The count of outstanding requests is
    snapshotted immediately before the purge fires, so a request that
    arrives mid-purge is deliberately left unserved and gets its own
    (throttled) purge. Dropping such a request would leave the edge
    holding stale content indefinitely, which is strictly worse than an
    extra purge call.

    Coalescing is per instance, and therefore per worker process: the
    process-lifetime instance lives on
    `~docverse_server.worker.main.WorkerFactoryBuilder` and is shared by
    every job that process runs. Replicas do not coordinate, so a burst
    spread over ``N`` worker pods folds to at most ``N`` concurrent
    purge streams rather than one. That is deliberate — cross-process
    coalescing would need shared state and a distributed lock, for a
    constant-factor gain over the per-process fold.
    """

    def __init__(
        self,
        *,
        min_interval: float = DEFAULT_PURGE_MIN_INTERVAL_SECONDS,
    ) -> None:
        self._min_interval = max(0.0, min_interval)
        self._states: dict[str, _HostnameState] = {}

    async def purge(
        self,
        hostname: str,
        purge: Callable[[], Awaitable[None]],
    ) -> bool:
        """Run ``purge`` for ``hostname`` unless another call covers it.

        Parameters
        ----------
        hostname
            Hostname the purge would invalidate. Requests are coalesced
            per hostname, which is the CDN-side resource identity: two
            projects never share a hostname, and every edition of one
            project shares exactly one.
        purge
            Zero-argument coroutine function performing the purge. It is
            invoked at most once per call and not at all when the
            request is coalesced. It must not touch the database:
            callers wait here — for the lock, and then for the throttle
            interval — with no transaction open precisely so those waits
            hold no connection, so anything the purge needs from the
            database (the org's CDN credentials) must already be
            resolved and captured by the closure.

        Returns
        -------
        bool
            `True` when this call ran ``purge``, `False` when it was
            absorbed into a concurrent purge that happened after it.

        Raises
        ------
        Exception
            Whatever ``purge`` raises, to the caller that ran it. A
            failed purge does **not** mark waiters served: the next
            waiter re-attempts once the throttle interval has elapsed,
            so a coalesced burst degrades to the un-coalesced call
            volume under a persistent CDN failure instead of hiding it.
            Callers that treat purging as best-effort are responsible
            for logging and swallowing the error.
        """
        state = self._states.get(hostname)
        if state is None:
            state = _HostnameState()
            self._states[hostname] = state
        # No await between here and the lock acquisition, so the
        # sequence number and the user count cannot interleave.
        state.requested += 1
        sequence = state.requested
        state.users += 1
        try:
            async with state.lock:
                if state.served >= sequence:
                    return False
                await self._wait_for_slot(state)
                # Snapshot before firing: everything requested up to
                # this point is covered by a purge that starts after it,
                # and everything later is not.
                covered = state.requested
                state.last_started = time.monotonic()
                await purge()
                state.served = max(state.served, covered)
                return True
        finally:
            state.users -= 1
            self._prune()

    async def _wait_for_slot(self, state: _HostnameState) -> None:
        """Sleep out the remainder of this hostname's throttle interval.

        Returns immediately for the first purge of a hostname, so an
        isolated publish — the interactive build-upload case — pays no
        added latency. Only a purge that follows a recent one waits, and
        the wait is what lets later requests pile into the same purge.
        """
        if self._min_interval <= 0 or state.last_started is None:
            return
        remaining = self._min_interval - (
            time.monotonic() - state.last_started
        )
        if remaining > 0:
            await asyncio.sleep(remaining)

    def _prune(self) -> None:
        """Drop state for hostnames that are idle and past their throttle.

        Keeps the map bounded in a long-lived worker without discarding
        the ``last_started`` watermark while it can still space a purge:
        an entry is only removed once nobody is using it *and* its
        throttle interval has long since elapsed.
        """
        retention = max(self._min_interval, 1.0) * _IDLE_RETENTION_FACTOR
        now = time.monotonic()
        stale = [
            hostname
            for hostname, state in self._states.items()
            if state.users == 0
            and (
                state.last_started is None
                or now - state.last_started > retention
            )
        ]
        for hostname in stale:
            del self._states[hostname]
