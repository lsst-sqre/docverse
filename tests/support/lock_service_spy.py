"""Recording spy for ``LockService`` used by worker-level lock tests.

The spy subclasses :class:`docverse.services.lock_service.LockService` so
worker code paths that go through ``Factory.create_lock_service`` keep
their real Postgres advisory-lock behaviour, while each ``acquire``
records an ``("enter" | "exit", lock_key)`` pair into a shared event
list. Tests assert the recorded sequence to verify *which* lock keys
each worker acquires and *in what order* (e.g. the
``BUILD_PROCESSING -> EDITION_UPDATE`` nesting inside
``build_processing``).
"""

from __future__ import annotations

import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Literal

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.factory import Factory
from docverse.services.lock_service import LockKey, LockService

__all__ = ["LockEvent", "RecordingLockService", "install_recording_lock_service"]


@dataclass(frozen=True, slots=True)
class LockEvent:
    """One observed lock acquire/release event."""

    event: Literal["enter", "exit"]
    lock_key: LockKey
    timestamp: float


class RecordingLockService(LockService):
    """``LockService`` that records every acquire/release event.

    Delegates to the real ``LockService.acquire`` so the worker still
    serializes against concurrent jobs in tests that need it. The
    recorded events live on the shared list passed at construction so
    multiple ``RecordingLockService`` instances created from the same
    factory share one event log.
    """

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
        events: list[LockEvent],
    ) -> None:
        super().__init__(session=session, logger=logger)
        self._events = events

    @asynccontextmanager
    async def acquire(self, lock_key: LockKey) -> AsyncGenerator[None]:
        """Acquire ``lock_key`` and record entry/exit on the shared list."""
        async with super().acquire(lock_key):
            self._events.append(
                LockEvent(
                    event="enter", lock_key=lock_key, timestamp=time.monotonic()
                )
            )
            try:
                yield
            finally:
                self._events.append(
                    LockEvent(
                        event="exit",
                        lock_key=lock_key,
                        timestamp=time.monotonic(),
                    )
                )


def install_recording_lock_service(
    monkeypatch: pytest.MonkeyPatch,
) -> list[LockEvent]:
    """Patch ``Factory.create_lock_service`` to return a recording spy.

    Returns a list that accumulates :class:`LockEvent` records as
    workers acquire and release advisory locks. All ``LockService``
    instances built by the patched factory share the same list so a
    single end-to-end run produces one ordered event log.
    """
    events: list[LockEvent] = []

    def _create(self: Factory) -> RecordingLockService:
        return RecordingLockService(
            session=self._session,  # noqa: SLF001
            logger=self._logger,  # noqa: SLF001
            events=events,
        )

    monkeypatch.setattr(Factory, "create_lock_service", _create)
    return events
