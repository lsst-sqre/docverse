"""Helpers for driving row-lock races between two database sessions.

A test that has to prove two transactions cannot both win a
read-then-write needs a way to say "the second session is now parked on
the first session's row lock" without sleeping and hoping. These helpers
answer that from ``pg_stat_activity``: take the backend PID of the
session that is about to block, then poll until PostgreSQL reports that
backend waiting on a lock.

Scoping the poll to one PID matters. Waiting for "some backend is
blocked" would also be satisfied before the racing session had issued
its statement at all, which would let the race be won by default and
quietly stop testing the thing under test.
"""

from __future__ import annotations

import asyncio
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

__all__ = [
    "LOCK_WAIT_POLL_INTERVAL",
    "LOCK_WAIT_TIMEOUT",
    "backend_pid",
    "wait_until_blocked_on_lock",
    "wait_until_blocked_or_finished",
]

LOCK_WAIT_TIMEOUT = 10.0
"""Seconds to wait for a backend to park on a lock before giving up."""

LOCK_WAIT_POLL_INTERVAL = 0.02
"""Seconds between ``pg_stat_activity`` polls."""

_BLOCKED_QUERY = text(
    "SELECT count(*) FROM pg_stat_activity"
    " WHERE pid = :pid AND wait_event_type = 'Lock'"
)


async def backend_pid(session: AsyncSession) -> int:
    """Return the PostgreSQL backend PID serving ``session``.

    Reading the PID autobegins a transaction, and that transaction is
    deliberately left open: an :class:`AsyncSession` releases its
    connection back to the engine pool the moment its transaction ends,
    so a session that committed or rolled back here could well run its
    next statement on a different backend — and the PID this returned
    would name a connection nobody is waiting on. The caller ends the
    transaction itself once the race is over.
    """
    pid = await session.scalar(text("SELECT pg_backend_pid()"))
    if pid is None:
        msg = "PostgreSQL did not report a backend PID"
        raise RuntimeError(msg)
    return int(pid)


async def wait_until_blocked_on_lock(
    probe: AsyncSession,
    *,
    pid: int,
    timeout: float = LOCK_WAIT_TIMEOUT,
) -> None:
    """Block until backend ``pid`` is waiting on a lock.

    Parameters
    ----------
    probe
        A session on a *third* connection, used only to read
        ``pg_stat_activity``. It must not be either of the racing
        sessions: both of those are busy, and one of them is blocked.
        Each poll ends its own transaction, because PostgreSQL caches
        the backend status array for the life of a transaction and a
        long-lived one would keep re-reading the same stale snapshot.
    pid
        Backend PID of the session expected to block, from
        `backend_pid`.
    timeout
        Seconds to wait before declaring the race undriveable.

    Raises
    ------
    AssertionError
        If ``pid`` never parks on a lock within ``timeout``. The race
        did not happen, so whatever the test went on to assert would be
        meaningless.
    """
    deadline = time.monotonic() + timeout
    while True:
        blocked = await probe.scalar(_BLOCKED_QUERY, {"pid": pid})
        await probe.rollback()
        if blocked:
            return
        if time.monotonic() >= deadline:
            msg = (
                f"Backend {pid} never blocked on a lock within {timeout}s; "
                f"the race under test did not happen"
            )
            raise AssertionError(msg)
        await asyncio.sleep(LOCK_WAIT_POLL_INTERVAL)


async def wait_until_blocked_or_finished(
    probe: AsyncSession,
    *,
    pid: int,
    task: asyncio.Task[None] | asyncio.Future[None],
    timeout: float = LOCK_WAIT_TIMEOUT,
) -> bool:
    """Wait until backend ``pid`` parks on a lock, or ``task`` finishes.

    The variant for a race whose *whole question* is whether the second
    writer blocks at all. `wait_until_blocked_on_lock` would turn a
    regression there into a ten-second timeout and an assertion about
    the helper rather than about the code under test; this hands the
    outcome back so the test can assert on the state the race left
    behind.

    Parameters
    ----------
    probe
        A read-only session on a third connection, as for
        `wait_until_blocked_on_lock`.
    pid
        Backend PID of the session expected to block.
    task
        The in-flight racing work, so a writer that sails through is
        noticed rather than waited out.
    timeout
        Seconds to wait before giving up on both outcomes.

    Returns
    -------
    bool
        `True` if ``pid`` parked on a lock, `False` if ``task`` finished
        without blocking.

    Raises
    ------
    AssertionError
        If neither happened within ``timeout``.
    """
    deadline = time.monotonic() + timeout
    while True:
        blocked = await probe.scalar(_BLOCKED_QUERY, {"pid": pid})
        await probe.rollback()
        if blocked:
            return True
        if task.done():
            return False
        if time.monotonic() >= deadline:
            msg = (
                f"Backend {pid} neither blocked on a lock nor finished "
                f"within {timeout}s"
            )
            raise AssertionError(msg)
        await asyncio.sleep(LOCK_WAIT_POLL_INTERVAL)
