"""Integration tests for ``LockService`` against a real Postgres.

These tests exercise ``pg_advisory_lock`` behaviour end-to-end rather
than mocking SQL strings: two concurrent sessions against the same
engine must serialize on the same key, run in parallel on different
classes, and release locks when the underlying connection dies
without a clean ``pg_advisory_unlock``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
import structlog
from fastapi import FastAPI
from safir.database import create_database_engine
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from docverse.config import config
from docverse.services.lock_service import LockKey, LockService


def _logger() -> structlog.stdlib.BoundLogger:
    logger: structlog.stdlib.BoundLogger = structlog.get_logger("docverse")
    return logger


@pytest_asyncio.fixture
async def lock_engine(
    app: FastAPI,
) -> AsyncGenerator[AsyncEngine]:
    """Yield a dedicated engine so the test controls connection disposal.

    Requests the ``app`` fixture so the database schema is initialised
    before this engine connects. Disposed at teardown so locks from a
    failed test never leak into another test.
    """
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_same_lock_serializes_concurrent_sessions(
    lock_engine: AsyncEngine,
) -> None:
    """Two sessions acquiring the same lock must serialize."""
    maker = async_sessionmaker(lock_engine, expire_on_commit=False)
    lock_key = LockKey.for_project(org_id=42, project_id=99)

    async with maker() as holder_session, maker() as waiter_session:
        holder = LockService(session=holder_session, logger=_logger())
        waiter = LockService(session=waiter_session, logger=_logger())

        acquired = asyncio.Event()
        release = asyncio.Event()

        async def hold() -> None:
            async with holder.acquire(lock_key):
                acquired.set()
                await release.wait()

        async def wait_for_lock() -> None:
            async with waiter.acquire(lock_key):
                pass

        hold_task = asyncio.create_task(hold())
        await asyncio.wait_for(acquired.wait(), timeout=5.0)

        wait_task = asyncio.create_task(wait_for_lock())

        # The waiter must block while the holder has the lock.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(wait_task), timeout=0.5)
        assert not wait_task.done()

        # Releasing the holder unblocks the waiter.
        release.set()
        await asyncio.wait_for(hold_task, timeout=5.0)
        await asyncio.wait_for(wait_task, timeout=5.0)


@pytest.mark.asyncio
async def test_different_classes_do_not_block(
    lock_engine: AsyncEngine,
) -> None:
    """Same ``(org, project)`` under different classes stays parallel."""
    maker = async_sessionmaker(lock_engine, expire_on_commit=False)

    project_key = LockKey.for_project(org_id=42, project_id=99)
    build_key = LockKey.for_build_processing(
        org_id=42, project_id=99, git_ref="main"
    )

    async with maker() as s1, maker() as s2:
        svc1 = LockService(session=s1, logger=_logger())
        svc2 = LockService(session=s2, logger=_logger())

        # Both locks must be acquirable simultaneously without either
        # blocking — a short timeout trips the test if they collide.
        async with asyncio.timeout(5.0):
            async with (
                svc1.acquire(project_key),
                svc2.acquire(build_key),
            ):
                pass


@pytest.mark.asyncio
async def test_session_close_releases_lock_on_crash(
    app: FastAPI,
) -> None:
    """Connection death releases the lock even without ``__aexit__``.

    Simulates a worker crash by cancelling the task while it holds a
    raw ``pg_advisory_lock`` — so no ``pg_advisory_unlock`` is issued
    — and disposing the engine to close the pooled connection. A
    fresh engine must then acquire the same lock without blocking.
    """
    engine_a = create_database_engine(
        config.database_url, config.database_password
    )
    engine_b = create_database_engine(
        config.database_url, config.database_password
    )
    maker_a = async_sessionmaker(engine_a, expire_on_commit=False)
    maker_b = async_sessionmaker(engine_b, expire_on_commit=False)
    lock_key = LockKey.for_project(org_id=123, project_id=456)

    try:
        acquired = asyncio.Event()

        async def crashing_holder() -> None:
            # Acquire the lock *without* going through
            # LockService.acquire's context manager, so cancellation
            # of this task doesn't trigger pg_advisory_unlock.
            async with maker_a() as session:
                await session.execute(
                    text("SELECT pg_advisory_lock(:id)"),
                    {"id": lock_key.lock_id},
                )
                acquired.set()
                await asyncio.sleep(60.0)

        task = asyncio.create_task(crashing_holder())
        await asyncio.wait_for(acquired.wait(), timeout=5.0)

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        # Closing the pool's connections is what actually releases
        # the lock in Postgres — the cancelled task's rollback alone
        # does not unlock, because advisory locks are session-scoped
        # at the connection level, not transaction-scoped.
        await engine_a.dispose()

        async with maker_b() as s2:
            svc = LockService(session=s2, logger=_logger())
            async with asyncio.timeout(5.0):
                async with svc.acquire(lock_key):
                    pass
    finally:
        await engine_a.dispose()
        await engine_b.dispose()
