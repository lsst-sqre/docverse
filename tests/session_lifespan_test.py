"""Pins for the session-scoped application lifespan in ``tests/conftest.py``.

The ``app`` fixture used to rebuild the entire schema
(``initialize_database(reset=True)`` plus an Alembic stamp) and cycle the
application lifespan for *every* test, which dominated the suite's
wall-clock. It now starts the lifespan once per pytest process and
resets state between tests with a ``TRUNCATE``.

That trade is only safe if nothing leaks across the per-test boundary,
so these tests pin both halves of it: the process-lifetime pieces really
are shared, and the mutable per-test pieces really are rebuilt. Each pin
is a pair of tests that run in file order — the first records or dirties
something, the second asserts what the next test actually sees.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dependencies.context import context_dependency
from docverse_server.metrics.enums import LifecycleAction
from docverse_server.metrics.payloads import ProjectLifecycleEvent

from .conftest import seed_org_with_admin
from .support.arq_testing import count_jobs_by_name

_engines: list[AsyncEngine | None] = []
"""Engine the application lifespan built, recorded once per test below."""

_queues: list[MockArqQueue] = []
"""Mock arq queue the ``app`` fixture installed, recorded per test below."""


@pytest.mark.asyncio
async def test_lifespan_engine_is_recorded(app: FastAPI) -> None:
    """Record the engine the application lifespan built for this test."""
    _engines.append(db_session_dependency._engine)
    assert _engines[0] is not None


@pytest.mark.asyncio
async def test_lifespan_engine_is_shared(app: FastAPI) -> None:
    """The lifespan is not cycled between tests.

    ``db_session_dependency.initialize`` builds a new engine on every
    startup and ``aclose`` drops it, so a per-test lifespan would hand
    this test a different engine object (and pay for a new connection
    pool). Identity across two tests is the cheapest true statement that
    the lifespan ran exactly once.
    """
    _engines.append(db_session_dependency._engine)
    assert _engines[1] is _engines[0]


@pytest.mark.asyncio
async def test_arq_queue_accepts_a_job(app: FastAPI) -> None:
    """Dirty the mock arq queue so the next test can prove it was reset."""
    queue = arq_dependency._arq_queue
    assert isinstance(queue, MockArqQueue)
    _queues.append(queue)
    await queue.enqueue("dashboard_build", 1)
    assert count_jobs_by_name(queue, "dashboard_build") == 1


@pytest.mark.asyncio
async def test_arq_queue_is_rebuilt_per_test(app: FastAPI) -> None:
    """Enqueued jobs do not leak into the next test.

    The queue lives on the process-wide ``arq_dependency``, so sharing
    the lifespan would otherwise carry every enqueued job forward.
    """
    queue = arq_dependency._arq_queue
    assert isinstance(queue, MockArqQueue)
    assert queue is not _queues[0]
    assert count_jobs_by_name(queue, "dashboard_build") == 0


type _LifecyclePublisher = MockEventPublisher[ProjectLifecycleEvent]


def _project_lifecycle_publisher() -> _LifecyclePublisher:
    """Return the running application's mocked project-lifecycle publisher."""
    events = context_dependency._events
    assert events is not None
    publisher = events.project_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    return publisher


@pytest.mark.asyncio
async def test_metrics_publisher_records_a_payload(app: FastAPI) -> None:
    """Record a metrics payload so the next test can prove it was dropped."""
    publisher = _project_lifecycle_publisher()
    await publisher.publish(
        ProjectLifecycleEvent(
            organization="session-pin-org",
            project="session-pin-project",
            action=LifecycleAction.create,
        )
    )
    assert len(publisher.published) == 1


@pytest.mark.asyncio
async def test_metrics_publishers_are_cleared_per_test(app: FastAPI) -> None:
    """Recorded metrics payloads do not leak into the next test.

    ``MockEventPublisher`` records every payload for the lifetime of the
    event manager, and that manager is now built once per session — so
    without an explicit reset every ``assert len(published) == 1`` in
    the suite would see the whole session's history.
    """
    assert _project_lifecycle_publisher().published == []


@pytest.mark.asyncio
async def test_seeded_org_takes_the_first_id(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """Seed an org and pin the identity value it is assigned."""
    await seed_org_with_admin(client, "session-pin-org", "testuser")
    org_id = await db_session.scalar(
        select(SqlOrganization.id).where(
            SqlOrganization.slug == "session-pin-org"
        )
    )
    assert org_id == 1


@pytest.mark.asyncio
async def test_seeded_org_takes_the_first_id_again(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """Rows and identity sequences both reset between tests.

    A plain ``TRUNCATE`` would leave the sequence advanced, so this pins
    the ``RESTART IDENTITY`` half of the reset as well as the empty-table
    half — reusing the previous test's slug proves the row is gone too.
    """
    await seed_org_with_admin(client, "session-pin-org", "testuser")
    org_id = await db_session.scalar(
        select(SqlOrganization.id).where(
            SqlOrganization.slug == "session-pin-org"
        )
    )
    assert org_id == 1
