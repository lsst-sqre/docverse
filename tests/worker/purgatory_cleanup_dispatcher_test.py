"""Tests for the ``purgatory_cleanup_dispatcher`` worker function.

The dispatcher is the nightly tick that decides which organizations get
a sweep. Three things are worth pinning: that the feature flag really
does stop it dead — the sweep deletes object-store content permanently,
so "off" has to mean no rows and no jobs — that an org whose deleted
builds are all still restorable costs nothing, and that an org already
holding the per-org mutex is stepped over rather than aborting the tick
for every org behind it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    JobKind,
    OrganizationCreate,
    ProjectCreate,
)
from docverse_server.config import config as runtime_config
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.purgatory_cleanup_dispatcher import (
    purgatory_cleanup_dispatcher,
)
from docverse_server.worker.queues import MAINTENANCE_QUEUE_NAME
from tests.support.arq_testing import get_jobs_by_name, register_queue
from tests.worker.conftest import make_worker_ctx

_HASH = "sha256:" + "e" * 64

#: Short enough that a build deleted "long ago" in these tests is always
#: out of the window, long enough that one deleted "just now" is inside.
_RETENTION = timedelta(days=7)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


@pytest.fixture
def _enable_sweep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Turn the purgatory sweep on for one test."""
    monkeypatch.setattr(runtime_config, "purgatory_cleanup_enabled", True)


async def _seed_org_with_deleted_build(
    db_session: AsyncSession, *, slug: str, deleted_days_ago: int
) -> int:
    """Create an org whose one build was soft-deleted N days ago."""
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug=slug,
            title=f"Purgatory Org {slug}",
            base_domain=f"{slug}.example.com",
            purgatory_retention=int(_RETENTION.total_seconds()),
        )
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug=f"{slug}-proj",
            title="Project",
            source_url=f"https://example.com/example/{slug}",
        ),
    )
    build = await BuildStore(session=db_session, logger=logger).create(
        project_id=project.id,
        project_slug=project.slug,
        data=BuildCreate(git_ref="main", content_hash=_HASH),
        uploader="testuser",
    )
    await db_session.execute(
        update(SqlBuild)
        .where(SqlBuild.id == build.id)
        .values(
            date_deleted=datetime.now(tz=UTC)
            - timedelta(days=deleted_days_ago)
        )
    )
    return org.id


def _make_ctx() -> tuple[dict[str, object], MockArqQueue]:
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, MAINTENANCE_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    return ctx, mock_arq


async def _purgatory_queue_jobs() -> list[SqlQueueJob]:
    """Read every ``purgatory_cleanup`` queue row on a fresh session."""
    async for session in db_session_dependency():
        async with session.begin():
            result = await session.execute(
                select(SqlQueueJob)
                .where(SqlQueueJob.kind == JobKind.purgatory_cleanup.value)
                .order_by(SqlQueueJob.id)
            )
            return list(result.scalars().all())
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_dispatcher_skips_entirely_when_the_flag_is_off(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A disabled sweep creates no queue rows even with work waiting.

    The flag ships false because the job deletes object-store content
    permanently, so "off" has to mean the tick does nothing at all —
    not that it plans work an operator then has to cancel.
    """
    async with db_session.begin():
        await _seed_org_with_deleted_build(
            db_session, slug="flag-off-org", deleted_days_ago=30
        )

    ctx, mock_arq = _make_ctx()

    assert await purgatory_cleanup_dispatcher(ctx) == "skipped"

    assert await _purgatory_queue_jobs() == []
    assert get_jobs_by_name(mock_arq, "purgatory_cleanup") == []


@pytest.mark.asyncio
@pytest.mark.usefixtures("_enable_sweep")
async def test_dispatcher_enqueues_only_orgs_holding_expired_builds(
    app: None,
    db_session: AsyncSession,
) -> None:
    """One job per org with work; an org still inside retention gets none.

    Retention is an org-level setting, so eligibility is asked per org
    rather than once per tick. The org whose build was deleted moments
    ago is still inside the window the restore endpoint promises, and
    giving it a queue row would burn a mutex slot to plan an empty work
    list.
    """
    async with db_session.begin():
        expired_org_id = await _seed_org_with_deleted_build(
            db_session, slug="has-expired", deleted_days_ago=30
        )
        await _seed_org_with_deleted_build(
            db_session, slug="still-fresh", deleted_days_ago=0
        )

    ctx, mock_arq = _make_ctx()

    assert await purgatory_cleanup_dispatcher(ctx) == "completed"

    rows = await _purgatory_queue_jobs()
    assert [row.org_id for row in rows] == [expired_org_id]
    assert [row.subject_label for row in rows] == ["has-expired"]
    assert rows[0].backend_job_id is not None

    jobs = get_jobs_by_name(
        mock_arq, "purgatory_cleanup", queue_name=MAINTENANCE_QUEUE_NAME
    )
    assert len(jobs) == 1
    assert jobs[0].kwargs["payload"] == {
        "org_id": expired_org_id,
        "org_slug": "has-expired",
        "queue_job_id": rows[0].id,
    }


@pytest.mark.asyncio
@pytest.mark.usefixtures("_enable_sweep")
async def test_dispatcher_steps_over_an_org_whose_mutex_is_held(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A busy org is stepped over and the rest of the tick still runs.

    The per-org mutex index makes a second active row for the same org
    impossible, and the sweep is a nightly cron: an org whose previous
    job is still running will meet the next tick. What must not happen
    is the rejected insert taking the whole fan-out down with it, since
    the orgs after the busy one in the loop would then wait a full day
    for no reason of their own.
    """
    async with db_session.begin():
        busy_org_id = await _seed_org_with_deleted_build(
            db_session, slug="aaa-busy-org", deleted_days_ago=30
        )
        free_org_id = await _seed_org_with_deleted_build(
            db_session, slug="zzz-free-org", deleted_days_ago=30
        )
        await QueueJobStore(session=db_session, logger=_logger()).create(
            kind=JobKind.purgatory_cleanup,
            org_id=busy_org_id,
            subject_label="aaa-busy-org",
            backend_job_id="already-running",
        )

    ctx, mock_arq = _make_ctx()

    assert await purgatory_cleanup_dispatcher(ctx) == "completed"

    rows = await _purgatory_queue_jobs()
    assert [row.org_id for row in rows] == [busy_org_id, free_org_id]

    jobs = get_jobs_by_name(
        mock_arq, "purgatory_cleanup", queue_name=MAINTENANCE_QUEUE_NAME
    )
    assert [job.kwargs["payload"]["org_id"] for job in jobs] == [free_org_id]
