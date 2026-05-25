"""Tests for ``DashboardBuildEnqueuer`` per-project dedup.

The cascade in :func:`docverse.worker.functions.publish_edition` calls
``try_enqueue_dashboard_build_by_id`` after every successful publish; on
a 1000-edition keeper-sync project that is 1000 redundant
``dashboard_build`` rows, only the last carrying final state. The
service-level dedup gate keeps at most one ``queued`` or ``in_progress``
row per ``(org_id, project_id)``; once that one terminates, the next
enqueue is fresh again — the window is "active jobs only", not
"lifetime".
"""

from __future__ import annotations

import pytest
import structlog
from safir.arq import MockArqQueue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate, ProjectCreate
from docverse.client.models.queue_enums import JobKind
from docverse.config import Configuration
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.services.dashboard.enqueue import DashboardBuildEnqueuer
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_backend import ArqQueueBackend
from docverse.storage.queue_job_store import QueueJobStore

_config = Configuration()


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


async def _seed_org_with_project(
    session: AsyncSession,
    *,
    org_slug: str,
    project_slug: str,
) -> tuple[int, int]:
    logger = _logger()
    org_store = OrganizationStore(session=session, logger=logger)
    proj_store = ProjectStore(session=session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title=f"Org {org_slug}",
            base_domain=f"{org_slug}.example.com",
        )
    )
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug=project_slug,
            title=f"Project {project_slug}",
            source_url=f"https://github.com/example/{project_slug}",
        ),
    )
    return org.id, project.id


def _make_enqueuer(
    session: AsyncSession,
    *,
    arq_queue: MockArqQueue,
) -> DashboardBuildEnqueuer:
    logger = _logger()
    return DashboardBuildEnqueuer(
        org_store=OrganizationStore(session=session, logger=logger),
        project_store=ProjectStore(session=session, logger=logger),
        queue_backend=ArqQueueBackend(
            arq_queue=arq_queue, default_queue_name=_config.arq_queue_name
        ),
        queue_job_store=QueueJobStore(session=session, logger=logger),
        logger=logger,
    )


async def _count_dashboard_rows(session: AsyncSession) -> int:
    result = await session.execute(
        select(SqlQueueJob).where(
            SqlQueueJob.kind == JobKind.dashboard_build.value
        )
    )
    return len(list(result.scalars().all()))


@pytest.mark.asyncio
async def test_enqueue_for_project_returns_none_when_active_row_exists(
    db_session: AsyncSession,
) -> None:
    """The second call returns ``None`` when the first job is still active."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    async with db_session.begin():
        org_id, project_id = await _seed_org_with_project(
            db_session,
            org_slug="dedup-org",
            project_slug="dedup-proj",
        )
        await db_session.commit()

    enqueuer = _make_enqueuer(db_session, arq_queue=arq_queue)
    async with db_session.begin():
        first = await enqueuer.enqueue_for_project(
            org_id=org_id, project_id=project_id
        )
        await db_session.commit()
    assert first is not None

    async with db_session.begin():
        second = await enqueuer.enqueue_for_project(
            org_id=org_id, project_id=project_id
        )
        await db_session.commit()
    assert second is None

    async with db_session.begin():
        assert await _count_dashboard_rows(db_session) == 1


@pytest.mark.asyncio
async def test_thousand_sequential_enqueues_produce_one_row(
    db_session: AsyncSession,
) -> None:
    """1000 sequential calls for one project insert exactly one row.

    Models the cascade burst: ``publish_edition`` cascades a
    ``try_enqueue_dashboard_build_by_id`` per successful publish, so a
    1000-edition keeper-sync project would otherwise produce 1000
    duplicate rows. The dedup gate collapses the burst to one.
    """
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    async with db_session.begin():
        org_id, project_id = await _seed_org_with_project(
            db_session,
            org_slug="burst-org",
            project_slug="burst-proj",
        )
        await db_session.commit()

    enqueuer = _make_enqueuer(db_session, arq_queue=arq_queue)
    skipped = 0
    async with db_session.begin():
        for _ in range(1000):
            result = await enqueuer.enqueue_for_project(
                org_id=org_id, project_id=project_id
            )
            if result is None:
                skipped += 1
        await db_session.commit()

    assert skipped == 999
    async with db_session.begin():
        assert await _count_dashboard_rows(db_session) == 1


@pytest.mark.asyncio
async def test_completed_row_does_not_block_next_enqueue(
    db_session: AsyncSession,
) -> None:
    """After the first row reaches ``completed``, a fresh row enqueues.

    Locks the "window is active jobs only, not lifetime" semantics: the
    dedup gate checks ``status IN ('queued', 'in_progress')``, so once a
    job terminates the next cascade can enqueue a fresh row to pick up
    later state.
    """
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    async with db_session.begin():
        org_id, project_id = await _seed_org_with_project(
            db_session,
            org_slug="cycle-org",
            project_slug="cycle-proj",
        )
        await db_session.commit()

    enqueuer = _make_enqueuer(db_session, arq_queue=arq_queue)
    async with db_session.begin():
        first = await enqueuer.enqueue_for_project(
            org_id=org_id, project_id=project_id
        )
        await db_session.commit()
    assert first is not None

    # Drive the first job through queued → in_progress → completed.
    queue_job_store = QueueJobStore(session=db_session, logger=_logger())
    async with db_session.begin():
        await queue_job_store.start(first.id)
        await queue_job_store.complete(first.id)
        await db_session.commit()

    async with db_session.begin():
        second = await enqueuer.enqueue_for_project(
            org_id=org_id, project_id=project_id
        )
        await db_session.commit()
    assert second is not None
    assert second.id != first.id

    async with db_session.begin():
        assert await _count_dashboard_rows(db_session) == 2


@pytest.mark.asyncio
async def test_enqueue_for_project_slug_propagates_none(
    db_session: AsyncSession,
) -> None:
    """Slug variant returns ``None`` on the same skip condition."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    async with db_session.begin():
        await _seed_org_with_project(
            db_session,
            org_slug="slug-dedup-org",
            project_slug="slug-dedup-proj",
        )
        await db_session.commit()

    enqueuer = _make_enqueuer(db_session, arq_queue=arq_queue)
    async with db_session.begin():
        first = await enqueuer.enqueue_for_project_slug(
            org_slug="slug-dedup-org",
            project_slug="slug-dedup-proj",
        )
        await db_session.commit()
    assert first is not None

    async with db_session.begin():
        second = await enqueuer.enqueue_for_project_slug(
            org_slug="slug-dedup-org",
            project_slug="slug-dedup-proj",
        )
        await db_session.commit()
    assert second is None


@pytest.mark.asyncio
async def test_enqueue_for_org_filters_skipped_projects(
    db_session: AsyncSession,
) -> None:
    """Org-wide rebuild filters out projects with active dashboard_builds.

    Operator-facing UX: rebuilding the whole org while one project is
    already mid-rebuild produces N-1 entries, not N (with one of them
    being a duplicate).
    """
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    async with db_session.begin():
        logger = _logger()
        org_store = OrganizationStore(session=db_session, logger=logger)
        proj_store = ProjectStore(session=db_session, logger=logger)
        org = await org_store.create(
            OrganizationCreate(
                slug="org-wide",
                title="Org Wide",
                base_domain="org-wide.example.com",
            )
        )
        kept_project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="keeper",
                title="Keeper",
                source_url="https://github.com/example/keeper",
            ),
        )
        already_active = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="already",
                title="Already",
                source_url="https://github.com/example/already",
            ),
        )
        await db_session.commit()

    enqueuer = _make_enqueuer(db_session, arq_queue=arq_queue)
    # Pre-seed an active dashboard_build for one project.
    async with db_session.begin():
        seeded = await enqueuer.enqueue_for_project(
            org_id=org.id, project_id=already_active.id
        )
        await db_session.commit()
    assert seeded is not None

    async with db_session.begin():
        results = await enqueuer.enqueue_for_org(org_id=org.id)
        await db_session.commit()

    # Only the project without an active row gets a fresh enqueue.
    assert len(results) == 1
    project, queue_job = results[0]
    assert project.id == kept_project.id
    assert queue_job is not None
    assert queue_job.project_id == kept_project.id
