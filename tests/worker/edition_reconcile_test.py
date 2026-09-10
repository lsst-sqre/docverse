"""Tests for the ``edition_reconcile`` per-org worker function.

The per-org job is where the reconciliation loop meets the rest of the
system, so these tests are about the wiring rather than the decision
table (which ``tests/domain/edition_reconcile_test.py`` pins case by
case): that a lost publish really does come back onto the queue tagged
as a repair, that the tick is idempotent once it has re-driven a pair,
and that the ``queue_jobs`` row an operator reads says what happened.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    JobKind,
    OrganizationCreate,
    ProjectCreate,
    PublishStatus,
    TrackingMode,
)
from docverse_server.config import Configuration
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.dbschema.edition_build_history import (
    SqlEditionBuildHistory,
)
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.queue import JobStatus
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_build_history_store import (
    EditionBuildHistoryStore,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.edition_reconcile import (
    edition_reconcile,
)
from tests.support.arq_testing import get_jobs_by_name
from tests.worker.conftest import make_worker_ctx

_config = Configuration()

_HASH = "sha256:" + "b" * 64
_ORG_SLUG = "recon-org"
_PROJECT_SLUG = "recon-proj"
_EDITION_SLUG = "trunk"

_SETTLED = timedelta(hours=1)
"""How far back the fixtures push timestamps out of the grace window."""


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


def _payload(*, org_id: int, queue_job_id: int) -> dict[str, Any]:
    return {
        "org_id": org_id,
        "org_slug": _ORG_SLUG,
        "queue_job_id": queue_job_id,
    }


async def _seed_lost_phase_b(
    db_session: AsyncSession,
) -> tuple[int, int, int, int]:
    """Seed the exact rows a lost Phase B leaves behind.

    A client-upload project — no keeper-sync state, so none of
    ``keeper_sync_project``'s self-heal legs would ever look at it —
    whose edition points at a completed build, whose history row for the
    pair reads ``pending``, and whose ``publish_edition`` queue row is
    ``queued`` with a NULL ``backend_job_id`` because the arq enqueue
    never happened. Both timestamps are pushed outside the grace window
    so the tick treats the pair as settled rather than mid-enqueue.

    Returns the org id, the edition id, the build id and its public id.
    """
    logger = _logger()
    org = await OrganizationStore(session=db_session, logger=logger).create(
        OrganizationCreate(
            slug=_ORG_SLUG,
            title="Recon Org",
            base_domain=f"{_ORG_SLUG}.example.com",
        )
    )
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org.id,
        data=ProjectCreate(
            slug=_PROJECT_SLUG,
            title="Recon Project",
            source_url="https://example.com/example/recon",
        ),
    )
    edition_store = EditionStore(session=db_session, logger=logger)
    edition = await edition_store.create(
        project_id=project.id,
        data=EditionCreate(
            slug=_EDITION_SLUG,
            title="Trunk",
            kind=EditionKind.draft,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": "main"},
        ),
    )
    build_store = BuildStore(session=db_session, logger=logger)
    build = await build_store.create(
        project_id=project.id,
        project_slug=project.slug,
        data=BuildCreate(git_ref="main", content_hash=_HASH),
        uploader="testuser",
    )
    await build_store.transition_status(
        build_id=build.id, new_status=BuildStatus.processing
    )
    await build_store.transition_status(
        build_id=build.id, new_status=BuildStatus.completed
    )
    await edition_store.set_current_build(
        edition_id=edition.id, build_id=build.id
    )
    history_store = EditionBuildHistoryStore(session=db_session, logger=logger)
    history = await history_store.record(
        edition_id=edition.id, build_id=build.id
    )
    await history_store.set_publish_status(
        history_id=history.id, status=PublishStatus.pending
    )
    await edition_store.set_publish_status(
        edition_id=edition.id, status=PublishStatus.pending
    )
    # The orphaned Phase A row: committed, never enqueued.
    await QueueJobStore(session=db_session, logger=logger).create(
        kind=JobKind.publish_edition,
        org_id=org.id,
        project_id=project.id,
        build_id=build.id,
        edition_id=edition.id,
    )
    settled = datetime.now(tz=UTC) - _SETTLED
    await db_session.execute(
        update(SqlEdition)
        .where(SqlEdition.id == edition.id)
        .values(date_updated=settled)
    )
    await db_session.execute(
        update(SqlEditionBuildHistory)
        .where(SqlEditionBuildHistory.id == history.id)
        .values(date_created=settled)
    )
    return org.id, edition.id, build.id, build.public_id


async def _seed_reconcile_job(db_session: AsyncSession, *, org_id: int) -> int:
    """Create the queued per-org row the dispatcher would have written."""
    job = await QueueJobStore(session=db_session, logger=_logger()).create(
        kind=JobKind.edition_reconcile,
        org_id=org_id,
        subject_label=_ORG_SLUG,
        backend_job_id="test-edition-reconcile-job",
    )
    return job.id


async def _read_queue_job(job_id: int) -> SqlQueueJob:
    async for session in db_session_dependency():
        async with session.begin():
            row = await session.get(SqlQueueJob, job_id)
            assert row is not None
            await session.refresh(row)
            return row
    msg = "No database session available"
    raise RuntimeError(msg)


async def _read_history_status(
    *, edition_id: int, build_id: int
) -> PublishStatus | None:
    async for session in db_session_dependency():
        async with session.begin():
            history = await EditionBuildHistoryStore(
                session=session, logger=_logger()
            ).get_by_edition_and_build(
                edition_id=edition_id, build_id=build_id
            )
            assert history is not None
            return history.publish_status
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_edition_reconcile_redrives_a_lost_publish(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A lost Phase B comes back onto the queue, tagged as a repair.

    This is the gap the loop was written for: a client-upload project
    has no keeper-sync run to heal it, the orphan sweep only fails the
    ``queue_jobs`` row, and nothing else in the tree ever re-reads the
    pair. The re-driven job carries ``trigger=reconcile`` so the publish
    is attributable to the repair rather than to a build fan-out that
    never happened.
    """
    async with db_session.begin():
        (
            org_id,
            edition_id,
            build_id,
            build_public_id,
        ) = await _seed_lost_phase_b(db_session)
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)

    result = await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )
    await ctx["http_client"].aclose()

    assert result == "completed"
    jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name=_config.arq_queue_name
    )
    assert len(jobs) == 1
    published_payload = jobs[0].kwargs["payload"]
    assert published_payload["trigger"] == "reconcile"
    assert published_payload["edition_id"] == edition_id
    assert published_payload["build_id"] == build_id
    assert published_payload["build_public_id"] == serialize_base32_id(
        build_public_id
    )
    assert (
        await _read_history_status(edition_id=edition_id, build_id=build_id)
        == PublishStatus.pending
    )

    row = await _read_queue_job(queue_job_id)
    assert row.status == JobStatus.completed.value
    assert row.progress is not None
    assert row.progress["republished"] == 1
    assert row.progress["republish_failed"] == 0
    assert row.progress["editions_scanned"] == 1
    assert row.progress["capped"] == 0


@pytest.mark.asyncio
async def test_edition_reconcile_is_idempotent_across_ticks(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A second tick enqueues nothing for a pair the first re-drove.

    The re-drive leaves a live ``publish_edition`` row behind, and the
    loop's whole safety story rests on recognising that: without it,
    every tick would pile another publish onto a queue that is already
    working on the pair.
    """
    async with db_session.begin():
        org_id, _, _, _ = await _seed_lost_phase_b(db_session)
        first_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=first_job_id)
    )

    async with db_session.begin():
        second_job_id = await _seed_reconcile_job(db_session, org_id=org_id)

    result = await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=second_job_id)
    )
    await ctx["http_client"].aclose()

    assert result == "completed"
    jobs = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name=_config.arq_queue_name
    )
    assert len(jobs) == 1
    row = await _read_queue_job(second_job_id)
    assert row.progress is not None
    assert row.progress["republished"] == 0


@pytest.mark.asyncio
async def test_edition_reconcile_skips_a_row_it_did_not_claim(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A row a reaper already failed is not re-run by a late delivery.

    Same late-delivery guard the other per-org jobs take first: the
    reaper failing the row is what releases the org's mutex, so running
    the tick anyway would put a second reconciler on an org that already
    has one.
    """
    async with db_session.begin():
        org_id, _, _, _ = await _seed_lost_phase_b(db_session)
        queue_job_id = await _seed_reconcile_job(db_session, org_id=org_id)
        await QueueJobStore(session=db_session, logger=_logger()).fail(
            queue_job_id, errors={"message": "reaped"}
        )

    mock_arq = MockArqQueue(default_queue_name=_config.arq_queue_name)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)

    result = await edition_reconcile(
        ctx, _payload(org_id=org_id, queue_job_id=queue_job_id)
    )
    await ctx["http_client"].aclose()

    assert result == "skipped"
    assert (
        get_jobs_by_name(
            mock_arq, "publish_edition", queue_name=_config.arq_queue_name
        )
        == []
    )
