"""Tests for the ``edition_reconcile_dispatcher`` worker function.

The dispatcher is the half-hourly tick that decides which organizations
get a reconciliation pass. It has no pre-flight — unlike the purgatory
sweep there is no cheap query that names the orgs holding drift, because
drift is exactly what nobody has noticed yet — so every org gets a row
every tick and the per-org job is what discovers there is nothing to do.

That shape makes four things worth pinning: that the fan-out really is
one row per org, that the arq job id lands back on the row it was
enqueued for, that an org still running a previous pass is stepped over
rather than aborting the tick for every org behind it, and that the
feature flag stops the tick before it writes anything. A fifth test
drives the enqueued payload straight into the per-org job, because the
two halves of the fan-out are only worth anything joined.
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
from structlog.testing import capture_logs

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionCreate,
    EditionKind,
    JobKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse_server.config import config as runtime_config
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.queue import JobStatus
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.edition_reconcile import (
    edition_reconcile,
)
from docverse_server.worker.functions.edition_reconcile_dispatcher import (
    edition_reconcile_dispatcher,
)
from docverse_server.worker.queues import MAINTENANCE_QUEUE_NAME
from tests.support.arq_testing import get_jobs_by_name, register_queue
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org(db_session: AsyncSession, *, slug: str) -> int:
    """Create a bare organization; the dispatcher needs nothing else."""
    org = await OrganizationStore(session=db_session, logger=_logger()).create(
        OrganizationCreate(
            slug=slug,
            title=f"Reconcile Org {slug}",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


async def _seed_unpublished_edition(
    db_session: AsyncSession, *, org_id: int
) -> tuple[int, int]:
    """Seed an edition on a completed build that was never published.

    No ``edition_build_history`` row at all — the residue of a publish
    enqueue lost before it wrote anything, which the planner reads as
    ``lost_enqueue``. ``date_updated`` is pushed outside the grace
    window so the tick treats the pair as settled rather than as an
    enqueue still in flight.

    Returns the edition id and the build id.
    """
    logger = _logger()
    project = await ProjectStore(session=db_session, logger=logger).create(
        org_id=org_id,
        data=ProjectCreate(
            slug="recon-e2e-proj",
            title="Reconcile Project",
            source_url="https://example.com/example/recon-e2e",
        ),
    )
    edition_store = EditionStore(session=db_session, logger=logger)
    edition = await edition_store.create(
        project_id=project.id,
        data=EditionCreate(
            slug="trunk",
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
        data=BuildCreate(git_ref="main", content_hash="sha256:" + "c" * 64),
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
    await db_session.execute(
        update(SqlEdition)
        .where(SqlEdition.id == edition.id)
        .values(date_updated=datetime.now(tz=UTC) - timedelta(hours=1))
    )
    return edition.id, build.id


def _make_ctx() -> tuple[dict[str, object], MockArqQueue]:
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, MAINTENANCE_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=httpx.AsyncClient(), arq_queue=mock_arq)
    return ctx, mock_arq


async def _reconcile_queue_jobs() -> list[SqlQueueJob]:
    """Read every ``edition_reconcile`` queue row on a fresh session."""
    async for session in db_session_dependency():
        async with session.begin():
            result = await session.execute(
                select(SqlQueueJob)
                .where(SqlQueueJob.kind == JobKind.edition_reconcile.value)
                .order_by(SqlQueueJob.id)
            )
            return list(result.scalars().all())
    msg = "No database session available"
    raise RuntimeError(msg)


async def _read_queue_job(job_id: int) -> SqlQueueJob:
    """Re-read one queue row on a fresh session, post-commit."""
    async for session in db_session_dependency():
        async with session.begin():
            row = await session.get(SqlQueueJob, job_id)
            assert row is not None
            await session.refresh(row)
            return row
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_dispatcher_claims_one_row_per_org(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Every org gets exactly one row, labelled by slug and enqueued.

    There is no eligibility question to ask first: an org's drift is by
    definition state nobody has looked at, so a pre-flight that named
    the orgs worth visiting would have to do the per-org job's own work.
    The dispatcher therefore fans out unconditionally and each per-org
    job discovers, cheaply, that it has nothing to repair.
    """
    async with db_session.begin():
        first_org_id = await _seed_org(db_session, slug="recon-alpha")
        second_org_id = await _seed_org(db_session, slug="recon-beta")

    ctx, mock_arq = _make_ctx()

    assert await edition_reconcile_dispatcher(ctx) == "completed"
    await ctx["http_client"].aclose()  # type: ignore[attr-defined]

    rows = await _reconcile_queue_jobs()
    assert [row.org_id for row in rows] == [first_org_id, second_org_id]
    assert [row.subject_label for row in rows] == [
        "recon-alpha",
        "recon-beta",
    ]

    jobs = get_jobs_by_name(
        mock_arq, "edition_reconcile", queue_name=MAINTENANCE_QUEUE_NAME
    )
    assert [job.kwargs["payload"] for job in jobs] == [
        {
            "org_id": first_org_id,
            "org_slug": "recon-alpha",
            "queue_job_id": rows[0].id,
        },
        {
            "org_id": second_org_id,
            "org_slug": "recon-beta",
            "queue_job_id": rows[1].id,
        },
    ]


@pytest.mark.asyncio
async def test_dispatcher_writes_the_backend_job_id_back(
    app: None,
    db_session: AsyncSession,
) -> None:
    """The arq job id lands on the row the enqueue was made for.

    The write-back is what closes the orphan tail: a row whose
    ``backend_job_id`` is still NULL well after its creation is a row
    whose arq enqueue was lost, which is precisely what
    ``edition_reconcile_reaper`` sweeps. Pinning the id here keeps that
    signal meaningful — if the dispatcher stopped writing it back, every
    healthy row would look orphaned to the reaper.
    """
    async with db_session.begin():
        await _seed_org(db_session, slug="recon-writeback")

    ctx, mock_arq = _make_ctx()

    assert await edition_reconcile_dispatcher(ctx) == "completed"
    await ctx["http_client"].aclose()  # type: ignore[attr-defined]

    rows = await _reconcile_queue_jobs()
    assert len(rows) == 1
    jobs = get_jobs_by_name(
        mock_arq, "edition_reconcile", queue_name=MAINTENANCE_QUEUE_NAME
    )
    assert len(jobs) == 1
    assert rows[0].backend_job_id == jobs[0].id
    assert rows[0].backend_queue_name == MAINTENANCE_QUEUE_NAME


@pytest.mark.asyncio
async def test_dispatcher_steps_over_an_org_whose_mutex_is_held(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A busy org is stepped over and the rest of the tick still runs.

    The per-org mutex index makes a second active row for the same org
    impossible, and a second reconciler over one org would read the same
    drifted editions and enqueue a duplicate ``publish_edition`` for
    each — doubling the publish load exactly when the org is already
    behind. What must not happen is the rejected insert taking the whole
    fan-out down with it, since the orgs after the busy one in the loop
    would then wait a full cycle for no reason of their own.
    """
    async with db_session.begin():
        busy_org_id = await _seed_org(db_session, slug="aaa-recon-busy")
        free_org_id = await _seed_org(db_session, slug="zzz-recon-free")
        await QueueJobStore(session=db_session, logger=_logger()).create(
            kind=JobKind.edition_reconcile,
            org_id=busy_org_id,
            subject_label="aaa-recon-busy",
            backend_job_id="already-running",
        )

    ctx, mock_arq = _make_ctx()

    assert await edition_reconcile_dispatcher(ctx) == "completed"
    await ctx["http_client"].aclose()  # type: ignore[attr-defined]

    rows = await _reconcile_queue_jobs()
    assert [row.org_id for row in rows] == [busy_org_id, free_org_id]

    jobs = get_jobs_by_name(
        mock_arq, "edition_reconcile", queue_name=MAINTENANCE_QUEUE_NAME
    )
    assert [job.kwargs["payload"]["org_id"] for job in jobs] == [free_org_id]


@pytest.mark.asyncio
async def test_dispatcher_skips_entirely_when_the_flag_is_off(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A disabled loop writes no row, enqueues nothing, and stays quiet.

    The flag exists for the operator who wants the repair load off the
    publishing queue while they look at a badly drifted org, so "off"
    has to mean the tick does nothing at all rather than planning work
    that then has to be cancelled. It logs at debug because a
    deliberately disabled loop firing twice an hour is not news.
    """
    monkeypatch.setattr(runtime_config, "edition_reconcile_enabled", False)
    async with db_session.begin():
        await _seed_org(db_session, slug="recon-flag-off")

    ctx, mock_arq = _make_ctx()

    with capture_logs() as captured:
        assert await edition_reconcile_dispatcher(ctx) == "skipped"
    await ctx["http_client"].aclose()  # type: ignore[attr-defined]

    assert await _reconcile_queue_jobs() == []
    assert get_jobs_by_name(mock_arq, "edition_reconcile") == []
    assert [entry["log_level"] for entry in captured] == ["debug"]


@pytest.mark.asyncio
async def test_dispatched_job_reconciles_the_org_end_to_end(
    app: None,
    db_session: AsyncSession,
) -> None:
    """The payload the dispatcher writes is the payload the job reads.

    The two halves of the fan-out are only useful together, and the seam
    between them is three fields and a ``queue_jobs`` row: the per-org
    job's late-delivery guard claims the very row this dispatcher
    created, so a row it never committed — or committed under a
    different id — would make every dispatched job return ``"skipped"``
    while the loop looked healthy. Driving the enqueued payload straight
    into the per-org function pins that contract, and proves the org it
    names is the one whose drift gets repaired.

    The drift here is the simplest real case: an edition pointing at a
    completed build with no ``edition_build_history`` row at all, the
    residue of a publish enqueue that was lost before it wrote anything.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, slug="recon-endtoend")
        edition_id, build_id = await _seed_unpublished_edition(
            db_session, org_id=org_id
        )

    ctx, mock_arq = _make_ctx()

    assert await edition_reconcile_dispatcher(ctx) == "completed"

    rows = await _reconcile_queue_jobs()
    assert len(rows) == 1
    jobs = get_jobs_by_name(
        mock_arq, "edition_reconcile", queue_name=MAINTENANCE_QUEUE_NAME
    )
    assert len(jobs) == 1

    assert await edition_reconcile(ctx, jobs[0].kwargs["payload"]) == (
        "completed"
    )
    await ctx["http_client"].aclose()  # type: ignore[attr-defined]

    published = get_jobs_by_name(
        mock_arq, "publish_edition", queue_name="docverse:queue"
    )
    assert len(published) == 1
    assert published[0].kwargs["payload"]["edition_id"] == edition_id
    assert published[0].kwargs["payload"]["build_id"] == build_id

    row = await _read_queue_job(rows[0].id)
    assert row.status == JobStatus.completed.value
    assert row.progress is not None
    assert row.progress["republished"] == 1
