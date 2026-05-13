"""Tests for the ``lifecycle_eval_dispatcher`` worker function.

Seeds a mix of orgs (rules at org level, rules only on a child project,
no rules anywhere) and asserts the cron handler creates one
``lifecycle_eval_runs`` row plus one per-org ``queue_jobs`` row for each
in-scope org, leaving the unconfigured org untouched.
"""

from __future__ import annotations

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    JobKind,
    LifecycleEvalRunStatus,
    OrganizationCreate,
    ProjectCreate,
)
from docverse.dbschema.lifecycle_eval_run import SqlLifecycleEvalRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.lifecycle import DraftInactivityRule, LifecycleRuleSet
from docverse.domain.queue import JobStatus
from docverse.storage.lifecycle_eval_run_store import LifecycleEvalRunStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.lifecycle_eval_dispatcher import (
    LIFECYCLE_EVAL_QUEUE_NAME,
    _create_run_with_children,
    lifecycle_eval_dispatcher,
)
from tests.support.arq_testing import get_jobs_by_name, register_queue
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


_ORG_RULES = LifecycleRuleSet(root=[DraftInactivityRule(max_days_inactive=30)])
_PROJECT_RULES = LifecycleRuleSet(
    root=[DraftInactivityRule(max_days_inactive=14)]
)


async def _seed_org(
    db_session: AsyncSession,
    *,
    slug: str,
    lifecycle_rules: LifecycleRuleSet | None = None,
) -> tuple[int, str]:
    org_store = OrganizationStore(session=db_session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"LED Org {slug}",
            base_domain=f"{slug}.example.com",
            lifecycle_rules=lifecycle_rules,
        )
    )
    return org.id, org.slug


async def _seed_project(
    db_session: AsyncSession,
    *,
    org_id: int,
    slug: str,
    lifecycle_rules: LifecycleRuleSet | None = None,
) -> int:
    project_store = ProjectStore(session=db_session, logger=_logger())
    project = await project_store.create(
        org_id=org_id,
        data=ProjectCreate(
            slug=slug,
            title=f"Project {slug}",
            doc_repo=f"https://example.com/{slug}",
            lifecycle_rules=lifecycle_rules,
        ),
    )
    return project.id


@pytest.mark.asyncio
async def test_dispatcher_fans_out_per_in_scope_org(
    app: None,
    db_session: AsyncSession,
) -> None:
    """One run row + one queue_job per in-scope org; unconfigured org skipped.

    Three orgs are seeded:

    * ``org-with-org-rules`` has org-level ``lifecycle_rules`` and is
      in-scope.
    * ``org-with-project-rules`` has *no* org-level rules but one of its
      projects has rules, so it is in-scope via the per-project signal.
    * ``org-without-rules`` has no rules at any level and must be
      skipped — no queue_jobs row written.

    The dispatcher must produce one ``lifecycle_eval_runs`` row, two
    ``queue_jobs`` rows of ``kind='lifecycle_eval'`` (one per in-scope
    org), a ``summary`` of ``{"orgs_enqueued": 2, "orgs_skipped": 1}``,
    and transition the run from ``pending`` to ``in_progress`` once the
    fan-out commits.
    """
    async with db_session.begin():
        org_a_id, org_a_slug = await _seed_org(
            db_session,
            slug="org-with-org-rules",
            lifecycle_rules=_ORG_RULES,
        )
        await _seed_project(db_session, org_id=org_a_id, slug="a-project")

        org_b_id, org_b_slug = await _seed_org(
            db_session, slug="org-with-project-rules"
        )
        await _seed_project(
            db_session,
            org_id=org_b_id,
            slug="b-project",
            lifecycle_rules=_PROJECT_RULES,
        )

        _, _ = await _seed_org(db_session, slug="org-without-rules")

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, LIFECYCLE_EVAL_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await lifecycle_eval_dispatcher(ctx)
    await http_client.aclose()
    assert result == "completed"

    # Two child ``lifecycle_eval`` jobs were enqueued on the dedicated
    # queue — one per in-scope org — and zero on the default queue.
    eval_jobs = get_jobs_by_name(
        mock_arq, "lifecycle_eval", queue_name=LIFECYCLE_EVAL_QUEUE_NAME
    )
    assert len(eval_jobs) == 2
    default_jobs = get_jobs_by_name(
        mock_arq, "lifecycle_eval", queue_name="docverse:queue"
    )
    assert default_jobs == []
    payload_slugs = {job.kwargs["payload"]["org_slug"] for job in eval_jobs}
    assert payload_slugs == {org_a_slug, org_b_slug}

    async for session in db_session_dependency():
        async with session.begin():
            run_stmt = select(SqlLifecycleEvalRun)
            runs = (await session.execute(run_stmt)).scalars().all()
            assert len(runs) == 1
            run = runs[0]
            assert run.status == LifecycleEvalRunStatus.in_progress.value
            assert run.summary == {
                "orgs_enqueued": 2,
                "orgs_skipped": 1,
            }

            qj_stmt = select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.lifecycle_eval.value
            )
            queue_jobs = (await session.execute(qj_stmt)).scalars().all()
            # The run row and its full set of per-org children are
            # created in the same transaction by
            # ``_create_run_with_children``; the run is never visible
            # without its children, so the reaper never has to chase
            # a partially-fanned-out parent.
            assert len(queue_jobs) == 2
            assert {qj.org_id for qj in queue_jobs} == {org_a_id, org_b_id}
            assert {qj.subject_label for qj in queue_jobs} == {
                org_a_slug,
                org_b_slug,
            }
            assert all(qj.lifecycle_eval_run_id == run.id for qj in queue_jobs)
            assert all(qj.backend_job_id is not None for qj in queue_jobs)
            assert all(
                qj.status == JobStatus.queued.value for qj in queue_jobs
            )

            run_store = LifecycleEvalRunStore(
                session=session, logger=_logger()
            )
            fetched = await run_store.get(run.id)
            assert fetched is not None
            assert fetched.status is LifecycleEvalRunStatus.in_progress


@pytest.mark.asyncio
async def test_dispatcher_with_no_in_scope_orgs_terminates_run_succeeded(
    app: None,
    db_session: AsyncSession,
) -> None:
    """An all-skipped tick finalises the run as ``succeeded`` immediately.

    When every org is filtered out by the pre-flight check, the
    dispatcher still records a run row (so operators can confirm the
    tick fired) and transitions it straight to ``succeeded`` — there
    are no per-org children that would otherwise drive finalisation.
    """
    async with db_session.begin():
        await _seed_org(db_session, slug="empty-org-1")
        await _seed_org(db_session, slug="empty-org-2")

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, LIFECYCLE_EVAL_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await lifecycle_eval_dispatcher(ctx)
    await http_client.aclose()
    assert result == "completed"

    eval_jobs = get_jobs_by_name(
        mock_arq, "lifecycle_eval", queue_name=LIFECYCLE_EVAL_QUEUE_NAME
    )
    assert eval_jobs == []

    async for session in db_session_dependency():
        async with session.begin():
            run_stmt = select(SqlLifecycleEvalRun)
            runs = (await session.execute(run_stmt)).scalars().all()
            assert len(runs) == 1
            run = runs[0]
            assert run.status == LifecycleEvalRunStatus.succeeded.value
            assert run.date_finished is not None
            assert run.summary == {
                "orgs_enqueued": 0,
                "orgs_skipped": 2,
            }

            qj_stmt = select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.lifecycle_eval.value
            )
            queue_jobs = (await session.execute(qj_stmt)).scalars().all()
            assert queue_jobs == []


@pytest.mark.asyncio
async def test_dispatcher_skips_when_create_loses_race(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The IntegrityError from a lost create race surfaces as ``"skipped"``.

    The pre-flight ``has_non_terminal_run`` check and the
    ``create`` insert run in separate transactions, so two cron
    firings can both clear the pre-check and both attempt the
    insert. The partial-unique non-terminal index rejects one of
    them with ``IntegrityError``. The dispatcher must catch that
    and return ``"skipped"`` — the run_store docstring promises
    the caller will translate the IntegrityError into a clean skip.

    Simulated by stubbing ``has_non_terminal_run`` to return False
    even though a non-terminal run already exists, which is the
    state a racing tick would observe between its pre-check commit
    and the other tick's create commit.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="org-race", lifecycle_rules=_ORG_RULES
        )
        run_store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        existing_run = await run_store.create()
        await run_store.transition_status(
            run_id=existing_run.id,
            new_status=LifecycleEvalRunStatus.in_progress,
        )
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        await queue_job_store.create(
            kind=JobKind.lifecycle_eval,
            org_id=org_id,
            subject_label="org-race",
            lifecycle_eval_run_id=existing_run.id,
        )

    async def _pretend_no_run_in_flight(self: LifecycleEvalRunStore) -> bool:
        return False

    monkeypatch.setattr(
        LifecycleEvalRunStore,
        "has_non_terminal_run",
        _pretend_no_run_in_flight,
    )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, LIFECYCLE_EVAL_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await lifecycle_eval_dispatcher(ctx)
    await http_client.aclose()
    assert result == "skipped"

    eval_jobs = get_jobs_by_name(
        mock_arq, "lifecycle_eval", queue_name=LIFECYCLE_EVAL_QUEUE_NAME
    )
    assert eval_jobs == []

    async for session in db_session_dependency():
        async with session.begin():
            run_stmt = select(SqlLifecycleEvalRun)
            runs = (await session.execute(run_stmt)).scalars().all()
            assert len(runs) == 1
            assert runs[0].id == existing_run.id


@pytest.mark.asyncio
async def test_dispatcher_skips_tick_when_prior_run_in_flight(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A pre-existing non-terminal run causes this tick to no-op.

    The partial-unique index on ``lifecycle_eval_runs`` enforces a
    singleton non-terminal run globally; the dispatcher pre-checks the
    same condition so the cron firing surfaces as a clean skip rather
    than an ``IntegrityError`` traceback.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org(
            db_session, slug="org-x", lifecycle_rules=_ORG_RULES
        )
        run_store = LifecycleEvalRunStore(session=db_session, logger=_logger())
        existing_run = await run_store.create()
        await run_store.transition_status(
            run_id=existing_run.id,
            new_status=LifecycleEvalRunStatus.in_progress,
        )
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        await queue_job_store.create(
            kind=JobKind.lifecycle_eval,
            org_id=org_id,
            subject_label="org-x",
            lifecycle_eval_run_id=existing_run.id,
        )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, LIFECYCLE_EVAL_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await lifecycle_eval_dispatcher(ctx)
    await http_client.aclose()
    assert result == "skipped"

    eval_jobs = get_jobs_by_name(
        mock_arq, "lifecycle_eval", queue_name=LIFECYCLE_EVAL_QUEUE_NAME
    )
    assert eval_jobs == []

    async for session in db_session_dependency():
        async with session.begin():
            run_stmt = select(SqlLifecycleEvalRun)
            runs = (await session.execute(run_stmt)).scalars().all()
            # No new run row was written; only the pre-existing one
            # remains.
            assert len(runs) == 1
            assert runs[0].id == existing_run.id


@pytest.mark.asyncio
async def test_create_run_with_children_is_atomic(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash mid-fanout rolls back the run row and every child row.

    ``_create_run_with_children`` writes the ``lifecycle_eval_runs``
    row, its ``summary``, every per-org ``queue_jobs`` child, and the
    ``pending → in_progress`` transition inside a single
    ``session.begin()`` block. If any child insert fails, the whole
    transaction must roll back — leaving neither a partial run row
    (which would wedge the next tick's ``has_non_terminal_run``
    pre-flight) nor an orphan ``queue_jobs`` row (which would mislead
    ``lifecycle_reaper``).

    Simulated by stubbing ``QueueJobStore.create`` to raise on the
    second per-org insert. The dispatcher's helper must propagate the
    exception and leave the database exactly as it was before the
    call.
    """
    async with db_session.begin():
        await _seed_org(
            db_session,
            slug="org-a-atomic",
            lifecycle_rules=_ORG_RULES,
        )
        await _seed_org(
            db_session,
            slug="org-b-atomic",
            lifecycle_rules=_ORG_RULES,
        )

    class _BoomError(RuntimeError):
        """Sentinel exception raised mid-fanout to trigger rollback."""

    original_create = QueueJobStore.create
    call_count = 0

    async def _flaky_create(self: QueueJobStore, **kwargs: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            msg = "simulated mid-fanout crash"
            raise _BoomError(msg)
        return await original_create(self, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(QueueJobStore, "create", _flaky_create)

    async for session in db_session_dependency():
        run_store = LifecycleEvalRunStore(session=session, logger=_logger())
        queue_job_store = QueueJobStore(session=session, logger=_logger())
        org_store = OrganizationStore(session=session, logger=_logger())
        async with session.begin():
            orgs = await org_store.list_all()
        assert len(orgs) == 2

        with pytest.raises(_BoomError):
            await _create_run_with_children(
                session=session,
                run_store=run_store,
                queue_job_store=queue_job_store,
                orgs=orgs,
                orgs_skipped=0,
            )

    # The whole transaction must have rolled back: no runs row and no
    # queue_jobs row should be visible from a fresh session.
    async for session in db_session_dependency():
        async with session.begin():
            runs = (
                (await session.execute(select(SqlLifecycleEvalRun)))
                .scalars()
                .all()
            )
            assert runs == []
            queue_jobs = (
                (
                    await session.execute(
                        select(SqlQueueJob).where(
                            SqlQueueJob.kind == JobKind.lifecycle_eval.value
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert queue_jobs == []
