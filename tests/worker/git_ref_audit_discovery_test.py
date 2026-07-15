"""Tests for the ``git_ref_audit_discovery`` worker function.

Seeds a mix of orgs (one with GitHub-bound projects, one with only
non-GitHub projects, one empty) and asserts the cron handler creates
one ``git_ref_audit_runs`` row plus one per-org ``queue_jobs`` row for
each in-scope org, leaving the other orgs untouched.
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
    GitRefAuditRunStatus,
    JobKind,
    OrganizationCreate,
    ProjectCreate,
)
from docverse.client.models.projects import ProjectGitHubBindingCreate
from docverse.config import config as runtime_config
from docverse.dbschema.git_ref_audit_run import SqlGitRefAuditRun
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.queue import JobStatus
from docverse.storage.git_ref_audit_run_store import GitRefAuditRunStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.git_ref_audit_discovery import (
    git_ref_audit_discovery,
)
from docverse.worker.queues import MAINTENANCE_QUEUE_NAME
from tests.support.arq_testing import get_jobs_by_name, register_queue
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


@pytest.fixture(autouse=True)
def _enable_audit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Turn on the audit feature flag for every test in this module."""
    monkeypatch.setattr(runtime_config, "git_ref_audit_enabled", True)


async def _seed_org(db_session: AsyncSession, *, slug: str) -> tuple[int, str]:
    org_store = OrganizationStore(session=db_session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"GAD Org {slug}",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id, org.slug


async def _seed_github_project(
    db_session: AsyncSession,
    *,
    org_id: int,
    slug: str,
    owner: str,
    repo: str,
) -> int:
    project_store = ProjectStore(session=db_session, logger=_logger())
    project = await project_store.create(
        org_id=org_id,
        data=ProjectCreate(
            slug=slug,
            title=f"Project {slug}",
            github=ProjectGitHubBindingCreate(owner=owner, repo=repo),
        ),
        github_owner=owner,
        github_repo=repo,
    )
    return project.id


async def _seed_non_github_project(
    db_session: AsyncSession, *, org_id: int, slug: str
) -> int:
    project_store = ProjectStore(session=db_session, logger=_logger())
    project = await project_store.create(
        org_id=org_id,
        data=ProjectCreate(
            slug=slug,
            title=f"Project {slug}",
            source_url=f"https://gitlab.example.com/{slug}",
        ),
    )
    return project.id


@pytest.mark.asyncio
async def test_discovery_fans_out_per_in_scope_org(
    app: None,
    db_session: AsyncSession,
) -> None:
    """One run row + one queue_job per org with GitHub-bound projects.

    Three orgs:

    * ``gad-gh-org``: has one GitHub-bound project and one non-GitHub
      project — in scope.
    * ``gad-non-gh-org``: every project is non-GitHub — out of scope.
    * ``gad-empty-org``: no projects at all — out of scope.

    Expected: one ``git_ref_audit_runs`` row in ``in_progress``, one
    ``queue_jobs`` row for the first org with the right
    ``git_ref_audit_run_id`` FK, and ``summary == {orgs_enqueued: 1,
    orgs_skipped: 2}``.
    """
    async with db_session.begin():
        org_a_id, org_a_slug = await _seed_org(db_session, slug="gad-gh-org")
        await _seed_github_project(
            db_session,
            org_id=org_a_id,
            slug="a-gh",
            owner="acme",
            repo="docs",
        )
        await _seed_non_github_project(
            db_session, org_id=org_a_id, slug="a-non-gh"
        )

        org_b_id, _ = await _seed_org(db_session, slug="gad-non-gh-org")
        await _seed_non_github_project(
            db_session, org_id=org_b_id, slug="b-only"
        )

        await _seed_org(db_session, slug="gad-empty-org")

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, MAINTENANCE_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await git_ref_audit_discovery(ctx)
    await http_client.aclose()
    assert result == "completed"

    audit_jobs = get_jobs_by_name(
        mock_arq, "git_ref_audit", queue_name=MAINTENANCE_QUEUE_NAME
    )
    assert len(audit_jobs) == 1
    payload_slugs = {job.kwargs["payload"]["org_slug"] for job in audit_jobs}
    assert payload_slugs == {org_a_slug}

    async for session in db_session_dependency():
        async with session.begin():
            runs = (
                (await session.execute(select(SqlGitRefAuditRun)))
                .scalars()
                .all()
            )
            assert len(runs) == 1
            run = runs[0]
            assert run.status == GitRefAuditRunStatus.in_progress.value
            assert run.summary == {
                "orgs_enqueued": 1,
                "orgs_skipped": 2,
            }

            qj_stmt = select(SqlQueueJob).where(
                SqlQueueJob.kind == JobKind.git_ref_audit.value
            )
            queue_jobs = (await session.execute(qj_stmt)).scalars().all()
            assert len(queue_jobs) == 1
            qj = queue_jobs[0]
            assert qj.org_id == org_a_id
            assert qj.subject_label == org_a_slug
            assert qj.git_ref_audit_run_id == run.id
            assert qj.backend_job_id is not None
            assert qj.status == JobStatus.queued.value


@pytest.mark.asyncio
async def test_discovery_with_no_in_scope_orgs_terminates_run_succeeded(
    app: None,
    db_session: AsyncSession,
) -> None:
    """An all-skipped tick finalises the run as ``succeeded`` immediately."""
    async with db_session.begin():
        org_id, _ = await _seed_org(db_session, slug="gad-empty-only")
        await _seed_non_github_project(
            db_session, org_id=org_id, slug="only-non-gh"
        )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, MAINTENANCE_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await git_ref_audit_discovery(ctx)
    await http_client.aclose()
    assert result == "completed"

    audit_jobs = get_jobs_by_name(
        mock_arq, "git_ref_audit", queue_name=MAINTENANCE_QUEUE_NAME
    )
    assert audit_jobs == []

    async for session in db_session_dependency():
        async with session.begin():
            runs = (
                (await session.execute(select(SqlGitRefAuditRun)))
                .scalars()
                .all()
            )
            assert len(runs) == 1
            run = runs[0]
            assert run.status == GitRefAuditRunStatus.succeeded.value
            assert run.date_finished is not None
            assert run.summary == {
                "orgs_enqueued": 0,
                "orgs_skipped": 1,
            }


@pytest.mark.asyncio
async def test_discovery_skips_tick_when_prior_run_in_flight(
    app: None,
    db_session: AsyncSession,
) -> None:
    """A pre-existing non-terminal run causes this tick to no-op.

    Mirrors the lifecycle_eval dispatcher's singleton-tick guard.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gad-prior")
        await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="a-gh",
            owner="acme",
            repo="docs",
        )
        run_store = GitRefAuditRunStore(session=db_session, logger=_logger())
        existing_run = await run_store.create()
        await run_store.transition_status(
            run_id=existing_run.id,
            new_status=GitRefAuditRunStatus.in_progress,
        )
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        await queue_job_store.create(
            kind=JobKind.git_ref_audit,
            org_id=org_id,
            subject_label=org_slug,
            git_ref_audit_run_id=existing_run.id,
        )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, MAINTENANCE_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await git_ref_audit_discovery(ctx)
    await http_client.aclose()
    assert result == "skipped"

    audit_jobs = get_jobs_by_name(
        mock_arq, "git_ref_audit", queue_name=MAINTENANCE_QUEUE_NAME
    )
    assert audit_jobs == []

    async for session in db_session_dependency():
        async with session.begin():
            runs = (
                (await session.execute(select(SqlGitRefAuditRun)))
                .scalars()
                .all()
            )
            assert len(runs) == 1
            assert runs[0].id == existing_run.id


@pytest.mark.asyncio
async def test_discovery_disabled_by_feature_flag(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``git_ref_audit_enabled=False`` returns ``skipped`` and writes no rows.

    Phalanx ships the flag false in production until the audit's
    GitHub API budget is observed live. The cron must stay registered
    so flipping the flag does not require a worker restart, but it
    must not create a ``git_ref_audit_runs`` row or any per-org
    ``queue_jobs`` children when disabled.
    """
    monkeypatch.setattr(runtime_config, "git_ref_audit_enabled", False)

    async with db_session.begin():
        org_id, _ = await _seed_org(db_session, slug="gad-disabled")
        await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="a-gh",
            owner="acme",
            repo="docs",
        )

    http_client = httpx.AsyncClient()
    mock_arq = MockArqQueue(default_queue_name="docverse:queue")
    register_queue(mock_arq, MAINTENANCE_QUEUE_NAME)
    ctx = make_worker_ctx(http_client=http_client, arq_queue=mock_arq)

    result = await git_ref_audit_discovery(ctx)
    await http_client.aclose()
    assert result == "skipped"

    async for session in db_session_dependency():
        async with session.begin():
            runs = (
                (await session.execute(select(SqlGitRefAuditRun)))
                .scalars()
                .all()
            )
            assert runs == []
            queue_jobs = (
                (
                    await session.execute(
                        select(SqlQueueJob).where(
                            SqlQueueJob.kind == JobKind.git_ref_audit.value
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert queue_jobs == []
