"""Tests for the ``git_ref_audit`` per-org worker function.

Seeds an org with a mix of GitHub-bound and non-GitHub projects, plus
literal-ref draft editions whose ``tracking_params['git_ref']`` does
or does not appear in the mocked GitHub matching-refs response, and
asserts the worker soft-deletes exactly the right editions, transitions
the ``queue_jobs`` row to the correct terminal status, and finalises
the parent ``git_ref_audit_runs`` row.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx
import structlog
from pydantic import SecretStr
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    EditionKind,
    GitRefAuditRunStatus,
    JobKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.client.models.projects import ProjectGitHubBindingCreate
from docverse.config import Configuration
from docverse.dbschema.edition import SqlEdition
from docverse.dbschema.project import SqlProject
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.domain.base32id import generate_base32_id, validate_base32_id
from docverse.domain.lifecycle import (
    DraftInactivityRule,
    LifecycleRuleSet,
    RefDeletedRule,
)
from docverse.domain.queue import JobStatus
from docverse.metrics import (
    DocverseEvents,
    LifecycleActionTrigger,
    LifecycleReapAction,
    build_event_manager,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.git_ref_audit_run_store import GitRefAuditRunStore
from docverse.storage.github import GITHUB_API_BASE_URL
from docverse.storage.keeper_sync import KeeperSyncStateStore, ResourceType
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.git_ref_audit import git_ref_audit
from tests.support.github_mock import GitHubMock
from tests.worker.conftest import make_worker_ctx


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


_REF_DELETED_RULES = LifecycleRuleSet(root=[RefDeletedRule()])


async def _seed_org(
    db_session: AsyncSession,
    *,
    slug: str = "gra-org",
    lifecycle_rules: LifecycleRuleSet | None = _REF_DELETED_RULES,
) -> tuple[int, str]:
    org_store = OrganizationStore(session=db_session, logger=_logger())
    org = await org_store.create(
        OrganizationCreate(
            slug=slug,
            title=f"GRA Org {slug}",
            base_domain=f"{slug}.example.com",
            lifecycle_rules=lifecycle_rules,
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
    installation_id: int | None = None,
) -> int:
    """Seed a GitHub-bound project with optional captured installation_id."""
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
    if installation_id is not None:
        await db_session.execute(
            update(SqlProject)
            .where(SqlProject.id == project.id)
            .values(github_installation_id=installation_id)
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


async def _seed_draft_edition(
    db_session: AsyncSession,
    *,
    project_id: int,
    slug: str,
    git_ref: str,
    tracking_mode: TrackingMode = TrackingMode.git_ref,
    lifecycle_exempt: bool = False,
    date_updated: datetime | None = None,
) -> int:
    edition_store = EditionStore(session=db_session, logger=_logger())
    edition = await edition_store.create_internal(
        project_id=project_id,
        slug=slug,
        title=f"Edition {slug}",
        kind=EditionKind.draft,
        tracking_mode=tracking_mode,
        tracking_params={"git_ref": git_ref},
        lifecycle_exempt=lifecycle_exempt,
    )
    if date_updated is not None:
        # Bypass ``onupdate=func.now()`` so the backdated timestamp
        # survives — mirrors ``lifecycle_eval_test._seed_edition``.
        await db_session.execute(
            update(SqlEdition)
            .where(SqlEdition.id == edition.id)
            .values(date_updated=date_updated)
        )
    return edition.id


async def _seed_run_and_queue_job(
    db_session: AsyncSession, *, org_id: int, org_slug: str
) -> tuple[int, int]:
    run_store = GitRefAuditRunStore(session=db_session, logger=_logger())
    run = await run_store.create()
    await run_store.transition_status(
        run_id=run.id, new_status=GitRefAuditRunStatus.in_progress
    )
    row = SqlQueueJob(
        public_id=validate_base32_id(generate_base32_id()),
        kind=JobKind.git_ref_audit.value,
        status=JobStatus.queued.value,
        org_id=org_id,
        git_ref_audit_run_id=run.id,
        subject_label=org_slug,
    )
    db_session.add(row)
    await db_session.flush()
    return run.id, row.id


def _ref_entry(ref: str) -> dict[str, object]:
    return {
        "ref": ref,
        "node_id": f"node-{ref}",
        "url": f"https://api.github.com/{ref}",
        "object": {"sha": "deadbeef", "type": "commit"},
    }


def _seed_refs(
    router: respx.Router,
    *,
    owner: str,
    repo: str,
    branches: list[str] | None = None,
    tags: list[str] | None = None,
) -> None:
    branches = branches if branches is not None else []
    tags = tags if tags is not None else []
    router.get(
        f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}/git/matching-refs/heads"
    ).mock(
        return_value=httpx.Response(
            200, json=[_ref_entry(f"refs/heads/{n}") for n in branches]
        )
    )
    router.get(
        f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}/git/matching-refs/tags"
    ).mock(
        return_value=httpx.Response(
            200, json=[_ref_entry(f"refs/tags/{n}") for n in tags]
        )
    )


def _seed_refs_404(router: respx.Router, *, owner: str, repo: str) -> None:
    """Seed 404 on the heads endpoint to simulate inaccessible repos."""
    router.get(
        f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}/git/matching-refs/heads"
    ).mock(return_value=httpx.Response(404, json={"message": "Not Found"}))


def _seed_refs_500(router: respx.Router, *, owner: str, repo: str) -> None:
    """Seed 500 on the heads endpoint to simulate a transient fetch error."""
    router.get(
        f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}/git/matching-refs/heads"
    ).mock(return_value=httpx.Response(500, json={"message": "boom"}))


def _make_ctx(
    *,
    http_client: httpx.AsyncClient,
    mock_github: GitHubMock,
    events: DocverseEvents | None = None,
) -> dict[str, object]:
    return make_worker_ctx(
        http_client=http_client,
        github_app_id=mock_github.app_id,
        github_app_private_key=SecretStr(mock_github.private_key_pem),
        github_webhook_secret=SecretStr("webhook-secret"),
        events=events,
    )


@pytest.mark.asyncio
async def test_git_ref_audit_soft_deletes_missing_refs_only(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Editions whose tracked ref is absent from GitHub are soft-deleted.

    Seeds two GitHub-bound projects (one with an installation, one
    anonymous) and one non-GitHub project. For the installed project,
    the mocked GitHub response carries one live branch (``main``) and
    one live tag (``v1.0``); the project has four draft editions —
    one matching the live branch (kept), one matching the live tag
    (kept), one tracking a deleted branch (soft-deleted), and one
    matching the deleted branch but ``lifecycle_exempt=True`` (kept).
    The anonymous project has one draft tracking a branch missing
    from the mocked anon-path response (soft-deleted). The non-GitHub
    project's draft is never touched.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session)
        installation_id = mock_github.seed_installation(
            "acme", "docs", installation_id=42, owner_id=111
        )
        proj_installed = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="acme-docs",
            owner="acme",
            repo="docs",
            installation_id=installation_id,
        )
        proj_anon = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="acme-public",
            owner="acme",
            repo="public",
            installation_id=None,
        )
        proj_non_github = await _seed_non_github_project(
            db_session, org_id=org_id, slug="gitlab-mirror"
        )

        kept_branch_id = await _seed_draft_edition(
            db_session,
            project_id=proj_installed,
            slug="kept-branch",
            git_ref="main",
        )
        kept_tag_id = await _seed_draft_edition(
            db_session,
            project_id=proj_installed,
            slug="kept-tag",
            git_ref="v1.0",
        )
        deleted_branch_id = await _seed_draft_edition(
            db_session,
            project_id=proj_installed,
            slug="deleted-branch",
            git_ref="tickets/DM-deleted",
        )
        exempt_deleted_id = await _seed_draft_edition(
            db_session,
            project_id=proj_installed,
            slug="exempt-deleted",
            git_ref="tickets/DM-also-deleted",
            lifecycle_exempt=True,
        )
        anon_deleted_id = await _seed_draft_edition(
            db_session,
            project_id=proj_anon,
            slug="anon-deleted-branch",
            git_ref="tickets/DM-anon-old",
        )
        non_github_draft_id = await _seed_draft_edition(
            db_session,
            project_id=proj_non_github,
            slug="non-github-draft",
            git_ref="develop",
        )

        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="docs",
        branches=["main"],
        tags=["v1.0"],
    )
    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="public",
        branches=["main"],
        tags=[],
    )

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await git_ref_audit(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "git_ref_audit_run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            kept_branch = await edition_store.get_by_slug(
                project_id=proj_installed, slug="kept-branch"
            )
            assert kept_branch is not None
            assert kept_branch.id == kept_branch_id
            kept_tag = await edition_store.get_by_slug(
                project_id=proj_installed, slug="kept-tag"
            )
            assert kept_tag is not None
            assert kept_tag.id == kept_tag_id
            exempt = await edition_store.get_by_slug(
                project_id=proj_installed, slug="exempt-deleted"
            )
            assert exempt is not None
            assert exempt.id == exempt_deleted_id
            non_github = await edition_store.get_by_slug(
                project_id=proj_non_github, slug="non-github-draft"
            )
            assert non_github is not None
            assert non_github.id == non_github_draft_id

            for deleted_id in (deleted_branch_id, anon_deleted_id):
                result_row = await session.execute(
                    select(SqlEdition.date_deleted).where(
                        SqlEdition.id == deleted_id
                    )
                )
                assert result_row.scalar_one() is not None

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.completed

            run_store = GitRefAuditRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status is GitRefAuditRunStatus.succeeded


@pytest.mark.asyncio
async def test_git_ref_audit_publishes_lifecycle_action(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Each ref-deleted reap publishes ``lifecycle_action`` (trigger=audit).

    Seeds one GitHub-bound project with a draft tracking a live branch
    (kept) and a draft tracking a branch absent from GitHub (reaped),
    runs the worker with an initialized event manager, and asserts a
    single ``lifecycle_action`` event for the reaped edition carrying
    ``action=ref_deleted``, ``trigger=git_ref_audit``, and
    ``success=True``.
    """
    manager, events = await build_event_manager(Configuration())

    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-event-org")
        installation_id = mock_github.seed_installation(
            "acme", "evented", installation_id=71, owner_id=333
        )
        project_id = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="acme-evented",
            owner="acme",
            repo="evented",
            installation_id=installation_id,
        )
        await _seed_draft_edition(
            db_session,
            project_id=project_id,
            slug="kept-branch",
            git_ref="main",
        )
        await _seed_draft_edition(
            db_session,
            project_id=project_id,
            slug="deleted-branch",
            git_ref="tickets/DM-gone",
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="evented",
        branches=["main"],
        tags=[],
    )

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(
            http_client=http_client, mock_github=mock_github, events=events
        )
        result = await git_ref_audit(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "git_ref_audit_run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )

    assert result == "completed"

    publisher = events.lifecycle_action
    assert isinstance(publisher, MockEventPublisher)
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.organization == org_slug
    assert event.project == "acme-evented"
    assert event.action is LifecycleReapAction.ref_deleted
    assert event.trigger is LifecycleActionTrigger.git_ref_audit
    assert event.success is True
    await manager.aclose()


@pytest.mark.asyncio
async def test_git_ref_audit_isolates_per_project_fetch_failure(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """One project's failed fetch does not block the rest of the org.

    Seeds two GitHub-bound projects. The first project's matching-refs
    endpoint returns 500 (transient failure); the second's returns a
    normal response. The worker must skip the failing project (log,
    continue), soft-delete the second project's matched edition, and
    end the queue-job row in ``completed_with_errors`` so the parent
    run rolls to ``partial_failure``.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-isolate")
        flaky_installation_id = mock_github.seed_installation(
            "acme", "flaky", installation_id=51, owner_id=222
        )
        good_installation_id = mock_github.seed_installation(
            "acme", "healthy", installation_id=52, owner_id=222
        )
        proj_flaky = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="flaky-project",
            owner="acme",
            repo="flaky",
            installation_id=flaky_installation_id,
        )
        proj_good = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="good-project",
            owner="acme",
            repo="healthy",
            installation_id=good_installation_id,
        )
        await _seed_draft_edition(
            db_session,
            project_id=proj_flaky,
            slug="flaky-draft",
            git_ref="should-not-be-deleted",
        )
        good_deleted_id = await _seed_draft_edition(
            db_session,
            project_id=proj_good,
            slug="good-deleted-draft",
            git_ref="missing-branch",
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    _seed_refs_500(mock_github.router, owner="acme", repo="flaky")
    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="healthy",
        branches=["main"],
        tags=[],
    )

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await git_ref_audit(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "git_ref_audit_run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )

    assert result == "completed_with_errors"

    async for session in db_session_dependency():
        async with session.begin():
            # Flaky project's draft is untouched — fetch failed, no
            # decisions ran against its editions.
            edition_store = EditionStore(session=session, logger=_logger())
            flaky_draft = await edition_store.get_by_slug(
                project_id=proj_flaky, slug="flaky-draft"
            )
            assert flaky_draft is not None
            # Good project's deleted-ref draft IS soft-deleted.
            result_row = await session.execute(
                select(SqlEdition.date_deleted).where(
                    SqlEdition.id == good_deleted_id
                )
            )
            assert result_row.scalar_one() is not None

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.completed_with_errors

            run_store = GitRefAuditRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status is GitRefAuditRunStatus.partial_failure


@pytest.mark.asyncio
async def test_git_ref_audit_404_is_per_pass_skip(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A 404 from GitHub logs + skips the project for this pass.

    Anonymous-mode audit: a public-repo project whose GitHub URL has
    started returning 404 (repo deleted, transferred out, App lost
    access). Acceptance criterion: "a 404 logs and skips for that
    pass". The 404 counts as a per-project failure so the queue-job
    rolls to ``completed_with_errors`` and the parent run to
    ``partial_failure``.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-404")
        proj = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="gone-project",
            owner="acme",
            repo="gone",
            installation_id=None,
        )
        edition_id = await _seed_draft_edition(
            db_session,
            project_id=proj,
            slug="orphan-draft",
            git_ref="develop",
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    _seed_refs_404(mock_github.router, owner="acme", repo="gone")

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await git_ref_audit(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "git_ref_audit_run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )

    assert result == "completed_with_errors"

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            still_there = await edition_store.get_by_slug(
                project_id=proj, slug="orphan-draft"
            )
            # Edition NOT soft-deleted: a 404 must not be confused
            # with "this ref is gone", because we have no evidence
            # of any live ref set at all. The matching-refs response
            # could be 404 for completely unrelated reasons.
            assert still_there is not None
            assert still_there.id == edition_id


@pytest.mark.asyncio
async def test_git_ref_audit_no_github_bound_projects_is_noop(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """An org whose every project is non-GitHub completes cleanly.

    Defensive path: the dispatcher pre-flight filters orgs with no
    GitHub-bound projects, but if the worker still runs (e.g. a
    GitHub binding was cleared between the snapshot and the job
    pickup), the per-org pass must complete cleanly with no
    soft-deletes and no per-project failures.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-no-gh")
        proj = await _seed_non_github_project(
            db_session, org_id=org_id, slug="non-gh-only"
        )
        edition_id = await _seed_draft_edition(
            db_session,
            project_id=proj,
            slug="non-gh-draft",
            git_ref="main",
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await git_ref_audit(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "git_ref_audit_run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            still_there = await edition_store.get_by_slug(
                project_id=proj, slug="non-gh-draft"
            )
            assert still_there is not None
            assert still_there.id == edition_id

            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.completed

            run_store = GitRefAuditRunStore(session=session, logger=_logger())
            run = await run_store.get(run_id)
            assert run is not None
            assert run.status is GitRefAuditRunStatus.succeeded


@pytest.mark.asyncio
async def test_git_ref_audit_no_ref_deleted_rule_no_deletions(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """An org without RefDeletedRule still completes but deletes nothing.

    The audit fetches refs even when the rule set carries no
    ``RefDeletedRule`` — the dispatcher's filter is "GitHub-bound
    projects", not "has the rule". The evaluator's other branches
    only consume context the audit deliberately leaves empty
    (``builds=[]``, ``edition_build_history=[]``), so no matches
    fire from this code path. The pass completes cleanly with no
    soft-deletes — the editions stay live until the hourly
    ``lifecycle_eval`` worker (which loads the full state) gets a
    chance to evaluate them.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, slug="gra-no-rule", lifecycle_rules=None
        )
        installation_id = mock_github.seed_installation(
            "acme", "no-rule", installation_id=61, owner_id=333
        )
        proj = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="no-rule-project",
            owner="acme",
            repo="no-rule",
            installation_id=installation_id,
        )
        edition_id = await _seed_draft_edition(
            db_session,
            project_id=proj,
            slug="deleted-ref-but-no-rule",
            git_ref="missing-branch",
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="no-rule",
        branches=["main"],
        tags=[],
    )

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await git_ref_audit(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "git_ref_audit_run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            still_there = await edition_store.get_by_slug(
                project_id=proj, slug="deleted-ref-but-no-rule"
            )
            assert still_there is not None
            assert still_there.id == edition_id


@pytest.mark.asyncio
async def test_git_ref_audit_missing_org_is_noop(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A missing org id (race with delete) completes cleanly without changes.

    Defensive path: the dispatcher pre-flight snapshot includes the
    org, but it gets soft-deleted before the per-org worker picks up
    the job. The worker logs a warning and exits cleanly so the
    parent run can still finalise.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-missing")
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await git_ref_audit(
            ctx,
            {
                "org_id": 99999,
                "org_slug": "no-such-org",
                "git_ref_audit_run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            queue_job_store = QueueJobStore(session=session, logger=_logger())
            qj = await queue_job_store.get(queue_job_id)
            assert qj is not None
            assert qj.status == JobStatus.completed
            assert qj.subject_label == org_slug
            assert qj.org_id == org_id


@pytest.mark.asyncio
async def test_git_ref_audit_does_not_fire_draft_inactivity(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """An org with both rules: the audit only ref-deletes, never inactivates.

    Regression guard for the cross-firing bug: the daily ref audit owns
    only ``RefDeletedRule``. It filters the resolved rule set down to
    that kind before evaluation, so a co-configured
    ``DraftInactivityRule`` must never soft-delete an inactive draft from
    this code path — that is the hourly ``lifecycle_eval`` worker's job.

    Seeds one GitHub-bound project with two drafts: draft **A** tracks a
    branch (``main``) that is still live but is backdated 60 days past
    the 30-day inactivity threshold, and draft **B** tracks a branch
    that is absent from the live refs. Asserts A survives (proving
    ``DraftInactivityRule`` does not fire) while B is soft-deleted by
    ``RefDeletedRule``.
    """
    both_rules = LifecycleRuleSet(
        root=[RefDeletedRule(), DraftInactivityRule(max_days_inactive=30)]
    )
    async with db_session.begin():
        org_id, org_slug = await _seed_org(
            db_session, slug="gra-both-rules", lifecycle_rules=both_rules
        )
        installation_id = mock_github.seed_installation(
            "acme", "both", installation_id=71, owner_id=444
        )
        proj = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="both-rules-project",
            owner="acme",
            repo="both",
            installation_id=installation_id,
        )
        stale_live_ref_id = await _seed_draft_edition(
            db_session,
            project_id=proj,
            slug="stale-but-live-ref",
            git_ref="main",
            date_updated=datetime.now(tz=UTC) - timedelta(days=60),
        )
        deleted_ref_id = await _seed_draft_edition(
            db_session,
            project_id=proj,
            slug="deleted-ref",
            git_ref="tickets/DM-gone",
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="both",
        branches=["main"],
        tags=[],
    )

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await git_ref_audit(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "git_ref_audit_run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            edition_store = EditionStore(session=session, logger=_logger())
            # Draft A survives: DraftInactivityRule is not owned by the
            # ref audit, so its 60-day staleness is irrelevant here, and
            # its tracked ref is still live.
            survivor = await edition_store.get_by_slug(
                project_id=proj, slug="stale-but-live-ref"
            )
            assert survivor is not None
            assert survivor.id == stale_live_ref_id
            assert survivor.date_deleted is None

            # Draft B is soft-deleted by RefDeletedRule (its ref is gone).
            gone = await edition_store.get_by_slug(
                project_id=proj, slug="deleted-ref"
            )
            assert gone is None
            row = await session.execute(
                select(SqlEdition.date_deleted).where(
                    SqlEdition.id == deleted_ref_id
                )
            )
            assert row.scalar_one() is not None


@pytest.mark.asyncio
async def test_git_ref_audit_writes_lifecycle_delete_tombstone(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Soft-delete via the worker stamps a ``lifecycle_delete`` tombstone."""
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-tomb")
        installation_id = mock_github.seed_installation(
            "acme", "tomb", installation_id=77, owner_id=333
        )
        proj = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="tomb-project",
            owner="acme",
            repo="tomb",
            installation_id=installation_id,
        )
        deleted_id = await _seed_draft_edition(
            db_session,
            project_id=proj,
            slug="dead-ref",
            git_ref="tickets/DM-dead",
        )
        state_store = KeeperSyncStateStore(
            session=db_session, logger=_logger()
        )
        await state_store.upsert(
            org_id=org_id,
            resource_type=ResourceType.edition,
            ltd_id=4242,
            ltd_slug="dead-ref",
            docverse_id=deleted_id,
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="tomb",
        branches=["main"],
        tags=[],
    )

    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(http_client=http_client, mock_github=mock_github)
        result = await git_ref_audit(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "git_ref_audit_run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            state_store = KeeperSyncStateStore(
                session=session, logger=_logger()
            )
            state = await state_store.get(
                org_id=org_id,
                resource_type=ResourceType.edition,
                ltd_id=4242,
                include_tombstoned=True,
            )
            assert state is not None
            assert state.date_tombstoned is not None
            assert state.tombstone_reason == "lifecycle_delete"
