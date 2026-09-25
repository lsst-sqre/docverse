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
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.models import (
    BuildCreate,
    BuildStatus,
    EditionKind,
    GitRefAuditRunStatus,
    JobKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.models.projects import ProjectGitHubBindingCreate
from docverse.models.queue_enums import PublishStatus
from docverse_server.config import Configuration
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.dbschema.project import SqlProject
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.domain.base32id import (
    generate_base32_id,
    validate_base32_id,
)
from docverse_server.domain.edition import DEFAULT_EDITION_SLUG, Edition
from docverse_server.domain.lifecycle import (
    DraftInactivityRule,
    LifecycleRuleSet,
    RefDeletedRule,
)
from docverse_server.domain.project import Project
from docverse_server.domain.queue import JobStatus
from docverse_server.metrics import (
    DocverseEvents,
    LifecycleAction,
    LifecycleActionTrigger,
    LifecycleReapAction,
    MetricsEditionKind,
    build_event_manager,
)
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.git_ref_audit_run_store import GitRefAuditRunStore
from docverse_server.storage.github import GITHUB_API_BASE_URL
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.queue_job_store import QueueJobStore
from docverse_server.worker.functions.git_ref_audit import git_ref_audit
from tests.support.arq_testing import count_jobs_by_name
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
    default_branch: str | None = "main",
) -> None:
    """Seed the live ref set and ``GET /repos`` the audit reads per project.

    The repository read reports ``default_branch`` (PRD #721); pass
    ``None`` to leave it unseeded when a test seeds its own response.
    """
    branches = branches if branches is not None else []
    tags = tags if tags is not None else []
    if default_branch is not None:
        router.get(f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": 1,
                    "name": repo,
                    "owner": {"login": owner, "id": 1},
                    "default_branch": default_branch,
                },
            )
        )
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


async def _run_audit(
    *,
    mock_github: GitHubMock,
    org_id: int,
    org_slug: str,
    run_id: int,
    queue_job_id: int,
    events: DocverseEvents | None = None,
    arq_queue: MockArqQueue | None = None,
) -> str:
    """Run one ``git_ref_audit`` pass for the org with a fresh client."""
    async with httpx.AsyncClient() as http_client:
        ctx = make_worker_ctx(
            http_client=http_client,
            arq_queue=arq_queue,
            github_app_id=mock_github.app_id,
            github_app_private_key=SecretStr(mock_github.private_key_pem),
            github_webhook_secret=SecretStr("webhook-secret"),
            events=events,
        )
        return await git_ref_audit(
            ctx,
            {
                "org_id": org_id,
                "org_slug": org_slug,
                "git_ref_audit_run_id": run_id,
                "queue_job_id": queue_job_id,
            },
        )


async def _load_project(project_id: int) -> Project:
    async for session in db_session_dependency():
        async with session.begin():
            project = await ProjectStore(
                session=session, logger=_logger()
            ).get_by_id(project_id)
        assert project is not None
        return project
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_git_ref_audit_fills_a_null_default_branch(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A tick records the default branch GitHub reports (PRD #721).

    The column starts ``NULL`` — a project created before the column
    existed, or one no resolve has reached — and the audit's one
    ``GET /repos/{owner}/{repo}`` per project fills it. This is the
    backfill for every existing project.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-db-fill")
        installation_id = mock_github.seed_installation(
            "acme", "fill", installation_id=81, owner_id=555
        )
        project_id = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="fill-project",
            owner="acme",
            repo="fill",
            installation_id=installation_id,
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )
    assert (await _load_project(project_id)).github_default_branch is None

    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="fill",
        branches=["master"],
        default_branch="master",
    )

    result = await _run_audit(
        mock_github=mock_github,
        org_id=org_id,
        org_slug=org_slug,
        run_id=run_id,
        queue_job_id=queue_job_id,
    )

    assert result == "completed"
    assert (await _load_project(project_id)).github_default_branch == "master"


_BUILD_BASE = datetime(2026, 1, 1, tzinfo=UTC)


async def _seed_main_edition(
    db_session: AsyncSession,
    *,
    project_id: int,
    git_ref: str,
    tracking_mode: TrackingMode = TrackingMode.git_ref,
) -> int:
    """Seed a project's ``__main`` edition tracking ``git_ref``."""
    edition = await EditionStore(
        session=db_session, logger=_logger()
    ).create_internal(
        project_id=project_id,
        slug=DEFAULT_EDITION_SLUG,
        title="Latest",
        kind=EditionKind.main,
        tracking_mode=tracking_mode,
        tracking_params=(
            {"git_ref": git_ref}
            if tracking_mode is TrackingMode.git_ref
            else None
        ),
    )
    return edition.id


async def _seed_completed_build(
    db_session: AsyncSession,
    *,
    project_id: int,
    project_slug: str,
    git_ref: str,
    days: int,
) -> int:
    """Seed a completed build on ``git_ref`` dated ``days`` in."""
    store = BuildStore(session=db_session, logger=_logger())
    build = await store.create(
        project_id=project_id,
        project_slug=project_slug,
        data=BuildCreate(git_ref=git_ref, content_hash="sha256:" + "d" * 64),
        uploader="testuser",
    )
    await store.transition_status(
        build_id=build.id, new_status=BuildStatus.processing
    )
    await store.transition_status(
        build_id=build.id, new_status=BuildStatus.completed
    )
    await db_session.execute(
        update(SqlBuild)
        .where(SqlBuild.id == build.id)
        .values(date_created=_BUILD_BASE + timedelta(days=days))
    )
    return build.id


async def _load_edition(edition_id: int) -> Edition | None:
    async for session in db_session_dependency():
        async with session.begin():
            return await EditionStore(
                session=session, logger=_logger()
            ).get_by_id(edition_id)
    msg = "No database session available"
    raise RuntimeError(msg)


@pytest.mark.asyncio
async def test_git_ref_audit_rewrites_main_tracking_a_gone_ref(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A ``__main`` on a ref missing from the live set follows the default.

    The missed-webhook case: the repository renamed ``master`` to
    ``main``, so ``master`` is absent from the live branches, builds
    arrived on ``main``, and tracking auto-created a ``main`` draft. The
    tick rewrites ``__main`` onto ``main``, retires the draft, repoints
    ``__main`` at the newest ``main`` build (publish job queued), and
    announces it as a ``PATCH`` would: one ``edition_lifecycle``
    ``update`` event and one ``dashboard_build``. Every service line it
    logs carries ``trigger=audit``.
    """
    manager, events = await build_event_manager(Configuration())
    arq_queue = MockArqQueue(default_queue_name=Configuration().arq_queue_name)
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-db-rewrite")
        installation_id = mock_github.seed_installation(
            "acme", "renamed", installation_id=82, owner_id=555
        )
        project_id = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="renamed-project",
            owner="acme",
            repo="renamed",
            installation_id=installation_id,
        )
        main_id = await _seed_main_edition(
            db_session, project_id=project_id, git_ref="master"
        )
        master_build = await _seed_completed_build(
            db_session,
            project_id=project_id,
            project_slug="renamed-project",
            git_ref="master",
            days=1,
        )
        await EditionStore(
            session=db_session, logger=_logger()
        ).set_current_build(edition_id=main_id, build_id=master_build)
        main_build = await _seed_completed_build(
            db_session,
            project_id=project_id,
            project_slug="renamed-project",
            git_ref="main",
            days=2,
        )
        draft_id = await _seed_draft_edition(
            db_session, project_id=project_id, slug="main", git_ref="main"
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="renamed",
        branches=["main"],
        tags=["v1.0"],
        default_branch="main",
    )

    with capture_logs() as captured:
        result = await _run_audit(
            mock_github=mock_github,
            org_id=org_id,
            org_slug=org_slug,
            run_id=run_id,
            queue_job_id=queue_job_id,
            events=events,
            arq_queue=arq_queue,
        )

    assert result == "completed"
    assert (await _load_project(project_id)).github_default_branch == "main"
    main = await _load_edition(main_id)
    assert main is not None
    assert main.tracking_params == {"git_ref": "main"}
    assert main.current_build_id == main_build
    assert main.publish_status is PublishStatus.pending
    assert await _load_edition(draft_id) is None
    assert count_jobs_by_name(arq_queue, "publish_edition") == 1
    assert count_jobs_by_name(arq_queue, "dashboard_build") == 1
    publisher = events.edition_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    [event] = publisher.published
    assert event.action is LifecycleAction.update
    assert event.edition_kind is MetricsEditionKind.main
    assert (event.organization, event.project) == (org_slug, "renamed-project")
    [rewrite_log] = [
        entry
        for entry in captured
        if entry["event"] == "Rewrote __main to track the default branch"
    ]
    assert rewrite_log["trigger"] == "audit"
    assert rewrite_log["main_rewritten_from"] == "master"
    await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("main_mode", "main_ref"),
    [
        (TrackingMode.git_ref, "docs"),
        (TrackingMode.git_ref, "v1.0"),
        (TrackingMode.lsst_doc, ""),
    ],
    ids=["live-branch", "live-tag", "lsst-doc"],
)
async def test_git_ref_audit_leaves_main_on_a_live_ref(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
    main_mode: TrackingMode,
    main_ref: str,
) -> None:
    """A ``__main`` pinned to a live branch or tag, or in ``lsst_doc``, stays.

    The tick still records the default branch, but ``__main`` keeps its
    tracking and pointer, the ``main`` draft survives, and nothing is
    queued or announced.
    """
    manager, events = await build_event_manager(Configuration())
    arq_queue = MockArqQueue(default_queue_name=Configuration().arq_queue_name)
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-db-pinned")
        installation_id = mock_github.seed_installation(
            "acme", "pinned", installation_id=83, owner_id=555
        )
        project_id = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="pinned-project",
            owner="acme",
            repo="pinned",
            installation_id=installation_id,
        )
        main_id = await _seed_main_edition(
            db_session,
            project_id=project_id,
            git_ref=main_ref,
            tracking_mode=main_mode,
        )
        await _seed_completed_build(
            db_session,
            project_id=project_id,
            project_slug="pinned-project",
            git_ref="main",
            days=2,
        )
        draft_id = await _seed_draft_edition(
            db_session, project_id=project_id, slug="main", git_ref="main"
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="pinned",
        branches=["main", "docs"],
        tags=["v1.0"],
        default_branch="main",
    )

    result = await _run_audit(
        mock_github=mock_github,
        org_id=org_id,
        org_slug=org_slug,
        run_id=run_id,
        queue_job_id=queue_job_id,
        events=events,
        arq_queue=arq_queue,
    )

    assert result == "completed"
    assert (await _load_project(project_id)).github_default_branch == "main"
    main = await _load_edition(main_id)
    assert main is not None
    assert main.tracking_mode is main_mode
    if main_mode is TrackingMode.git_ref:
        assert main.tracking_params == {"git_ref": main_ref}
    assert main.current_build_id is None
    assert await _load_edition(draft_id) is not None
    assert count_jobs_by_name(arq_queue, "publish_edition") == 0
    assert count_jobs_by_name(arq_queue, "dashboard_build") == 0
    publisher = events.edition_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    assert publisher.published == []
    await manager.aclose()


@pytest.mark.asyncio
async def test_git_ref_audit_isolates_a_repository_metadata_failure(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """One project's failed ``GET /repos`` does not stop the org's pass.

    The failing project keeps its ``NULL`` column until a later tick,
    but its ref set was fetched, so its ``ref_deleted`` reaping still
    runs; the healthy project's column is filled. The failure is logged
    and rolls the queue-job row to ``completed_with_errors`` like a
    ref-set failure.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-db-flaky")
        flaky_installation = mock_github.seed_installation(
            "acme", "meta-flaky", installation_id=84, owner_id=555
        )
        healthy_installation = mock_github.seed_installation(
            "acme", "meta-healthy", installation_id=85, owner_id=555
        )
        flaky_id = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="meta-flaky",
            owner="acme",
            repo="meta-flaky",
            installation_id=flaky_installation,
        )
        healthy_id = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="meta-healthy",
            owner="acme",
            repo="meta-healthy",
            installation_id=healthy_installation,
        )
        reaped_id = await _seed_draft_edition(
            db_session,
            project_id=flaky_id,
            slug="gone-branch",
            git_ref="tickets/DM-gone",
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="meta-flaky",
        branches=["main"],
        default_branch=None,
    )
    mock_github.router.get(
        f"{GITHUB_API_BASE_URL}/repos/acme/meta-flaky"
    ).mock(return_value=httpx.Response(502, json={"message": "Bad Gateway"}))
    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="meta-healthy",
        branches=["master"],
        default_branch="master",
    )

    with capture_logs() as captured:
        result = await _run_audit(
            mock_github=mock_github,
            org_id=org_id,
            org_slug=org_slug,
            run_id=run_id,
            queue_job_id=queue_job_id,
        )

    assert result == "completed_with_errors"
    assert (await _load_project(flaky_id)).github_default_branch is None
    assert (await _load_project(healthy_id)).github_default_branch == "master"
    assert await _load_edition(reaped_id) is None
    [failure_log] = [
        entry
        for entry in captured
        if entry["event"].startswith(
            "Git ref audit: GitHub repository metadata fetch failed"
        )
    ]
    assert failure_log["project"] == "meta-flaky"
    assert failure_log["error_type"] == "RepositoryRefFetchError"

    async for session in db_session_dependency():
        async with session.begin():
            qj = await QueueJobStore(session=session, logger=_logger()).get(
                queue_job_id
            )
            assert qj is not None
            assert qj.status == JobStatus.completed_with_errors
            run = await GitRefAuditRunStore(
                session=session, logger=_logger()
            ).get(run_id)
            assert run is not None
            assert run.status is GitRefAuditRunStatus.partial_failure


@pytest.mark.asyncio
async def test_git_ref_audit_reads_an_anonymous_default_branch(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A project with no installation reads ``GET /repos`` anonymously.

    The same public-API path its ref set takes, so a public repository
    the App was never installed on still gets its column filled.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-db-anon")
        project_id = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="anon-project",
            owner="acme",
            repo="anon",
            installation_id=None,
        )
        run_id, queue_job_id = await _seed_run_and_queue_job(
            db_session, org_id=org_id, org_slug=org_slug
        )

    _seed_refs(
        mock_github.router,
        owner="acme",
        repo="anon",
        branches=["master"],
        default_branch="master",
    )

    result = await _run_audit(
        mock_github=mock_github,
        org_id=org_id,
        org_slug=org_slug,
        run_id=run_id,
        queue_job_id=queue_job_id,
    )

    assert result == "completed"
    assert (await _load_project(project_id)).github_default_branch == "master"
    [request] = [
        call.request
        for call in mock_github.router.calls
        if call.request.url.path == "/repos/acme/anon"
    ]
    assert "authorization" not in {k.lower() for k in request.headers}


@pytest.mark.asyncio
async def test_git_ref_audit_summary_counts_default_branch_work(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """The per-org summary reports updates and rewrites, then zeros.

    The first tick fills one ``NULL`` column and rewrites one ``__main``
    (a second project's column only fills); a repeat tick against the
    same GitHub state finds nothing left to change.
    """
    async with db_session.begin():
        org_id, org_slug = await _seed_org(db_session, slug="gra-db-summary")
        for repo, installation_id in (("sum-a", 86), ("sum-b", 87)):
            mock_github.seed_installation(
                "acme", repo, installation_id=installation_id, owner_id=555
            )
        project_a = await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="sum-a",
            owner="acme",
            repo="sum-a",
            installation_id=86,
        )
        await _seed_github_project(
            db_session,
            org_id=org_id,
            slug="sum-b",
            owner="acme",
            repo="sum-b",
            installation_id=87,
        )
        await _seed_main_edition(
            db_session, project_id=project_a, git_ref="master"
        )

    for repo in ("sum-a", "sum-b"):
        _seed_refs(
            mock_github.router,
            owner="acme",
            repo=repo,
            branches=["main"],
            default_branch="main",
        )

    summaries = []
    for _tick in range(2):
        async with db_session.begin():
            run_id, queue_job_id = await _seed_run_and_queue_job(
                db_session, org_id=org_id, org_slug=org_slug
            )
        with capture_logs() as captured:
            result = await _run_audit(
                mock_github=mock_github,
                org_id=org_id,
                org_slug=org_slug,
                run_id=run_id,
                queue_job_id=queue_job_id,
            )
        assert result == "completed"
        [summary] = [
            entry
            for entry in captured
            if entry["event"] == "Git ref audit completed for org"
        ]
        summaries.append(
            (summary["default_branch_updates"], summary["main_rewrites"])
        )

    assert summaries == [(2, 1), (0, 0)]
