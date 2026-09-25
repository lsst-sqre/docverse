"""Handler-level tests for the GitHub ``repository.edited`` webhook.

A default-branch change converges every project backed by the
repository on the new branch (PRD #721); see
:mod:`tests.services.default_branch_test` for the rule itself. These
tests pin what the delivery adds around it: finding the projects, the
transaction, the post-commit ``publish_edition`` and
``dashboard_build`` enqueues, and the ``edition_lifecycle`` event.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple

import pytest
import pytest_asyncio
import structlog
from fastapi import FastAPI
from httpx import AsyncClient
from pydantic import SecretStr
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.metrics import MockEventPublisher
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

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
from docverse.models.projects import ProjectGitHubBindingCreate
from docverse.models.queue_enums import PublishStatus
from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.queue_job import SqlQueueJob
from docverse_server.dependencies.context import context_dependency
from docverse_server.domain.edition import DEFAULT_EDITION_SLUG, Edition
from docverse_server.domain.project import Project
from docverse_server.metrics import (
    EditionLifecycleEvent,
    GitHubWebhookReceivedEvent,
    LifecycleAction,
    MetricsEditionKind,
    WebhookOutcome,
)
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from tests.support.arq_testing import count_jobs_by_name
from tests.support.github_mock import GitHubMock

_WEBHOOK_PATH = "/docverse/webhooks/github"
_WEBHOOK_SECRET = "test-webhook-secret"
_BASE = datetime(2026, 1, 1, tzinfo=UTC)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


@pytest_asyncio.fixture
async def github_app_enabled(
    app: FastAPI,
    mock_github: GitHubMock,
) -> AsyncIterator[None]:
    saved = (
        context_dependency._github_app_id,
        context_dependency._github_app_private_key,
        context_dependency._github_webhook_secret,
    )
    context_dependency.set_github_secrets(
        app_id=mock_github.app_id,
        private_key=SecretStr(mock_github.private_key_pem),
        webhook_secret=SecretStr(_WEBHOOK_SECRET),
    )
    try:
        yield
    finally:
        context_dependency.set_github_secrets(
            app_id=saved[0],
            private_key=saved[1],
            webhook_secret=saved[2],
        )


class _Seeded(NamedTuple):
    project_id: int
    main_id: int


async def _seed(
    db_session: AsyncSession,
    *,
    org_slug: str,
    main_mode: TrackingMode = TrackingMode.git_ref,
    main_ref: str = "master",
    repo_id: int = 12345,
) -> _Seeded:
    """Seed an org and a project bound to ``acme/docs`` with a ``__main``."""
    async with db_session.begin():
        org = await OrganizationStore(
            session=db_session, logger=_logger()
        ).create(
            OrganizationCreate(
                slug=org_slug,
                title=f"Org {org_slug}",
                base_domain=f"{org_slug}.example.com",
            )
        )
        project_store = ProjectStore(session=db_session, logger=_logger())
        project = await project_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="docs",
                title="Docs",
                github=ProjectGitHubBindingCreate(owner="acme", repo="docs"),
            ),
            github_owner="acme",
            github_repo="docs",
        )
        await project_store.apply_installation_scope(
            installation_id=99,
            owner="acme",
            owner_id=999,
            repo="docs",
            repo_id=repo_id,
        )
        main = await EditionStore(
            session=db_session, logger=_logger()
        ).create_internal(
            project_id=project.id,
            slug=DEFAULT_EDITION_SLUG,
            title="Latest",
            kind=EditionKind.main,
            tracking_mode=main_mode,
            tracking_params=(
                {"git_ref": main_ref}
                if main_mode is TrackingMode.git_ref
                else None
            ),
        )
        await db_session.commit()
    return _Seeded(project_id=project.id, main_id=main.id)


async def _seed_build(
    db_session: AsyncSession, *, project_id: int, git_ref: str, days: int
) -> int:
    """Create a completed build on ``git_ref`` dated ``days`` in."""
    async with db_session.begin():
        store = BuildStore(session=db_session, logger=_logger())
        build = await store.create(
            project_id=project_id,
            project_slug="docs",
            data=BuildCreate(
                git_ref=git_ref, content_hash="sha256:" + "c" * 64
            ),
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
            .values(date_created=_BASE + timedelta(days=days))
        )
        await db_session.commit()
    return build.id


async def _serve(
    db_session: AsyncSession, *, edition_id: int, build_id: int
) -> None:
    async with db_session.begin():
        await EditionStore(
            session=db_session, logger=_logger()
        ).set_current_build(edition_id=edition_id, build_id=build_id)
        await db_session.commit()


async def _seed_main_draft(db_session: AsyncSession, project_id: int) -> int:
    """Seed the ``main`` draft tracking auto-creates before a rename."""
    async with db_session.begin():
        edition = await EditionStore(
            session=db_session, logger=_logger()
        ).create(
            project_id=project_id,
            data=EditionCreate(
                slug="main",
                title="main",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "main"},
            ),
        )
        await db_session.commit()
    return edition.id


async def _project(db_session: AsyncSession, project_id: int) -> Project:
    async with db_session.begin():
        project = await ProjectStore(
            session=db_session, logger=_logger()
        ).get_by_id(project_id)
    assert project is not None
    return project


async def _edition(
    db_session: AsyncSession, edition_id: int
) -> Edition | None:
    async with db_session.begin():
        return await EditionStore(
            session=db_session, logger=_logger()
        ).get_by_id(edition_id)


async def _job_kinds(db_session: AsyncSession) -> list[JobKind]:
    async with db_session.begin():
        result = await db_session.execute(
            select(SqlQueueJob.kind).order_by(SqlQueueJob.id)
        )
        return [JobKind(kind) for kind in result.scalars()]


def _edited_payload(
    *,
    repo_id: int = 12345,
    default_branch: str = "main",
    changes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "action": "edited",
        "changes": (
            changes
            if changes is not None
            else {"default_branch": {"from": "master"}}
        ),
        "repository": {
            "id": repo_id,
            "name": "docs",
            "full_name": "acme/docs",
            "owner": {"login": "acme", "id": 999},
            "default_branch": default_branch,
        },
    }


async def _post(client: AsyncClient, payload: dict[str, Any]) -> None:
    body = json.dumps(payload).encode("utf-8")
    digest = hmac.new(
        _WEBHOOK_SECRET.encode("utf-8"), msg=body, digestmod=hashlib.sha256
    ).hexdigest()
    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "repository",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000721",
            "X-Hub-Signature-256": f"sha256={digest}",
        },
    )
    assert response.status_code == 200


def _lifecycle_events() -> list[EditionLifecycleEvent]:
    events = context_dependency._events
    assert events is not None
    publisher = events.edition_lifecycle
    assert isinstance(publisher, MockEventPublisher)
    return list(publisher.published)


def _received_events() -> list[GitHubWebhookReceivedEvent]:
    events = context_dependency._events
    assert events is not None
    publisher = events.github_webhook_received
    assert isinstance(publisher, MockEventPublisher)
    return list(publisher.published)


def _arq_count(job_name: str) -> int:
    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    return count_jobs_by_name(mock_arq, job_name)


@pytest.mark.asyncio
async def test_default_branch_rename_converges_main(
    client: AsyncClient,
    github_app_enabled: None,
    db_session: AsyncSession,
) -> None:
    """A ``master`` → ``main`` rename moves ``__main`` onto ``main``.

    The column is set, ``__main`` is rewritten, the stray ``main`` draft
    is retired, and ``__main`` is repointed at the newest ``main`` build
    and published, with a dashboard rebuild and one ``update`` event —
    what an operator's ``PATCH`` would have announced.
    """
    seeded = await _seed(db_session, org_slug="db-rename")
    master_build = await _seed_build(
        db_session, project_id=seeded.project_id, git_ref="master", days=1
    )
    await _serve(db_session, edition_id=seeded.main_id, build_id=master_build)
    main_build = await _seed_build(
        db_session, project_id=seeded.project_id, git_ref="main", days=2
    )
    draft_id = await _seed_main_draft(db_session, seeded.project_id)
    clock_before = (await _project(db_session, seeded.project_id)).date_updated

    await _post(client, _edited_payload())

    project = await _project(db_session, seeded.project_id)
    assert project.github_default_branch == "main"
    assert project.date_updated > clock_before
    main = await _edition(db_session, seeded.main_id)
    assert main is not None
    assert main.tracking_params == {"git_ref": "main"}
    assert main.current_build_id == main_build
    assert main.publish_status is PublishStatus.pending
    assert await _edition(db_session, draft_id) is None
    assert await _job_kinds(db_session) == [
        JobKind.publish_edition,
        JobKind.dashboard_build,
    ]
    assert _arq_count("publish_edition") == 1
    assert _arq_count("dashboard_build") == 1
    [event] = _lifecycle_events()
    assert event.action is LifecycleAction.update
    assert event.edition_kind is MetricsEditionKind.main
    assert event.organization == "db-rename"
    assert event.project == "docs"
    [received] = _received_events()
    assert received.outcome is WebhookOutcome.dispatched
    assert received.event_type == "repository"
    assert received.jobs_enqueued == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("main_mode", "main_ref"),
    [(TrackingMode.git_ref, "docs"), (TrackingMode.lsst_doc, "")],
    ids=["tracks-a-live-branch", "lsst-doc"],
)
async def test_default_branch_change_leaves_a_pinned_main(
    client: AsyncClient,
    github_app_enabled: None,
    db_session: AsyncSession,
    main_mode: TrackingMode,
    main_ref: str,
) -> None:
    """Only the column moves when ``__main`` does not track the old branch.

    No rewrite, no retired draft, no repoint, no jobs, and no lifecycle
    event.
    """
    seeded = await _seed(
        db_session,
        org_slug="db-pinned",
        main_mode=main_mode,
        main_ref=main_ref,
    )
    await _seed_build(
        db_session, project_id=seeded.project_id, git_ref="main", days=2
    )
    draft_id = await _seed_main_draft(db_session, seeded.project_id)

    await _post(client, _edited_payload())

    project = await _project(db_session, seeded.project_id)
    assert project.github_default_branch == "main"
    main = await _edition(db_session, seeded.main_id)
    assert main is not None
    assert main.tracking_mode is main_mode
    assert main.current_build_id is None
    assert await _edition(db_session, draft_id) is not None
    assert await _job_kinds(db_session) == []
    assert _lifecycle_events() == []
    [received] = _received_events()
    assert received.outcome is WebhookOutcome.dispatched
    assert received.jobs_enqueued == 0


@pytest.mark.asyncio
async def test_edit_without_a_default_branch_change_is_a_no_op(
    client: AsyncClient,
    github_app_enabled: None,
    db_session: AsyncSession,
) -> None:
    """A description or topics edit leaves the project untouched."""
    seeded = await _seed(db_session, org_slug="db-description")

    await _post(
        client,
        _edited_payload(changes={"description": {"from": "Old words"}}),
    )

    project = await _project(db_session, seeded.project_id)
    assert project.github_default_branch is None
    main = await _edition(db_session, seeded.main_id)
    assert main is not None
    assert main.tracking_params == {"git_ref": "master"}
    assert _lifecycle_events() == []
    [received] = _received_events()
    assert received.outcome is WebhookOutcome.dispatched
    assert received.jobs_enqueued == 0


@pytest.mark.asyncio
async def test_default_branch_change_for_an_unbound_repo_is_ignored(
    client: AsyncClient,
    github_app_enabled: None,
    db_session: AsyncSession,
) -> None:
    """A repository no project backs is answered 200 and changes nothing.

    The project here is bound to a different numeric id, so neither the
    id path nor the name fallback (which only covers projects whose id
    is still unresolved) reaches it.
    """
    seeded = await _seed(db_session, org_slug="db-unbound")

    await _post(client, _edited_payload(repo_id=99999))

    project = await _project(db_session, seeded.project_id)
    assert project.github_default_branch is None
    assert await _job_kinds(db_session) == []
    assert _lifecycle_events() == []


@pytest.mark.asyncio
async def test_default_branch_redelivery_is_inert(
    client: AsyncClient,
    github_app_enabled: None,
    db_session: AsyncSession,
) -> None:
    """The same delivery a second time announces nothing and queues nothing."""
    seeded = await _seed(db_session, org_slug="db-redelivery")
    await _seed_build(
        db_session, project_id=seeded.project_id, git_ref="main", days=2
    )
    await _post(client, _edited_payload())
    jobs_after_first = await _job_kinds(db_session)
    clock_after_first = (
        await _project(db_session, seeded.project_id)
    ).date_updated

    await _post(client, _edited_payload())

    assert await _job_kinds(db_session) == jobs_after_first
    assert len(_lifecycle_events()) == 1
    project = await _project(db_session, seeded.project_id)
    assert project.date_updated == clock_after_first
    assert _received_events()[-1].jobs_enqueued == 0


@pytest.mark.asyncio
async def test_default_branch_change_without_the_new_branch_is_ignored(
    client: AsyncClient,
    github_app_enabled: None,
    db_session: AsyncSession,
) -> None:
    """A payload whose ``repository.default_branch`` is unusable is a no-op.

    Answered 200 so GitHub does not redeliver a payload that will never
    parse, with nothing written.
    """
    seeded = await _seed(db_session, org_slug="db-malformed")
    payload = _edited_payload()
    del payload["repository"]["default_branch"]

    await _post(client, payload)

    project = await _project(db_session, seeded.project_id)
    assert project.github_default_branch is None
    main = await _edition(db_session, seeded.main_id)
    assert main is not None
    assert main.tracking_params == {"git_ref": "master"}
    assert _lifecycle_events() == []
