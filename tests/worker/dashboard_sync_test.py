"""Integration tests for the dashboard_sync worker function."""

from __future__ import annotations

import httpx
import pytest
import structlog
from pydantic import SecretStr
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate, ProjectCreate
from docverse.config import Configuration
from docverse.dbschema.organization import SqlOrganization
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.services.lock_service import LockClass, LockKey
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.dashboard_sync import dashboard_sync
from tests.support.arq_testing import count_jobs_by_name, get_jobs_by_name
from tests.support.github_mock import GitHubMock
from tests.support.lock_service_spy import install_recording_lock_service
from tests.worker.conftest import make_worker_ctx

_config = Configuration()


_VALID_TEMPLATE_TOML = b"""\
[dashboard]
template = "dashboard.html.jinja"
"""


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _setup_org_and_projects(
    db_session: AsyncSession,
    *,
    org_slug: str = "sync-org",
    project_slugs: tuple[str, ...] = ("one", "two"),
) -> tuple[int, list[int]]:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title=f"Org {org_slug}",
            base_domain=f"{org_slug}.example.com",
        )
    )
    await db_session.execute(
        update(SqlOrganization)
        .where(SqlOrganization.id == org.id)
        .values(publishing_store_label="mock-store")
    )
    await db_session.flush()
    project_ids: list[int] = []
    for slug in project_slugs:
        project = await proj_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug=slug,
                title=f"Project {slug}",
                doc_repo=f"https://github.com/example/{slug}",
            ),
        )
        project_ids.append(project.id)
    return org.id, project_ids


async def _create_binding(
    db_session: AsyncSession,
    *,
    org_id: int,
    github_owner: str = "acme",
    github_repo: str = "templates",
    github_ref: str = "main",
    root_path: str = "/",
) -> int:
    binding_store = DashboardGitHubTemplateBindingStore(
        session=db_session, logger=_logger()
    )
    binding = await binding_store.create(
        DashboardGitHubTemplateBindingCreate(
            org_id=org_id,
            project_id=None,
            github_owner=github_owner,
            github_repo=github_repo,
            github_ref=github_ref,
            root_path=root_path,
        )
    )
    return binding.id


def _make_ctx(
    *,
    arq_queue: MockArqQueue,
    http_client: httpx.AsyncClient,
    mock_github: GitHubMock,
) -> dict[str, object]:
    return make_worker_ctx(
        http_client=http_client,
        arq_queue=arq_queue,
        github_app_id=mock_github.app_id,
        github_app_private_key=SecretStr(mock_github.private_key_pem),
        github_webhook_secret=SecretStr("webhook-secret"),
    )


@pytest.mark.asyncio
async def test_dashboard_sync_happy_path_fans_out_dashboard_builds(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A successful sync upserts content and enqueues one build per project."""
    logger = _logger()
    async with db_session.begin():
        org_id, project_ids = await _setup_org_and_projects(
            db_session, org_slug="sync-happy", project_slugs=("alpha", "beta")
        )
        binding_id = await _create_binding(db_session, org_id=org_id)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_sync,
            org_id=org_id,
            backend_job_id="arq-sync-1",
        )

    mock_github.seed_installation("acme", "templates", installation_id=42)
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            "template.toml": _VALID_TEMPLATE_TOML,
            "dashboard.html.jinja": b"<html>dash</html>",
        },
        etag='W/"etag-sync-1"',
    )

    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        payload = {
            "binding_id": binding_id,
            "queue_job_id": queue_job.id,
            "queue_job_public_id": serialize_base32_id(queue_job.public_id),
        }
        result = await dashboard_sync(ctx, payload)

    assert result == "completed"

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.phase == "complete"
            assert job.progress is not None
            assert job.progress["changed"] is True
            assert job.progress["fan_out_count"] == len(project_ids)

            binding_store = DashboardGitHubTemplateBindingStore(
                session=session, logger=_logger()
            )
            binding = await binding_store.get_by_id(binding_id)
            assert binding is not None
            assert binding.last_sync_status == "succeeded"
            assert binding.github_template_id is not None

    build_jobs = get_jobs_by_name(arq_queue, "dashboard_build")
    assert len(build_jobs) == len(project_ids)


@pytest.mark.asyncio
async def test_dashboard_sync_acquires_dashboard_template_lock(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The worker acquires ``LockKey.for_dashboard_template`` first."""
    logger = _logger()
    async with db_session.begin():
        org_id, _ = await _setup_org_and_projects(
            db_session, org_slug="sync-lock", project_slugs=("alpha",)
        )
        binding_id = await _create_binding(
            db_session,
            org_id=org_id,
            github_owner="acme",
            github_repo="templates",
            github_ref="main",
            root_path="/",
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_sync,
            org_id=org_id,
            backend_job_id="arq-sync-lock",
        )

    mock_github.seed_installation("acme", "templates", installation_id=42)
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            "template.toml": _VALID_TEMPLATE_TOML,
            "dashboard.html.jinja": b"<html>dash</html>",
        },
    )

    events = install_recording_lock_service(monkeypatch)
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        payload = {
            "binding_id": binding_id,
            "queue_job_id": queue_job.id,
            "queue_job_public_id": serialize_base32_id(queue_job.public_id),
        }
        result = await dashboard_sync(ctx, payload)

    assert result == "completed"

    template_enters = [
        e
        for e in events
        if e.event == "enter"
        and e.lock_key.lock_class == LockClass.DASHBOARD_TEMPLATE
    ]
    assert len(template_enters) == 1
    expected = LockKey.for_dashboard_template(
        owner="acme", repo="templates", ref="main", root_path="/"
    )
    assert template_enters[0].lock_key == expected


@pytest.mark.asyncio
async def test_dashboard_sync_etag_short_circuit_skips_fanout(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A second sync with the same ETag enqueues no dashboard_build jobs."""
    logger = _logger()
    async with db_session.begin():
        org_id, _ = await _setup_org_and_projects(
            db_session,
            org_slug="sync-etag",
            project_slugs=("alpha", "beta"),
        )
        binding_id = await _create_binding(db_session, org_id=org_id)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        first_job = await queue_job_store.create(
            kind=JobKind.dashboard_sync,
            org_id=org_id,
            backend_job_id="arq-etag-1",
        )
        second_job = await queue_job_store.create(
            kind=JobKind.dashboard_sync,
            org_id=org_id,
            backend_job_id="arq-etag-2",
        )

    mock_github.seed_installation("acme", "templates", installation_id=42)
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={
            "template.toml": _VALID_TEMPLATE_TOML,
            "dashboard.html.jinja": b"<html>dash</html>",
        },
        etag='W/"etag-stable"',
    )

    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        first_result = await dashboard_sync(
            ctx,
            {
                "binding_id": binding_id,
                "queue_job_id": first_job.id,
                "queue_job_public_id": serialize_base32_id(
                    first_job.public_id
                ),
            },
        )
        enqueues_after_first = count_jobs_by_name(arq_queue, "dashboard_build")
        second_result = await dashboard_sync(
            ctx,
            {
                "binding_id": binding_id,
                "queue_job_id": second_job.id,
                "queue_job_public_id": serialize_base32_id(
                    second_job.public_id
                ),
            },
        )

    assert first_result == "completed"
    assert second_result == "completed"

    enqueues_after_second = count_jobs_by_name(arq_queue, "dashboard_build")
    assert enqueues_after_second == enqueues_after_first, (
        "Unchanged re-sync must not enqueue any additional dashboard_build"
    )


@pytest.mark.asyncio
async def test_dashboard_sync_invalid_template_marks_job_failed(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """An invalid ``template.toml`` fails the job and records the reason."""
    logger = _logger()
    async with db_session.begin():
        org_id, _ = await _setup_org_and_projects(
            db_session, org_slug="sync-bad", project_slugs=("alpha",)
        )
        binding_id = await _create_binding(db_session, org_id=org_id)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_sync,
            org_id=org_id,
            backend_job_id="arq-bad",
        )

    mock_github.seed_installation("acme", "templates", installation_id=42)
    mock_github.seed_tree(
        "acme",
        "templates",
        "main",
        files={"template.toml": b"[dashboard\n= BROKEN"},
    )

    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        payload = {
            "binding_id": binding_id,
            "queue_job_id": queue_job.id,
            "queue_job_public_id": serialize_base32_id(queue_job.public_id),
        }
        result = await dashboard_sync(ctx, payload)

    assert result == "failed"

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.failed
            assert job.errors is not None
            assert "template.toml" in job.errors["message"].lower()

            binding_store = DashboardGitHubTemplateBindingStore(
                session=session, logger=_logger()
            )
            binding = await binding_store.get_by_id(binding_id)
            assert binding is not None
            assert binding.last_sync_status == "failed"

    assert count_jobs_by_name(arq_queue, "dashboard_build") == 0


@pytest.mark.asyncio
async def test_dashboard_sync_missing_binding_fails_the_job(
    app: None,
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A deleted binding between enqueue and dequeue fails the job cleanly."""
    logger = _logger()
    async with db_session.begin():
        org_id, _ = await _setup_org_and_projects(
            db_session, org_slug="sync-missing", project_slugs=("alpha",)
        )
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_sync,
            org_id=org_id,
            backend_job_id="arq-missing",
        )

    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    async with httpx.AsyncClient() as http_client:
        ctx = _make_ctx(
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        payload = {
            "binding_id": 999999,
            "queue_job_id": queue_job.id,
            "queue_job_public_id": serialize_base32_id(queue_job.public_id),
        }
        result = await dashboard_sync(ctx, payload)

    assert result == "failed"
    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.failed
