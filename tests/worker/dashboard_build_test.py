"""Integration tests for the dashboard_build worker function."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import structlog
from cryptography.fernet import Fernet
from safir.arq import MockArqQueue
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from docverse.client.models import (
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.config import Configuration
from docverse.dbschema.organization import SqlOrganization
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import JobKind, JobStatus
from docverse.factory import Factory
from docverse.services.credential_encryptor import CredentialEncryptor
from docverse.storage.edition_store import EditionStore
from docverse.storage.objectstore import MockObjectStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_job_store import QueueJobStore
from docverse.worker.functions.dashboard_build import dashboard_build

_config = Configuration()


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _setup_org_and_project(
    db_session: AsyncSession,
) -> tuple[Any, Any]:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    proj_store = ProjectStore(session=db_session, logger=logger)
    edition_store = EditionStore(session=db_session, logger=logger)

    org = await org_store.create(
        OrganizationCreate(
            slug="dash-org",
            title="Dash Org",
            base_domain="dash.example.com",
        )
    )
    await db_session.execute(
        update(SqlOrganization)
        .where(SqlOrganization.id == org.id)
        .values(publishing_store_label="mock-store")
    )
    await db_session.flush()
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug="dash-proj",
            title="Dash Project",
            doc_repo="https://github.com/example/dash",
        ),
    )
    await edition_store.create_internal(
        project_id=project.id,
        slug="__main",
        title="Latest",
        kind=EditionKind.main,
        tracking_mode=TrackingMode.git_ref,
        tracking_params={"git_ref": "main"},
    )
    return org, project


def _mock_create_objectstore(
    mock_store: MockObjectStore,
) -> Any:
    async def _create(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
    ) -> MockObjectStore:
        # Mimic the real helper's DB access so callers that invoke it
        # outside an explicit ``session.begin()`` block trigger autobegin
        # and surface the same ``InvalidRequestError`` as production.
        await self._session.execute(select(1))
        return mock_store

    return _create


@pytest.mark.asyncio
async def test_dashboard_build_completes_with_phase_transitions(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful dashboard_build job uploads artifacts and finalizes."""
    logger = _logger()
    mock_store = MockObjectStore()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dashboard",
        )

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _mock_create_objectstore(mock_store),
    )

    encryptor = CredentialEncryptor(current_key=Fernet.generate_key().decode())
    ctx: dict[str, Any] = {
        "encryptor": encryptor,
        "http_client": httpx.AsyncClient(),
        "job_id": "test-arq-dashboard",
        "arq_queue": MockArqQueue(default_queue_name=_config.arq_queue_name),
    }
    queue_job_public_id = serialize_base32_id(queue_job.public_id)
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": queue_job_public_id,
    }

    with capture_logs() as captured:
        result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "completed"

    # Log records bind ``queue_job_id`` to the base32 public ID, never the
    # integer database id.
    bound_ids = {
        event.get("queue_job_id")
        for event in captured
        if "queue_job_id" in event
    }
    assert bound_ids == {queue_job_public_id}
    assert queue_job.id not in bound_ids

    # All MVP artifacts written, plus one per-edition JSON for __main.
    assert "dash-proj/__dashboard.html" in mock_store.objects
    assert "dash-proj/__switcher.json" in mock_store.objects
    assert "dash-proj/__404.html" in mock_store.objects
    assert "dash-proj/__editions/__main.json" in mock_store.objects

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.completed
            assert job.phase == "complete"
            assert job.progress is not None
            assert job.progress["object_count"] == 4
            assert job.progress["total_size_bytes"] > 0


@pytest.mark.asyncio
async def test_dashboard_build_marks_failed_on_render_exception(
    app: None,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render exceptions mark the queue job failed and return ``failed``."""
    logger = _logger()

    async with db_session.begin():
        org, project = await _setup_org_and_project(db_session)
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_build,
            org_id=org.id,
            project_id=project.id,
            backend_job_id="test-arq-dash-fail",
        )

    async def _broken_create_objectstore(
        self: Factory,
        *,
        org_id: int,
        service_label: str,
    ) -> MockObjectStore:
        msg = "Simulated objectstore resolution failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        Factory,
        "create_objectstore_for_org",
        _broken_create_objectstore,
    )

    encryptor = CredentialEncryptor(current_key=Fernet.generate_key().decode())
    ctx: dict[str, Any] = {
        "encryptor": encryptor,
        "http_client": httpx.AsyncClient(),
        "job_id": "test-arq-dash-fail",
        "arq_queue": MockArqQueue(default_queue_name=_config.arq_queue_name),
    }
    payload: dict[str, Any] = {
        "org_id": org.id,
        "org_slug": org.slug,
        "project_id": project.id,
        "project_slug": project.slug,
        "queue_job_id": queue_job.id,
        "queue_job_public_id": serialize_base32_id(queue_job.public_id),
    }

    result = await dashboard_build(ctx, payload)
    await ctx["http_client"].aclose()

    assert result == "failed"

    async for session in db_session_dependency():
        async with session.begin():
            qjs = QueueJobStore(session=session, logger=_logger())
            job = await qjs.get(queue_job.id)
            assert job is not None
            assert job.status == JobStatus.failed
            assert job.errors is not None
            assert "Simulated objectstore" in job.errors["message"]
