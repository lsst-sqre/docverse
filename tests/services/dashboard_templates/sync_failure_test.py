"""Focused tests for the ``mark_dashboard_sync_failed`` helper.

The helper centralises the "mark dashboard sync as failed in the DB"
write that previously lived inline at two call sites (the worker
function and ``try_enqueue_dashboard_sync``). These tests pin its
contract directly via stores + ``db_session`` so a future refactor
that splits the responsibility back out cannot silently regress one
mode at a time.
"""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate
from docverse.client.models.queue_enums import JobKind, JobStatus
from docverse.services.dashboard_templates._sync_failure import (
    mark_dashboard_sync_failed,
)
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_job_store import QueueJobStore


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


async def _seed_org_and_binding(
    db_session: AsyncSession, *, org_slug: str
) -> int:
    logger = _logger()
    org_store = OrganizationStore(session=db_session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title=f"Org {org_slug}",
            base_domain=f"{org_slug}.example.com",
        )
    )
    binding_store = DashboardGitHubTemplateBindingStore(
        session=db_session, logger=logger
    )
    binding = await binding_store.create(
        DashboardGitHubTemplateBindingCreate(
            org_id=org.id,
            project_id=None,
            github_owner="acme",
            github_repo="templates",
            github_ref="main",
            root_path="/",
        )
    )
    return binding.id


@pytest.mark.asyncio
async def test_mark_dashboard_sync_failed_binding_only(
    app: None,
    db_session: AsyncSession,
) -> None:
    """Without queue-job args, only the binding row is touched."""
    async with db_session.begin():
        binding_id = await _seed_org_and_binding(
            db_session, org_slug="helper-binding-only"
        )

    binding_store = DashboardGitHubTemplateBindingStore(
        session=db_session, logger=_logger()
    )
    exc = RuntimeError("kaboom")
    await mark_dashboard_sync_failed(
        session=db_session,
        binding_store=binding_store,
        binding_id=binding_id,
        exc=exc,
        error_message=f"Enqueue failed: {exc}",
    )

    async with db_session.begin():
        binding = await binding_store.get_by_id(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "failed"
    assert binding.last_sync_error == "Enqueue failed: kaboom"


@pytest.mark.asyncio
async def test_mark_dashboard_sync_failed_full_mode(
    app: None,
    db_session: AsyncSession,
) -> None:
    """With queue-job args, the helper fails the queue job too."""
    logger = _logger()
    async with db_session.begin():
        binding_id = await _seed_org_and_binding(
            db_session, org_slug="helper-full-mode"
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=logger
        )
        binding = await binding_store.get_by_id(binding_id)
        assert binding is not None
        queue_job_store = QueueJobStore(session=db_session, logger=logger)
        queue_job = await queue_job_store.create(
            kind=JobKind.dashboard_sync,
            org_id=binding.org_id,
            backend_job_id="arq-helper-full",
        )
        # QueueJobStore.fail rejects rows still in queued, so put it in
        # in_progress first to mirror the worker's pre-failure state.
        await queue_job_store.start(queue_job.id)

    exc = RuntimeError("syncer exploded")
    await mark_dashboard_sync_failed(
        session=db_session,
        binding_store=binding_store,
        binding_id=binding_id,
        exc=exc,
        error_message=f"{type(exc).__name__}: {exc}",
        queue_job_store=queue_job_store,
        queue_job_id=queue_job.id,
    )

    async with db_session.begin():
        refreshed_binding = await binding_store.get_by_id(binding_id)
        refreshed_job = await queue_job_store.get(queue_job.id)
    assert refreshed_binding is not None
    assert refreshed_binding.last_sync_status == "failed"
    assert refreshed_binding.last_sync_error == "RuntimeError: syncer exploded"
    assert refreshed_job is not None
    assert refreshed_job.status == JobStatus.failed
    assert refreshed_job.errors is not None
    assert refreshed_job.errors["message"] == "syncer exploded"
    assert refreshed_job.errors["type"] == "RuntimeError"
    assert "Traceback" in refreshed_job.errors["traceback"] or (
        refreshed_job.errors["traceback"] == "NoneType: None\n"
    )
