"""Tests for DashboardRebuildFanout."""

from __future__ import annotations

import pytest
import structlog
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate, ProjectCreate
from docverse.client.models.queue_enums import JobKind
from docverse.config import Configuration
from docverse.services.dashboard.enqueue import DashboardBuildEnqueuer
from docverse.services.dashboard_templates.fanout import DashboardRebuildFanout
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from docverse.storage.queue_backend import ArqQueueBackend
from docverse.storage.queue_job_store import QueueJobStore

_config = Configuration()


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


async def _seed_template(
    session: AsyncSession,
    *,
    key: GitHubTemplateKey,
    etag: str = "etag-1",
) -> int:
    store = DashboardGitHubTemplateStore(session=session, logger=_logger())
    result = await store.upsert(
        key=key,
        commit_sha="cafebabe",
        etag=etag,
        template_toml=b"[dashboard]\n",
        files=[
            GitHubTemplateFileInput(
                relative_path="dashboard.html.jinja",
                is_text=True,
                data=b"<html>ok</html>",
            ),
        ],
    )
    return result.template.id


async def _seed_org_with_projects(
    session: AsyncSession,
    *,
    org_slug: str,
    project_slugs: list[str],
) -> tuple[int, list[int]]:
    logger = _logger()
    org_store = OrganizationStore(session=session, logger=logger)
    proj_store = ProjectStore(session=session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title=f"Org {org_slug}",
            base_domain=f"{org_slug}.example.com",
        )
    )
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


def _make_fanout(
    session: AsyncSession,
    *,
    arq_queue: MockArqQueue,
) -> DashboardRebuildFanout:
    logger = _logger()
    org_store = OrganizationStore(session=session, logger=logger)
    proj_store = ProjectStore(session=session, logger=logger)
    binding_store = DashboardGitHubTemplateBindingStore(
        session=session, logger=logger
    )
    queue_backend = ArqQueueBackend(
        arq_queue=arq_queue, default_queue_name=_config.arq_queue_name
    )
    queue_job_store = QueueJobStore(session=session, logger=logger)
    enqueuer = DashboardBuildEnqueuer(
        org_store=org_store,
        project_store=proj_store,
        queue_backend=queue_backend,
        queue_job_store=queue_job_store,
        logger=logger,
    )
    return DashboardRebuildFanout(
        binding_store=binding_store,
        project_store=proj_store,
        enqueuer=enqueuer,
        logger=logger,
    )


@pytest.mark.asyncio
async def test_fanout_from_org_default_hits_every_project_without_override(
    db_session: AsyncSession,
) -> None:
    """Org-default fan-out enqueues one job per project without override."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        org_id, project_ids = await _seed_org_with_projects(
            db_session,
            org_slug="fanout-org-default",
            project_slugs=["alpha", "beta", "charlie"],
        )
        template_id = await _seed_template(
            db_session,
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="templates",
                github_ref="main",
                root_path="/",
            ),
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await binding_store.create(
            DashboardGitHubTemplateBindingCreate(
                org_id=org_id,
                project_id=None,
                github_owner="acme",
                github_repo="templates",
                github_ref="main",
                root_path="/",
            )
        )
        await binding_store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="succeeded",
            github_template_id=template_id,
        )
        await db_session.commit()

    fanout = _make_fanout(db_session, arq_queue=arq_queue)
    async with db_session.begin():
        jobs = await fanout.fan_out(template_id)
        await db_session.commit()

    assert len(jobs) == 3
    async with db_session.begin():
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        project_ids_for_jobs = []
        for job in jobs:
            loaded = await queue_job_store.get(job.id)
            assert loaded is not None
            assert loaded.kind == JobKind.dashboard_build
            project_ids_for_jobs.append(loaded.project_id)
    assert set(project_ids_for_jobs) == set(project_ids)


@pytest.mark.asyncio
async def test_fanout_skips_projects_with_override_to_other_template(
    db_session: AsyncSession,
) -> None:
    """A project pinned to a different template is excluded from fan-out."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        org_id, project_ids = await _seed_org_with_projects(
            db_session,
            org_slug="fanout-org-skip",
            project_slugs=["keep", "override"],
        )
        default_template_id = await _seed_template(
            db_session,
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="templates",
                github_ref="main",
                root_path="/",
            ),
            etag="etag-default",
        )
        other_template_id = await _seed_template(
            db_session,
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="other-templates",
                github_ref="main",
                root_path="/",
            ),
            etag="etag-other",
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        default_binding = await binding_store.create(
            DashboardGitHubTemplateBindingCreate(
                org_id=org_id,
                project_id=None,
                github_owner="acme",
                github_repo="templates",
                github_ref="main",
                root_path="/",
            )
        )
        await binding_store.update_sync_state(
            binding_id=default_binding.id,
            last_sync_status="succeeded",
            github_template_id=default_template_id,
        )
        override_binding = await binding_store.create(
            DashboardGitHubTemplateBindingCreate(
                org_id=org_id,
                project_id=project_ids[1],
                github_owner="acme",
                github_repo="other-templates",
                github_ref="main",
                root_path="/",
            )
        )
        await binding_store.update_sync_state(
            binding_id=override_binding.id,
            last_sync_status="succeeded",
            github_template_id=other_template_id,
        )
        await db_session.commit()

    fanout = _make_fanout(db_session, arq_queue=arq_queue)
    async with db_session.begin():
        jobs = await fanout.fan_out(default_template_id)
        await db_session.commit()

    # Only the non-override project should have been fanned out.
    assert len(jobs) == 1
    async with db_session.begin():
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        loaded = await queue_job_store.get(jobs[0].id)
        assert loaded is not None
        assert loaded.project_id == project_ids[0]


@pytest.mark.asyncio
async def test_fanout_project_override_triggers_that_project_only(
    db_session: AsyncSession,
) -> None:
    """Fanning out for a template that only an override uses hits one."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        org_id, project_ids = await _seed_org_with_projects(
            db_session,
            org_slug="fanout-override",
            project_slugs=["alpha", "beta"],
        )
        template_id = await _seed_template(
            db_session,
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="override-templates",
                github_ref="main",
                root_path="/",
            ),
        )
        binding_store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        override = await binding_store.create(
            DashboardGitHubTemplateBindingCreate(
                org_id=org_id,
                project_id=project_ids[1],
                github_owner="acme",
                github_repo="override-templates",
                github_ref="main",
                root_path="/",
            )
        )
        await binding_store.update_sync_state(
            binding_id=override.id,
            last_sync_status="succeeded",
            github_template_id=template_id,
        )
        await db_session.commit()

    fanout = _make_fanout(db_session, arq_queue=arq_queue)
    async with db_session.begin():
        jobs = await fanout.fan_out(template_id)
        await db_session.commit()

    assert len(jobs) == 1
    async with db_session.begin():
        queue_job_store = QueueJobStore(session=db_session, logger=_logger())
        loaded = await queue_job_store.get(jobs[0].id)
        assert loaded is not None
        assert loaded.project_id == project_ids[1]


@pytest.mark.asyncio
async def test_fanout_returns_empty_when_no_bindings_reference_template(
    db_session: AsyncSession,
) -> None:
    """Unreferenced templates produce no enqueues."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        template_id = await _seed_template(
            db_session,
            key=GitHubTemplateKey(
                github_owner="acme",
                github_repo="orphan",
                github_ref="main",
                root_path="/",
            ),
        )
        await db_session.commit()

    fanout = _make_fanout(db_session, arq_queue=arq_queue)
    async with db_session.begin():
        jobs = await fanout.fan_out(template_id)
        await db_session.commit()

    assert jobs == []
