"""Tests for DashboardGitHubTemplateBindingStore."""

from __future__ import annotations

import pytest
import structlog
from sqlalchemy import Table
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate, ProjectCreate
from docverse.dbschema.dashboard_github_template_binding import (
    SqlDashboardGitHubTemplateBinding,
)
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore


async def _seed_org_and_project(
    session: AsyncSession,
    *,
    org_slug: str = "tmpl-org",
    project_slug: str = "tmpl-proj",
) -> tuple[int, int]:
    logger = structlog.get_logger("test")
    org_store = OrganizationStore(session=session, logger=logger)
    proj_store = ProjectStore(session=session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title="Tmpl Org",
            base_domain=f"{org_slug}.example.com",
        )
    )
    project = await proj_store.create(
        org_id=org.id,
        data=ProjectCreate(
            slug=project_slug,
            title="Tmpl Project",
            doc_repo="https://github.com/example/repo",
        ),
    )
    return org.id, project.id


def _store(session: AsyncSession) -> DashboardGitHubTemplateBindingStore:
    logger = structlog.get_logger("test")
    return DashboardGitHubTemplateBindingStore(session=session, logger=logger)


def _binding(
    *, org_id: int, project_id: int | None = None
) -> DashboardGitHubTemplateBindingCreate:
    return DashboardGitHubTemplateBindingCreate(
        org_id=org_id,
        project_id=project_id,
        github_owner="acme",
        github_repo="dashboard-templates",
        github_ref="main",
        root_path="/",
    )


@pytest.mark.asyncio
async def test_create_org_default_binding(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        org_id, _ = await _seed_org_and_project(db_session)
        store = _store(db_session)
        binding = await store.create(_binding(org_id=org_id))
        await db_session.commit()
    assert binding.org_id == org_id
    assert binding.project_id is None
    assert binding.github_owner == "acme"
    assert binding.github_repo == "dashboard-templates"
    assert binding.github_ref == "main"
    assert binding.root_path == "/"
    assert binding.github_template_id is None
    assert binding.last_sync_status == "pending"
    assert binding.last_sync_error is None
    assert binding.github_owner_id is None
    assert binding.github_repo_id is None
    assert binding.github_installation_id is None


@pytest.mark.asyncio
async def test_create_binding_round_trips_github_numeric_ids(
    db_session: AsyncSession,
) -> None:
    """Populated numeric IDs survive a create → re-fetch round-trip."""
    async with db_session.begin():
        org_id, _ = await _seed_org_and_project(db_session)
        store = _store(db_session)
        created = await store.create(
            DashboardGitHubTemplateBindingCreate(
                org_id=org_id,
                project_id=None,
                github_owner="acme",
                github_repo="dashboard-templates",
                github_ref="main",
                root_path="/",
                github_owner_id=12345,
                github_repo_id=67890,
                github_installation_id=111222,
            )
        )
        await db_session.commit()
    async with db_session.begin():
        store = _store(db_session)
        fetched = await store.get_by_id(created.id)
        await db_session.rollback()
    assert fetched is not None
    assert fetched.github_owner_id == 12345
    assert fetched.github_repo_id == 67890
    assert fetched.github_installation_id == 111222


@pytest.mark.asyncio
async def test_create_project_override_binding(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        store = _store(db_session)
        binding = await store.create(
            _binding(org_id=org_id, project_id=project_id)
        )
        await db_session.commit()
    assert binding.org_id == org_id
    assert binding.project_id == project_id


@pytest.mark.asyncio
async def test_get_org_default_returns_none_when_missing(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        org_id, _ = await _seed_org_and_project(db_session)
        store = _store(db_session)
        result = await store.get_org_default(org_id)
        await db_session.rollback()
    assert result is None


@pytest.mark.asyncio
async def test_get_org_default_returns_binding(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        org_id, _ = await _seed_org_and_project(db_session)
        store = _store(db_session)
        await store.create(_binding(org_id=org_id))
        await db_session.commit()
    async with db_session.begin():
        store = _store(db_session)
        fetched = await store.get_org_default(org_id)
        await db_session.rollback()
    assert fetched is not None
    assert fetched.project_id is None


@pytest.mark.asyncio
async def test_get_project_override_returns_binding(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        store = _store(db_session)
        await store.create(_binding(org_id=org_id, project_id=project_id))
        await db_session.commit()
    async with db_session.begin():
        store = _store(db_session)
        fetched = await store.get_project_override(
            org_id=org_id, project_id=project_id
        )
        await db_session.rollback()
    assert fetched is not None
    assert fetched.project_id == project_id


async def _attempt_create(
    session: AsyncSession, data: DashboardGitHubTemplateBindingCreate
) -> None:
    """Open a transaction and try to insert a binding — for raises tests."""
    async with session.begin():
        store = _store(session)
        await store.create(data)


@pytest.mark.asyncio
async def test_unique_constraint_blocks_duplicate_project_override(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        org_id, project_id = await _seed_org_and_project(db_session)
        store = _store(db_session)
        await store.create(_binding(org_id=org_id, project_id=project_id))
        await db_session.commit()
    duplicate = _binding(org_id=org_id, project_id=project_id)
    with pytest.raises(IntegrityError):
        await _attempt_create(db_session, duplicate)


@pytest.mark.asyncio
async def test_partial_unique_index_blocks_second_org_default(
    db_session: AsyncSession,
) -> None:
    """Two org-default rows (project_id IS NULL) for one org are rejected.

    PostgreSQL treats ``NULL`` as distinct in standard unique
    constraints, so this exercises the partial unique index that
    enforces "at most one default per org".
    """
    async with db_session.begin():
        org_id, _ = await _seed_org_and_project(db_session)
        store = _store(db_session)
        await store.create(_binding(org_id=org_id))
        await db_session.commit()
    duplicate = _binding(org_id=org_id)
    with pytest.raises(IntegrityError):
        await _attempt_create(db_session, duplicate)


@pytest.mark.asyncio
async def test_update_sync_state_records_success(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        org_id, _ = await _seed_org_and_project(db_session)
        store = _store(db_session)
        binding = await store.create(_binding(org_id=org_id))
        await db_session.commit()
    async with db_session.begin():
        store = _store(db_session)
        updated = await store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="succeeded",
            last_sync_error=None,
            github_template_id=None,
        )
        await db_session.commit()
    assert updated is not None
    assert updated.last_sync_status == "succeeded"
    assert updated.last_sync_error is None


@pytest.mark.asyncio
async def test_update_sync_state_writes_github_numeric_ids(
    db_session: AsyncSession,
) -> None:
    """Sync worker can record GitHub IDs via the existing update path."""
    async with db_session.begin():
        org_id, _ = await _seed_org_and_project(db_session)
        store = _store(db_session)
        binding = await store.create(_binding(org_id=org_id))
        await db_session.commit()
    async with db_session.begin():
        store = _store(db_session)
        updated = await store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="succeeded",
            github_template_id=None,
            github_owner_id=12345,
            github_repo_id=67890,
            github_installation_id=111222,
        )
        await db_session.commit()
    assert updated is not None
    assert updated.github_owner_id == 12345
    assert updated.github_repo_id == 67890
    assert updated.github_installation_id == 111222


@pytest.mark.asyncio
async def test_update_sync_state_keeps_github_ids_when_none_passed(
    db_session: AsyncSession,
) -> None:
    """Passing no IDs on a later update leaves previously-captured IDs."""
    async with db_session.begin():
        org_id, _ = await _seed_org_and_project(db_session)
        store = _store(db_session)
        binding = await store.create(_binding(org_id=org_id))
        await store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="succeeded",
            github_template_id=None,
            github_owner_id=12345,
            github_repo_id=67890,
            github_installation_id=111222,
        )
        await db_session.commit()
    async with db_session.begin():
        store = _store(db_session)
        updated = await store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="failed",
            last_sync_error="boom",
            github_template_id=None,
        )
        await db_session.commit()
    assert updated is not None
    assert updated.github_owner_id == 12345
    assert updated.github_repo_id == 67890
    assert updated.github_installation_id == 111222


def test_bindings_table_has_repo_id_ref_composite_index() -> None:
    """The ID-preferential push-lookup index is declared on the ORM table."""
    table = SqlDashboardGitHubTemplateBinding.__table__
    assert isinstance(table, Table)
    index_names = {ix.name for ix in table.indexes}
    assert "idx_dashboard_github_template_bindings_repo_id_ref" in index_names
    target = next(
        ix
        for ix in table.indexes
        if ix.name == "idx_dashboard_github_template_bindings_repo_id_ref"
    )
    assert [col.name for col in target.columns] == [
        "github_repo_id",
        "github_ref",
    ]
    assert target.unique is False


@pytest.mark.asyncio
async def test_update_sync_state_records_failure_keeps_template(
    db_session: AsyncSession,
) -> None:
    """Failure must not blank the binding's template pointer.

    The PRD requires sync failures to leave dashboards rendering from
    the last-good template; the store enforces this by only assigning
    ``github_template_id`` when the caller explicitly passes one.
    """
    async with db_session.begin():
        org_id, _ = await _seed_org_and_project(db_session)
        store = _store(db_session)
        binding = await store.create(_binding(org_id=org_id))
        await store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="succeeded",
            github_template_id=None,
        )
        await db_session.commit()
    async with db_session.begin():
        store = _store(db_session)
        failed = await store.update_sync_state(
            binding_id=binding.id,
            last_sync_status="failed",
            last_sync_error="boom",
            github_template_id=None,
        )
        await db_session.commit()
    assert failed is not None
    assert failed.last_sync_status == "failed"
    assert failed.last_sync_error == "boom"


@pytest.mark.asyncio
async def test_delete_returns_true_when_present(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        org_id, _ = await _seed_org_and_project(db_session)
        store = _store(db_session)
        binding = await store.create(_binding(org_id=org_id))
        await db_session.commit()
    async with db_session.begin():
        store = _store(db_session)
        deleted = await store.delete(binding.id)
        await db_session.commit()
    assert deleted is True
    async with db_session.begin():
        store = _store(db_session)
        fetched = await store.get_by_id(binding.id)
        await db_session.rollback()
    assert fetched is None


@pytest.mark.asyncio
async def test_delete_returns_false_when_missing(
    db_session: AsyncSession,
) -> None:
    async with db_session.begin():
        await _seed_org_and_project(db_session)
        store = _store(db_session)
        deleted = await store.delete(999_999)
        await db_session.rollback()
    assert deleted is False
