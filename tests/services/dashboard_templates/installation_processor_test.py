"""Tests for the InstallationEventProcessor service."""

from __future__ import annotations

from typing import Any

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate, ProjectCreate
from docverse.client.models.projects import ProjectGitHubBindingCreate
from docverse.services.dashboard_templates.installation_processor import (
    INSTALLATION_DELETED_REASON,
    INSTALLATION_SUSPENDED_REASON,
    InstallationEventProcessor,
)
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


def _make_processor(session: AsyncSession) -> InstallationEventProcessor:
    return InstallationEventProcessor(
        binding_store=DashboardGitHubTemplateBindingStore(
            session=session, logger=_logger()
        ),
        project_store=ProjectStore(session=session, logger=_logger()),
        logger=_logger(),
    )


async def _seed_org(session: AsyncSession, slug: str) -> int:
    store = OrganizationStore(session=session, logger=_logger())
    org = await store.create(
        OrganizationCreate(
            slug=slug,
            title=f"Org {slug}",
            base_domain=f"{slug}.example.com",
        )
    )
    return org.id


async def _seed_binding(
    session: AsyncSession,
    *,
    org_id: int,
    github_installation_id: int | None,
    github_owner: str = "acme",
    github_repo: str = "templates",
    github_ref: str = "main",
) -> int:
    store = DashboardGitHubTemplateBindingStore(
        session=session, logger=_logger()
    )
    binding = await store.create(
        DashboardGitHubTemplateBindingCreate(
            org_id=org_id,
            project_id=None,
            github_owner=github_owner,
            github_repo=github_repo,
            github_ref=github_ref,
            root_path="/",
            github_installation_id=github_installation_id,
        )
    )
    return binding.id


def _installation_payload(
    *, action: str, installation_id: int = 99
) -> dict[str, Any]:
    return {
        "action": action,
        "installation": {
            "id": installation_id,
            "account": {"login": "acme", "id": 999},
        },
        "repositories": [],
    }


async def _seed_project(
    session: AsyncSession,
    *,
    org_id: int,
    slug: str,
    github_owner: str | None,
    github_repo: str | None,
) -> int:
    """Seed a project with optional structured GitHub coordinates."""
    store = ProjectStore(session=session, logger=_logger())
    binding = (
        ProjectGitHubBindingCreate(owner=github_owner, repo=github_repo)
        if github_owner is not None and github_repo is not None
        else None
    )
    project = await store.create(
        org_id=org_id,
        data=ProjectCreate(
            slug=slug,
            title=f"Project {slug}",
            github=binding,
        ),
        github_owner=github_owner,
        github_repo=github_repo,
    )
    return project.id


@pytest.mark.asyncio
async def test_installation_suspend_marks_bindings_failed(
    db_session: AsyncSession,
) -> None:
    """``installation.suspend`` flips matching bindings to ``failed``.

    The ``last_sync_error`` carries a machine-readable tag so the
    matching ``installation.unsuspend`` can target the same set of
    rows without sweeping up unrelated failures.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "install-suspend")
        binding_id = await _seed_binding(
            db_session, org_id=org_id, github_installation_id=99
        )
        await db_session.commit()

    payload = _installation_payload(action="suspend", installation_id=99)

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "failed"
    assert binding.last_sync_error == INSTALLATION_SUSPENDED_REASON


@pytest.mark.asyncio
async def test_installation_deleted_marks_bindings_failed(
    db_session: AsyncSession,
) -> None:
    """``installation.deleted`` flips matching bindings with a distinct tag."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "install-delete")
        binding_id = await _seed_binding(
            db_session, org_id=org_id, github_installation_id=99
        )
        await db_session.commit()

    payload = _installation_payload(action="deleted", installation_id=99)

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "failed"
    assert binding.last_sync_error == INSTALLATION_DELETED_REASON


@pytest.mark.asyncio
async def test_installation_unsuspend_clears_suspend_flag(
    db_session: AsyncSession,
) -> None:
    """``installation.unsuspend`` clears the suspend tag on matching bindings.

    The clear only fires for rows whose ``last_sync_error`` matches the
    suspend tag — a non-installation failure (e.g. a real syncer 5xx)
    that landed between suspend and unsuspend stays put.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "install-unsuspend")
        binding_id = await _seed_binding(
            db_session, org_id=org_id, github_installation_id=99
        )
        # Suspend first to set up the failed state.
        processor = _make_processor(db_session)
        await processor.process(
            _installation_payload(action="suspend", installation_id=99)
        )
        await db_session.commit()

    async with db_session.begin():
        await _make_processor(db_session).process(
            _installation_payload(action="unsuspend", installation_id=99)
        )
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "pending"
    assert binding.last_sync_error is None


@pytest.mark.asyncio
async def test_installation_unsuspend_does_not_clear_unrelated_failure(
    db_session: AsyncSession,
) -> None:
    """A non-suspend failure on the same installation is preserved."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "install-unsuspend-keep")
        binding_id = await _seed_binding(
            db_session, org_id=org_id, github_installation_id=99
        )
        # Set a failure that did not come from a suspend — e.g. a
        # syncer 5xx that ended up tagging the binding as failed.
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        await store.update_sync_state(
            binding_id=binding_id,
            last_sync_status="failed",
            last_sync_error="Sync failed: GitHub 503",
        )
        await db_session.commit()

    async with db_session.begin():
        await _make_processor(db_session).process(
            _installation_payload(action="unsuspend", installation_id=99)
        )
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "failed"
    assert binding.last_sync_error == "Sync failed: GitHub 503"


@pytest.mark.asyncio
async def test_installation_created_is_a_no_op(
    db_session: AsyncSession,
) -> None:
    """``installation.created`` does not mutate any row.

    Per the issue, only deleted/suspend/unsuspend change DB state;
    created is registered so unrelated installation actions return
    200 cleanly through the gidgethub router.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "install-create")
        binding_id = await _seed_binding(
            db_session, org_id=org_id, github_installation_id=99
        )
        await db_session.commit()

    async with db_session.begin():
        await _make_processor(db_session).process(
            _installation_payload(action="created", installation_id=99)
        )
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "pending"
    assert binding.last_sync_error is None


@pytest.mark.asyncio
async def test_installation_event_does_not_touch_other_installations(
    db_session: AsyncSession,
) -> None:
    """A binding under a different installation id is left untouched."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "install-isolation")
        keep_id = await _seed_binding(
            db_session, org_id=org_id, github_installation_id=42
        )
        await db_session.commit()

    async with db_session.begin():
        await _make_processor(db_session).process(
            _installation_payload(action="suspend", installation_id=99)
        )
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(keep_id)
    assert binding is not None
    assert binding.last_sync_status == "pending"
    assert binding.last_sync_error is None


def _installation_created_payload(
    *,
    installation_id: int = 99,
    owner: str = "acme",
    owner_id: int = 999,
    repos: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build an ``installation.created`` payload with seeded repositories."""
    return {
        "action": "created",
        "installation": {
            "id": installation_id,
            "account": {"login": owner, "id": owner_id},
        },
        "repositories": repos or [],
    }


@pytest.mark.asyncio
async def test_installation_created_backfills_project_installation_id(
    db_session: AsyncSession,
) -> None:
    """``installation.created`` writes the three github_*_id columns.

    Reproduces the post-install steady state of PRD #346 user story 12:
    an admin installed the Docverse App on a repo that already has a
    Docverse project bound to it. The project row's three opportunistic
    id columns are populated from the webhook payload alone, without a
    follow-up GitHub API round-trip.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "install-create-backfill")
        project_id = await _seed_project(
            db_session,
            org_id=org_id,
            slug="docs",
            github_owner="acme",
            github_repo="templates",
        )
        await db_session.commit()

    payload = _installation_created_payload(
        installation_id=42,
        owner="acme",
        owner_id=999,
        repos=[
            {
                "id": 12345,
                "name": "templates",
                "full_name": "acme/templates",
            }
        ],
    )

    async with db_session.begin():
        await _make_processor(db_session).process(payload)
        await db_session.commit()

    async with db_session.begin():
        project = await ProjectStore(
            session=db_session, logger=_logger()
        ).get_by_id(project_id)
    assert project is not None
    assert project.github_installation_id == 42
    assert project.github_owner_id == 999
    assert project.github_repo_id == 12345


@pytest.mark.asyncio
async def test_installation_created_ignores_non_matching_repo(
    db_session: AsyncSession,
) -> None:
    """Projects whose owner/repo do not match the payload are untouched.

    The webhook fires once for every Docverse-App installation, not
    once per project; a payload covering ``acme/other`` must not flip
    the columns on a project bound to ``acme/templates``.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "install-create-no-match")
        project_id = await _seed_project(
            db_session,
            org_id=org_id,
            slug="docs",
            github_owner="acme",
            github_repo="templates",
        )
        await db_session.commit()

    payload = _installation_created_payload(
        installation_id=42,
        owner="acme",
        owner_id=999,
        repos=[
            {
                "id": 88888,
                "name": "other",
                "full_name": "acme/other",
            }
        ],
    )

    async with db_session.begin():
        await _make_processor(db_session).process(payload)
        await db_session.commit()

    async with db_session.begin():
        project = await ProjectStore(
            session=db_session, logger=_logger()
        ).get_by_id(project_id)
    assert project is not None
    assert project.github_installation_id is None
    assert project.github_owner_id is None
    assert project.github_repo_id is None


@pytest.mark.asyncio
async def test_installation_created_matches_case_insensitive(
    db_session: AsyncSession,
) -> None:
    """Owner/repo matching is case-insensitive (GitHub canonicalisation).

    The seeded project may have been registered against ``Acme/Docs``
    while GitHub delivers ``acme/docs`` in the payload. The case-
    insensitive index on ``(lower(github_owner), lower(github_repo))``
    is what makes the webhook lookup robust.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "install-create-case")
        project_id = await _seed_project(
            db_session,
            org_id=org_id,
            slug="docs",
            github_owner="Acme",
            github_repo="Templates",
        )
        await db_session.commit()

    payload = _installation_created_payload(
        repos=[
            {
                "id": 12345,
                "name": "templates",
                "full_name": "acme/templates",
            }
        ],
    )

    async with db_session.begin():
        await _make_processor(db_session).process(payload)
        await db_session.commit()

    async with db_session.begin():
        project = await ProjectStore(
            session=db_session, logger=_logger()
        ).get_by_id(project_id)
    assert project is not None
    assert project.github_installation_id == 99
    assert project.github_repo_id == 12345


@pytest.mark.asyncio
async def test_installation_repositories_added_backfills_projects(
    db_session: AsyncSession,
) -> None:
    """``installation_repositories.added`` backfills like ``installation``.

    An operator can scope an existing app installation to new repos
    after the fact; that path lands as ``installation_repositories
    .added`` rather than a fresh ``installation.created``. Both event
    shapes must end up in the same column-write contract.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "install-repos-added")
        project_id = await _seed_project(
            db_session,
            org_id=org_id,
            slug="docs",
            github_owner="acme",
            github_repo="templates",
        )
        await db_session.commit()

    payload = {
        "action": "added",
        "installation": {
            "id": 42,
            "account": {"login": "acme", "id": 999},
        },
        "repositories_added": [
            {
                "id": 12345,
                "name": "templates",
                "full_name": "acme/templates",
            }
        ],
    }

    async with db_session.begin():
        await _make_processor(db_session).process_installation_repositories(
            payload
        )
        await db_session.commit()

    async with db_session.begin():
        project = await ProjectStore(
            session=db_session, logger=_logger()
        ).get_by_id(project_id)
    assert project is not None
    assert project.github_installation_id == 42
    assert project.github_owner_id == 999
    assert project.github_repo_id == 12345


@pytest.mark.asyncio
async def test_installation_created_does_not_touch_non_github_project(
    db_session: AsyncSession,
) -> None:
    """A project without ``github_owner``/``github_repo`` stays NULL.

    Non-GitHub projects (story 14) have NULL in the structured columns;
    the case-insensitive comparison never matches them, so no row is
    accidentally bound to an installation that has no claim on it.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "install-non-github")
        project_id = await _seed_project(
            db_session,
            org_id=org_id,
            slug="docs",
            github_owner=None,
            github_repo=None,
        )
        await db_session.commit()

    payload = _installation_created_payload(
        repos=[
            {
                "id": 12345,
                "name": "templates",
                "full_name": "acme/templates",
            }
        ],
    )

    async with db_session.begin():
        await _make_processor(db_session).process(payload)
        await db_session.commit()

    async with db_session.begin():
        project = await ProjectStore(
            session=db_session, logger=_logger()
        ).get_by_id(project_id)
    assert project is not None
    assert project.github_installation_id is None
