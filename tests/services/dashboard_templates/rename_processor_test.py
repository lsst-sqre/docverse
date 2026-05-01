"""Tests for the RenameEventProcessor service."""

from __future__ import annotations

from typing import Any

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate
from docverse.services.dashboard_templates.rename_processor import (
    RenameEventProcessor,
)
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
)
from docverse.storage.organization_store import OrganizationStore


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


def _make_processor(session: AsyncSession) -> RenameEventProcessor:
    logger = _logger()
    return RenameEventProcessor(
        binding_store=DashboardGitHubTemplateBindingStore(
            session=session, logger=logger
        ),
        template_store=DashboardGitHubTemplateStore(
            session=session, logger=logger
        ),
        logger=logger,
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
    github_owner: str = "acme",
    github_repo: str = "templates",
    github_ref: str = "main",
    root_path: str = "/",
    github_owner_id: int | None = None,
    github_repo_id: int | None = None,
    github_installation_id: int | None = None,
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
            root_path=root_path,
            github_owner_id=github_owner_id,
            github_repo_id=github_repo_id,
            github_installation_id=github_installation_id,
        )
    )
    return binding.id


async def _seed_template(
    session: AsyncSession,
    *,
    github_owner: str = "acme",
    github_repo: str = "templates",
    github_ref: str = "main",
    root_path: str = "/",
    github_owner_id: int | None = None,
    github_repo_id: int | None = None,
) -> int:
    store = DashboardGitHubTemplateStore(session=session, logger=_logger())
    result = await store.upsert(
        key=GitHubTemplateKey(
            github_owner=github_owner,
            github_repo=github_repo,
            github_ref=github_ref,
            root_path=root_path,
        ),
        commit_sha="cafebabe",
        etag=f"etag-{github_owner}-{github_repo}-{github_ref}",
        template_toml=b"[dashboard]\n",
        files=[
            GitHubTemplateFileInput(
                relative_path="dashboard.html.jinja",
                is_text=True,
                data=b"<html></html>",
            )
        ],
        github_owner_id=github_owner_id,
        github_repo_id=github_repo_id,
    )
    return result.template.id


def _repository_renamed_payload(
    *,
    repo_id: int,
    owner: str,
    owner_id: int,
    new_name: str,
    old_name: str,
) -> dict[str, Any]:
    return {
        "action": "renamed",
        "changes": {"repository": {"name": {"from": old_name}}},
        "repository": {
            "id": repo_id,
            "name": new_name,
            "full_name": f"{owner}/{new_name}",
            "owner": {"login": owner, "id": owner_id},
        },
        "installation": {"id": 99},
    }


def _repository_transferred_payload(
    *,
    repo_id: int,
    new_owner: str,
    new_owner_id: int,
    new_name: str,
    old_owner: str,
    old_owner_id: int,
) -> dict[str, Any]:
    return {
        "action": "transferred",
        "changes": {
            "owner": {
                "from": {
                    "user": {"login": old_owner, "id": old_owner_id},
                }
            }
        },
        "repository": {
            "id": repo_id,
            "name": new_name,
            "full_name": f"{new_owner}/{new_name}",
            "owner": {"login": new_owner, "id": new_owner_id},
        },
        "installation": {"id": 99},
    }


def _organization_renamed_payload(
    *,
    org_id: int,
    new_login: str,
    old_login: str,
) -> dict[str, Any]:
    return {
        "action": "renamed",
        "changes": {"login": {"from": old_login}},
        "organization": {"id": org_id, "login": new_login},
        "installation": {"id": 99},
    }


@pytest.mark.asyncio
async def test_repository_renamed_updates_binding_keyed_by_repo_id(
    db_session: AsyncSession,
) -> None:
    """A synced binding gets its ``github_repo`` rewritten on rename.

    Reproduces the post-rename steady state: the binding's stable
    ``github_repo_id`` was captured on first sync, GitHub now sends a
    ``repository.renamed`` event with the new display name, and we
    expect the binding row to land on the new name without churn.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "rename-by-id")
        binding_id = await _seed_binding(
            db_session,
            org_id=org_id,
            github_owner="acme",
            github_repo="old-name",
            github_repo_id=12345,
            github_owner_id=999,
        )
        await db_session.commit()

    payload = _repository_renamed_payload(
        repo_id=12345,
        owner="acme",
        owner_id=999,
        new_name="new-name",
        old_name="old-name",
    )

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process_repository_renamed(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(binding_id)
    assert binding is not None
    assert binding.github_repo == "new-name"
    assert binding.github_repo_id == 12345
    assert binding.github_owner == "acme"
    assert binding.github_owner_id == 999


@pytest.mark.asyncio
async def test_repository_renamed_updates_template_content_row(
    db_session: AsyncSession,
) -> None:
    """The synced ``dashboard_github_templates`` row also gets renamed.

    Content rows are deduplicated by ``(owner, repo, ref, root_path)``
    so renaming the binding without renaming the content would break
    the next ETag short-circuit lookup. Both rows must move together.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "rename-content-by-id")
        await _seed_binding(
            db_session,
            org_id=org_id,
            github_owner="acme",
            github_repo="old-name",
            github_repo_id=12345,
            github_owner_id=999,
        )
        template_id = await _seed_template(
            db_session,
            github_owner="acme",
            github_repo="old-name",
            github_repo_id=12345,
            github_owner_id=999,
        )
        await db_session.commit()

    payload = _repository_renamed_payload(
        repo_id=12345,
        owner="acme",
        owner_id=999,
        new_name="new-name",
        old_name="old-name",
    )

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process_repository_renamed(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateStore(
            session=db_session, logger=_logger()
        )
        template = await store.get_by_id(template_id)
    assert template is not None
    assert template.github_repo == "new-name"
    assert template.github_repo_id == 12345


@pytest.mark.asyncio
async def test_repository_renamed_falls_back_to_old_name_for_unsynced(
    db_session: AsyncSession,
) -> None:
    """Un-synced bindings (no repo id) match by ``(owner, old_name)``.

    A binding registered via the API but never synced has
    ``github_repo_id IS NULL``; the rename event must still update its
    display name so the next manual sync attempt resolves to the new
    upstream.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "rename-unsynced")
        binding_id = await _seed_binding(
            db_session,
            org_id=org_id,
            github_owner="acme",
            github_repo="old-name",
            github_repo_id=None,
            github_owner_id=None,
        )
        await db_session.commit()

    payload = _repository_renamed_payload(
        repo_id=12345,
        owner="acme",
        owner_id=999,
        new_name="new-name",
        old_name="old-name",
    )

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process_repository_renamed(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(binding_id)
    assert binding is not None
    assert binding.github_repo == "new-name"
    # The fallback path does not back-fill the ID — that remains the
    # syncer's job on the next successful sync.
    assert binding.github_repo_id is None


@pytest.mark.asyncio
async def test_repository_renamed_does_not_touch_unrelated_bindings(
    db_session: AsyncSession,
) -> None:
    """A binding for a different repo with the same name is not modified."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "rename-isolation")
        keep_id = await _seed_binding(
            db_session,
            org_id=org_id,
            github_owner="acme",
            github_repo="old-name",
            github_repo_id=99999,
            github_owner_id=999,
        )
        await db_session.commit()

    payload = _repository_renamed_payload(
        repo_id=12345,
        owner="acme",
        owner_id=999,
        new_name="new-name",
        old_name="old-name",
    )

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process_repository_renamed(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(keep_id)
    assert binding is not None
    assert binding.github_repo == "old-name"


@pytest.mark.asyncio
async def test_repository_transferred_updates_owner_keyed_by_repo_id(
    db_session: AsyncSession,
) -> None:
    """A repo transfer rewrites ``github_owner`` and ``github_owner_id``.

    The transfer leaves ``repository.id`` stable but moves the repo to
    a new owner; the binding has to switch its owner-side identity to
    keep matching push events from the new namespace.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "transfer-by-id")
        binding_id = await _seed_binding(
            db_session,
            org_id=org_id,
            github_owner="old-owner",
            github_repo="templates",
            github_repo_id=12345,
            github_owner_id=111,
        )
        await db_session.commit()

    payload = _repository_transferred_payload(
        repo_id=12345,
        new_owner="new-owner",
        new_owner_id=222,
        new_name="templates",
        old_owner="old-owner",
        old_owner_id=111,
    )

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process_repository_transferred(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(binding_id)
    assert binding is not None
    assert binding.github_owner == "new-owner"
    assert binding.github_owner_id == 222
    assert binding.github_repo == "templates"
    assert binding.github_repo_id == 12345


@pytest.mark.asyncio
async def test_repository_transferred_updates_template_content_row(
    db_session: AsyncSession,
) -> None:
    """The dedup-key on the content row also gets the new owner login."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "transfer-content")
        await _seed_binding(
            db_session,
            org_id=org_id,
            github_owner="old-owner",
            github_repo="templates",
            github_repo_id=12345,
            github_owner_id=111,
        )
        template_id = await _seed_template(
            db_session,
            github_owner="old-owner",
            github_repo="templates",
            github_repo_id=12345,
            github_owner_id=111,
        )
        await db_session.commit()

    payload = _repository_transferred_payload(
        repo_id=12345,
        new_owner="new-owner",
        new_owner_id=222,
        new_name="templates",
        old_owner="old-owner",
        old_owner_id=111,
    )

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process_repository_transferred(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateStore(
            session=db_session, logger=_logger()
        )
        template = await store.get_by_id(template_id)
    assert template is not None
    assert template.github_owner == "new-owner"
    assert template.github_owner_id == 222


@pytest.mark.asyncio
async def test_organization_renamed_updates_binding_keyed_by_owner_id(
    db_session: AsyncSession,
) -> None:
    """An org rename rewrites ``github_owner`` on every matching binding."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "org-rename-by-id")
        binding_id = await _seed_binding(
            db_session,
            org_id=org_id,
            github_owner="old-org",
            github_repo="templates",
            github_repo_id=12345,
            github_owner_id=999,
        )
        await db_session.commit()

    payload = _organization_renamed_payload(
        org_id=999,
        new_login="new-org",
        old_login="old-org",
    )

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process_organization_renamed(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(binding_id)
    assert binding is not None
    assert binding.github_owner == "new-org"
    assert binding.github_owner_id == 999


@pytest.mark.asyncio
async def test_organization_renamed_updates_template_content_row(
    db_session: AsyncSession,
) -> None:
    """An org rename also rewrites the synced content row's owner string."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "org-rename-content")
        await _seed_binding(
            db_session,
            org_id=org_id,
            github_owner="old-org",
            github_repo="templates",
            github_repo_id=12345,
            github_owner_id=999,
        )
        template_id = await _seed_template(
            db_session,
            github_owner="old-org",
            github_repo="templates",
            github_repo_id=12345,
            github_owner_id=999,
        )
        await db_session.commit()

    payload = _organization_renamed_payload(
        org_id=999,
        new_login="new-org",
        old_login="old-org",
    )

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process_organization_renamed(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateStore(
            session=db_session, logger=_logger()
        )
        template = await store.get_by_id(template_id)
    assert template is not None
    assert template.github_owner == "new-org"
    assert template.github_owner_id == 999


@pytest.mark.asyncio
async def test_organization_renamed_falls_back_to_old_login_for_unsynced(
    db_session: AsyncSession,
) -> None:
    """Un-synced bindings match by old login when owner ID is null."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "org-rename-unsynced")
        binding_id = await _seed_binding(
            db_session,
            org_id=org_id,
            github_owner="old-org",
            github_repo="templates",
            github_repo_id=None,
            github_owner_id=None,
        )
        await db_session.commit()

    payload = _organization_renamed_payload(
        org_id=999,
        new_login="new-org",
        old_login="old-org",
    )

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process_organization_renamed(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(binding_id)
    assert binding is not None
    assert binding.github_owner == "new-org"
    assert binding.github_owner_id is None


@pytest.mark.asyncio
async def test_repository_renamed_no_id_match_no_writes(
    db_session: AsyncSession,
) -> None:
    """A rename event for a repo with no matching binding is a no-op.

    The processor still returns cleanly — it does not raise — so a
    GitHub redelivery attempt does not loop on a transient error.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "rename-no-match")
        keep_id = await _seed_binding(
            db_session,
            org_id=org_id,
            github_owner="other",
            github_repo="other",
            github_repo_id=42,
            github_owner_id=42,
        )
        await db_session.commit()

    payload = _repository_renamed_payload(
        repo_id=12345,
        owner="acme",
        owner_id=999,
        new_name="new-name",
        old_name="old-name",
    )

    async with db_session.begin():
        processor = _make_processor(db_session)
        await processor.process_repository_renamed(payload)
        await db_session.commit()

    async with db_session.begin():
        store = DashboardGitHubTemplateBindingStore(
            session=db_session, logger=_logger()
        )
        binding = await store.get_by_id(keep_id)
    assert binding is not None
    assert binding.github_repo == "other"
