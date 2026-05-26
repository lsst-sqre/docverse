"""Tests for the RefDeletedWebhookProcessor service."""

from __future__ import annotations

from typing import Any

import pytest
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import (
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.client.models.projects import ProjectGitHubBindingCreate
from docverse.services.ref_deleted_processor import (
    RefDeletedResult,
    RefDeletedWebhookProcessor,
)
from docverse.storage.edition_store import EditionStore
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


def _make_processor(session: AsyncSession) -> RefDeletedWebhookProcessor:
    return RefDeletedWebhookProcessor(
        project_store=ProjectStore(session=session, logger=_logger()),
        edition_store=EditionStore(session=session, logger=_logger()),
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


async def _seed_project(
    session: AsyncSession,
    *,
    org_id: int,
    slug: str,
    github_owner: str = "acme",
    github_repo: str = "docs",
    repo_id: int | None = None,
) -> int:
    store = ProjectStore(session=session, logger=_logger())
    project = await store.create(
        org_id=org_id,
        data=ProjectCreate(
            slug=slug,
            title=f"Project {slug}",
            github=ProjectGitHubBindingCreate(
                owner=github_owner, repo=github_repo
            ),
        ),
        github_owner=github_owner,
        github_repo=github_repo,
    )
    if repo_id is not None:
        await store.apply_installation_scope(
            installation_id=99,
            owner=github_owner,
            owner_id=999,
            repo=github_repo,
            repo_id=repo_id,
        )
    return project.id


async def _seed_draft_edition(
    session: AsyncSession,
    *,
    project_id: int,
    slug: str,
    git_ref: str,
    lifecycle_exempt: bool = False,
    tracking_mode: TrackingMode = TrackingMode.git_ref,
    alternate_name: str | None = None,
) -> int:
    store = EditionStore(session=session, logger=_logger())
    params: dict[str, Any] = {"git_ref": git_ref}
    if alternate_name is not None:
        params["alternate_name"] = alternate_name
    edition = await store.create(
        project_id=project_id,
        data=EditionCreate(
            slug=slug,
            title=slug,
            kind=EditionKind.draft,
            tracking_mode=tracking_mode,
            tracking_params=params,
            lifecycle_exempt=lifecycle_exempt,
        ),
    )
    return edition.id


async def _seed_release_edition(
    session: AsyncSession,
    *,
    project_id: int,
    slug: str,
    git_ref: str,
) -> int:
    store = EditionStore(session=session, logger=_logger())
    edition = await store.create(
        project_id=project_id,
        data=EditionCreate(
            slug=slug,
            title=slug,
            kind=EditionKind.release,
            tracking_mode=TrackingMode.git_ref,
            tracking_params={"git_ref": git_ref},
        ),
    )
    return edition.id


async def _is_deleted(
    session: AsyncSession, *, project_id: int, slug: str
) -> bool:
    store = EditionStore(session=session, logger=_logger())
    return await store.get_by_slug(project_id=project_id, slug=slug) is None


def _delete_payload(
    *,
    owner: str = "acme",
    repo: str = "docs",
    repo_id: int | None = 12345,
    ref: str = "tickets/DM-1",
    ref_type: str = "branch",
) -> dict[str, Any]:
    repository: dict[str, Any] = {
        "name": repo,
        "full_name": f"{owner}/{repo}",
        "owner": {"login": owner, "id": 999},
    }
    if repo_id is not None:
        repository["id"] = repo_id
    return {
        "ref": ref,
        "ref_type": ref_type,
        "repository": repository,
    }


@pytest.mark.asyncio
async def test_process_soft_deletes_matching_draft_edition(
    db_session: AsyncSession,
) -> None:
    """A delete event soft-deletes the matching draft edition."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ref-del-happy")
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="docs", repo_id=12345
        )
        edition_id = await _seed_draft_edition(
            db_session,
            project_id=project_id,
            slug="dm-1",
            git_ref="tickets/DM-1",
        )
        await db_session.commit()

    async with db_session.begin():
        result = await _make_processor(db_session).process(_delete_payload())
        await db_session.commit()

    assert isinstance(result, RefDeletedResult)
    assert result.deleted_edition_ids == [edition_id]
    async with db_session.begin():
        assert await _is_deleted(
            db_session, project_id=project_id, slug="dm-1"
        )


@pytest.mark.asyncio
async def test_process_sweeps_across_projects_sharing_repo(
    db_session: AsyncSession,
) -> None:
    """Multiple projects on the same repo all see matching editions deleted.

    One upstream GitHub repo may back several Docverse project slugs.
    A single ``delete`` delivery sweeps every matching draft edition
    across that set.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ref-del-multi")
        a_id = await _seed_project(
            db_session, org_id=org_id, slug="docs-a", repo_id=12345
        )
        b_id = await _seed_project(
            db_session, org_id=org_id, slug="docs-b", repo_id=12345
        )
        await _seed_draft_edition(
            db_session,
            project_id=a_id,
            slug="dm-1",
            git_ref="tickets/DM-1",
        )
        await _seed_draft_edition(
            db_session,
            project_id=b_id,
            slug="dm-1",
            git_ref="tickets/DM-1",
        )
        await db_session.commit()

    async with db_session.begin():
        result = await _make_processor(db_session).process(_delete_payload())
        await db_session.commit()

    assert len(result.deleted_edition_ids) == 2
    async with db_session.begin():
        assert await _is_deleted(db_session, project_id=a_id, slug="dm-1")
        assert await _is_deleted(db_session, project_id=b_id, slug="dm-1")


@pytest.mark.asyncio
async def test_process_skips_lifecycle_exempt_edition(
    db_session: AsyncSession,
) -> None:
    """A matching-but-exempt draft is left in place."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ref-del-exempt")
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="docs", repo_id=12345
        )
        await _seed_draft_edition(
            db_session,
            project_id=project_id,
            slug="demo",
            git_ref="tickets/DM-1",
            lifecycle_exempt=True,
        )
        await db_session.commit()

    async with db_session.begin():
        result = await _make_processor(db_session).process(_delete_payload())
        await db_session.commit()

    assert result.deleted_edition_ids == []
    async with db_session.begin():
        assert not await _is_deleted(
            db_session, project_id=project_id, slug="demo"
        )


@pytest.mark.asyncio
async def test_process_skips_release_edition_on_same_ref(
    db_session: AsyncSession,
) -> None:
    """A release-kind edition pinned to the deleted ref survives."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ref-del-release")
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="docs", repo_id=12345
        )
        await _seed_release_edition(
            db_session,
            project_id=project_id,
            slug="v1",
            git_ref="tickets/DM-1",
        )
        await db_session.commit()

    async with db_session.begin():
        result = await _make_processor(db_session).process(_delete_payload())
        await db_session.commit()

    assert result.deleted_edition_ids == []
    async with db_session.begin():
        assert not await _is_deleted(
            db_session, project_id=project_id, slug="v1"
        )


@pytest.mark.asyncio
async def test_process_handles_tag_deletion_identically(
    db_session: AsyncSession,
) -> None:
    """A tag delete sweeps draft editions just like a branch delete.

    The ``tracking_params['git_ref']`` value is the bare ref name in
    both cases, so the filter is identical; only ``ref_type`` differs.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ref-del-tag")
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="docs", repo_id=12345
        )
        edition_id = await _seed_draft_edition(
            db_session,
            project_id=project_id,
            slug="v0-9",
            git_ref="v0.9",
        )
        await db_session.commit()

    payload = _delete_payload(ref="v0.9", ref_type="tag")
    async with db_session.begin():
        result = await _make_processor(db_session).process(payload)
        await db_session.commit()

    assert result.deleted_edition_ids == [edition_id]


@pytest.mark.asyncio
async def test_process_ignores_non_branch_or_tag_ref_type(
    db_session: AsyncSession,
) -> None:
    """A ``ref_type`` other than branch/tag is a silent no-op."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ref-del-bad-type")
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="docs", repo_id=12345
        )
        await _seed_draft_edition(
            db_session,
            project_id=project_id,
            slug="dm-1",
            git_ref="tickets/DM-1",
        )
        await db_session.commit()

    payload = _delete_payload(ref_type="repository")
    async with db_session.begin():
        result = await _make_processor(db_session).process(payload)
        await db_session.commit()

    assert result.deleted_edition_ids == []
    async with db_session.begin():
        assert not await _is_deleted(
            db_session, project_id=project_id, slug="dm-1"
        )


@pytest.mark.asyncio
async def test_process_no_op_when_no_project_matches(
    db_session: AsyncSession,
) -> None:
    """A repo with no matching project returns a 0-count result."""
    async with db_session.begin():
        await _seed_org(db_session, "ref-del-no-match")
        await db_session.commit()

    async with db_session.begin():
        result = await _make_processor(db_session).process(
            _delete_payload(owner="ghost", repo="repo", repo_id=99999)
        )
        await db_session.commit()
    assert result.deleted_edition_ids == []


@pytest.mark.asyncio
async def test_process_no_op_when_ref_matches_no_edition(
    db_session: AsyncSession,
) -> None:
    """A delete for an unrelated ref leaves all editions in place."""
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ref-del-no-edition")
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="docs", repo_id=12345
        )
        await _seed_draft_edition(
            db_session,
            project_id=project_id,
            slug="feature-y",
            git_ref="feature-y",
        )
        await db_session.commit()

    payload = _delete_payload(ref="feature-x")
    async with db_session.begin():
        result = await _make_processor(db_session).process(payload)
        await db_session.commit()

    assert result.deleted_edition_ids == []
    async with db_session.begin():
        assert not await _is_deleted(
            db_session, project_id=project_id, slug="feature-y"
        )


@pytest.mark.asyncio
async def test_process_handles_payload_with_no_repo_id(
    db_session: AsyncSession,
) -> None:
    """Match falls back to ``(lower(owner), lower(repo))`` when id absent.

    Pre-resolve projects (``github_repo_id IS NULL``) still need to
    receive webhook deliveries; the fallback path mirrors the dashboard
    binding store's ``list_unsynced_by_repo_ref`` shape.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ref-del-no-id")
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="docs", repo_id=None
        )
        edition_id = await _seed_draft_edition(
            db_session,
            project_id=project_id,
            slug="dm-1",
            git_ref="tickets/DM-1",
        )
        await db_session.commit()

    async with db_session.begin():
        result = await _make_processor(db_session).process(
            _delete_payload(repo_id=None)
        )
        await db_session.commit()
    assert result.deleted_edition_ids == [edition_id]


@pytest.mark.asyncio
async def test_process_malformed_payload_logs_and_returns_empty(
    db_session: AsyncSession,
) -> None:
    """A payload missing ``ref_type``/``ref`` is logged and ignored.

    The handler returns 200 to GitHub regardless, so the test asserts
    only on the in-DB outcome (no edition mutations) and the result's
    empty deletion list.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ref-del-malformed")
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="docs", repo_id=12345
        )
        await _seed_draft_edition(
            db_session,
            project_id=project_id,
            slug="dm-1",
            git_ref="tickets/DM-1",
        )
        await db_session.commit()

    async with db_session.begin():
        result = await _make_processor(db_session).process(
            {"ref_type": "branch", "repository": {"name": "docs"}}
        )
        await db_session.commit()
    assert result.deleted_edition_ids == []
    async with db_session.begin():
        assert not await _is_deleted(
            db_session, project_id=project_id, slug="dm-1"
        )


@pytest.mark.asyncio
async def test_process_normalizes_fully_qualified_ref_defensively(
    db_session: AsyncSession,
) -> None:
    """A ``refs/heads/...`` ref is stripped before matching.

    GitHub's ``delete`` payload delivers the bare ref name, but the
    processor still runs ``normalize_github_ref`` defensively so a
    future payload-shape change cannot leave deleted-ref webhooks
    silently no-op'ing.
    """
    async with db_session.begin():
        org_id = await _seed_org(db_session, "ref-del-norm")
        project_id = await _seed_project(
            db_session, org_id=org_id, slug="docs", repo_id=12345
        )
        edition_id = await _seed_draft_edition(
            db_session,
            project_id=project_id,
            slug="dm-1",
            git_ref="tickets/DM-1",
        )
        await db_session.commit()

    async with db_session.begin():
        result = await _make_processor(db_session).process(
            _delete_payload(ref="refs/heads/tickets/DM-1")
        )
        await db_session.commit()
    assert result.deleted_edition_ids == [edition_id]
