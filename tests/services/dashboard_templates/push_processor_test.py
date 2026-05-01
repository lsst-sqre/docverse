"""Tests for the PushEventProcessor service."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from safir.github import GitHubAppClientFactory
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.client.models import OrganizationCreate
from docverse.client.models.queue_enums import JobKind
from docverse.config import Configuration
from docverse.services.dashboard_templates.enqueue import DashboardSyncEnqueuer
from docverse.services.dashboard_templates.push_processor import (
    PushEventProcessor,
)
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.github import GitHubAppClient
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.queue_backend import ArqQueueBackend
from docverse.storage.queue_job_store import QueueJobStore
from tests.support.github_mock import DEFAULT_APP_NAME, GitHubMock

_config = Configuration()


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


async def _seed_org_with_bindings(
    session: AsyncSession,
    *,
    org_slug: str,
    bindings: list[DashboardGitHubTemplateBindingCreate],
) -> tuple[int, list[int]]:
    """Create one org and a list of bindings under it, returning IDs."""
    logger = _logger()
    org_store = OrganizationStore(session=session, logger=logger)
    org = await org_store.create(
        OrganizationCreate(
            slug=org_slug,
            title=f"Org {org_slug}",
            base_domain=f"{org_slug}.example.com",
        )
    )
    binding_store = DashboardGitHubTemplateBindingStore(
        session=session, logger=logger
    )
    binding_ids: list[int] = []
    for raw in bindings:
        # Inject the freshly-created org id so callers don't need to know it.
        new = DashboardGitHubTemplateBindingCreate(
            org_id=org.id,
            project_id=raw.project_id,
            github_owner=raw.github_owner,
            github_repo=raw.github_repo,
            github_ref=raw.github_ref,
            root_path=raw.root_path,
        )
        created = await binding_store.create(new)
        binding_ids.append(created.id)
    return org.id, binding_ids


def _make_processor(
    session: AsyncSession,
    *,
    arq_queue: MockArqQueue,
    http_client: httpx.AsyncClient,
    mock_github: GitHubMock,
) -> PushEventProcessor:
    logger = _logger()
    binding_store = DashboardGitHubTemplateBindingStore(
        session=session, logger=logger
    )
    queue_backend = ArqQueueBackend(
        arq_queue=arq_queue, default_queue_name=_config.arq_queue_name
    )
    queue_job_store = QueueJobStore(session=session, logger=logger)
    enqueuer = DashboardSyncEnqueuer(
        binding_store=binding_store,
        queue_backend=queue_backend,
        queue_job_store=queue_job_store,
        logger=logger,
    )
    factory = GitHubAppClientFactory(
        id=mock_github.app_id,
        key=mock_github.private_key_pem,
        name=DEFAULT_APP_NAME,
        http_client=http_client,
    )
    app_client = GitHubAppClient(
        factory=factory, http_client=http_client, logger=logger
    )
    return PushEventProcessor(
        binding_store=binding_store,
        enqueuer=enqueuer,
        app_client=app_client,
        http_client=http_client,
        logger=logger,
    )


def _make_push_payload(
    *,
    owner: str,
    repo: str,
    ref: str,
    before: str = "before-sha",
    after: str = "after-sha",
    commits: list[dict[str, Any]] | None = None,
    size: int | None = None,
    installation_id: int = 99,
) -> dict[str, Any]:
    """Build a minimal push-event payload."""
    payload: dict[str, Any] = {
        "ref": ref,
        "before": before,
        "after": after,
        "repository": {
            "name": repo,
            "full_name": f"{owner}/{repo}",
            "owner": {"login": owner, "name": owner},
        },
        "installation": {"id": installation_id},
    }
    if commits is not None:
        payload["commits"] = commits
    if size is not None:
        payload["size"] = size
    return payload


@pytest.mark.asyncio
async def test_process_filters_bindings_by_root_path_intersection(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Only bindings whose ``root_path`` matches the push paths get synced."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        _, _binding_ids = await _seed_org_with_bindings(
            db_session,
            org_slug="push-root-path",
            bindings=[
                DashboardGitHubTemplateBindingCreate(
                    org_id=0,  # replaced inside helper
                    project_id=None,
                    github_owner="acme",
                    github_repo="templates",
                    github_ref="main",
                    root_path="templates/blue",
                ),
            ],
        )
        await db_session.commit()

    payload = _make_push_payload(
        owner="acme",
        repo="templates",
        ref="refs/heads/main",
        commits=[
            {
                "id": "after-sha",
                "modified": ["templates/blue/dashboard.html.jinja"],
                "added": [],
                "removed": [],
            }
        ],
        size=1,
    )

    async with httpx.AsyncClient() as http_client, db_session.begin():
        processor = _make_processor(
            db_session,
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        jobs = await processor.process(payload)
        await db_session.commit()

    assert len(jobs) == 1
    async with db_session.begin():
        store = QueueJobStore(session=db_session, logger=_logger())
        loaded = await store.get(jobs[0].id)
    assert loaded is not None
    assert loaded.kind == JobKind.dashboard_sync


@pytest.mark.asyncio
async def test_process_skips_bindings_with_no_intersecting_root_path(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A binding whose ``root_path`` is outside the push paths is skipped."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        _, _ = await _seed_org_with_bindings(
            db_session,
            org_slug="push-no-intersect",
            bindings=[
                DashboardGitHubTemplateBindingCreate(
                    org_id=0,
                    project_id=None,
                    github_owner="acme",
                    github_repo="templates",
                    github_ref="main",
                    root_path="templates/red",
                ),
            ],
        )
        await db_session.commit()

    payload = _make_push_payload(
        owner="acme",
        repo="templates",
        ref="refs/heads/main",
        commits=[
            {
                "id": "after-sha",
                "modified": ["templates/blue/dashboard.html.jinja"],
                "added": [],
                "removed": [],
            }
        ],
        size=1,
    )

    async with httpx.AsyncClient() as http_client, db_session.begin():
        processor = _make_processor(
            db_session,
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        jobs = await processor.process(payload)
        await db_session.commit()

    assert jobs == []


@pytest.mark.asyncio
async def test_process_returns_empty_when_no_bindings_match_repo_ref(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Push to a ref with no bindings registered is a no-op."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        await _seed_org_with_bindings(
            db_session,
            org_slug="push-no-bindings",
            bindings=[
                DashboardGitHubTemplateBindingCreate(
                    org_id=0,
                    project_id=None,
                    github_owner="acme",
                    github_repo="templates",
                    github_ref="main",
                    root_path="/",
                ),
            ],
        )
        await db_session.commit()

    payload = _make_push_payload(
        owner="someone-else",
        repo="unrelated",
        ref="refs/heads/main",
        commits=[
            {
                "id": "after-sha",
                "modified": ["README.md"],
                "added": [],
                "removed": [],
            }
        ],
        size=1,
    )

    async with httpx.AsyncClient() as http_client, db_session.begin():
        processor = _make_processor(
            db_session,
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        jobs = await processor.process(payload)
        await db_session.commit()

    assert jobs == []


@pytest.mark.asyncio
async def test_process_root_path_slash_matches_any_change(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """``root_path="/"`` is a "whole repo" binding — any change matches."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        _, _ = await _seed_org_with_bindings(
            db_session,
            org_slug="push-root-slash",
            bindings=[
                DashboardGitHubTemplateBindingCreate(
                    org_id=0,
                    project_id=None,
                    github_owner="acme",
                    github_repo="templates",
                    github_ref="main",
                    root_path="/",
                ),
            ],
        )
        await db_session.commit()

    payload = _make_push_payload(
        owner="acme",
        repo="templates",
        ref="refs/heads/main",
        commits=[
            {
                "id": "after-sha",
                "modified": ["docs/index.md"],
                "added": [],
                "removed": [],
            }
        ],
        size=1,
    )

    async with httpx.AsyncClient() as http_client, db_session.begin():
        processor = _make_processor(
            db_session,
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        jobs = await processor.process(payload)
        await db_session.commit()

    assert len(jobs) == 1


@pytest.mark.asyncio
async def test_process_matches_bare_branch_against_refs_heads_payload(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A binding stored as ``main`` matches a push with ``refs/heads/main``.

    Reproduces DM-54689: GitHub push payloads always carry the
    fully-qualified ``refs/heads/<branch>`` form, but bindings store the
    bare branch name. Without per-side normalization the lookup misses
    silently and the push is ignored.
    """
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        _, _ = await _seed_org_with_bindings(
            db_session,
            org_slug="push-bare-branch",
            bindings=[
                DashboardGitHubTemplateBindingCreate(
                    org_id=0,
                    project_id=None,
                    github_owner="acme",
                    github_repo="templates",
                    github_ref="main",
                    root_path="/",
                ),
            ],
        )
        await db_session.commit()

    payload = _make_push_payload(
        owner="acme",
        repo="templates",
        ref="refs/heads/main",
        commits=[
            {
                "id": "after-sha",
                "modified": ["templates/blue/dashboard.html.jinja"],
                "added": [],
                "removed": [],
            }
        ],
        size=1,
    )

    async with httpx.AsyncClient() as http_client, db_session.begin():
        processor = _make_processor(
            db_session,
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        jobs = await processor.process(payload)
        await db_session.commit()

    assert len(jobs) == 1


@pytest.mark.asyncio
async def test_process_matches_bare_tag_against_refs_tags_payload(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A binding stored as ``v1.0`` matches a push with ``refs/tags/v1.0``."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        _, _ = await _seed_org_with_bindings(
            db_session,
            org_slug="push-bare-tag",
            bindings=[
                DashboardGitHubTemplateBindingCreate(
                    org_id=0,
                    project_id=None,
                    github_owner="acme",
                    github_repo="templates",
                    github_ref="v1.0",
                    root_path="/",
                ),
            ],
        )
        await db_session.commit()

    payload = _make_push_payload(
        owner="acme",
        repo="templates",
        ref="refs/tags/v1.0",
        commits=[
            {
                "id": "after-sha",
                "modified": ["templates/blue/dashboard.html.jinja"],
                "added": [],
                "removed": [],
            }
        ],
        size=1,
    )

    async with httpx.AsyncClient() as http_client, db_session.begin():
        processor = _make_processor(
            db_session,
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        jobs = await processor.process(payload)
        await db_session.commit()

    assert len(jobs) == 1


@pytest.mark.asyncio
async def test_process_falls_back_to_compare_api_when_payload_truncated(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """A truncated push payload triggers the compare-API fallback.

    The ``size`` field exceeds ``len(commits)`` so
    :func:`extract_changed_paths_from_push` returns ``None``; the
    processor must then fetch the full file list from the compare API
    and use it to filter bindings.
    """
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        _, _ = await _seed_org_with_bindings(
            db_session,
            org_slug="push-truncated",
            bindings=[
                DashboardGitHubTemplateBindingCreate(
                    org_id=0,
                    project_id=None,
                    github_owner="acme",
                    github_repo="templates",
                    github_ref="main",
                    root_path="templates/blue",
                ),
            ],
        )
        await db_session.commit()

    # Truncated push: size says 30 commits, payload only has the
    # first one and its file list looks like docs only — but the real
    # changed-path set (from compare) DOES include
    # templates/blue/dashboard.html.jinja.
    payload = _make_push_payload(
        owner="acme",
        repo="templates",
        ref="refs/heads/main",
        before="before-sha",
        after="after-sha",
        commits=[
            {
                "id": "first-sha",
                "modified": ["docs/index.md"],
                "added": [],
                "removed": [],
            }
        ],
        size=30,
    )

    mock_github.seed_installation("acme", "templates", installation_id=99)
    mock_github.seed_compare(
        "acme",
        "templates",
        before="before-sha",
        after="after-sha",
        changed_paths=["docs/index.md", "templates/blue/dashboard.html.jinja"],
    )

    async with httpx.AsyncClient() as http_client, db_session.begin():
        processor = _make_processor(
            db_session,
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        jobs = await processor.process(payload)
        await db_session.commit()

    assert len(jobs) == 1


@pytest.mark.asyncio
async def test_process_compare_fallback_filters_correctly(
    db_session: AsyncSession,
    mock_github: GitHubMock,
) -> None:
    """Compare-fallback paths still filter bindings by root_path."""
    arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)

    async with db_session.begin():
        _, _ = await _seed_org_with_bindings(
            db_session,
            org_slug="push-truncated-skip",
            bindings=[
                DashboardGitHubTemplateBindingCreate(
                    org_id=0,
                    project_id=None,
                    github_owner="acme",
                    github_repo="templates",
                    github_ref="main",
                    root_path="templates/red",
                ),
            ],
        )
        await db_session.commit()

    payload = _make_push_payload(
        owner="acme",
        repo="templates",
        ref="refs/heads/main",
        commits=[
            {"id": "first", "modified": ["x"], "added": [], "removed": []}
        ],
        size=30,
    )
    mock_github.seed_installation("acme", "templates", installation_id=99)
    mock_github.seed_compare(
        "acme",
        "templates",
        before="before-sha",
        after="after-sha",
        changed_paths=["templates/blue/dashboard.html.jinja"],
    )

    async with httpx.AsyncClient() as http_client, db_session.begin():
        processor = _make_processor(
            db_session,
            arq_queue=arq_queue,
            http_client=http_client,
            mock_github=mock_github,
        )
        jobs = await processor.process(payload)
        await db_session.commit()

    assert jobs == []
