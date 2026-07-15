"""Handler-level tests for the GitHub ``delete`` webhook event."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
import structlog
from docverse.client.models import (
    EditionCreate,
    EditionKind,
    JobKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.client.models.projects import ProjectGitHubBindingCreate
from fastapi import FastAPI
from httpx import AsyncClient
from pydantic import SecretStr
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import select, update

from docverse.dbschema.organization import SqlOrganization
from docverse.dbschema.queue_job import SqlQueueJob
from docverse.dependencies.context import context_dependency
from docverse.factory import Factory
from docverse.storage.edition_store import EditionStore
from docverse.storage.editionpublisher import (
    EditionPublisher,
    MockEditionPublisher,
)
from docverse.storage.organization_store import OrganizationStore
from docverse.storage.project_store import ProjectStore
from tests.support.arq_testing import count_jobs_by_name
from tests.support.github_mock import GitHubMock

_WEBHOOK_PATH = "/docverse/webhooks/github"
_WEBHOOK_SECRET = "test-webhook-secret"


def _sign(secret: str, body: bytes) -> str:
    digest = hmac.new(
        secret.encode("utf-8"), msg=body, digestmod=hashlib.sha256
    ).hexdigest()
    return f"sha256={digest}"


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


async def _seed_project_with_edition(
    *,
    org_slug: str,
    project_slug: str,
    github_owner: str = "acme",
    github_repo: str = "docs",
    repo_id: int | None = 12345,
    edition_slug: str = "dm-1",
    git_ref: str = "tickets/DM-1",
    edition_kind: EditionKind = EditionKind.draft,
    lifecycle_exempt: bool = False,
) -> tuple[int, int]:
    """Seed (org, project, edition) and return ``(project_id, edition_id)``."""
    logger = structlog.get_logger("test")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.create(
                OrganizationCreate(
                    slug=org_slug,
                    title=f"Org {org_slug}",
                    base_domain=f"{org_slug}.example.com",
                )
            )
            project_store = ProjectStore(session=session, logger=logger)
            project = await project_store.create(
                org_id=org.id,
                data=ProjectCreate(
                    slug=project_slug,
                    title=f"Project {project_slug}",
                    github=ProjectGitHubBindingCreate(
                        owner=github_owner, repo=github_repo
                    ),
                ),
                github_owner=github_owner,
                github_repo=github_repo,
            )
            if repo_id is not None:
                await project_store.apply_installation_scope(
                    installation_id=99,
                    owner=github_owner,
                    owner_id=999,
                    repo=github_repo,
                    repo_id=repo_id,
                )
            edition_store = EditionStore(session=session, logger=logger)
            edition = await edition_store.create(
                project_id=project.id,
                data=EditionCreate(
                    slug=edition_slug,
                    title=edition_slug,
                    kind=edition_kind,
                    tracking_mode=TrackingMode.git_ref,
                    tracking_params={"git_ref": git_ref},
                    lifecycle_exempt=lifecycle_exempt,
                ),
            )
            await session.commit()
        return project.id, edition.id
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


async def _is_deleted(project_id: int, slug: str) -> bool:
    logger = structlog.get_logger("test")
    async for session in db_session_dependency():
        async with session.begin():
            store = EditionStore(session=session, logger=logger)
            return (
                await store.get_by_slug(project_id=project_id, slug=slug)
                is None
            )
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


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


def _post_signed(
    payload: dict[str, Any],
    *,
    delivery_id: str = "00000000-0000-0000-0000-000000000020",
) -> tuple[bytes, dict[str, str]]:
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "X-GitHub-Event": "delete",
        "X-GitHub-Delivery": delivery_id,
        "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
    }
    return body, headers


@pytest.mark.asyncio
async def test_signed_delete_soft_deletes_matching_draft(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed ``delete`` event soft-deletes the matching draft edition."""
    project_id, _ = await _seed_project_with_edition(
        org_slug="delete-happy", project_slug="docs"
    )
    body, headers = _post_signed(_delete_payload())
    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)
    assert response.status_code == 200
    assert await _is_deleted(project_id, "dm-1")


@pytest.mark.asyncio
async def test_signed_delete_skips_lifecycle_exempt(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A matching but ``lifecycle_exempt`` draft is left in place."""
    project_id, _ = await _seed_project_with_edition(
        org_slug="delete-exempt",
        project_slug="docs",
        edition_slug="demo",
        lifecycle_exempt=True,
    )
    body, headers = _post_signed(_delete_payload())
    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)
    assert response.status_code == 200
    assert not await _is_deleted(project_id, "demo")


@pytest.mark.asyncio
async def test_signed_delete_skips_release_edition(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A ``release``-kind edition on the same ref survives a delete event.

    The fast path is restricted to ``kind='draft'`` editions. A release
    edition pinned to a tag stays put even if that tag is later
    force-deleted upstream.
    """
    project_id, _ = await _seed_project_with_edition(
        org_slug="delete-release",
        project_slug="docs",
        edition_slug="v1",
        edition_kind=EditionKind.release,
        git_ref="v1",
    )
    body, headers = _post_signed(_delete_payload(ref="v1", ref_type="tag"))
    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)
    assert response.status_code == 200
    assert not await _is_deleted(project_id, "v1")


@pytest.mark.asyncio
async def test_signed_delete_no_match_returns_200(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A delete for a repo with no matching project is a 200 no-op.

    Shared GitHub Apps installed on repos Docverse doesn't track must
    not produce noise: returning anything but 200 here would queue a
    redelivery storm.
    """
    body, headers = _post_signed(
        _delete_payload(owner="ghost", repo="repo", repo_id=99999)
    )
    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_signed_delete_unrelated_ref_returns_200(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A delete whose ref matches no draft edition returns 200 silently."""
    project_id, _ = await _seed_project_with_edition(
        org_slug="delete-no-edition",
        project_slug="docs",
        edition_slug="feature-y",
        git_ref="feature-y",
    )
    body, headers = _post_signed(_delete_payload(ref="feature-x"))
    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)
    assert response.status_code == 200
    assert not await _is_deleted(project_id, "feature-y")


@pytest.mark.asyncio
async def test_signed_delete_malformed_payload_returns_200(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A malformed payload returns 200 and does not mutate editions."""
    project_id, _ = await _seed_project_with_edition(
        org_slug="delete-malformed", project_slug="docs"
    )
    # Missing ``ref`` field — the processor warns and returns empty.
    body, headers = _post_signed(
        {
            "ref_type": "branch",
            "repository": {
                "name": "docs",
                "full_name": "acme/docs",
                "owner": {"login": "acme", "id": 999},
                "id": 12345,
            },
        }
    )
    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)
    assert response.status_code == 200
    assert not await _is_deleted(project_id, "dm-1")


@pytest.mark.asyncio
async def test_signed_delete_tag_handled_like_branch(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """Tag deletions sweep matching draft editions the same as branches."""
    project_id, _ = await _seed_project_with_edition(
        org_slug="delete-tag",
        project_slug="docs",
        edition_slug="v0-9",
        git_ref="v0.9",
    )
    body, headers = _post_signed(_delete_payload(ref="v0.9", ref_type="tag"))
    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)
    assert response.status_code == 200
    assert await _is_deleted(project_id, "v0-9")


async def _set_cdn_service_label(org_slug: str) -> None:
    async for session in db_session_dependency():
        async with session.begin():
            await session.execute(
                update(SqlOrganization)
                .where(SqlOrganization.slug == org_slug)
                .values(cdn_service_label="cdn-prod")
            )
            await session.commit()
        return
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


async def _count_dashboard_jobs() -> int:
    async for session in db_session_dependency():
        async with session.begin():
            result = await session.execute(
                select(SqlQueueJob).where(
                    SqlQueueJob.kind == JobKind.dashboard_build.value
                )
            )
            return len(list(result.scalars().all()))
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


def _dashboard_build_arq_count() -> int:
    mock_arq = arq_dependency._arq_queue
    assert isinstance(mock_arq, MockArqQueue)
    return count_jobs_by_name(mock_arq, "dashboard_build")


@pytest.mark.asyncio
async def test_signed_delete_unpublishes_from_cdn(
    client: AsyncClient,
    github_app_enabled: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The webhook fast path removes the CDN pointer for each match.

    Mirrors :func:`tests.handlers.editions_test
    .test_delete_edition_unpublishes_from_cdn`'s factory-patch shape:
    seed an org with a ``cdn_service_label`` so the publishing
    service resolves a publisher, then assert the recorded
    ``unpublish`` call matches the soft-deleted edition.
    """
    project_id, _ = await _seed_project_with_edition(
        org_slug="delete-cdn", project_slug="docs"
    )
    await _set_cdn_service_label("delete-cdn")

    mock_publisher = MockEditionPublisher()

    async def _create(
        self: Factory, *, org_id: int, service_label: str
    ) -> EditionPublisher:
        _ = (self, org_id, service_label)
        return mock_publisher

    monkeypatch.setattr(Factory, "create_edition_publisher_for_org", _create)

    body, headers = _post_signed(_delete_payload())
    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)
    assert response.status_code == 200
    assert await _is_deleted(project_id, "dm-1")
    assert len(mock_publisher.unpublish_calls) == 1
    call = mock_publisher.unpublish_calls[0]
    assert call.project_slug == "docs"
    assert call.edition_slug == "dm-1"


@pytest.mark.asyncio
async def test_signed_delete_enqueues_dashboard_build_per_affected_project(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """Each affected project gets one ``dashboard_build`` post-commit.

    Asserts at the queue-row level so the test is independent of the
    arq backend's job naming. The handler enqueues post-commit (after
    the soft-delete + unpublish block exits), matching the daily
    audit worker's order so an enqueue failure cannot roll back the
    soft-delete.
    """
    await _seed_project_with_edition(
        org_slug="delete-dashboard", project_slug="docs"
    )

    before = await _count_dashboard_jobs()
    body, headers = _post_signed(_delete_payload())
    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)
    assert response.status_code == 200

    after = await _count_dashboard_jobs()
    assert after - before == 1
    assert _dashboard_build_arq_count() >= 1


@pytest.mark.asyncio
async def test_signed_delete_no_match_does_not_enqueue_or_unpublish(
    client: AsyncClient,
    github_app_enabled: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A delivery with no matched project fires neither side-effect.

    Pairs with :func:`test_signed_delete_no_match_returns_200`: the
    no-match path stays a silent 200 with zero CDN traffic and zero
    queue rows.
    """
    mock_publisher = MockEditionPublisher()

    async def _create(
        self: Factory, *, org_id: int, service_label: str
    ) -> EditionPublisher:
        _ = (self, org_id, service_label)
        return mock_publisher

    monkeypatch.setattr(Factory, "create_edition_publisher_for_org", _create)

    before = await _count_dashboard_jobs()
    body, headers = _post_signed(
        _delete_payload(owner="ghost", repo="repo", repo_id=99999)
    )
    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)
    assert response.status_code == 200
    assert mock_publisher.unpublish_calls == []
    after = await _count_dashboard_jobs()
    assert after == before


@pytest.mark.asyncio
async def test_signed_delete_cdn_failure_rolls_back_soft_delete(
    client: AsyncClient,
    github_app_enabled: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A CDN unpublish failure rolls back the soft-delete + skips enqueue.

    The unpublish runs inside the handler's open transaction, so a
    raise propagates out of the webhook handler and the soft-delete is
    rolled back along with it. In production the ASGI server returns
    500 and GitHub redelivers the webhook; in this ASGI-transport test
    rig the uncaught ``RuntimeError`` bubbles up through ``httpx``
    without being translated, so the assertion uses ``pytest.raises``.
    The contract being pinned is the rollback: the edition is still
    present and no ``dashboard_build`` row was written.
    """
    project_id, _ = await _seed_project_with_edition(
        org_slug="delete-cdn-fail", project_slug="docs"
    )
    await _set_cdn_service_label("delete-cdn-fail")

    class _FailingPublisher(MockEditionPublisher):
        async def unpublish(
            self, *, project_slug: str, edition_slug: str
        ) -> None:
            await super().unpublish(
                project_slug=project_slug, edition_slug=edition_slug
            )
            msg = "kv failure"
            raise RuntimeError(msg)

    failing_publisher = _FailingPublisher()

    async def _create(
        self: Factory, *, org_id: int, service_label: str
    ) -> EditionPublisher:
        _ = (self, org_id, service_label)
        return failing_publisher

    monkeypatch.setattr(Factory, "create_edition_publisher_for_org", _create)

    before = await _count_dashboard_jobs()
    body, headers = _post_signed(_delete_payload())
    with pytest.raises(RuntimeError, match="kv failure"):
        await client.post(_WEBHOOK_PATH, content=body, headers=headers)

    assert len(failing_publisher.unpublish_calls) == 1
    assert not await _is_deleted(project_id, "dm-1")
    after = await _count_dashboard_jobs()
    assert after == before
