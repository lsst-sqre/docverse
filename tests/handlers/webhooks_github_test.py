"""Tests for the GitHub webhook handler."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
import structlog
from fastapi import FastAPI
from httpx import AsyncClient
from pydantic import SecretStr
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency

from docverse.client.models import OrganizationCreate
from docverse.dependencies.context import context_dependency
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.organization_store import OrganizationStore
from tests.support.arq_testing import count_jobs_by_name
from tests.support.github_mock import GitHubMock

_WEBHOOK_PATH = "/docverse/webhooks/github"
_WEBHOOK_SECRET = "test-webhook-secret"  # noqa: S105


def _sign(secret: str, body: bytes) -> str:
    """Compute the ``sha256=<hex>`` signature GitHub sends."""
    digest = hmac.new(
        secret.encode("utf-8"), msg=body, digestmod=hashlib.sha256
    ).hexdigest()
    return f"sha256={digest}"


@pytest_asyncio.fixture
async def github_app_enabled(
    app: FastAPI,
    mock_github: GitHubMock,
) -> AsyncIterator[None]:
    """Flip the GitHub App secrets on for the lifetime of one test.

    Saves and restores the previous values so disabled-by-default
    tests in the same session are not affected.
    """
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


def _push_payload(
    *,
    owner: str = "acme",
    repo: str = "templates",
    ref: str = "refs/heads/main",
    changed_files: list[str] | None = None,
    commits: list[dict[str, Any]] | None = None,
    size: int = 1,
) -> dict[str, Any]:
    if commits is None:
        commits = [
            {
                "id": "after-sha",
                "modified": changed_files or [],
                "added": [],
                "removed": [],
            }
        ]
    return {
        "ref": ref,
        "before": "before-sha",
        "after": "after-sha",
        "repository": {
            "name": repo,
            "full_name": f"{owner}/{repo}",
            "owner": {"login": owner, "name": owner},
        },
        "installation": {"id": 99},
        "size": size,
        "commits": commits,
    }


async def _seed_binding(
    *,
    owner: str = "acme",
    repo: str = "templates",
    ref: str = "refs/heads/main",
    root_path: str = "/",
) -> None:
    logger = structlog.get_logger("test")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.create(
                OrganizationCreate(
                    slug=f"webhook-{owner}-{repo}",
                    title="Webhook Org",
                    base_domain="webhook.example.com",
                )
            )
            binding_store = DashboardGitHubTemplateBindingStore(
                session=session, logger=logger
            )
            await binding_store.create(
                DashboardGitHubTemplateBindingCreate(
                    org_id=org.id,
                    project_id=None,
                    github_owner=owner,
                    github_repo=repo,
                    github_ref=ref,
                    root_path=root_path,
                )
            )
            await session.commit()


@pytest.mark.asyncio
async def test_post_returns_404_when_feature_disabled(
    client: AsyncClient,
) -> None:
    """No GitHub App secrets configured → endpoint responds 404."""
    response = await client.post(
        _WEBHOOK_PATH,
        content=b"{}",
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000000",
        },
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_post_returns_401_when_unsigned(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """Missing ``X-Hub-Signature-256`` → 401."""
    body = json.dumps(_push_payload()).encode("utf-8")
    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000000",
        },
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_post_returns_401_when_signature_wrong(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """Signature computed with the wrong secret → 401."""
    body = json.dumps(_push_payload()).encode("utf-8")
    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000000",
            "X-Hub-Signature-256": _sign("wrong-secret", body),
        },
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_post_signed_push_enqueues_dashboard_sync(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A valid signed push lands a ``dashboard_sync`` job on the queue."""
    await _seed_binding(root_path="/")
    payload = _push_payload(
        changed_files=["templates/blue/dashboard.html.jinja"],
    )
    body = json.dumps(payload).encode("utf-8")

    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000000",
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )
    assert response.status_code == 200
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    assert count_jobs_by_name(arq_queue, "dashboard_sync") >= 1


@pytest.mark.asyncio
async def test_post_signed_push_with_no_matching_root_path_no_enqueue(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed push that does not touch any binding's root_path is a no-op."""
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    before = count_jobs_by_name(arq_queue, "dashboard_sync")
    await _seed_binding(root_path="templates/red")
    payload = _push_payload(
        changed_files=["templates/blue/dashboard.html.jinja"],
    )
    body = json.dumps(payload).encode("utf-8")

    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000000",
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )
    assert response.status_code == 200
    after = count_jobs_by_name(arq_queue, "dashboard_sync")
    assert after == before


@pytest.mark.asyncio
async def test_post_signed_truncated_push_falls_back_to_compare(
    client: AsyncClient,
    github_app_enabled: None,
    mock_github: GitHubMock,
) -> None:
    """A truncated push (``size > len(commits)``) hits the compare API.

    The in-payload ``commits`` list reports only ``docs/index.md``, but
    the compare API — authoritative for truncated pushes — reports a
    template path inside the binding's ``root_path``. Exactly one
    ``dashboard_sync`` job lands on the queue.
    """
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    before = count_jobs_by_name(arq_queue, "dashboard_sync")
    await _seed_binding(root_path="/")
    payload = _push_payload(
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
        changed_paths=["templates/blue/dashboard.html.jinja"],
    )
    body = json.dumps(payload).encode("utf-8")

    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000002",
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )
    assert response.status_code == 200
    after = count_jobs_by_name(arq_queue, "dashboard_sync")
    assert after - before == 1


@pytest.mark.asyncio
async def test_post_signed_empty_commits_no_enqueue(
    client: AsyncClient,
    github_app_enabled: None,
    mock_github: GitHubMock,
) -> None:
    """A push with ``commits=[]`` and ``size=0`` enqueues nothing.

    The processor's cheap path returns an empty changed-path set
    without falling back to the compare API; assert the compare route
    sees zero requests so a future regression that swaps the
    truncation signal can't sneak past us.
    """
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    before = count_jobs_by_name(arq_queue, "dashboard_sync")
    await _seed_binding(root_path="/")
    payload = _push_payload(commits=[], size=0)
    body = json.dumps(payload).encode("utf-8")

    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000003",
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )
    assert response.status_code == 200
    after = count_jobs_by_name(arq_queue, "dashboard_sync")
    assert after == before
    compare_calls = [
        call
        for call in mock_github.router.calls
        if "/compare/" in call.request.url.path
    ]
    assert compare_calls == []


@pytest.mark.asyncio
async def test_post_signed_unrelated_event_is_no_op(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """An event we do not subscribe to (``ping``) returns 200, no enqueue."""
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    before = count_jobs_by_name(arq_queue, "dashboard_sync")
    body = json.dumps({"zen": "Speak like a human."}).encode("utf-8")
    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "ping",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000001",
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )
    assert response.status_code == 200
    after = count_jobs_by_name(arq_queue, "dashboard_sync")
    assert after == before
