"""Handler-level tests for the rename / transfer / installation events."""

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
from docverse.services.dashboard_templates.installation_processor import (
    INSTALLATION_DELETED_REASON,
    INSTALLATION_SUSPENDED_REASON,
)
from docverse.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
)
from docverse.storage.organization_store import OrganizationStore
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


async def _seed_binding(
    *,
    org_slug: str,
    github_owner: str = "acme",
    github_repo: str = "templates",
    github_ref: str = "main",
    github_owner_id: int | None = None,
    github_repo_id: int | None = None,
    github_installation_id: int | None = None,
) -> int:
    logger = structlog.get_logger("test")
    binding_id = 0
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
            store = DashboardGitHubTemplateBindingStore(
                session=session, logger=logger
            )
            binding = await store.create(
                DashboardGitHubTemplateBindingCreate(
                    org_id=org.id,
                    project_id=None,
                    github_owner=github_owner,
                    github_repo=github_repo,
                    github_ref=github_ref,
                    root_path="/",
                    github_owner_id=github_owner_id,
                    github_repo_id=github_repo_id,
                    github_installation_id=github_installation_id,
                )
            )
            binding_id = binding.id
            await session.commit()
        return binding_id
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


async def _read_binding(binding_id: int) -> Any:
    logger = structlog.get_logger("test")
    async for session in db_session_dependency():
        async with session.begin():
            store = DashboardGitHubTemplateBindingStore(
                session=session, logger=logger
            )
            return await store.get_by_id(binding_id)
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


def _post_signed_event(
    *,
    event: str,
    payload: dict[str, Any],
    delivery_id: str = "00000000-0000-0000-0000-000000000010",
) -> tuple[bytes, dict[str, str]]:
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "X-GitHub-Event": event,
        "X-GitHub-Delivery": delivery_id,
        "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
    }
    return body, headers


@pytest.mark.asyncio
async def test_repository_renamed_signed_post_updates_binding(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed ``repository.renamed`` rewrites the binding's display name."""
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    sync_jobs_before = count_jobs_by_name(arq_queue, "dashboard_sync")

    binding_id = await _seed_binding(
        org_slug="rename-handler",
        github_owner="acme",
        github_repo="old-name",
        github_repo_id=12345,
        github_owner_id=999,
    )
    payload = {
        "action": "renamed",
        "changes": {"repository": {"name": {"from": "old-name"}}},
        "repository": {
            "id": 12345,
            "name": "new-name",
            "full_name": "acme/new-name",
            "owner": {"login": "acme", "id": 999},
        },
        "installation": {"id": 99},
    }
    body, headers = _post_signed_event(event="repository", payload=payload)

    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)

    assert response.status_code == 200
    binding = await _read_binding(binding_id)
    assert binding is not None
    assert binding.github_repo == "new-name"
    # No syncs are enqueued for a rename — only display strings change.
    assert count_jobs_by_name(arq_queue, "dashboard_sync") == sync_jobs_before


@pytest.mark.asyncio
async def test_repository_transferred_signed_post_updates_owner(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed ``repository.transferred`` rewrites owner + owner id."""
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    sync_jobs_before = count_jobs_by_name(arq_queue, "dashboard_sync")

    binding_id = await _seed_binding(
        org_slug="transfer-handler",
        github_owner="old-owner",
        github_repo="templates",
        github_repo_id=12345,
        github_owner_id=111,
    )
    payload = {
        "action": "transferred",
        "changes": {
            "owner": {
                "from": {
                    "user": {"login": "old-owner", "id": 111},
                }
            }
        },
        "repository": {
            "id": 12345,
            "name": "templates",
            "full_name": "new-owner/templates",
            "owner": {"login": "new-owner", "id": 222},
        },
        "installation": {"id": 99},
    }
    body, headers = _post_signed_event(event="repository", payload=payload)

    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)

    assert response.status_code == 200
    binding = await _read_binding(binding_id)
    assert binding is not None
    assert binding.github_owner == "new-owner"
    assert binding.github_owner_id == 222
    assert count_jobs_by_name(arq_queue, "dashboard_sync") == sync_jobs_before


@pytest.mark.asyncio
async def test_organization_renamed_signed_post_updates_owner(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed ``organization.renamed`` rewrites the binding's owner login."""
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    sync_jobs_before = count_jobs_by_name(arq_queue, "dashboard_sync")

    binding_id = await _seed_binding(
        org_slug="org-rename-handler",
        github_owner="old-org",
        github_owner_id=999,
        github_repo_id=12345,
    )
    payload = {
        "action": "renamed",
        "changes": {"login": {"from": "old-org"}},
        "organization": {"id": 999, "login": "new-org"},
        "installation": {"id": 99},
    }
    body, headers = _post_signed_event(event="organization", payload=payload)

    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)

    assert response.status_code == 200
    binding = await _read_binding(binding_id)
    assert binding is not None
    assert binding.github_owner == "new-org"
    assert count_jobs_by_name(arq_queue, "dashboard_sync") == sync_jobs_before


@pytest.mark.asyncio
async def test_installation_suspend_signed_post_marks_binding_failed(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed ``installation.suspend`` flips matching bindings to failed."""
    binding_id = await _seed_binding(
        org_slug="install-suspend-handler", github_installation_id=99
    )
    payload = {
        "action": "suspend",
        "installation": {"id": 99, "account": {"login": "acme", "id": 999}},
        "repositories": [],
    }
    body, headers = _post_signed_event(event="installation", payload=payload)

    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)

    assert response.status_code == 200
    binding = await _read_binding(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "failed"
    assert binding.last_sync_error == INSTALLATION_SUSPENDED_REASON


@pytest.mark.asyncio
async def test_installation_unsuspend_signed_post_clears_failure(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed ``installation.unsuspend`` clears a prior suspend flag."""
    binding_id = await _seed_binding(
        org_slug="install-unsuspend-handler", github_installation_id=99
    )
    suspend_payload = {
        "action": "suspend",
        "installation": {"id": 99, "account": {"login": "acme", "id": 999}},
        "repositories": [],
    }
    body, headers = _post_signed_event(
        event="installation",
        payload=suspend_payload,
        delivery_id="00000000-0000-0000-0000-000000000020",
    )
    suspend_response = await client.post(
        _WEBHOOK_PATH, content=body, headers=headers
    )
    assert suspend_response.status_code == 200

    unsuspend_payload = {
        "action": "unsuspend",
        "installation": {"id": 99, "account": {"login": "acme", "id": 999}},
        "repositories": [],
    }
    body, headers = _post_signed_event(
        event="installation",
        payload=unsuspend_payload,
        delivery_id="00000000-0000-0000-0000-000000000021",
    )
    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)

    assert response.status_code == 200
    binding = await _read_binding(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "pending"
    assert binding.last_sync_error is None


@pytest.mark.asyncio
async def test_installation_deleted_signed_post_marks_binding_failed(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """``installation.deleted`` lands the distinct ``deleted`` tag."""
    binding_id = await _seed_binding(
        org_slug="install-delete-handler", github_installation_id=99
    )
    payload = {
        "action": "deleted",
        "installation": {"id": 99, "account": {"login": "acme", "id": 999}},
        "repositories": [],
    }
    body, headers = _post_signed_event(event="installation", payload=payload)

    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)

    assert response.status_code == 200
    binding = await _read_binding(binding_id)
    assert binding is not None
    assert binding.last_sync_status == "failed"
    assert binding.last_sync_error == INSTALLATION_DELETED_REASON


@pytest.mark.asyncio
async def test_unsigned_rename_event_returns_401_no_writes(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """An unsigned rename event returns 401 and writes nothing."""
    binding_id = await _seed_binding(
        org_slug="unsigned-rename",
        github_owner="acme",
        github_repo="old-name",
        github_repo_id=12345,
        github_owner_id=999,
    )
    payload = {
        "action": "renamed",
        "changes": {"repository": {"name": {"from": "old-name"}}},
        "repository": {
            "id": 12345,
            "name": "new-name",
            "full_name": "acme/new-name",
            "owner": {"login": "acme", "id": 999},
        },
        "installation": {"id": 99},
    }
    body = json.dumps(payload).encode("utf-8")

    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "repository",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000030",
        },
    )

    assert response.status_code == 401
    binding = await _read_binding(binding_id)
    assert binding is not None
    assert binding.github_repo == "old-name"


@pytest.mark.asyncio
async def test_unrelated_repository_action_signed_no_writes(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed ``repository.created`` is a no-op (unsubscribed action)."""
    binding_id = await _seed_binding(
        org_slug="unrelated-action",
        github_owner="acme",
        github_repo="templates",
        github_repo_id=12345,
        github_owner_id=999,
    )
    payload = {
        "action": "created",
        "repository": {
            "id": 12345,
            "name": "templates",
            "owner": {"login": "acme", "id": 999},
        },
        "installation": {"id": 99},
    }
    body, headers = _post_signed_event(event="repository", payload=payload)

    response = await client.post(_WEBHOOK_PATH, content=body, headers=headers)

    assert response.status_code == 200
    binding = await _read_binding(binding_id)
    assert binding is not None
    assert binding.github_repo == "templates"


@pytest.mark.asyncio
async def test_rename_event_does_not_enqueue_dashboard_sync(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """All four event handlers respect "no syncs enqueued" — pin the contract.

    A regression where a rename or installation event accidentally
    enqueued a sync would cost real GitHub API calls; this test checks
    the queue counter across all four signed events.
    """
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    sync_jobs_before = count_jobs_by_name(arq_queue, "dashboard_sync")

    await _seed_binding(
        org_slug="no-sync-enqueue",
        github_owner="acme",
        github_repo="templates",
        github_repo_id=12345,
        github_owner_id=999,
        github_installation_id=99,
    )

    events: list[tuple[str, dict[str, Any], str]] = [
        (
            "repository",
            {
                "action": "renamed",
                "changes": {"repository": {"name": {"from": "templates"}}},
                "repository": {
                    "id": 12345,
                    "name": "new-templates",
                    "owner": {"login": "acme", "id": 999},
                },
                "installation": {"id": 99},
            },
            "00000000-0000-0000-0000-000000000040",
        ),
        (
            "repository",
            {
                "action": "transferred",
                "changes": {
                    "owner": {"from": {"user": {"login": "acme", "id": 999}}}
                },
                "repository": {
                    "id": 12345,
                    "name": "new-templates",
                    "owner": {"login": "other-org", "id": 1000},
                },
                "installation": {"id": 99},
            },
            "00000000-0000-0000-0000-000000000041",
        ),
        (
            "organization",
            {
                "action": "renamed",
                "changes": {"login": {"from": "other-org"}},
                "organization": {"id": 1000, "login": "renamed-org"},
                "installation": {"id": 99},
            },
            "00000000-0000-0000-0000-000000000042",
        ),
        (
            "installation",
            {
                "action": "suspend",
                "installation": {
                    "id": 99,
                    "account": {"login": "renamed-org", "id": 1000},
                },
                "repositories": [],
            },
            "00000000-0000-0000-0000-000000000043",
        ),
    ]
    for event, payload, delivery_id in events:
        body, headers = _post_signed_event(
            event=event, payload=payload, delivery_id=delivery_id
        )
        response = await client.post(
            _WEBHOOK_PATH, content=body, headers=headers
        )
        assert response.status_code == 200

    assert count_jobs_by_name(arq_queue, "dashboard_sync") == sync_jobs_before
