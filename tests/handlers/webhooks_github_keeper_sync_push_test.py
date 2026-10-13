"""Handler-level tests for the keeper-sync step of a ``push`` delivery.

A signed ``push`` runs the dashboard-template processor and then, in a
transaction of its own, the keeper-sync push processor, which stamps the
pushed ref onto every LTD-synced project bound to the repository
(PRD #803). These tests post signed deliveries end to end and read the
stamps back from ``keeper_sync_state``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio
import sentry_sdk
import structlog
from fastapi import FastAPI
from httpx import AsyncClient
from pydantic import SecretStr
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher

from docverse.models import KeeperSyncConfig, OrganizationCreate, ProjectCreate
from docverse.models.projects import ProjectGitHubBindingCreate
from docverse_server.config import config
from docverse_server.dependencies.context import context_dependency
from docverse_server.metrics import GitHubWebhookReceivedEvent, WebhookOutcome
from docverse_server.services.keeper_sync.push_hints import (
    ANNOTATION_GITHUB_PUSHED_REFS,
)
from docverse_server.services.keeper_sync_push_processor import (
    KeeperSyncPushProcessor,
)
from docverse_server.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
)
from docverse_server.storage.keeper_sync import (
    KeeperSyncState,
    KeeperSyncStateStore,
    ResourceType,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from tests.support.arq_testing import count_jobs_by_name
from tests.support.github_mock import GitHubMock

_WEBHOOK_PATH = "/docverse/webhooks/github"
_WEBHOOK_SECRET = "test-webhook-secret"
_OWNER = "lsst-sqre"
_REPO = "sqr-112"
_REPO_ID = 4242


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


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


async def _seed_synced_project(
    *,
    org_slug: str,
    project_slug: str = _REPO,
    sync: KeeperSyncConfig | None = None,
) -> int:
    """Seed an org syncing from LTD and a bound project with a state row.

    Returns the organization's id.
    """
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=_logger())
            org = await org_store.create(
                OrganizationCreate(
                    slug=org_slug,
                    title=f"Org {org_slug}",
                    base_domain=f"{org_slug}.example.com",
                )
            )
            await org_store.update_keeper_sync_config(
                org_slug,
                sync or KeeperSyncConfig(enabled=True, project_slugs="*"),
            )
            project_store = ProjectStore(session=session, logger=_logger())
            project = await project_store.create(
                org_id=org.id,
                data=ProjectCreate(
                    slug=project_slug,
                    title=f"Project {project_slug}",
                    github=ProjectGitHubBindingCreate(
                        owner=_OWNER, repo=_REPO
                    ),
                ),
                github_owner=_OWNER,
                github_repo=_REPO,
            )
            await project_store.apply_installation_scope(
                installation_id=99,
                owner=_OWNER,
                owner_id=999,
                repo=_REPO,
                repo_id=_REPO_ID,
            )
            await KeeperSyncStateStore(
                session=session, logger=_logger()
            ).upsert(
                org_id=org.id,
                resource_type=ResourceType.project,
                ltd_slug=project_slug,
                docverse_id=project.id,
            )
            await session.commit()
        return org.id
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


async def _seed_dashboard_binding(*, org_slug: str) -> None:
    """Pin a whole-repo dashboard-template binding to the pushed branch."""
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=_logger())
            org = await org_store.get_by_slug(org_slug)
            assert org is not None
            await DashboardGitHubTemplateBindingStore(
                session=session, logger=_logger()
            ).create(
                DashboardGitHubTemplateBindingCreate(
                    org_id=org.id,
                    project_id=None,
                    github_owner=_OWNER,
                    github_repo=_REPO,
                    github_ref="tickets/DM-1",
                    root_path="/",
                )
            )
            await session.commit()
        return
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


async def _project_state(
    *, org_id: int, ltd_slug: str = _REPO
) -> KeeperSyncState | None:
    async for session in db_session_dependency():
        async with session.begin():
            return await KeeperSyncStateStore(
                session=session, logger=_logger()
            ).get(
                org_id=org_id,
                resource_type=ResourceType.project,
                ltd_slug=ltd_slug,
            )
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


async def _pushed_refs(*, org_id: int) -> dict[str, Any] | None:
    state = await _project_state(org_id=org_id)
    assert state is not None
    if state.annotations is None:
        return None
    return state.annotations.get(ANNOTATION_GITHUB_PUSHED_REFS)


def _push_payload(
    *,
    ref: str = "refs/heads/tickets/DM-1",
    deleted: bool = False,
    commits: list[dict[str, Any]] | None = None,
    size: int = 1,
) -> dict[str, Any]:
    if commits is None:
        commits = [
            {
                "id": "after-sha",
                "modified": ["index.rst"],
                "added": [],
                "removed": [],
            }
        ]
    return {
        "ref": ref,
        "before": "before-sha",
        "after": "after-sha",
        "deleted": deleted,
        "repository": {
            "id": _REPO_ID,
            "name": _REPO,
            "full_name": f"{_OWNER}/{_REPO}",
            "owner": {"login": _OWNER, "name": _OWNER},
        },
        "installation": {"id": 99},
        "size": size,
        "commits": commits,
    }


async def _post_push(
    client: AsyncClient,
    payload: dict[str, Any],
    *,
    delivery_id: str = "00000000-0000-0000-0000-000000000803",
) -> int:
    body = json.dumps(payload).encode("utf-8")
    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": delivery_id,
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )
    return response.status_code


def _received_event() -> GitHubWebhookReceivedEvent:
    """Return the one ``github_webhook_received`` event this test made."""
    publisher = context_dependency.events.github_webhook_received
    assert isinstance(publisher, MockEventPublisher)
    events: list[GitHubWebhookReceivedEvent] = list(publisher.published)
    assert len(events) == 1
    return events[0]


@pytest.mark.asyncio
async def test_signed_push_stamps_synced_project(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A branch push stamps the bound project's state row with its time.

    The stamp is the delivery's processing time, and the delivery's
    ``github_webhook_received`` event counts the project stamped.
    """
    org_id = await _seed_synced_project(org_slug="ks-push-e2e")

    before = datetime.now(tz=UTC)
    assert await _post_push(client, _push_payload()) == 200
    after = datetime.now(tz=UTC)

    refs = await _pushed_refs(org_id=org_id)
    assert refs is not None
    assert set(refs) == {"tickets/DM-1"}
    assert before <= datetime.fromisoformat(refs["tickets/DM-1"]) <= after
    event = _received_event()
    assert event.outcome == WebhookOutcome.dispatched
    assert event.projects_stamped == 1


@pytest.mark.asyncio
async def test_second_push_overwrites_the_stamp(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A second push to the same ref moves the ref's time forward."""
    org_id = await _seed_synced_project(org_slug="ks-push-e2e-repeat")

    assert await _post_push(client, _push_payload()) == 200
    first = await _pushed_refs(org_id=org_id)
    assert first is not None
    assert await _post_push(client, _push_payload()) == 200
    second = await _pushed_refs(org_id=org_id)
    assert second is not None

    assert set(second) == {"tickets/DM-1"}
    assert datetime.fromisoformat(
        second["tickets/DM-1"]
    ) > datetime.fromisoformat(first["tickets/DM-1"])


@pytest.mark.asyncio
async def test_tag_push_stamps_the_tag(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A tag push stamps the bare tag name."""
    org_id = await _seed_synced_project(org_slug="ks-push-e2e-tag")

    assert await _post_push(client, _push_payload(ref="refs/tags/v1.0")) == 200

    refs = await _pushed_refs(org_id=org_id)
    assert refs is not None
    assert set(refs) == {"v1.0"}
    assert _received_event().projects_stamped == 1


@pytest.mark.asyncio
async def test_deleted_push_stamps_nothing(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A push that deleted its branch answers 200 and stamps nothing."""
    org_id = await _seed_synced_project(org_slug="ks-push-e2e-deleted")

    assert await _post_push(client, _push_payload(deleted=True)) == 200

    assert await _pushed_refs(org_id=org_id) is None
    event = _received_event()
    assert event.outcome == WebhookOutcome.dispatched
    assert event.projects_stamped == 0


@pytest.mark.asyncio
async def test_push_to_unsynced_repository_stamps_nothing(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A push to a repository with no synced project answers 200."""
    assert await _post_push(client, _push_payload()) == 200

    event = _received_event()
    assert event.outcome == WebhookOutcome.dispatched
    assert event.projects_stamped == 0


@pytest.mark.asyncio
async def test_out_of_scope_project_is_not_stamped(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A project its org's scope excludes is not stamped."""
    org_id = await _seed_synced_project(
        org_slug="ks-push-e2e-scope",
        sync=KeeperSyncConfig(
            enabled=True, project_slugs="*", exclude_project_slugs=[_REPO]
        ),
    )

    assert await _post_push(client, _push_payload()) == 200

    assert await _pushed_refs(org_id=org_id) is None
    assert _received_event().projects_stamped == 0


@pytest.mark.asyncio
async def test_disabled_hot_path_stamps_nothing(
    client: AsyncClient,
    github_app_enabled: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With ``keeper_sync_push_hot_path_enabled`` off, nothing is stamped."""
    monkeypatch.setattr(config, "keeper_sync_push_hot_path_enabled", False)
    org_id = await _seed_synced_project(org_slug="ks-push-e2e-off")

    assert await _post_push(client, _push_payload()) == 200

    assert await _pushed_refs(org_id=org_id) is None
    assert _received_event().projects_stamped == 0


@pytest.mark.asyncio
async def test_keeper_sync_failure_keeps_dashboard_work(
    client: AsyncClient,
    github_app_enabled: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing keeper-sync step still answers 200 for the dashboard work.

    The same delivery's ``dashboard_sync`` is enqueued, the failure
    reaches Sentry, and the delivery is recorded ``dispatched`` with no
    ``projects_stamped`` count.
    """

    async def _fail(
        self: KeeperSyncPushProcessor,
        payload: Mapping[str, Any],
        **kwargs: Any,
    ) -> None:
        _ = (self, payload, kwargs)
        msg = "keeper-sync stamp failed"
        raise RuntimeError(msg)

    captured: list[BaseException] = []
    monkeypatch.setattr(KeeperSyncPushProcessor, "process", _fail)
    monkeypatch.setattr(sentry_sdk, "capture_exception", captured.append)
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    before = count_jobs_by_name(arq_queue, "dashboard_sync")
    await _seed_synced_project(org_slug="ks-push-e2e-fail")
    await _seed_dashboard_binding(org_slug="ks-push-e2e-fail")

    assert await _post_push(client, _push_payload()) == 200

    assert count_jobs_by_name(arq_queue, "dashboard_sync") - before == 1
    assert len(captured) == 1
    assert str(captured[0]) == "keeper-sync stamp failed"
    event = _received_event()
    assert event.outcome == WebhookOutcome.dispatched
    assert event.jobs_enqueued == 1
    assert event.projects_stamped is None


@pytest.mark.asyncio
async def test_keeper_sync_step_never_calls_compare(
    client: AsyncClient,
    github_app_enabled: None,
    mock_github: GitHubMock,
) -> None:
    """A truncated push stamps without asking GitHub's compare API.

    The dashboard processor falls back to the compare API for a
    truncated push only when a binding matches; with no binding, any
    compare call would have come from the keeper-sync step.
    """
    org_id = await _seed_synced_project(org_slug="ks-push-e2e-compare")
    payload = _push_payload(size=30)

    assert await _post_push(client, payload) == 200

    refs = await _pushed_refs(org_id=org_id)
    assert refs is not None
    assert set(refs) == {"tickets/DM-1"}
    compare_calls = [
        call
        for call in mock_github.router.calls
        if "/compare/" in call.request.url.path
    ]
    assert compare_calls == []


@pytest.mark.asyncio
async def test_non_push_delivery_has_no_stamp_count(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """Only a push carries ``projects_stamped``; a ``ping`` leaves it null."""
    body = json.dumps({"zen": "Keep it logically awesome."}).encode("utf-8")
    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "ping",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000804",
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )

    assert response.status_code == 200
    assert _received_event().projects_stamped is None
