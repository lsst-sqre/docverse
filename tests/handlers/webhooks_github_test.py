"""Tests for the GitHub webhook handler."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import AsyncIterator, Mapping
from datetime import timedelta
from typing import Any

import gidgethub
import pytest
import pytest_asyncio
import structlog
from fastapi import FastAPI
from httpx import AsyncClient
from pydantic import SecretStr
from safir.arq import MockArqQueue
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import MockEventPublisher

from docverse.models import OrganizationCreate
from docverse_server.dependencies.context import context_dependency
from docverse_server.metrics import GitHubWebhookReceivedEvent, WebhookOutcome
from docverse_server.services.dashboard_templates import PushEventProcessor
from docverse_server.storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
)
from docverse_server.storage.organization_store import OrganizationStore
from tests.support.arq_testing import count_jobs_by_name
from tests.support.github_mock import GitHubMock

_WEBHOOK_PATH = "/docverse/webhooks/github"
_WEBHOOK_SECRET = "test-webhook-secret"


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


def _received_events() -> list[GitHubWebhookReceivedEvent]:
    """Return the ``github_webhook_received`` events recorded so far.

    The ``app`` fixture clears the mock publishers before each test, so
    this is exactly the current test's deliveries.
    """
    publisher = context_dependency.events.github_webhook_received
    assert isinstance(publisher, MockEventPublisher)
    return list(publisher.published)


def _assert_unattributed(event: GitHubWebhookReceivedEvent) -> None:
    """Assert the invariants every recorded delivery shares today.

    ``elapsed`` is measured on every path, and the reserved
    ``organization`` and ``project`` are not yet resolved on any.
    """
    assert event.elapsed > timedelta(0)
    assert event.organization is None
    assert event.project is None


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
    ref: str = "main",
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
async def test_post_signed_push_matches_bare_branch_binding(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed push with ``refs/heads/main`` matches a bare ``main`` binding.

    Reproduces DM-54689: GitHub push payloads always carry the
    fully-qualified ``refs/heads/<branch>`` form, but operators register
    bindings with the bare branch name. The handler must enqueue exactly
    one ``dashboard_sync`` for this asymmetric pair.
    """
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    before = count_jobs_by_name(arq_queue, "dashboard_sync")
    await _seed_binding(ref="main", root_path="/")
    payload = _push_payload(
        ref="refs/heads/main",
        changed_files=["templates/blue/dashboard.html.jinja"],
    )
    body = json.dumps(payload).encode("utf-8")

    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000004",
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )
    assert response.status_code == 200
    after = count_jobs_by_name(arq_queue, "dashboard_sync")
    assert after - before == 1


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


@pytest.mark.asyncio
async def test_delivery_records_anonymous_api_request(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A delivery is one ``api_request`` with no caller and no org.

    GitHub posts without passing through Gafaelfawr, so the event is
    unauthenticated, and the webhook route names no organization or
    project for the event to be sliced by.
    """
    publisher = context_dependency.events.api_request
    assert isinstance(publisher, MockEventPublisher)
    publisher.published.clear()
    body = json.dumps({"zen": "Speak like a human."}).encode("utf-8")

    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "ping",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000002",
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )

    assert response.status_code == 200
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.method == "POST"
    assert event.route == "/webhooks/github"
    assert event.status_code == 200
    assert event.authenticated is False
    assert event.organization is None
    assert event.project is None


@pytest.mark.asyncio
async def test_unconfigured_delivery_records_not_configured(
    client: AsyncClient,
) -> None:
    """A delivery to an unconfigured deployment is one ``not_configured``.

    The header is unverifiable without a webhook secret, so the event
    names no event type, and the 404 is unchanged.
    """
    response = await client.post(
        _WEBHOOK_PATH,
        content=b"{}",
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000010",
        },
    )

    assert response.status_code == 404
    events = _received_events()
    assert len(events) == 1
    event = events[0]
    assert event.outcome == WebhookOutcome.not_configured
    assert event.event_type is None
    assert event.jobs_enqueued == 0
    assert event.github_repository is None
    _assert_unattributed(event)


@pytest.mark.asyncio
async def test_bad_signature_records_invalid_signature(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A delivery whose HMAC does not verify is one ``invalid_signature``.

    Neither the header nor the payload of an unverified delivery is
    trusted, so the event names no event type and no repository, and
    the 401 is unchanged.
    """
    body = json.dumps(_push_payload()).encode("utf-8")

    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000011",
            "X-Hub-Signature-256": _sign("wrong-secret", body),
        },
    )

    assert response.status_code == 401
    events = _received_events()
    assert len(events) == 1
    event = events[0]
    assert event.outcome == WebhookOutcome.invalid_signature
    assert event.event_type is None
    assert event.jobs_enqueued == 0
    assert event.github_repository is None
    _assert_unattributed(event)


@pytest.mark.asyncio
async def test_unsubscribed_event_records_ignored(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed ``ping`` no callback subscribes to is one ``ignored``.

    The delivery is still answered 200 so GitHub does not redeliver it,
    and a ``ping`` names no repository.
    """
    body = json.dumps({"zen": "Speak like a human."}).encode("utf-8")

    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "ping",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000012",
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )

    assert response.status_code == 200
    events = _received_events()
    assert len(events) == 1
    event = events[0]
    assert event.outcome == WebhookOutcome.ignored
    assert event.event_type == "ping"
    assert event.jobs_enqueued == 0
    assert event.github_repository is None
    _assert_unattributed(event)


@pytest.mark.asyncio
async def test_bound_push_records_dispatched_with_jobs_enqueued(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed push matching a binding is one ``dispatched`` event.

    ``jobs_enqueued`` is the number of ``dashboard_sync`` jobs the push
    landed on the queue, and ``github_repository`` is the payload's
    ``owner/repo``.
    """
    arq_queue = arq_dependency._arq_queue
    assert isinstance(arq_queue, MockArqQueue)
    before = count_jobs_by_name(arq_queue, "dashboard_sync")
    await _seed_binding(root_path="/")
    body = json.dumps(
        _push_payload(changed_files=["templates/blue/dashboard.html.jinja"])
    ).encode("utf-8")

    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000013",
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )

    assert response.status_code == 200
    enqueued = count_jobs_by_name(arq_queue, "dashboard_sync") - before
    assert enqueued == 1
    events = _received_events()
    assert len(events) == 1
    event = events[0]
    assert event.outcome == WebhookOutcome.dispatched
    assert event.event_type == "push"
    assert event.jobs_enqueued == enqueued
    assert event.github_repository == "acme/templates"
    _assert_unattributed(event)


@pytest.mark.asyncio
async def test_failing_callback_records_error(
    client: AsyncClient,
    github_app_enabled: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A callback that raises is one ``error`` event, then re-raises.

    The exception still escapes the handler unchanged, so the 500 and
    Sentry's capture of it are what they were before the event existed.
    In this ASGI-transport rig the uncaught exception bubbles up through
    ``httpx`` rather than becoming a response, hence ``pytest.raises``.
    """

    async def _fail(
        self: PushEventProcessor, payload: Mapping[str, Any]
    ) -> None:
        _ = (self, payload)
        msg = "push processing failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(PushEventProcessor, "process", _fail)
    body = json.dumps(_push_payload()).encode("utf-8")

    with pytest.raises(RuntimeError, match="push processing failed"):
        await client.post(
            _WEBHOOK_PATH,
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-GitHub-Event": "push",
                "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000014",
                "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
            },
        )

    events = _received_events()
    assert len(events) == 1
    event = events[0]
    assert event.outcome == WebhookOutcome.error
    assert event.event_type == "push"
    assert event.jobs_enqueued == 0
    assert event.github_repository == "acme/templates"
    _assert_unattributed(event)


@pytest.mark.asyncio
async def test_unparseable_delivery_records_error(
    client: AsyncClient,
    github_app_enabled: None,
) -> None:
    """A signed delivery gidgethub cannot parse is one ``error`` event.

    A content type other than JSON or a form is refused by gidgethub
    after the signature verifies. The exception still escapes as before;
    with no event parsed, the event names no event type or repository.
    """
    body = json.dumps(_push_payload()).encode("utf-8")

    with pytest.raises(gidgethub.BadRequest):
        await client.post(
            _WEBHOOK_PATH,
            content=body,
            headers={
                "Content-Type": "text/plain",
                "X-GitHub-Event": "push",
                "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000015",
                "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
            },
        )

    events = _received_events()
    assert len(events) == 1
    event = events[0]
    assert event.outcome == WebhookOutcome.error
    assert event.event_type is None
    assert event.jobs_enqueued == 0
    assert event.github_repository is None
    _assert_unattributed(event)


@pytest.mark.asyncio
async def test_publish_failure_does_not_change_the_response(
    client: AsyncClient,
    github_app_enabled: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A metrics backend that raises never fails a delivery.

    The event is best-effort: its publish error is logged and swallowed,
    so the delivery is answered exactly as it would be without metrics.
    """
    publisher = context_dependency.events.github_webhook_received

    async def _fail(payload: GitHubWebhookReceivedEvent) -> None:
        _ = payload
        msg = "metrics backend down"
        raise RuntimeError(msg)

    monkeypatch.setattr(publisher, "publish", _fail)
    body = json.dumps({"zen": "Speak like a human."}).encode("utf-8")

    response = await client.post(
        _WEBHOOK_PATH,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "ping",
            "X-GitHub-Delivery": "00000000-0000-0000-0000-000000000016",
            "X-Hub-Signature-256": _sign(_WEBHOOK_SECRET, body),
        },
    )

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
