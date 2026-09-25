"""Tests for the ``api_request`` metrics middleware.

The request-level tests run through the real application, so they pin
what a deployment actually records: the route template the router
matched, the status the handler answered with, and whether the
Gafaelfawr ingress vouched for the caller. The unit tests drive the
middleware over a throw-away app to reach the corners the real routes
cannot, such as a route template that still carries the path prefix.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from safir.metrics import MockEventPublisher
from starlette.types import Receive, Scope, Send
from structlog.testing import capture_logs

from docverse_server.dependencies.context import (
    ContextDependency,
    context_dependency,
)
from docverse_server.metrics import (
    ApiRequestEvent,
    DocverseEvents,
    HttpStatusClass,
)
from docverse_server.middleware import ApiRequestMiddleware
from tests.conftest import seed_org_with_admin


def _api_request_publisher() -> MockEventPublisher[ApiRequestEvent]:
    publisher = context_dependency.events.api_request
    assert isinstance(publisher, MockEventPublisher)
    return publisher


@pytest.mark.asyncio
async def test_authorized_route_records_one_event(
    client: AsyncClient,
) -> None:
    """A routed request records its template, status, and timing.

    ``route`` is the template the router matched with the application's
    path prefix removed, never the concrete path the caller sent, so the
    tag's cardinality is the size of the API rather than of its traffic.
    """
    await seed_org_with_admin(client, "api-req-org", "testuser")
    publisher = _api_request_publisher()
    publisher.published.clear()

    response = await client.get(
        "/docverse/orgs/api-req-org",
        headers={"X-Auth-Request-User": "testuser"},
    )

    assert response.status_code == 200
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.method == "GET"
    assert event.route == "/orgs/{org}"
    assert event.status_code == 200
    assert event.status_class == HttpStatusClass.successful
    assert event.duration > timedelta(0)
    assert event.authenticated is True


@pytest.mark.asyncio
async def test_request_without_auth_header_is_unauthenticated(
    client: AsyncClient,
) -> None:
    """A request Gafaelfawr did not vouch for records ``False``.

    The event still carries whatever status the route answered with, so
    refused anonymous traffic is visible as its own slice of volume.
    """
    await seed_org_with_admin(client, "api-req-anon-org", "testuser")
    publisher = _api_request_publisher()
    publisher.published.clear()

    response = await client.get("/docverse/orgs/api-req-anon-org")

    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.authenticated is False
    assert event.route == "/orgs/{org}"
    assert event.status_code == response.status_code
    assert event.status_class == HttpStatusClass.from_status_code(
        response.status_code
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/", "/health"])
async def test_probe_endpoints_record_nothing(
    client: AsyncClient, path: str
) -> None:
    """The probe endpoints are answered but never recorded.

    Kubernetes polls both from outside the Gafaelfawr ingress; counting
    them would bury the API's own traffic under the probes'.
    """
    publisher = _api_request_publisher()

    response = await client.get(path)

    assert response.status_code == 200
    assert publisher.published == []


@pytest.mark.asyncio
async def test_publish_failure_leaves_response_unchanged(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A metrics failure is logged and never reaches the caller.

    The publish runs after the handler has answered, so the response the
    caller reads is the one the handler produced, and the failure shows
    up only as an error log line.
    """
    await seed_org_with_admin(client, "api-req-fail-org", "testuser")
    headers = {"X-Auth-Request-User": "testuser"}
    expected = await client.get(
        "/docverse/orgs/api-req-fail-org", headers=headers
    )
    publisher = _api_request_publisher()
    monkeypatch.setattr(
        publisher, "publish", AsyncMock(side_effect=RuntimeError("kafka down"))
    )

    with capture_logs() as captured:
        response = await client.get(
            "/docverse/orgs/api-req-fail-org", headers=headers
        )

    assert response.status_code == expected.status_code
    assert response.json() == expected.json()
    errors = [
        entry
        for entry in captured
        if entry["event"] == "Failed to publish api_request metrics event"
    ]
    assert len(errors) == 1
    assert errors[0]["log_level"] == "error"
    assert errors[0]["route"] == "/orgs/{org}"


@pytest.mark.asyncio
async def test_conditional_request_records_both_events(
    client: AsyncClient,
) -> None:
    """A conditional GET is counted by both events, each once.

    ``conditional_get`` answers a different question (how often the
    caller's validator matched), so the middleware leaves it alone: the
    304 is one ``conditional_get`` and one ``api_request`` in ``3xx``.
    """
    await seed_org_with_admin(client, "api-req-cg-org", "testuser")
    headers = {"X-Auth-Request-User": "testuser"}
    first = await client.get("/docverse/orgs/api-req-cg-org", headers=headers)
    api_requests = _api_request_publisher()
    api_requests.published.clear()
    conditional_gets = context_dependency.events.conditional_get
    assert isinstance(conditional_gets, MockEventPublisher)
    conditional_gets.published.clear()

    response = await client.get(
        "/docverse/orgs/api-req-cg-org",
        headers={**headers, "If-None-Match": first.headers["ETag"]},
    )

    assert response.status_code == 304
    assert len(conditional_gets.published) == 1
    assert len(api_requests.published) == 1
    event = api_requests.published[0]
    assert event.route == "/orgs/{org}"
    assert event.status_code == 304
    assert event.status_class == HttpStatusClass.redirection


def _prefixed_app(
    events: Callable[[], DocverseEvents],
) -> ApiRequestMiddleware:
    """Wrap an app whose route template still carries the path prefix.

    Declared on the application itself rather than through a prefixed
    ``include_router``, so the router records ``/docverse/orgs/{org}``
    as the matched template.
    """
    app = FastAPI()

    @app.get("/docverse/orgs/{org}")
    async def get_org(org: str) -> dict[str, str]:
        return {"org": org}

    return ApiRequestMiddleware(app, events=events, path_prefix="/docverse")


@pytest.mark.asyncio
async def test_route_template_loses_path_prefix(
    mock_events: DocverseEvents,
) -> None:
    """A template declared with the prefix is recorded without it.

    Every deployment's routes then share one vocabulary whatever prefix
    the deployment mounts the API under.
    """
    app = _prefixed_app(lambda: mock_events)
    publisher = mock_events.api_request
    assert isinstance(publisher, MockEventPublisher)

    async with AsyncClient(
        base_url="https://example.com/", transport=ASGITransport(app=app)
    ) as client:
        response = await client.get("/docverse/orgs/rubin")

    assert response.status_code == 200
    assert [event.route for event in publisher.published] == ["/orgs/{org}"]


@pytest.mark.asyncio
async def test_uninitialized_events_leave_response_unchanged() -> None:
    """A request before the lifespan registered publishers still succeeds.

    The events are looked up per request, so the failure to find them is
    a publish failure like any other: logged, never raised.
    """

    def uninitialized() -> DocverseEvents:
        return ContextDependency().events

    app = _prefixed_app(uninitialized)

    with capture_logs() as captured:
        async with AsyncClient(
            base_url="https://example.com/",
            transport=ASGITransport(app=app),
        ) as client:
            response = await client.get("/docverse/orgs/rubin")

    assert response.status_code == 200
    assert response.json() == {"org": "rubin"}
    assert [entry["event"] for entry in captured] == [
        "Failed to publish api_request metrics event"
    ]


@pytest.mark.asyncio
async def test_non_http_scopes_pass_through(
    mock_events: DocverseEvents,
) -> None:
    """Lifespan and websocket scopes reach the app untouched and unrecorded."""
    seen: list[Scope] = []

    async def inner(scope: Scope, receive: Receive, send: Send) -> None:
        seen.append(scope)

    middleware = ApiRequestMiddleware(
        inner, events=lambda: mock_events, path_prefix="/docverse"
    )
    publisher = mock_events.api_request
    assert isinstance(publisher, MockEventPublisher)
    lifespan: Scope = {"type": "lifespan", "asgi": {"version": "3.0"}}
    websocket: Scope = {"type": "websocket", "path": "/docverse/ws"}

    for scope in (lifespan, websocket):
        await middleware(scope, AsyncMock(), AsyncMock())

    assert len(seen) == 2
    assert seen[0] is lifespan
    assert seen[1] is websocket
    assert publisher.published == []
