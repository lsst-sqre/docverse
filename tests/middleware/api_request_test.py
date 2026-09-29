"""Tests for the ``api_request`` metrics middleware.

The request-level tests run through the real application, so they pin
what a deployment actually records: the route template the router
matched, the status the handler answered with, and whether the
Gafaelfawr ingress vouched for the caller. The unit tests drive the
middleware over a throw-away app to reach the corners the real routes
cannot, such as a route template that still carries the path prefix or
a handler exception escaping to the server-error layer.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, MutableMapping
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, Request, Response
from fastapi.responses import PlainTextResponse, StreamingResponse
from httpx import ASGITransport, AsyncClient
from safir.metrics import MockEventPublisher
from starlette.types import Message, Receive, Scope, Send
from structlog.testing import capture_logs

from docverse_server.dependencies.context import (
    ContextDependency,
    context_dependency,
)
from docverse_server.metrics import (
    ApiRequestEvent,
    DocverseEvents,
    HttpMethod,
    HttpStatusClass,
)
from docverse_server.middleware import ApiRequestMiddleware
from docverse_server.middleware import api_request as api_request_module
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
async def test_project_route_records_org_and_project(
    client: AsyncClient,
) -> None:
    """A route addressing a project records both of its path params.

    The slugs come from the path parameters the router matched, so the
    event can be sliced by organization and project wherever the route
    names them.
    """
    await seed_org_with_admin(client, "api-req-proj-org", "testuser")
    headers = {"X-Auth-Request-User": "testuser"}
    created = await client.post(
        "/docverse/orgs/api-req-proj-org/projects",
        json={
            "slug": "api-req-proj",
            "title": "API request project",
            "source_url": "https://example.com/example/api-req-proj",
        },
        headers=headers,
    )
    assert created.status_code == 201
    publisher = _api_request_publisher()
    publisher.published.clear()

    response = await client.get(
        "/docverse/orgs/api-req-proj-org/projects/api-req-proj",
        headers=headers,
    )

    assert response.status_code == 200
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.route == "/orgs/{org}/projects/{project}"
    assert event.organization == "api-req-proj-org"
    assert event.project == "api-req-proj"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "route", "username"),
    [
        ("/docverse/orgs", "/orgs", "testuser"),
        ("/docverse/admin/orgs", "/admin/orgs", "superadmin"),
    ],
)
async def test_route_without_org_records_neither(
    client: AsyncClient, path: str, route: str, username: str
) -> None:
    """A route that names no organization records neither dimension.

    Listing and admin routes address the whole deployment, so the event
    carries ``None`` rather than guessing a slug from the caller.
    """
    await seed_org_with_admin(client, "api-req-none-org", "testuser")
    publisher = _api_request_publisher()
    publisher.published.clear()

    response = await client.get(
        path, headers={"X-Auth-Request-User": username}
    )

    assert response.status_code == 200
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.route == route
    assert event.organization is None
    assert event.project is None


@pytest.mark.asyncio
async def test_unmatched_path_records_no_route(client: AsyncClient) -> None:
    """A path no route matches is still counted, as a routeless ``4xx``.

    Scanner probes then stay visible as client-error volume, while the
    event carries no trace of the path they tried, so they add nothing to
    any tag's cardinality.
    """
    publisher = _api_request_publisher()

    response = await client.get("/docverse/no/such/path")

    assert response.status_code == 404
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.route is None
    assert event.status_code == 404
    assert event.status_class == HttpStatusClass.client_error
    assert event.organization is None
    assert event.project is None


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


class _HandlerFailureError(Exception):
    """The unhandled error a test route raises past the middleware."""


def _failing_app(
    events: Callable[[], DocverseEvents],
    *,
    error: Exception,
    reached: list[Exception],
    after_start: Callable[[], None] | None = None,
) -> FastAPI:
    """Build an app whose one route raises ``error`` unhandled.

    The middleware is installed with ``add_middleware``, as ``main.py``
    does, so it sits inside Starlette's ``ServerErrorMiddleware`` exactly
    as in a deployment. That outer layer's handler appends every
    exception it receives to ``reached``, and answers ``500`` when no
    response has started yet.

    By default the handler raises before any response starts. With
    ``after_start``, it instead answers ``200`` with a streaming body
    that sends one chunk, calls ``after_start``, and then raises, so the
    exception escapes after ``http.response.start`` has gone out.
    """
    app = FastAPI()

    @app.get("/orgs/{org}")
    async def get_org(org: str) -> Response:
        if after_start is None:
            raise error
        hook = after_start

        async def body() -> AsyncIterator[bytes]:
            yield b"partial"
            hook()
            raise error

        return StreamingResponse(body(), media_type="text/plain")

    async def server_error(request: Request, exc: Exception) -> Response:
        reached.append(exc)
        return PlainTextResponse("Internal Server Error", status_code=500)

    app.add_exception_handler(Exception, server_error)
    app.add_middleware(
        ApiRequestMiddleware, events=events, path_prefix="/docverse"
    )
    return app


@pytest.mark.asyncio
async def test_exception_before_response_start_records_5xx_and_propagates(
    mock_events: DocverseEvents,
) -> None:
    """An exception before any response starts is recorded as a ``500``.

    No status line has gone out, so the middleware records the ``500``
    the server-error layer is about to send, then re-raises the very
    same exception: the error handler, Sentry's capture, and the
    caller's ``500`` are exactly what they would be without the
    middleware.
    """
    error = _HandlerFailureError("handler failed")
    reached: list[Exception] = []
    app = _failing_app(lambda: mock_events, error=error, reached=reached)
    publisher = mock_events.api_request
    assert isinstance(publisher, MockEventPublisher)

    async with AsyncClient(
        base_url="https://example.com/",
        transport=ASGITransport(app=app, raise_app_exceptions=False),
    ) as client:
        response = await client.get("/orgs/rubin")

    assert response.status_code == 500
    assert len(reached) == 1
    assert reached[0] is error
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.route == "/orgs/{org}"
    assert event.status_code == 500
    assert event.status_class == HttpStatusClass.server_error
    assert event.organization == "rubin"


def _above_debug(
    captured: list[MutableMapping[str, Any]],
) -> list[MutableMapping[str, Any]]:
    """Drop debug entries, such as the mock publisher's own log lines."""
    return [entry for entry in captured if entry["log_level"] != "debug"]


class _FakeClock:
    """A monotonic clock the test advances by hand."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


@pytest.mark.asyncio
async def test_exception_after_response_start_records_sent_status(
    mock_events: DocverseEvents,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exception escaping mid-body records the response that started.

    The caller has already received the ``200`` status line (with a
    truncated body), and the server-error layer cannot replace a started
    response, so recording a ``500`` would count a server error no
    client ever saw. The event keeps the status and the time to that
    status line, while the exception still propagates unchanged.
    """
    clock = _FakeClock()
    monkeypatch.setattr(api_request_module, "time", clock)

    def advance_clock() -> None:
        clock.now += 60.0

    error = _HandlerFailureError("body failed")
    reached: list[Exception] = []
    app = _failing_app(
        lambda: mock_events,
        error=error,
        reached=reached,
        after_start=advance_clock,
    )
    publisher = mock_events.api_request
    assert isinstance(publisher, MockEventPublisher)

    async with AsyncClient(
        base_url="https://example.com/",
        transport=ASGITransport(app=app, raise_app_exceptions=False),
    ) as client:
        response = await client.get("/orgs/rubin")

    assert response.status_code == 200
    assert len(reached) == 1
    assert reached[0] is error
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.route == "/orgs/{org}"
    assert event.status_code == 200
    assert event.status_class == HttpStatusClass.successful
    assert event.duration == timedelta(0)


@pytest.mark.asyncio
async def test_exception_after_response_start_logs_truncated_response(
    mock_events: DocverseEvents,
) -> None:
    """A response that failed mid-body is flagged in its own log line.

    The event records the status the caller received, so it looks like
    any other ``200``; the warning is what tells an operator that this
    one's body was cut short. It names the route and status but, like
    the event, never the concrete path.
    """
    error = _HandlerFailureError("body failed")
    reached: list[Exception] = []
    app = _failing_app(
        lambda: mock_events,
        error=error,
        reached=reached,
        after_start=lambda: None,
    )

    with capture_logs() as captured:
        async with AsyncClient(
            base_url="https://example.com/",
            transport=ASGITransport(app=app, raise_app_exceptions=False),
        ) as client:
            await client.get("/orgs/rubin")

    assert _above_debug(captured) == [
        {
            "event": "API response failed after it started",
            "log_level": "warning",
            "route": "/orgs/{org}",
            "status_code": 200,
        }
    ]


@pytest.mark.asyncio
async def test_exception_before_response_start_logs_nothing(
    mock_events: DocverseEvents,
) -> None:
    """An exception before any response starts adds no log line.

    The server-error layer answers ``500`` and the event records it, so
    there is no truncated response to flag.
    """
    error = _HandlerFailureError("handler failed")
    reached: list[Exception] = []
    app = _failing_app(lambda: mock_events, error=error, reached=reached)

    with capture_logs() as captured:
        async with AsyncClient(
            base_url="https://example.com/",
            transport=ASGITransport(app=app, raise_app_exceptions=False),
        ) as client:
            response = await client.get("/orgs/rubin")

    assert response.status_code == 500
    assert _above_debug(captured) == []


@pytest.mark.asyncio
async def test_publish_failure_does_not_replace_handler_exception(
    mock_events: DocverseEvents,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A metrics failure while recording a ``500`` leaves the error intact.

    The publish runs while the handler's exception is in flight, so a
    publish error that escaped would replace it; instead the server-error
    layer still receives the handler's own exception.
    """
    error = _HandlerFailureError("handler failed")
    reached: list[Exception] = []
    app = _failing_app(lambda: mock_events, error=error, reached=reached)
    monkeypatch.setattr(
        mock_events.api_request,
        "publish",
        AsyncMock(side_effect=RuntimeError("kafka down")),
    )

    with capture_logs() as captured:
        async with AsyncClient(
            base_url="https://example.com/",
            transport=ASGITransport(app=app, raise_app_exceptions=False),
        ) as client:
            response = await client.get("/orgs/rubin")

    assert response.status_code == 500
    assert len(reached) == 1
    assert reached[0] is error
    assert [entry["event"] for entry in captured] == [
        "Failed to publish api_request metrics event"
    ]


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


async def _send_raw(
    app: ApiRequestMiddleware, *, method: str, path: str
) -> list[Message]:
    """Drive one request straight through the ASGI interface.

    An HTTP client may refuse to put a junk token on the request line,
    but an ASGI server hands the application whatever token the request
    line carried, so the scope is where such a method is reproduced.
    Returns the messages the application sent.
    """
    scope: Scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "https",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(b"host", b"example.com")],
        "server": ("example.com", 443),
        "client": ("127.0.0.1", 50000),
    }
    sent: list[Message] = []

    async def receive() -> Message:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: Message) -> None:
        sent.append(message)

    await app(scope, receive, send)
    return sent


@pytest.mark.asyncio
async def test_nonstandard_method_records_other(
    mock_events: DocverseEvents,
) -> None:
    """A method token outside RFC 9110's set is recorded as ``OTHER``.

    The ASGI server accepts any token on the request line, and the router
    answers it ``405``; recording the token itself would let every
    scanner mint a new ``method`` tag value.
    """
    app = _prefixed_app(lambda: mock_events)
    publisher = mock_events.api_request
    assert isinstance(publisher, MockEventPublisher)

    sent = await _send_raw(app, method="FOOBAR", path="/docverse/orgs/rubin")

    assert sent[0]["status"] == 405
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.method == "OTHER"
    assert event.status_class == HttpStatusClass.client_error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("token", "expected"),
    [
        *[
            (method.value, method.value)
            for method in HttpMethod
            if method is not HttpMethod.other
        ],
        ("get", "GET"),
        ("Patch", "PATCH"),
    ],
)
async def test_standard_method_records_its_token(
    mock_events: DocverseEvents, token: str, expected: str
) -> None:
    """Each of RFC 9110's methods is recorded as itself, upper-cased.

    Only tokens outside that set collapse to ``OTHER``; a standard method
    keeps its own tag value whatever case it arrived in and whatever the
    router answered it with.
    """
    app = _prefixed_app(lambda: mock_events)
    publisher = mock_events.api_request
    assert isinstance(publisher, MockEventPublisher)

    await _send_raw(app, method=token, path="/docverse/orgs/rubin")

    assert [event.method for event in publisher.published] == [expected]
