"""Record one ``api_request`` metrics event per API response.

A pure-ASGI middleware, modelled on
:class:`safir.middleware.x_forwarded.XForwardedMiddleware`, rather than a
Starlette ``BaseHTTPMiddleware``: it only has to watch the
``http.response.start`` message go by and read what the router left in
the scope, and a pure-ASGI wrapper does that without buffering the
response or moving the handler onto another task.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import timedelta

import structlog
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ..metrics import ApiRequestEvent, DocverseEvents, HttpStatusClass

__all__ = ["ApiRequestMiddleware"]

_EXCLUDED_PATHS = frozenset({"/", "/health"})
"""Paths answered to probes rather than API clients, never recorded.

Both sit outside the Gafaelfawr ingress, where Kubernetes polls them;
recording them would bury the API's own traffic under the probes'.
"""

_AUTH_USER_HEADER = "X-Auth-Request-User"
"""The header Gafaelfawr's ingress sets on a request it authenticated."""


class ApiRequestMiddleware:
    """Publish an ``api_request`` event for every API response.

    The event is published after the wrapped application returns, which
    is when the router's match is readable from the scope: FastAPI
    records the matched route as ``scope["route"]``, and its template
    (``/orgs/{org}``, not the concrete path) becomes the event's
    ``route``. The status code and latency come from the
    ``http.response.start`` message, timed on the monotonic clock from
    the moment the request reached this middleware.

    Publishing is best-effort. The publish runs after the response has
    been handed to the server, and any exception it raises is logged and
    swallowed, so a metrics outage can never change or fail a response.

    This middleware must run inside
    :class:`~safir.middleware.x_forwarded.XForwardedMiddleware`, which
    passes the application a copy of the scope: only a middleware on the
    inner side of that copy sees the route the router records on it.

    Parameters
    ----------
    app
        The ASGI application to wrap.
    events
        Zero-argument callable returning the process's
        :class:`~docverse_server.metrics.DocverseEvents`. Resolved per
        request, so the publishers the lifespan registers after this
        middleware was constructed are the ones used, and a test can
        substitute its own.
    path_prefix
        The application's URL prefix (``config.path_prefix``), removed
        from a route template that still carries it.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        events: Callable[[], DocverseEvents],
        path_prefix: str,
    ) -> None:
        self._app = app
        self._events = events
        self._path_prefix = path_prefix.rstrip("/")

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        if scope["type"] != "http" or scope["path"] in _EXCLUDED_PATHS:
            await self._app(scope, receive, send)
            return

        started = time.monotonic()
        status_code: int | None = None
        duration: timedelta | None = None

        async def send_and_time(message: Message) -> None:
            nonlocal status_code, duration
            if message["type"] == "http.response.start":
                duration = timedelta(seconds=time.monotonic() - started)
                status_code = message["status"]
            await send(message)

        await self._app(scope, receive, send_and_time)

        route = self._route_template(scope)
        if route is None or status_code is None or duration is None:
            return
        await self._publish(
            scope, route=route, status_code=status_code, duration=duration
        )

    async def _publish(
        self,
        scope: Scope,
        *,
        route: str,
        status_code: int,
        duration: timedelta,
    ) -> None:
        """Publish the event, logging and swallowing any failure."""
        try:
            payload = ApiRequestEvent(
                method=scope["method"].upper(),
                route=route,
                status_code=status_code,
                status_class=HttpStatusClass.from_status_code(status_code),
                duration=duration,
                authenticated=bool(
                    Headers(scope=scope).get(_AUTH_USER_HEADER)
                ),
                organization=None,
                project=None,
            )
            await self._events().api_request.publish(payload)
        except Exception:
            # Fetched here rather than held from construction: a logger
            # cached before the lifespan configured logging would keep
            # the pre-configuration processors.
            logger = structlog.get_logger("docverse")
            logger.exception(
                "Failed to publish api_request metrics event",
                route=route,
                status_code=status_code,
            )

    def _route_template(self, scope: Scope) -> str | None:
        """Return the matched route's template without the path prefix.

        ``None`` when the router matched no route. FastAPI's lazily
        included routers leave the route as it was declared, so the
        template usually arrives unprefixed already; the prefix is
        removed for a route that was declared or flattened with it.
        """
        route = scope.get("route")
        path = getattr(route, "path", None)
        if not isinstance(path, str):
            return None
        prefix = self._path_prefix
        if prefix and (path == prefix or path.startswith(f"{prefix}/")):
            return path[len(prefix) :] or "/"
        return path
