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
from starlette import status
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

    Every HTTP request except the probe endpoints (``/`` and ``/health``)
    records exactly one event, whatever became of it: a routed response,
    a path no route matched, or an exception that escaped the
    application. The event is published once the wrapped application
    returns (or raises), which is when the router's match is readable
    from the scope. Its fields are filled as follows.

    ``method``
        The request method, upper-cased.
    ``route``
        The template of the route the router matched, with
        ``path_prefix`` removed: ``/orgs/{org}/projects/{project}``,
        never ``/docverse/orgs/rubin/projects/sqr-000``. FastAPI records
        the matched API route as ``scope["route"]``. ``None`` when there
        is no such route: a path nothing matched (scanner noise, which
        FastAPI answers ``404``), and the documentation pages FastAPI
        serves itself (``openapi.json``, ``docs``, ``redoc``), whose
        plain Starlette routes record no template.
    ``status_code`` and ``status_class``
        The status of the ``http.response.start`` message. When an
        exception escapes the application instead, ``500`` and ``5xx``:
        the response Starlette's server-error layer, outside this
        middleware, sends for it.
    ``duration``
        Time on the monotonic clock from the request reaching this
        middleware to its response starting, or to the exception
        escaping.
    ``authenticated``
        Whether Gafaelfawr's ingress set ``X-Auth-Request-User``. Only
        the header's presence is recorded, never the username in it.
    ``organization`` and ``project``
        The matched route's ``org`` and ``project`` path parameters, as
        the caller addressed them; each is ``None`` when the route
        declares no such parameter or no route matched. Only callers
        Gafaelfawr admits reach a route that declares them: the one
        anonymous route, the GitHub webhook, declares neither.

    Never put the concrete request path, the username, or any other
    free-text identifier in the event, and derive no field from them.
    Phalanx turns ``method``, ``route``, ``status_class``, and
    ``authenticated`` into InfluxDB tags, and each stays bounded only
    because it is a closed vocabulary or a template bounded by the size
    of the API; an unmatched path is counted, but with ``route=None``,
    so scanner noise adds volume without adding a tag value.

    Publishing is best-effort. The publish runs after the response has
    been handed to the server, and any exception it raises is logged and
    swallowed, so a metrics outage can never change or fail a response.
    An exception raised by the application is recorded and then
    re-raised unchanged, so Starlette's server-error handling, Sentry's
    capture, and the ``500`` the caller receives are all untouched.

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

        try:
            await self._app(scope, receive, send_and_time)
        except Exception:
            if duration is None:
                duration = timedelta(seconds=time.monotonic() - started)
            await self._publish(
                scope,
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                duration=duration,
            )
            raise

        if status_code is None or duration is None:
            return
        await self._publish(scope, status_code=status_code, duration=duration)

    async def _publish(
        self,
        scope: Scope,
        *,
        status_code: int,
        duration: timedelta,
    ) -> None:
        """Publish the event, logging and swallowing any failure."""
        route = self._route_template(scope)
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
                organization=_path_param(scope, "org"),
                project=_path_param(scope, "project"),
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


def _path_param(scope: Scope, name: str) -> str | None:
    """Return a string path parameter the router matched, if any.

    ``None`` when no route matched (the router then records no path
    parameters) or when the matched route does not declare ``name``.
    """
    value = (scope.get("path_params") or {}).get(name)
    return value if isinstance(value, str) else None
