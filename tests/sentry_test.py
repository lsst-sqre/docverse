"""End-to-end Sentry wiring tests.

Exercises :func:`docverse.sentry.initialize_sentry` against a minimal
fixture-only FastAPI app so the per-process wiring (DSN gating, release,
``service``/``component`` global tags) is locked down without depending
on the real Docverse app's lifespan, DB, or GitHub validator.
"""

from __future__ import annotations

from collections.abc import Iterator
from importlib.metadata import version
from typing import Any

import pytest
import sentry_sdk
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from safir.testing.sentry import (
    TestTransport,
    capture_events_fixture,
    sentry_init_fixture,
)

from docverse.dependencies.auth import require_superadmin
from docverse.dependencies.context import context_dependency
from docverse.handlers.admin import admin_router
from docverse.sentry import initialize_sentry


class _StubContext:
    """Duck-typed :class:`RequestContext` for endpoint-level Sentry tests.

    The ``/admin/sentry/test`` handler only touches ``rebind_logger`` before
    raising, so the test substitutes a no-op stub via
    :pyattr:`FastAPI.dependency_overrides` rather than wiring the full DB /
    factory / arq machinery just to reach a deliberate ``RuntimeError``.
    """

    def rebind_logger(self, **_values: Any) -> None:
        return None


async def _stub_context_dependency() -> _StubContext:
    return _StubContext()


async def _stub_require_superadmin() -> None:
    return None


@pytest.fixture(autouse=True)
def _isolate_sentry_global_scope() -> Iterator[None]:
    """Strip ``initialize_sentry``'s global-scope tags around each test.

    ``sentry_init_fixture`` saves and restores the *client* on the
    global scope, but ``initialize_sentry`` also writes ``service`` and
    ``component`` tags directly to that scope, which would otherwise
    persist across tests in the session. Running on both sides isolates
    this file from prior Sentry state and prevents tag bleed into any
    later test that asserts tag absence.
    """
    scope = sentry_sdk.get_global_scope()
    scope.remove_tag("service")
    scope.remove_tag("component")
    yield
    scope = sentry_sdk.get_global_scope()
    scope.remove_tag("service")
    scope.remove_tag("component")


def _patch_sentry_init_with_test_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force every ``sentry_sdk.init`` to use ``TestTransport``.

    :func:`docverse.sentry.initialize_sentry` delegates to
    :func:`safir.sentry.initialize_sentry` → ``sentry_sdk.init`` and does
    not accept a transport argument, so the test redirects via
    monkey-patch rather than asking the production wrapper to expose a
    testing-only knob.
    """
    real_init = sentry_sdk.init

    def init_with_test_transport(*args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("transport", TestTransport())
        return real_init(*args, **kwargs)

    monkeypatch.setattr(sentry_sdk, "init", init_with_test_transport)


@pytest.mark.asyncio
async def test_api_initialize_sentry_captures_event_with_release_and_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An uncaught FastAPI error reaches Sentry with the right metadata.

    Locks the contract:

    * ``release`` matches ``importlib.metadata.version("docverse")``.
    * ``tags.component == "api"`` distinguishes API events from worker /
      CLI ones once the same wrapper runs from those entry points.
    * ``tags.service == "docverse"`` separates Docverse from sibling
      SQuaRE services on the shared Sentry tenant.
    """
    monkeypatch.setenv("SENTRY_DSN", "https://test@example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "test")
    _patch_sentry_init_with_test_transport(monkeypatch)

    boom_message = "intentional test failure"

    with sentry_init_fixture():
        initialize_sentry(component="api")
        captured = capture_events_fixture(monkeypatch)()

        app = FastAPI()

        @app.get("/boom")
        async def boom() -> None:
            raise RuntimeError(boom_message)

        async with AsyncClient(
            base_url="https://example.com",
            transport=ASGITransport(app=app, raise_app_exceptions=False),
        ) as client:
            await client.get("/boom")

    assert len(captured.errors) == 1
    event = captured.errors[0]
    assert event["release"] == version("docverse")
    assert event["tags"]["component"] == "api"
    assert event["tags"]["service"] == "docverse"


@pytest.mark.asyncio
async def test_admin_sentry_test_endpoint_captures_event_with_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``POST /admin/sentry/test`` produces one Sentry event carrying marker.

    Locks the validation-tooling contract for the SRE-facing
    ``/admin/sentry/test`` route from PRD #338: hitting the endpoint must
    produce exactly one captured event tagged ``service=docverse`` /
    ``component=api`` / ``release=<docverse version>``, and the request-body
    ``message`` must land in the exception ``value`` so an operator can grep
    for their marker after a deploy.

    The router-level ``require_superadmin`` and ``context_dependency`` are
    overridden because the contract under test is the Sentry envelope, not
    the auth gate (covered in ``tests/handlers/admin_test.py``).
    """
    monkeypatch.setenv("SENTRY_DSN", "https://test@example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "test")
    _patch_sentry_init_with_test_transport(monkeypatch)

    marker = "validation-marker-42"

    with sentry_init_fixture():
        initialize_sentry(component="api")
        captured = capture_events_fixture(monkeypatch)()

        app = FastAPI()
        app.include_router(admin_router)
        app.dependency_overrides[context_dependency] = _stub_context_dependency
        app.dependency_overrides[require_superadmin] = _stub_require_superadmin

        async with AsyncClient(
            base_url="https://example.com",
            transport=ASGITransport(app=app, raise_app_exceptions=False),
        ) as client:
            response = await client.post(
                "/admin/sentry/test",
                json={"message": marker},
            )
        assert response.status_code == 500

    assert len(captured.errors) == 1
    event = captured.errors[0]
    assert event["release"] == version("docverse")
    assert event["tags"]["service"] == "docverse"
    assert event["tags"]["component"] == "api"
    exc_values = event["exception"]["values"]
    assert any(marker in exc["value"] for exc in exc_values)


def test_initialize_sentry_is_noop_when_dsn_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No ``SENTRY_DSN`` env var → wrapper does not configure a client.

    Pins the local-dev / CI contract that ``nox -s test`` never reports.
    """
    monkeypatch.delenv("SENTRY_DSN", raising=False)

    with sentry_init_fixture():
        initialize_sentry(component="api")
        # ``sentry_init_fixture`` clears any prior client on entry; the
        # wrapper's early return leaves Sentry uninitialized.
        assert sentry_sdk.is_initialized() is False
        # The early return must also skip the global-scope tag writes
        # so a DSN-less run leaves no fingerprint for the next test.
        scope_tags = getattr(sentry_sdk.get_global_scope(), "_tags", {})
        assert "service" not in scope_tags
        assert "component" not in scope_tags
