"""Test fixtures for docverse tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Iterator
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
import respx
import sentry_sdk
import structlog
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from rubin.repertoire import DiscoveryClient, register_mock_discovery
from safir.arq import MockArqQueue
from safir.database import create_database_engine
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)

from docverse.models import (
    BuildAnnotations,
    BuildCreate,
    OrgMembershipCreate,
    OrgRole,
    PrincipalType,
)
from docverse_server.config import config
from docverse_server.dependencies.context import (
    RequestContext,
    context_dependency,
)
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.main import app as docverse_app
from docverse_server.metrics import build_event_manager
from docverse_server.metrics.events import DocverseEvents
from docverse_server.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.membership_store import OrgMembershipStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore
from docverse_server.storage.user_info_store import StubUserInfoStore
from docverse_server.worker.queues import MAINTENANCE_QUEUE_NAME

from .support.arq_testing import register_queue
from .support.database import (
    ddl_database_url_for,
    provision_database,
    reset_database_for_test,
)
from .support.github_mock import GitHubMock, make_rsa_pem
from .support.metrics import reset_mock_event_publishers

__all__ = [
    "GitHubMock",
    "SessionLifespan",
    "seed_build",
    "seed_group_member",
    "seed_member",
    "seed_org_with_admin",
]

_DISCOVERY_FIXTURE = Path(__file__).parent / "data" / "discovery.json"


class SessionLifespan:
    """The suite's single run of the application lifespan.

    Startup and shutdown are decoupled from any one ``with`` block so a
    test that has to drive the real lifespan itself — ``tests/main_test``
    exercises the GitHub-App startup validator that way — can borrow the
    process first. Only one lifespan may run at a time: shutdown closes
    process-global dependencies (``db_session_dependency``,
    ``http_client_dependency``, the metrics manager), so a nested
    lifespan's exit would otherwise leave the shared one hollowed out
    for every test that follows.

    Restarting is lazy, via :meth:`ensure_running` from the ``app``
    fixture, rather than eager on the borrowing test's teardown: by then
    the borrower may still have ``config`` monkeypatched, and startup
    reads config to decide whether to call out to GitHub.
    """

    def __init__(self, app: FastAPI) -> None:
        self._app = app
        self._stack: AsyncExitStack | None = None

    async def ensure_running(self) -> None:
        """Start the application lifespan unless it is already running.

        Startup is entered under its own discovery mock: the autouse
        ``mock_discovery`` router may not be installed yet (session
        fixtures are set up before function-scoped ones), and startup
        would otherwise be the one place in the suite that could reach
        the network. The `AsyncExitStack` keeps the lifespan open after
        that mock is torn down, so per-test routers govern everything
        the running application does afterwards.
        """
        if self._stack is not None:
            return
        stack = AsyncExitStack()
        with respx.mock(
            assert_all_called=False, assert_all_mocked=False
        ) as router:
            register_mock_discovery(router, _DISCOVERY_FIXTURE)
            await stack.enter_async_context(LifespanManager(self._app))
        self._stack = stack

    async def stop(self) -> None:
        """Shut the application lifespan down if it is running."""
        stack, self._stack = self._stack, None
        if stack is not None:
            await stack.aclose()


@pytest.fixture(autouse=True)
def _isolate_sentry_global_scope() -> Iterator[None]:
    """Snapshot and restore global-scope Sentry tags around every test.

    ``safir.testing.sentry.sentry_init_fixture`` saves and restores the
    Sentry *client* on the global scope, but it does not touch tags.
    :func:`docverse.sentry.initialize_sentry` (and any other code that
    writes to ``sentry_sdk.get_global_scope()``) leaves ``service`` /
    ``component`` (and potentially other) tags on that scope, which
    would otherwise persist across tests in the session and bleed into
    any later test that asserts tag absence. Snapshotting all tags on
    entry and restoring on exit isolates every test regardless of
    which tags it sets.
    """
    scope = sentry_sdk.get_global_scope()
    saved_tags = dict(scope._tags)
    try:
        yield
    finally:
        scope = sentry_sdk.get_global_scope()
        for key in list(scope._tags):
            if key not in saved_tags:
                scope.remove_tag(key)
        for key, value in saved_tags.items():
            scope.set_tag(key, value)


@pytest.fixture(autouse=True)
def mock_discovery() -> Iterator[respx.Router]:
    """Mock the Repertoire discovery endpoint for the whole test suite.

    Passes through any other HTTP requests so the ASGI transport used by
    the FastAPI test client still reaches the app.
    """
    with respx.mock(assert_all_called=False, assert_all_mocked=False) as mock:
        register_mock_discovery(mock, _DISCOVERY_FIXTURE)
        yield mock


@pytest.fixture(scope="session")
def github_app_private_key_pem() -> str:
    """Session-scoped RSA PEM so the 2048-bit keygen only runs once."""
    return make_rsa_pem()


@pytest.fixture
def mock_github(
    mock_discovery: respx.Router,
    github_app_private_key_pem: str,
) -> GitHubMock:
    """Seeder for GitHub REST API responses on the autouse respx router.

    Composes with ``mock_discovery`` by sharing its router, so a single
    ``respx.mock`` context covers both mocks. Tests that need GitHub
    fakes depend on this fixture; tests that don't are unaffected.
    """
    return GitHubMock(
        router=mock_discovery, private_key_pem=github_app_private_key_pem
    )


@pytest_asyncio.fixture
async def discovery_client() -> AsyncGenerator[DiscoveryClient]:
    """Return a ``DiscoveryClient`` backed by the autouse respx mock."""
    async with httpx.AsyncClient() as http_client:
        yield DiscoveryClient(http_client)


@pytest_asyncio.fixture
async def mock_events() -> AsyncGenerator[DocverseEvents]:
    """Build an initialized ``DocverseEvents`` over a ``MockEventManager``.

    Mirrors what the FastAPI lifespan and worker startup build, for unit
    tests that construct a ``ContextDependency`` or worker ``ctx``
    directly and therefore need to supply the metrics events themselves.
    """
    manager, events = await build_event_manager(config)
    try:
        yield events
    finally:
        await manager.aclose()


@pytest_asyncio.fixture(scope="session")
async def database_engine() -> AsyncGenerator[AsyncEngine]:
    """Yield one engine per pytest process, with the schema built once.

    Creating the ``pg_trgm`` extension, running ``create_all`` and
    stamping Alembic used to happen once per *test*; it happens once per
    session here, and :func:`~tests.support.database.reset_database_for_test`
    truncates instead on every subsequent call. The engine is shared so
    the per-test reset does not pay for a new connection pool either.
    """
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    await reset_database_for_test(engine)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def ddl_database_url() -> str:
    """Provision the database the schema/migration tests own.

    ``tests/dbschema`` drops the schema and steps Alembic through
    individual revisions, which the shared session-scoped engine,
    application lifespan, and truncate-based reset cannot survive. Those
    tests get this sibling database instead, created once per pytest
    process with the same server-side provisioning (currently
    ``pg_trgm``) every test database needs.
    """
    url = ddl_database_url_for(config.database_url)
    await provision_database(url, config.database_password)
    return url


@pytest.fixture(scope="session")
def db_session_factory(
    database_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """Return a session factory bound to the shared test engine."""
    return async_sessionmaker(database_engine, expire_on_commit=False)


@pytest_asyncio.fixture(scope="session")
async def app_lifespan(
    database_engine: AsyncEngine,
) -> AsyncGenerator[SessionLifespan]:
    """Own the suite's single run of the application lifespan.

    The schema has to exist before startup, since the lifespan refuses
    to run against a database that is not at the current Alembic
    revision — hence the ``database_engine`` dependency. Startup itself
    is left to the ``app`` fixture's :meth:`SessionLifespan.ensure_running`
    so that a test which borrows the process can hand it back lazily.
    """
    lifespan = SessionLifespan(docverse_app)
    try:
        yield lifespan
    finally:
        await lifespan.stop()


@pytest_asyncio.fixture
async def own_app_lifespan(
    app_lifespan: SessionLifespan, database_engine: AsyncEngine
) -> None:
    """Give this test exclusive use of the process-global lifespan state.

    For tests that run the real application lifespan themselves: the
    suite's lifespan is stopped so the test's own startup and shutdown
    own ``db_session_dependency`` and friends outright. The database is
    left empty and Alembic-stamped, which is what the lifespan's schema
    check requires. The next test that asks for ``app`` restarts the
    suite's lifespan.
    """
    await reset_database_for_test(database_engine)
    await app_lifespan.stop()


@pytest_asyncio.fixture
async def app(
    app_lifespan: SessionLifespan, database_engine: AsyncEngine
) -> FastAPI:
    """Return the running test application, reset for this test.

    The lifespan itself is session-scoped, so everything a test can
    dirty inside process-lifetime state is rebuilt here instead: the
    database is truncated back to empty, the recorded metrics payloads
    are dropped, and the dependency singletons that tests mutate get
    fresh values.
    """
    await app_lifespan.ensure_running()
    await reset_database_for_test(database_engine)
    events = context_dependency._events
    if events is not None:
        reset_mock_event_publishers(events)
    context_dependency._user_info_store = StubUserInfoStore()
    context_dependency._superadmin_usernames = ["superadmin"]
    # Replace the MockArqQueue with one that uses the configured
    # queue name so ArqQueueBackend can enqueue to the right queue.
    # Register the dedicated keeper-sync and maintenance queues too —
    # MockArqQueue only auto-creates the slot for its
    # ``default_queue_name``, so the ``POST /orgs/{org}/keeper-sync/
    # runs`` enqueue and the project_github_resolve enqueue onto the
    # maintenance pool (PRD #419) would otherwise hit ``KeyError`` on
    # first use. Rebuilding it per test also keeps enqueued jobs from
    # leaking forward, which no lifespan cycle bounds any more.
    arq_queue = MockArqQueue(default_queue_name=config.arq_queue_name)
    register_queue(arq_queue, KEEPER_SYNC_QUEUE_NAME)
    register_queue(arq_queue, MAINTENANCE_QUEUE_NAME)
    arq_dependency._arq_queue = arq_queue
    return docverse_app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncGenerator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(
        base_url="https://example.com/",
        transport=ASGITransport(app=app),
    ) as client:
        yield client


@pytest_asyncio.fixture
async def db_session(
    app: FastAPI,
    db_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession]:
    """Provide a database session for direct store tests.

    The ``app`` parameter orders this fixture after the per-test
    database reset, so the session always starts against an empty
    schema. The session comes from the shared session-scoped engine
    rather than the application's own pool, so a test that holds it open
    across requests cannot starve the app of a connection.
    """
    async with db_session_factory() as session:
        yield session


async def seed_org_with_admin(
    client: AsyncClient,
    org_slug: str,
    admin_username: str,
) -> None:
    """Create an org via admin API with an initial admin member."""
    response = await client.post(
        "/docverse/admin/orgs",
        json={
            "slug": org_slug,
            "title": f"Test Org {org_slug}",
            "base_domain": f"{org_slug}.example.com",
            "members": [
                {
                    "principal": admin_username,
                    "principal_type": "user",
                    "role": "admin",
                }
            ],
        },
        headers={"X-Auth-Request-User": "superadmin"},
    )
    assert response.status_code == 201


async def seed_build(
    org_slug: str,
    project_slug: str,
    *,
    git_ref: str = "main",
    content_hash: str = (
        "sha256:abcdef0123456789abcdef0123456789"
        "abcdef0123456789abcdef0123456789"
    ),
    uploader: str = "testuser",
    annotations: BuildAnnotations | None = None,
) -> str:
    """Create a build directly via the DB and return its base32 ID."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug(org_slug)
            assert org is not None
            proj_store = ProjectStore(session=session, logger=logger)
            project = await proj_store.get_by_slug(
                org_id=org.id, slug=project_slug
            )
            assert project is not None
            build_store = BuildStore(session=session, logger=logger)
            build = await build_store.create(
                project_id=project.id,
                project_slug=project.slug,
                data=BuildCreate(
                    git_ref=git_ref,
                    content_hash=content_hash,
                    annotations=annotations,
                ),
                uploader=uploader,
            )
            await session.commit()
        return serialize_base32_id(build.public_id)
    msg = "db_session_dependency yielded nothing"
    raise AssertionError(msg)


async def seed_member(
    org_slug: str,
    username: str,
    role: OrgRole,
) -> None:
    """Seed a membership with a given role directly via the DB."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug(org_slug)
            assert org is not None
            membership_store = OrgMembershipStore(
                session=session, logger=logger
            )
            await membership_store.create(
                org_id=org.id,
                data=OrgMembershipCreate(
                    principal=username,
                    principal_type=PrincipalType.user,
                    role=role,
                ),
            )
            await session.commit()


async def seed_group_member(
    org_slug: str,
    group_name: str,
    role: OrgRole,
) -> None:
    """Seed a group membership with a given role directly via the DB."""
    logger = structlog.get_logger("docverse")
    async for session in db_session_dependency():
        async with session.begin():
            org_store = OrganizationStore(session=session, logger=logger)
            org = await org_store.get_by_slug(org_slug)
            assert org is not None
            membership_store = OrgMembershipStore(
                session=session, logger=logger
            )
            await membership_store.create(
                org_id=org.id,
                data=OrgMembershipCreate(
                    principal=group_name,
                    principal_type=PrincipalType.group,
                    role=role,
                ),
            )
            await session.commit()


@pytest.fixture
def rebind_spy(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture all calls to ``RequestContext.rebind_logger``."""
    calls: list[dict[str, Any]] = []
    original = RequestContext.rebind_logger

    def spy(self: RequestContext, **values: Any) -> None:
        calls.append(values)
        original(self, **values)

    monkeypatch.setattr(RequestContext, "rebind_logger", spy)
    return calls
