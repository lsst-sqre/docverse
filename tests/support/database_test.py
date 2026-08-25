"""Tests for ``tests.support.database``'s reset and DDL-database helpers.

The tests that need a specific driver error inject a canned one through
`_TruncateFailingEngine` rather than racing PostgreSQL for it, but they
still run the real retry loop against a real database so the blocker
diagnostics are exercised too.

The DDL-database tests all run against the ``*_ddl`` database, never the
shared one: they drop the schema, which is the very thing that database
exists to contain.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

import pytest
from asyncpg.exceptions import (
    DeadlockDetectedError,
    InsufficientPrivilegeError,
    LockNotAvailableError,
)
from safir.database import create_database_engine
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from docverse_server.config import config
from docverse_server.dbschema import Base
from tests.support.database import (
    DDL_DATABASE_SUFFIX,
    TruncateLockError,
    ddl_database,
    invalidate_schema_ready,
    reset_database_for_test,
    schema_is_ready,
    truncate_all_tables,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import CursorResult
    from sqlalchemy.engine.url import URL
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
    from sqlalchemy.sql.elements import TextClause


class _FailingConnection:
    def __init__(
        self, owner: _TruncateFailingEngine, conn: AsyncConnection
    ) -> None:
        self._owner = owner
        self._conn = conn

    async def execute(self, statement: TextClause) -> CursorResult[Any]:
        """Run the statement, unless it is the truncate this engine fails."""
        if str(statement).lstrip().startswith("TRUNCATE"):
            self._owner.attempts += 1
            raise self._owner.error
        return await self._conn.execute(statement)


class _TruncateFailingEngine:
    """A real engine whose ``TRUNCATE`` always fails with a canned error.

    Only the members `truncate_all_tables` reaches for are wrapped;
    ``connect`` and ``url`` are the real engine's, so the give-up path
    still queries ``pg_stat_activity`` for real.
    """

    def __init__(self, engine: AsyncEngine, error: DBAPIError) -> None:
        self.error = error
        self.attempts = 0
        self._engine = engine

    @property
    def url(self) -> URL:
        """URL of the wrapped engine."""
        return self._engine.url

    def connect(self) -> AsyncConnection:
        """Return a real connection, which the blocker report needs."""
        return self._engine.connect()

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[_FailingConnection]:
        """Begin a real transaction whose truncate is doomed."""
        async with self._engine.begin() as conn:
            yield _FailingConnection(self, conn)

    def as_engine(self) -> AsyncEngine:
        """Return this wrapper typed as the engine it stands in for."""
        return cast("AsyncEngine", self)


def _canned_error(
    orig: Exception, *, statement: str = "TRUNCATE"
) -> DBAPIError:
    """Build the wrapper SQLAlchemy raises around an asyncpg error.

    The asyncpg adapter copies asyncpg's ``sqlstate`` onto the DBAPI
    error it raises, so handing the asyncpg exception itself to
    `DBAPIError` reproduces what the truncate classifier actually sees.
    """
    return DBAPIError(statement=statement, params=None, orig=orig)


_INSERT_ORG = text(
    'INSERT INTO "organizations"'
    " (slug, title, base_domain, url_scheme, root_path_prefix,"
    " purgatory_retention_seconds)"
    " VALUES (:slug, 'Test Org', 'example.com', 'subdomain', '/', 2592000)"
    " RETURNING id"
)
"""Insert one organization, naming every column without a server default."""


@pytest.mark.asyncio
async def test_truncate_gives_up_and_names_the_blocker() -> None:
    """A truncate blocked by another session fails loudly instead of hanging.

    Without the ``lock_timeout`` this would wait forever, which is
    exactly how a CI job goes silent until its own timeout kills it.
    """
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    blocker = create_database_engine(
        config.database_url, config.database_password
    )
    try:
        await reset_database_for_test(engine)

        async with blocker.connect() as conn:
            # An open transaction holding ACCESS SHARE on any truncated
            # table conflicts with the ACCESS EXCLUSIVE lock TRUNCATE
            # requires.
            await conn.execute(
                text('LOCK TABLE "organizations" IN ACCESS SHARE MODE')
            )
            with pytest.raises(TruncateLockError) as excinfo:
                await truncate_all_tables(
                    engine,
                    lock_timeout="150ms",
                    attempts=2,
                    retry_delay=0.01,
                )

        message = str(excinfo.value)
        assert "after 2 attempts" in message
        assert "pid=" in message
        assert "LOCK TABLE" in message

        # Once the blocking transaction is gone the truncate succeeds
        # again, which is why a bounded retry is worth having at all.
        await truncate_all_tables(engine)
    finally:
        await blocker.dispose()
        await engine.dispose()


@pytest.mark.asyncio
async def test_reset_database_restarts_identity_sequences() -> None:
    """Reset leaves sequences where a freshly created schema would.

    ``TRUNCATE`` alone would leave the ``organizations`` identity
    sequence advanced, so a test asserting a specific autoincrement ID
    would pass in isolation and fail after any earlier test inserted a
    row. ``RESTART IDENTITY`` is what makes the truncate-based reset
    equivalent to a drop-and-create.
    """
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    try:
        await reset_database_for_test(engine)
        async with engine.begin() as conn:
            first = await conn.scalar(_INSERT_ORG, {"slug": "first"})
            second = await conn.scalar(_INSERT_ORG, {"slug": "second"})
        assert second == first + 1

        # The second reset truncates rather than rebuilding the schema,
        # since the first call marked this database's schema ready.
        await reset_database_for_test(engine)
        async with engine.begin() as conn:
            count = await conn.scalar(
                text('SELECT count(*) FROM "organizations"')
            )
            reused = await conn.scalar(_INSERT_ORG, {"slug": "first"})
        assert count == 0
        assert reused == first
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "orig",
    [
        pytest.param(
            LockNotAvailableError("canceling statement due to lock timeout"),
            id="lock-timeout",
        ),
        pytest.param(
            DeadlockDetectedError("deadlock detected"), id="deadlock"
        ),
    ],
)
async def test_truncate_retries_transient_lock_contention(
    orig: Exception,
) -> None:
    """A deadlock gets the same bounded retry as a lock timeout.

    ``TRUNCATE`` takes its ``ACCESS EXCLUSIVE`` locks table by table, so
    a concurrent transaction can close a lock cycle with it.
    PostgreSQL's deadlock detector (``deadlock_timeout``, 1 s) fires
    before the truncate's own 3 s ``lock_timeout``, and it may pick the
    truncate as the victim — so the very contention the retry exists to
    absorb often arrives as ``40P01`` rather than ``55P03``.
    """
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    failing = _TruncateFailingEngine(engine, _canned_error(orig))
    try:
        with pytest.raises(TruncateLockError) as excinfo:
            await truncate_all_tables(
                failing.as_engine(), attempts=3, retry_delay=0.01
            )

        assert failing.attempts == 3
        assert excinfo.value.__cause__ is failing.error
        assert "after 3 attempts" in str(excinfo.value)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_truncate_propagates_unrelated_sqlstate() -> None:
    """An unrelated failure propagates even if its text names a lock timeout.

    Classification consults only ``orig.sqlstate``. Falling back to a
    ``"due to lock timeout"`` substring match on ``str(exc)`` would
    misfire, because a `DBAPIError`'s ``str`` renders the failing
    statement as well as the driver message: any error whose text
    happened to contain the phrase would be retried and then reported as
    a `TruncateLockError`, burying the real failure.
    """
    error = _canned_error(
        InsufficientPrivilegeError(
            'permission denied for table "organizations"'
        ),
        statement=(
            'SELECT * FROM "organizations" WHERE title ='
            " 'canceling statement due to lock timeout'"
        ),
    )
    assert "due to lock timeout" in str(error)

    engine = create_database_engine(
        config.database_url, config.database_password
    )
    failing = _TruncateFailingEngine(engine, error)
    try:
        with pytest.raises(DBAPIError) as excinfo:
            await truncate_all_tables(
                failing.as_engine(), attempts=3, retry_delay=0.01
            )

        assert excinfo.value is error
        assert failing.attempts == 1
    finally:
        await engine.dispose()


class _SchemaTestError(Exception):
    """Stands in for whatever a schema test raises when it fails."""


async def _canonical_schema_present(engine: AsyncEngine) -> bool:
    """Report whether the database holds a stamped ``create_all`` schema."""
    async with engine.connect() as conn:
        organizations = await conn.scalar(
            text("SELECT to_regclass('organizations')")
        )
        stamp = await conn.scalar(
            text("SELECT to_regclass('alembic_version')")
        )
        version = None
        if stamp is not None:
            version = await conn.scalar(
                text("SELECT version_num FROM alembic_version")
            )
    return organizations is not None and version is not None


@pytest.mark.asyncio
async def test_ddl_database_is_a_provisioned_sibling(
    ddl_database_url: str,
) -> None:
    """The schema tests get their own database, provisioned like the rest.

    The name is the shared database's plus a suffix rather than a fixed
    string, so a per-worker database keeps composing with it once xdist
    lands. ``pg_trgm`` comes from the same `provision_database` helper
    the per-worker databases will use, so a future extension requirement
    lands on every test database at once.
    """
    shared = make_url(config.database_url)
    ddl = make_url(ddl_database_url)
    assert ddl.database == f"{shared.database}{DDL_DATABASE_SUFFIX}"
    assert ddl.database != shared.database

    engine = create_database_engine(ddl_database_url, config.database_password)
    try:
        async with engine.connect() as conn:
            extensions = await conn.scalar(
                text(
                    "SELECT count(*) FROM pg_extension"
                    " WHERE extname = 'pg_trgm'"
                )
            )
    finally:
        await engine.dispose()
    assert extensions == 1


@pytest.mark.asyncio
async def test_ddl_database_invalidates_schema_ready_on_failure(
    ddl_database_url: str,
) -> None:
    """A failed schema test leaves the next reset to rebuild, not truncate.

    Invalidation has to happen on the way out of a *failing* test as much
    as a passing one: a migration that blew up halfway leaves the most
    damaged schema of all, and the tracking is what tells the next reset
    to throw it away.
    """
    engine = create_database_engine(ddl_database_url, config.database_password)
    try:
        await reset_database_for_test(engine)
        assert schema_is_ready(engine)
        assert await _canonical_schema_present(engine)

        with pytest.raises(_SchemaTestError):
            async with ddl_database(
                ddl_database_url, config.database_password
            ):
                raise _SchemaTestError

        assert not schema_is_ready(engine)

        # The failed test dropped the schema and never rebuilt it, so the
        # next reset has to be a rebuild rather than a truncate.
        await reset_database_for_test(engine)
        assert await _canonical_schema_present(engine)
    finally:
        invalidate_schema_ready(engine)
        await engine.dispose()


@pytest.mark.asyncio
async def test_reset_rebuilds_when_the_schema_vanished(
    ddl_database_url: str,
) -> None:
    """A forgotten invalidation costs a warning, not a cascade of failures.

    Truncating tables that no longer exist raises ``UndefinedTable``, and
    on a session-scoped database every later test inherits it. Treating
    that as "the tracking is stale" and rebuilding contains the damage to
    the one test that dropped the schema.
    """
    engine = create_database_engine(ddl_database_url, config.database_password)
    try:
        await reset_database_for_test(engine)

        # Drop the tables the way a half-finished migration would, without
        # telling the reset helper about it.
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        assert schema_is_ready(engine)

        await reset_database_for_test(engine)

        assert await _canonical_schema_present(engine)
    finally:
        invalidate_schema_ready(engine)
        await engine.dispose()
