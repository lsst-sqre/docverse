"""Database reset helpers for tests.

Rebuilding the whole schema with ``initialize_database(..., reset=True)``
per test (drop_all + create_all) is expensive. Instead, the schema is
created (and Alembic-stamped) once per pytest process and each test
starts by truncating every table, which preserves the empty-database
isolation guarantee at a fraction of the cost. ``TRUNCATE ... RESTART
IDENTITY`` also resets sequences, so tests that assert specific
autoincrement IDs behave identically to a freshly created schema.

The ``alembic_version`` table is deliberately not truncated: the
application lifespan checks the schema version at startup, so the stamp
applied at schema-creation time must survive between tests. Only tables
declared on :data:`docverse_server.dbschema.Base` metadata are
truncated, and ``alembic_version`` is not one of them.

``TRUNCATE`` takes an ``ACCESS EXCLUSIVE`` lock on every table, so it
waits behind *any* other session holding a conflicting lock. Once the
application is shared across a whole test session, a background arq or
lifespan task started by an earlier test can still be inside a
transaction when the next test resets the database. With PostgreSQL's
default (infinite) ``lock_timeout`` that wait is unbounded — a CI job
goes silent until its own timeout kills it. Every truncate therefore
runs under a short ``lock_timeout`` and is retried a bounded number of
times (a background transaction commits in milliseconds, so a retry
practically always wins), and a truncate that never gets its lock raises
`TruncateLockError` naming the PIDs, states, and queries of the sessions
that were in the way. Worst case the whole thing gives up in a handful
of seconds instead of hanging the job.

The locks are taken table by table, so the same contention can also
close a lock cycle and get the truncate killed as a deadlock victim
before its ``lock_timeout`` even fires; that is retried on the same
terms.

One corner of the suite cannot live on a truncate-based reset at all:
``tests/dbschema`` drops the schema outright and steps Alembic through
individual revisions. Those tests get their own database — see
`ddl_database_url_for` and `ddl_database` — so a migration under test
never pulls the schema out from under the session-scoped engine, the
application lifespan, or the test that runs next.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from pydantic import SecretStr
from safir.database import (
    create_database_engine,
    drop_database,
    initialize_database,
    stamp_database_async,
)
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql.elements import TextClause

from docverse_server.dbschema import Base

__all__ = [
    "DDL_DATABASE_SUFFIX",
    "TRUNCATE_ATTEMPTS",
    "TRUNCATE_LOCK_TIMEOUT",
    "TRUNCATE_RETRY_DELAY",
    "TruncateLockError",
    "ddl_database",
    "ddl_database_url_for",
    "invalidate_schema_ready",
    "provision_database",
    "reset_database_for_test",
    "schema_is_ready",
    "truncate_all_tables",
]

_RETRYABLE_TRUNCATE_SQLSTATES = frozenset({"55P03", "40P01"})
"""PostgreSQL SQLSTATEs for a truncate worth retrying.

``55P03`` (``lock_not_available``) is the ``lock_timeout`` cancellation.
``40P01`` (``deadlock_detected``) is what PostgreSQL raises when it picks
the truncate as the victim of a lock cycle: ``TRUNCATE`` takes its
``ACCESS EXCLUSIVE`` locks table by table, so a concurrent transaction
can close a cycle with it, and the deadlock detector
(``deadlock_timeout``, 1 s) fires before the 3 s ``lock_timeout``. Both
are the same transient contention, and the next attempt practically
always wins.
"""

TRUNCATE_LOCK_TIMEOUT = "3s"
"""How long one truncate attempt waits for its ``ACCESS EXCLUSIVE`` locks."""

TRUNCATE_ATTEMPTS = 4
"""How many times a lock-blocked truncate is retried before giving up."""

TRUNCATE_RETRY_DELAY = 0.25
"""Seconds to wait between lock-blocked truncate attempts."""

DDL_DATABASE_SUFFIX = "_ddl"
"""Suffix naming the database the schema/migration tests own.

The name is derived from whichever database the rest of the suite is
using, so per-worker suffixing keeps composing once pytest-xdist lands:
``docverse_gw0`` gets ``docverse_gw0_ddl``.
"""

_MAINTENANCE_DATABASE = "postgres"
"""Database to connect to in order to create or drop another one."""

_SAFE_DATABASE_NAME = re.compile(r"\A[A-Za-z0-9_]+\Z")
"""Database names `provision_database` is willing to interpolate into DDL."""

_UNDEFINED_TABLE_SQLSTATE = "42P01"
"""PostgreSQL SQLSTATE for a reference to a table that does not exist."""

_schema_ready: set[str] = set()
"""Names of the databases whose schema this pytest process has created."""


class TruncateLockError(RuntimeError):
    """A per-test database truncate could not acquire its table locks.

    The message names the other sessions on the database so a CI failure
    is diagnosable without reproducing the race.
    """


async def reset_database_for_test(engine: AsyncEngine) -> None:
    """Give the current test an empty, Alembic-stamped database.

    The first call for a given database in a pytest process performs the
    full ``initialize_database`` (drop_all + create_all) and stamps the
    Alembic version. Subsequent calls truncate all schema tables
    (restarting identity sequences) instead, which is much faster and
    equivalent for test isolation.

    Parameters
    ----------
    engine
        Database engine to use.

    Raises
    ------
    TruncateLockError
        Raised if another session held conflicting table locks for the
        whole bounded retry window.
    """
    database = _database_name(engine)
    logger = structlog.get_logger("docverse")
    if database in _schema_ready:
        try:
            await truncate_all_tables(engine)
        except DBAPIError as exc:
            # A test that drops or structurally mutates the schema is
            # expected to call `invalidate_schema_ready` on the way out.
            # If one forgets, the truncate hits tables that are gone and
            # every later test on this database inherits the failure.
            # Rebuilding instead keeps the damage to one warning.
            if _sqlstate(exc) != _UNDEFINED_TABLE_SQLSTATE:
                raise
            logger.warning(
                "Test database schema is missing tables; rebuilding it",
                database=database,
                error=str(exc),
            )
            _schema_ready.discard(database)
        else:
            return

    await initialize_database(engine, logger, schema=Base.metadata, reset=True)
    await stamp_database_async(engine)
    _schema_ready.add(database)


def schema_is_ready(engine: AsyncEngine) -> bool:
    """Report whether a database is tracked as holding the canonical schema.

    Parameters
    ----------
    engine
        Database engine naming the database to check.

    Returns
    -------
    bool
        `True` if the next `reset_database_for_test` for this database
        will truncate rather than rebuild.
    """
    return _database_name(engine) in _schema_ready


def invalidate_schema_ready(engine: AsyncEngine) -> None:
    """Forget that a database holds the canonical schema.

    Anything that drops the schema or steps Alembic away from head must
    call this, so the next `reset_database_for_test` rebuilds the
    canonical ``create_all`` schema instead of truncating tables that may
    no longer exist or no longer have the columns the suite expects.

    Parameters
    ----------
    engine
        Database engine naming the database to invalidate.
    """
    _schema_ready.discard(_database_name(engine))


def ddl_database_url_for(database_url: str) -> str:
    """Return the URL of the DDL sibling of a test database URL.

    Parameters
    ----------
    database_url
        URL of the database the rest of the suite shares.

    Returns
    -------
    str
        The same URL with `DDL_DATABASE_SUFFIX` appended to the database
        name.
    """
    url = make_url(database_url)
    if url.database is None:  # pragma: no cover - Postgres URLs name one
        msg = f"Database URL {database_url} does not name a database"
        raise RuntimeError(msg)
    sibling = url.set(database=url.database + DDL_DATABASE_SUFFIX)
    return sibling.render_as_string(hide_password=False)


async def provision_database(
    database_url: str, password: SecretStr | str | None
) -> None:
    """Drop and recreate the database a URL names, with ``pg_trgm``.

    Every test database in this suite needs the same server-side
    provisioning, so the per-worker databases of a future xdist run and
    the DDL database share this one helper: a new extension requirement
    then lands on all of them at once.

    Parameters
    ----------
    database_url
        URL of the database to (re)create. Its database must not be the
        one the connection is made through.
    password
        Password for the database connection.

    Raises
    ------
    RuntimeError
        Raised if the URL names no database, or names one whose name
        cannot be safely quoted into a ``CREATE DATABASE`` statement.
    """
    url = make_url(database_url)
    name = url.database
    if name is None or not _SAFE_DATABASE_NAME.match(name):
        msg = f"Refusing to provision test database with name {name!r}"
        raise RuntimeError(msg)

    # CREATE DATABASE and DROP DATABASE cannot run inside a transaction,
    # hence AUTOCOMMIT, and they cannot run from a connection to the
    # database being replaced, hence the maintenance database.
    admin_url = url.set(database=_MAINTENANCE_DATABASE)
    admin = create_database_engine(
        admin_url.render_as_string(hide_password=False),
        password,
        isolation_level="AUTOCOMMIT",
    )
    try:
        async with admin.connect() as conn:
            await conn.execute(
                text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            )
            await conn.execute(text(f'CREATE DATABASE "{name}"'))
    finally:
        await admin.dispose()
    _schema_ready.discard(name)

    engine = create_database_engine(database_url, password)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    finally:
        await engine.dispose()


@asynccontextmanager
async def ddl_database(
    database_url: str, password: SecretStr | str | None
) -> AsyncIterator[AsyncEngine]:
    """Yield an engine on an emptied database for a schema test.

    The schema is dropped on entry, so the caller starts from nothing and
    can step Alembic forward from any revision. On exit — whether the
    test passed, failed, or raised out of the drop itself — the database
    is marked as no longer holding the canonical schema, so the next
    `reset_database_for_test` rebuilds it rather than truncating whatever
    the migration under test happened to leave behind.

    Parameters
    ----------
    database_url
        URL of the DDL database, from `ddl_database_url_for`.
    password
        Password for the database connection.

    Yields
    ------
    sqlalchemy.ext.asyncio.AsyncEngine
        Engine on the emptied database.
    """
    engine = create_database_engine(database_url, password)
    try:
        await drop_database(engine, Base.metadata)
        yield engine
    finally:
        invalidate_schema_ready(engine)
        await engine.dispose()


def _database_name(engine: AsyncEngine) -> str:
    """Return the database an engine is connected to."""
    database = engine.url.database
    if database is None:  # pragma: no cover - Postgres URLs always name one
        msg = "Test database engine URL does not name a database"
        raise RuntimeError(msg)
    return database


async def truncate_all_tables(
    engine: AsyncEngine,
    *,
    lock_timeout: str = TRUNCATE_LOCK_TIMEOUT,
    attempts: int = TRUNCATE_ATTEMPTS,
    retry_delay: float = TRUNCATE_RETRY_DELAY,
) -> None:
    """Truncate every schema table, bounded by a lock timeout.

    An attempt that loses its table locks is retried; any other database
    error propagates immediately.

    Parameters
    ----------
    engine
        Database engine to use.
    lock_timeout
        PostgreSQL ``lock_timeout`` value applied to each attempt.
    attempts
        Number of attempts before giving up.
    retry_delay
        Seconds to sleep between attempts.

    Raises
    ------
    TruncateLockError
        Raised if every attempt lost its table locks. The message names
        the other sessions on the database.
    """
    tables = ", ".join(
        f'"{table.name}"' for table in Base.metadata.sorted_tables
    )
    statement = text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")
    for attempt in range(1, attempts + 1):
        error = await _attempt_truncate(engine, statement, lock_timeout)
        if error is None:
            return
        if attempt == attempts:
            blockers = await _describe_other_sessions(engine)
            raise TruncateLockError(
                f"Could not truncate the test database after {attempts}"
                f" attempts with lock_timeout={lock_timeout}: another"
                " session is holding conflicting table locks. Sessions on"
                f" {engine.url.database}:\n{blockers}"
            ) from error
        await asyncio.sleep(retry_delay)


async def _attempt_truncate(
    engine: AsyncEngine, statement: TextClause, lock_timeout: str
) -> DBAPIError | None:
    """Run one truncate attempt, returning its retryable error, if any.

    Any other database error propagates: only transient lock contention
    is worth retrying.
    """
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(f"SET LOCAL lock_timeout = '{lock_timeout}'")
            )
            await conn.execute(statement)
    except DBAPIError as exc:
        if not _is_retryable_truncate_error(exc):
            raise
        return exc
    return None


def _sqlstate(exc: DBAPIError) -> str | None:
    """Return the PostgreSQL SQLSTATE a database error carries.

    SQLAlchemy's asyncpg adapter sets both ``sqlstate`` and ``pgcode`` on
    the DBAPI error it raises (see ``_handle_exception`` in
    ``sqlalchemy/dialects/postgresql/asyncpg.py``), so ``orig.sqlstate``
    is the single authoritative place to look.
    """
    return getattr(exc.orig, "sqlstate", None)


def _is_retryable_truncate_error(exc: DBAPIError) -> bool:
    """Return whether a failed truncate is transient lock contention."""
    return _sqlstate(exc) in _RETRYABLE_TRUNCATE_SQLSTATES


async def _describe_other_sessions(engine: AsyncEngine) -> str:
    """Return a report of the other sessions connected to this database.

    Any of them holding an open transaction is a candidate blocker of the
    truncate that just timed out; ``pg_blocking_pids`` additionally shows
    lock chains among them.
    """
    query = text(
        "SELECT pid, state, wait_event_type, wait_event,"
        " pg_blocking_pids(pid) AS blocked_by,"
        " extract(epoch from (now() - xact_start)) AS xact_age,"
        " left(query, 300) AS query"
        " FROM pg_stat_activity"
        " WHERE datname = current_database() AND pid <> pg_backend_pid()"
        " ORDER BY xact_start NULLS LAST"
    )
    try:
        async with engine.connect() as conn:
            rows = (await conn.execute(query)).mappings().all()
    except DBAPIError as exc:  # pragma: no cover - diagnostics must not mask
        return f"(could not query pg_stat_activity: {exc})"
    if not rows:
        return "(no other sessions; the blocker disconnected)"
    lines = []
    for row in rows:
        age = row["xact_age"]
        age_text = "no transaction" if age is None else f"xact {age:.1f}s old"
        lines.append(
            f"  pid={row['pid']} state={row['state']} {age_text}"
            f" wait={row['wait_event_type']}/{row['wait_event']}"
            f" blocked_by={list(row['blocked_by'])}\n"
            f"    query: {row['query']!r}"
        )
    return "\n".join(lines)
