"""Per-worker isolation for pytest-xdist runs.

**Nothing this module imports may reach ``docverse_server.config``, at any
depth.** That module builds its ``config = Configuration()`` singleton at
import time, snapshotting the environment exactly as it stands then. The
whole point of `isolate_xdist_worker` is to rewrite that environment
first, so if importing this module dragged the config in, the rewrite
would land after the snapshot: every worker would keep the base database,
and the per-test truncate in one worker would clobber the tests running in
the others -- silently, because everything still "works". The only
application module reachable from here is ``docverse_server.dbschema`` (by
way of `tests.support.database`), which imports SQLAlchemy and the shared
Pydantic models and nothing else of the application. Keep it that way;
``tests/xdist_isolation_test.py`` guards the chain, and
`verify_worker_isolation` catches a broken import order at collection time.
"""

from __future__ import annotations

import asyncio
import os

from sqlalchemy.engine import make_url

from .database import DDL_DATABASE_SUFFIX, provision_database

__all__ = [
    "DATABASE_PASSWORD_ENV",
    "DATABASE_URL_ENV",
    "XdistIsolationError",
    "isolate_xdist_worker",
    "verify_worker_isolation",
]

DATABASE_URL_ENV = "DOCVERSE_DATABASE_URL"
"""Environment variable naming the database the suite runs against."""

DATABASE_PASSWORD_ENV = "DOCVERSE_DATABASE_PASSWORD"
"""Environment variable holding the test database password."""

_WORKER_DATABASE_SUFFIXES = {
    "database_url": "",
    "ddl_database_url": DDL_DATABASE_SUFFIX,
}
"""Per-worker settings, keyed by the name `verify_worker_isolation` reports.

Each value is what follows the ``_{worker_id}`` marker in that setting's
database name. ``database_url`` is what `isolate_xdist_worker` rewrites
directly; ``ddl_database_url`` is
:func:`~tests.support.database.ddl_database_url_for` applied to it, and is
checked as well because it isolates the schema tests only for as long as
it keeps *appending* to the shared name rather than naming a database of
its own.
"""

_IMPORT_ORDER_CONTRACT = (
    "The import-order contract: tests/conftest.py must call"
    " tests.support.xdist.isolate_xdist_worker() before anything imports"
    " docverse_server.config, which snapshots the environment into its"
    " module-level Configuration() singleton as it is imported. An import"
    " that gets ahead of the shim -- one moved above the call in"
    " conftest.py, one tests/worker/conftest.py makes before its own"
    " Configuration(), or one tests.support.xdist newly reaches"
    " transitively -- leaves every worker on the same database, where each"
    " worker's per-test truncate clobbers the tests running in the others."
)

_isolated_worker: str | None = None
"""Worker this process has already provisioned a database for, if any."""


class XdistIsolationError(RuntimeError):
    """A pytest-xdist worker is running without its own database.

    Raised at collection time by `verify_worker_isolation`, which is the
    only thing standing between a broken import order and a run whose
    failures look like flaky tests.
    """


def isolate_xdist_worker() -> None:
    """Give this pytest-xdist worker its own database.

    Under pytest-xdist every worker is a separate process that would
    otherwise share the single testcontainers PostgreSQL database, and the
    per-test truncate in the ``app`` fixture would clobber the tests
    running concurrently in the other workers. Call this at conftest
    import time in each worker process -- before ``docverse_server.config``
    is imported, so before the module-level ``Configuration()`` singleton
    reads the environment. It creates a dedicated database named after the
    worker (``<base>_gw0``, ...) on the shared container, provisioned the
    same way every other test database is, and points
    ``DOCVERSE_DATABASE_URL`` at it. The schema tests follow along for
    free: :func:`~tests.support.database.ddl_database_url_for` derives
    their sibling from whatever database this leaves behind, so ``gw0``
    gets ``<base>_gw0_ddl``.

    In a non-xdist (serial) run ``PYTEST_XDIST_WORKER`` is unset and this
    is a no-op: the suite runs against the base database exactly as it
    would without xdist.

    Calling this a second time in the same process is also a no-op.
    Provisioning drops the database first, so a repeat call -- from a
    second conftest that wanted to be sure -- would otherwise delete the
    schema out from under a run already using it.

    Raises
    ------
    XdistIsolationError
        Raised if there is no database URL in the environment to derive a
        per-worker name from.
    """
    global _isolated_worker

    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id is None or worker_id == _isolated_worker:
        return

    base = os.environ.get(DATABASE_URL_ENV)
    if base is None:
        raise XdistIsolationError(
            f"{DATABASE_URL_ENV} is not set, so pytest-xdist worker"
            f" {worker_id} has no database server to create its own"
            " database on. Run the suite through 'nox -s test', which"
            " starts PostgreSQL and sets it."
        )
    url = make_url(base)
    if url.database is None:
        raise XdistIsolationError(
            f"{DATABASE_URL_ENV} ({base}) does not name a database, so"
            f" pytest-xdist worker {worker_id} has no name to derive its"
            " own database from."
        )

    worker_url = url.set(
        database=f"{url.database}_{worker_id}"
    ).render_as_string(hide_password=False)
    asyncio.run(
        provision_database(worker_url, os.environ.get(DATABASE_PASSWORD_ENV))
    )
    os.environ[DATABASE_URL_ENV] = worker_url
    _isolated_worker = worker_id


def verify_worker_isolation(
    *, database_url: str, ddl_database_url: str
) -> None:
    """Check that the configuration picked up this worker's isolation.

    `isolate_xdist_worker` only takes effect if it runs before
    ``docverse_server.config`` is imported, and that ordering is otherwise
    enforced by nothing but a comment -- the ``noqa: E402`` in
    ``tests/conftest.py`` silences the one lint rule that would notice an
    import jumping the queue. Call this from a conftest *after* the
    ``docverse_server`` imports, passing the settings the shim is
    responsible for, so a broken order fails collection instead of quietly
    collapsing every worker onto one database.

    Outside pytest-xdist there is nothing to isolate and this is a no-op.

    Parameters
    ----------
    database_url
        ``config.database_url``.
    ddl_database_url
        :func:`~tests.support.database.ddl_database_url_for` applied to
        ``config.database_url``.

    Raises
    ------
    XdistIsolationError
        Raised if either database is not the one this worker's shim wrote.
        The message names each mismatch and the import-order contract that
        was broken.
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id is None:
        return

    values = {
        "database_url": database_url,
        "ddl_database_url": ddl_database_url,
    }
    problems = []
    for setting, suffix in _WORKER_DATABASE_SUFFIXES.items():
        database = make_url(values[setting]).database
        expected = f"_{worker_id}{suffix}"
        if database is None or not database.endswith(expected):
            problems.append(
                f"{setting} names database {database!r}, expected a name"
                f" ending in {expected!r}"
            )
    if problems:
        raise XdistIsolationError(
            f"pytest-xdist worker {worker_id} is not isolated:"
            f" {'; '.join(problems)}. {_IMPORT_ORDER_CONTRACT}"
        )
