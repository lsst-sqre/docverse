"""Tests for the pytest-xdist worker isolation shim.

The shim in `tests.support.xdist` rewrites the environment so each xdist
worker gets its own PostgreSQL database, and it only works if it runs
before ``docverse_server.config`` builds its module-level
``Configuration()`` singleton. These tests pin down both halves of that
contract: the import chain that keeps the shim reachable without dragging
``docverse_server.config`` in, and the collection-time check that fails
loudly when the order is broken anyway.

They need no database: the import guard runs in a subprocess that imports
and exits, and the verification tests call the checker directly with the
values a mis-ordered import would have left in the configuration.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from docverse_server.config import config

from .support.database import DDL_DATABASE_SUFFIX, ddl_database_url_for
from .support.xdist import XdistIsolationError, verify_worker_isolation

_REPO_ROOT = Path(__file__).parents[1]

_DATABASE_SERVER = "postgresql+asyncpg://docverse@127.0.0.1:5432"

_UNISOLATED_SETTINGS = {
    "database_url": f"{_DATABASE_SERVER}/docverse",
    "ddl_database_url": f"{_DATABASE_SERVER}/docverse{DDL_DATABASE_SUFFIX}",
}
"""The settings the suite holds when the shim never reached the config.

These are the base testcontainers database and the DDL sibling derived
from it -- exactly what a ``Configuration()`` built before
`isolate_xdist_worker` ran would carry, and what every worker would then
share.
"""


def _isolated_settings(worker_id: str) -> dict[str, str]:
    """Return the settings a correctly ordered import leaves for a worker."""
    base = f"{_DATABASE_SERVER}/docverse_{worker_id}"
    return {
        "database_url": base,
        "ddl_database_url": f"{base}{DDL_DATABASE_SUFFIX}",
    }


_IMPORT_PROBE = """
import sys

import tests.support.xdist  # noqa: F401

print(",".join(sorted(m for m in sys.modules if m.startswith("docverse"))))
"""


def test_shim_import_does_not_reach_server_config() -> None:
    """Importing the shim must not import ``docverse_server.config``.

    That module instantiates ``Configuration()`` at import time,
    snapshotting the environment as it stands then. If anything the shim
    reaches imported it, the rewrite would land after the snapshot and
    every worker would silently share one database, with each worker's
    per-test truncate clobbering the tests running in the others. This
    runs in a subprocess because the pytest process has the config
    imported already, by way of this very module.
    """
    env = {
        key: value
        for key, value in os.environ.items()
        if key != "PYTEST_XDIST_WORKER"
    }
    env["PYTHONPATH"] = str(_REPO_ROOT)
    result = subprocess.run(
        [sys.executable, "-c", _IMPORT_PROBE],
        cwd=_REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"importing tests.support.xdist failed:\n{result.stderr}"
    )
    imported = [name for name in result.stdout.split(",") if name]
    assert "docverse_server.config" not in imported, (
        "importing tests.support.xdist pulled in docverse_server.config"
        f" (via {imported}), which snapshots the environment the shim is"
        " supposed to rewrite first"
    )


def test_verification_rejects_a_configuration_that_missed_the_shim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A mis-ordered import fails collection instead of sharing a database.

    This simulates the outcome of importing ``docverse_server.config``
    ahead of the shim: the configuration keeps the base database while
    the worker believes it is isolated. Without the check the run looks
    healthy and the workers truncate each other's databases mid-test.
    """
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")

    with pytest.raises(XdistIsolationError) as excinfo:
        verify_worker_isolation(**_UNISOLATED_SETTINGS)

    message = str(excinfo.value)
    assert "gw0" in message
    # Every setting the shim owns is named, so the failure is diagnosable.
    assert "database_url" in message
    assert "ddl_database_url" in message
    assert "'docverse'" in message
    assert f"'docverse{DDL_DATABASE_SUFFIX}'" in message
    # ... and so is the contract that was broken.
    assert "import-order contract" in message
    assert "isolate_xdist_worker" in message
    assert "docverse_server.config" in message


def test_verification_rejects_a_shared_ddl_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DDL name that stopped composing with the worker name is caught.

    ``ddl_database_url_for`` appends its suffix to whatever database the
    suite is on, so per-worker isolation reaches the schema tests for
    free. A fixed DDL name would put every worker's Alembic run back on
    one database while the rest of the suite still looked isolated.
    """
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw1")
    settings = _isolated_settings("gw1")
    settings["ddl_database_url"] = _UNISOLATED_SETTINGS["ddl_database_url"]

    with pytest.raises(XdistIsolationError) as excinfo:
        verify_worker_isolation(**settings)

    message = str(excinfo.value)
    assert "ddl_database_url" in message
    # Only the DDL database is named: the rest of the suite is isolated.
    assert message.count("expected a name ending in") == 1


def test_verification_accepts_an_isolated_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The settings the shim actually writes pass the check."""
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")

    verify_worker_isolation(**_isolated_settings("gw3"))


def test_verification_is_a_no_op_outside_xdist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A serial run has nothing to isolate, so base settings are correct."""
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)

    verify_worker_isolation(**_UNISOLATED_SETTINGS)


def test_this_worker_really_is_isolated() -> None:
    """The live configuration carries this worker's database.

    Everything else here exercises the checker with made-up settings, and
    the checker itself returns early when ``PYTEST_XDIST_WORKER`` is
    unset. This asserts against the real ``config``, so the parallel run
    the whole shim exists to protect cannot pass while quietly sharing one
    database.
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id is None:
        pytest.skip("not running under pytest-xdist")

    verify_worker_isolation(
        database_url=config.database_url,
        ddl_database_url=ddl_database_url_for(config.database_url),
    )
