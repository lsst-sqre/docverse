from __future__ import annotations

import json
import os
import re
import subprocess
from itertools import pairwise
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

import nox
from nox_uv import session

if TYPE_CHECKING:
    from collections.abc import Sequence

    from testcontainers.postgres import PostgresContainer

nox.needs_version = ">=2024.4.15"
nox.options.default_venv_backend = "uv"
nox.options.sessions = ["lint", "typing", "test", "client_test"]

TESTS_ROOT = "tests"
"""Directory holding the server test suite."""

INTEGRATION_TEST_PATH = "tests/integration"
"""Path excluded from the ``test`` session; it needs live services."""

# The attached spellings of pytest-xdist's short worker option: a count,
# or one of the two names it derives a count from. Matching the bare "-n"
# prefix instead would read any single-dash option starting with an "n" as
# a worker request.
XDIST_ATTACHED_WORKERS = re.compile(r"-n(?P<workers>\d+|auto|logical)")

# Posargs that ask for something pytest-xdist workers take away. xdist
# refuses --pdb outright ("--pdb is incompatible with distributing tests")
# and the workers swallow the output -s exists to show, so an invocation
# carrying one of these runs in a single process. --capture also accepts
# its value as the following posarg, which is handled separately.
#
# --trace belongs here for a worse reason than --pdb does: xdist's refusal
# reads pytest's "usepdb" option, which only --pdb sets, so --trace gets
# no refusal at all and its workers instead die the moment pdb reads EOF
# from a worker stdin nobody can type into. That is the
# start-the-container-then-die failure this list exists to avoid, only
# louder.
#
# --pdbcls is deliberately absent. It only names the class that --pdb,
# --trace, or pytest.set_trace() would instantiate, so on its own it
# starts no debugger and takes nothing away from the workers.
SERIAL_DEBUG_OPTIONS = frozenset({"--pdb", "--trace", "-s", "--capture=no"})

# The short options pytest and pytest-xdist take a value for. Short
# options run together into one posarg -- "-xs" is "-x -s", "-xn2" is
# "-x -n 2" -- are consumed left to right until one of these, which takes
# whatever is left of the bundle as its value, or the next posarg when
# nothing is. So the "s" ending "-rs" is part of -r's report characters
# and the one ending "-ps" is a plugin name; neither is --capture=no, and
# scanning a bundle for bare letters would read both as one.
SHORT_VALUE_OPTIONS = frozenset("ckmnoprW")

# Ceiling on the worker count detected for pytest-xdist below.
#
# The workers themselves run on the host, but everything a worker drives
# lives in the one Docker VM the PostgreSQL container sits in: a database,
# an application lifespan, and a connection pool apiece, all against that
# single server. Colima's default profile gives the VM 8 CPUs, so on a
# wider host the extra workers only queue against each other inside it.
XDIST_WORKER_CAP = 8

# Pytest options that take their value as a separate argument, skipped
# outright so a value that does live under ``tests/`` -- ``--basetemp
# tests/tmp``, ``--ignore tests/services`` -- is not read as a selection.
# This list is a shortcut, not the rule: it is hand-curated and pytest and
# its plugins keep growing options, so `_path_selections` decides on the
# tests/-rooted check instead of on membership here.
PYTEST_VALUE_OPTIONS = frozenset(
    {
        "-c",
        "--basetemp",
        "--config-file",
        "--deselect",
        "--dist",
        "-k",
        "-m",
        "--ignore",
        "--ignore-glob",
        "--maxfail",
        "-n",
        "--numprocesses",
        "-o",
        "--override-ini",
        "-p",
        "--rootdir",
    }
)


def _expand_short_bundles(posargs: Sequence[str]) -> list[str]:
    """Return ``posargs`` with bundled short options written out singly.

    pytest honors every option in a bundle, so the reading below has to
    see them all: ``-xs`` really does turn capturing off and ``-xn2``
    really does ask for two workers. Each bundle is split the way
    ``argparse`` splits it -- flag by flag until a `SHORT_VALUE_OPTIONS`
    member, which keeps the rest of the bundle as its attached value and
    ends it.

    Long options, plain posargs, and short options that are already single
    pass through untouched, so this only ever adds detail.
    """
    expanded: list[str] = []
    for arg in posargs:
        is_bundle = (
            arg.startswith("-") and not arg.startswith("--") and len(arg) > 2
        )
        if not is_bundle:
            expanded.append(arg)
            continue
        for index, letter in enumerate(arg[1:], start=1):
            if letter in SHORT_VALUE_OPTIONS:
                expanded.append(f"-{letter}{arg[index + 1 :]}")
                break
            expanded.append(f"-{letter}")
    return expanded


def _default_xdist_workers() -> int:
    """Return the number of pytest-xdist workers to run by default.

    One per usable CPU, capped at `XDIST_WORKER_CAP` because the workers
    share the container's Docker VM. ``os.process_cpu_count`` reports the
    CPUs this process may actually use rather than the machine's total, so
    an affinity-restricted or cgroup-limited runner is read correctly; it
    returns `None` when it cannot tell, which is worth one worker.
    """
    return min(os.process_cpu_count() or 1, XDIST_WORKER_CAP)


def _requested_xdist_workers(posargs: Sequence[str]) -> str | None:
    """Return the worker count ``posargs`` asks pytest-xdist for, if any.

    Recognizes every spelling pytest-xdist accepts -- ``-n 4``, ``-n4``,
    ``--numprocesses 4``, ``--numprocesses=4`` -- and returns the value as
    written, or ``""`` for a trailing ``-n`` with no value at all.
    Anything else, including an unrelated single-dash option that merely
    starts with an ``n``, is not a worker request and returns `None`.
    """
    for index, arg in enumerate(posargs):
        if arg in {"-n", "--numprocesses"}:
            return posargs[index + 1] if index + 1 < len(posargs) else ""
        if arg.startswith("--numprocesses="):
            return arg.removeprefix("--numprocesses=")
        attached = XDIST_ATTACHED_WORKERS.fullmatch(arg)
        if attached is not None:
            return attached["workers"]
    return None


def _wants_single_process(posargs: Sequence[str]) -> bool:
    """Report whether ``posargs`` asks for something workers would defeat."""
    if not SERIAL_DEBUG_OPTIONS.isdisjoint(posargs):
        return True
    return any(
        arg == "--capture" and value == "no"
        for arg, value in pairwise(posargs)
    )


def _xdist_args(posargs: Sequence[str]) -> list[str]:
    """Return default pytest-xdist arguments for a pytest invocation.

    The suite runs under pytest-xdist by default, one worker per usable
    CPU up to `XDIST_WORKER_CAP`; each worker gets its own PostgreSQL
    database via the isolation shim in ``tests/support/xdist.py``. Pass
    your own ``-n`` in posargs (e.g. ``-n 0`` for a serial run) to
    override.

    Debugging invocations get the single process they need without asking:
    posargs carrying ``--pdb``, ``--trace``, ``-s``, or ``--capture=no``
    suppress the injection too, because the workers would otherwise refuse
    the debugger, crash on it, or swallow the output -- after the
    container has already started, and with nothing to say the ``-n`` came
    from here.

    Both readings run over `_expand_short_bundles`, because pytest accepts
    either request run together with other short options and honors it
    just the same.
    """
    expanded = _expand_short_bundles(posargs)
    if _requested_xdist_workers(expanded) is not None:
        return []
    if _wants_single_process(expanded):
        return []
    return ["-n", str(_default_xdist_workers())]


def _test_relative_path(arg: str) -> PurePosixPath | None:
    """Return ``arg`` as a repository-relative path under ``tests/``.

    Strips any ``::`` node identifier, resolves the rest against the
    session's working directory (nox runs sessions from the directory
    holding this noxfile, which is what pytest resolves a relative posarg
    against too), and returns `None` unless the result exists and lives in
    the test suite -- ``tests`` itself included.
    """
    path = Path(arg.split("::", 1)[0])
    if not path.exists():
        return None
    root = Path.cwd().resolve()
    try:
        relative = path.resolve().relative_to(root / TESTS_ROOT)
    except (OSError, ValueError):
        return None
    return PurePosixPath(TESTS_ROOT, relative.as_posix())


def _path_selections(posargs: Sequence[str]) -> list[str]:
    """Return the posargs that select tests by path.

    A posarg selects tests when it is not an option and it resolves to an
    existing path under ``tests/``, once any ``::`` node identifier is
    stripped. Everything else -- ``-x``, ``-k expr``, ``-n 2`` -- is a way
    of running the selection, not a selection.

    Deciding on the tests/-rooted path rather than on
    `PYTEST_VALUE_OPTIONS` is what keeps an option this noxfile has never
    heard of from having its value misread: ``--cov-config pyproject.toml``
    names a file that exists, but not one this suite could ever collect.
    An unknown option whose value does point into ``tests/`` is still
    misread, which is what `PYTEST_VALUE_OPTIONS` remains good for.

    Reading `_expand_short_bundles` rather than ``posargs`` is what lets a
    value option reached through a bundle claim its value: the expression
    in ``-xk tests`` belongs to the ``-k`` ending that bundle, not to this
    session's selection.
    """
    selections: list[str] = []
    skip_value = False
    for arg in _expand_short_bundles(posargs):
        if skip_value:
            skip_value = False
        elif arg.startswith("-"):
            skip_value = arg in PYTEST_VALUE_OPTIONS
        elif _test_relative_path(arg) is not None:
            selections.append(arg)
    return selections


def _test_args(posargs: Sequence[str]) -> list[str]:
    """Compose the pytest arguments for the ``test`` session.

    The `INTEGRATION_TEST_PATH` ignore always applies, so an invocation
    that only passes options (``nox -s test -- -x``) still runs the whole
    container-backed selection. A posarg that names a path under
    ``tests/`` *replaces* the default ``tests`` selection instead of
    adding to it, so ``nox -s test -- tests/handlers`` runs the handlers
    and nothing else.
    """
    args = [*_xdist_args(posargs), f"--ignore={INTEGRATION_TEST_PATH}"]
    if not _path_selections(posargs):
        args.append(TESTS_ROOT)
    args.extend(posargs)
    return args


def _setup_testcontainers_env() -> None:
    """Configure testcontainers environment variables for Colima on macOS.

    This must be called before any containers are started to ensure the
    Reaper can connect properly when using Colima as the Docker runtime.
    """
    # Set testcontainers host override for Colima on macOS
    # This fixes "nodename nor servname provided, or not known" errors
    docker_host = os.getenv("DOCKER_HOST", "")
    m = re.search(r"\.colima/(?P<profile>[^/]+)/docker\.sock$", docker_host)
    if m:
        # Extract the Colima VM IP address for the active profile.
        # colima ls -j emits one JSON object per line (one per profile).
        try:
            result = subprocess.run(
                ["colima", "ls", "-j"],
                capture_output=True,
                text=True,
                check=True,
            )
            for line in result.stdout.splitlines():
                if not line.strip():
                    continue
                colima_info = json.loads(line)
                if colima_info.get("name") == m["profile"] and colima_info.get(
                    "address"
                ):
                    os.environ["TESTCONTAINERS_HOST_OVERRIDE"] = colima_info[
                        "address"
                    ]
                    break
        except (
            subprocess.CalledProcessError,
            json.JSONDecodeError,
            KeyError,
        ):
            # If we can't get the Colima address, don't set override
            pass


def _install_pg_extensions(postgres: PostgresContainer) -> None:
    """Install PostgreSQL extensions in the container via docker exec."""
    cmd = (
        f"psql -U {postgres.username} -d {postgres.dbname}"
        " -c 'CREATE EXTENSION IF NOT EXISTS pg_trgm'"
    )
    result = postgres.exec(cmd)
    if result.exit_code != 0:
        msg = f"Failed to install pg_trgm: {result.output.decode()}"
        raise RuntimeError(msg)


@session(uv_only_groups=["lint"], uv_no_install_project=True)
def lint(session: nox.Session) -> None:
    """Run pre-commit hooks with prek."""
    session.run("prek", "run", "--all-files", *session.posargs)


@session(uv_groups=["typing"])
def typing(session: nox.Session) -> None:
    session.run(
        "mypy",
        "noxfile.py",
        "src/",
        "client/src/",
        "tests/",
        *session.posargs,
    )


@session(uv_groups=["dev"])
def test(session: nox.Session) -> None:
    _setup_testcontainers_env()

    # Import after setting environment variables so config is read correctly
    from testcontainers.postgres import PostgresContainer  # noqa: PLC0415

    with PostgresContainer("postgres:17") as postgres:
        _install_pg_extensions(postgres)
        url = postgres.get_connection_url(driver="asyncpg")
        session.run(
            "pytest",
            *_test_args(session.posargs),
            env={
                "DOCVERSE_DATABASE_URL": url,
                "DOCVERSE_DATABASE_PASSWORD": postgres.password,
                "DOCVERSE_ALEMBIC_CONFIG_PATH": "alembic.ini",
                "DOCVERSE_ARQ_MODE": "test",
                "DOCVERSE_CREDENTIAL_ENCRYPTION_KEY": (
                    "nz4oCndEIQhi-PlZBzYzmK_jlacf05Hz3VnrRrZhq-k="
                ),
                "REPERTOIRE_BASE_URL": (
                    "https://roundtable.lsst.cloud/repertoire"
                ),
                # Resolve Configuration.metrics to a MockEventManager so
                # the app/workers start without Kafka and tests can assert
                # on published events.
                "METRICS_APPLICATION": "docverse",
                "METRICS_ENABLED": "false",
                "METRICS_MOCK": "true",
            },
        )


@session(uv_groups=["dev"])
def client_test(session: nox.Session) -> None:
    session.run(
        "pytest",
        "client/tests/",
        *session.posargs,
    )


@nox.session(python=["3.12", "3.13"], venv_backend="uv")
def client_test_compat(session: nox.Session) -> None:
    session.install(
        "-e",
        "./client",
        "pytest",
        "pytest-asyncio",
        "respx",
        "coverage[toml]",
    )
    session.run(
        "pytest",
        "client/tests/",
        *session.posargs,
    )


@nox.session(python=["3.12"], venv_backend="uv")
def client_test_oldest(session: nox.Session) -> None:
    """Test the client with the oldest supported dependencies."""
    session.install(
        "-e",
        "./client",
        "pytest>=8",
        "pytest-asyncio>=0.24",
        "respx>=0.21",
        "coverage[toml]>=7",
        env={"UV_RESOLUTION": "lowest-direct"},
    )
    session.run(
        "pytest",
        "client/tests/",
        *session.posargs,
    )


@session(uv_groups=["dev"])
def deploy_worker_test(session: nox.Session) -> None:
    """Integration test for deploy-worker (requires Node.js/npm)."""
    session.run(
        "pytest",
        "client/tests/deploy_worker_integration_test.py",
        *session.posargs,
    )


@nox.session
def scriv_create(session: nox.Session) -> None:
    session.install("scriv")
    config = "scriv-server.ini"
    if session.posargs and session.posargs[0] == "client":
        config = "scriv-client.ini"
    session.run("scriv", "create", "--config", config)


@session(uv_groups=["dev"])
def create_migration(session: nox.Session) -> None:
    """Create an Alembic migration.

    Pass the migration message as a positional argument:
        nox -s create-migration -- "Add organization table"
    """
    if not session.posargs:
        session.error(
            'Provide a migration message: nox -s create-migration -- "message"'
        )

    message = session.posargs[0]

    _setup_testcontainers_env()

    # Import after setting environment variables so config is read correctly
    from testcontainers.postgres import PostgresContainer  # noqa: PLC0415

    with PostgresContainer("postgres:17") as postgres:
        _install_pg_extensions(postgres)
        url = postgres.get_connection_url(driver=None)
        env = {
            "DOCVERSE_DATABASE_URL": url,
            "DOCVERSE_DATABASE_PASSWORD": postgres.password,
            "DOCVERSE_ARQ_MODE": "test",
            "DOCVERSE_CREDENTIAL_ENCRYPTION_KEY": (
                "nz4oCndEIQhi-PlZBzYzmK_jlacf05Hz3VnrRrZhq-k="
            ),
            "REPERTOIRE_BASE_URL": (
                "https://roundtable.lsst.cloud/repertoire"
            ),
            # ``alembic/env.py`` imports ``docverse_server.config``, whose
            # ``metrics`` field resolves from these vars; the mock
            # manager keeps migration autogeneration Kafka-free.
            "METRICS_APPLICATION": "docverse",
            "METRICS_ENABLED": "false",
            "METRICS_MOCK": "true",
        }
        session.run("alembic", "upgrade", "head", env=env)
        session.run(
            "alembic",
            "revision",
            "--autogenerate",
            "-m",
            message,
            env=env,
        )
