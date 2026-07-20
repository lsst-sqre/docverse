from __future__ import annotations

import json
import os
import re
import subprocess
from typing import TYPE_CHECKING

import nox
from nox_uv import session

if TYPE_CHECKING:
    from testcontainers.postgres import PostgresContainer

nox.needs_version = ">=2024.4.15"
nox.options.default_venv_backend = "uv"
nox.options.sessions = ["lint", "typing", "test", "client_test"]


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
    """Run pre-commit hooks."""
    session.run("pre-commit", "run", "--all-files", *session.posargs)


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
            "--ignore=tests/integration",
            "tests/",
            *session.posargs,
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
            # ``alembic/env.py`` imports ``docverse.config``, whose
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
