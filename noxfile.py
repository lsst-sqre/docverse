import nox
from nox_uv import session

nox.needs_version = ">=2024.4.15"
nox.options.default_venv_backend = "uv"
nox.options.sessions = ["lint", "typing", "test"]


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
    session.run(
        "pytest",
        "tests/",
        *session.posargs,
        env={
            "REPERTOIRE_BASE_URL": "https://roundtable.lsst.cloud/repertoire"
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


@nox.session
def scriv_create(session: nox.Session) -> None:
    session.install("scriv")
    config = "scriv-server.ini"
    if session.posargs and session.posargs[0] == "client":
        config = "scriv-client.ini"
    session.run("scriv", "create", "--config", config)
