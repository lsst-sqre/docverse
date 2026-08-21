"""Tests for the pytest arguments the ``test`` nox session composes.

The session injects a pytest-xdist worker count that an invocation can
turn down or take away, and it applies the ``tests/integration`` ignore on
top of whatever posargs a developer passes -- which only stays correct
while a path posarg *replaces* the default ``tests`` selection instead of
adding to it. These tests load ``noxfile.py`` and pin both compositions
down.

Nox itself is not installed in the test environment -- it lives in the
``nox`` dependency group, which bootstraps the sessions from outside -- so
the two modules the noxfile imports for its decorators are stubbed before
it is loaded.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).parents[1]

PytestArgs = Callable[[Sequence[str]], list[str]]
"""The signature of the noxfile's argument composers."""


def _session_decorator_stub(*args: Any, **kwargs: Any) -> Any:
    """Stand in for ``nox.session`` and ``nox_uv.session``.

    The noxfile applies both as ``@session(...)``; ``nox.session`` also
    supports a bare ``@session``.
    """
    if args and callable(args[0]):
        return args[0]

    def decorate(func: Any) -> Any:
        return func

    return decorate


@pytest.fixture(scope="module")
def noxfile() -> Iterator[Any]:
    """Load ``noxfile.py`` with the nox packages stubbed out."""
    nox_stub: Any = types.ModuleType("nox")
    nox_stub.options = types.SimpleNamespace()
    nox_stub.session = _session_decorator_stub
    nox_stub.Session = object
    nox_uv_stub: Any = types.ModuleType("nox_uv")
    nox_uv_stub.session = _session_decorator_stub

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setitem(sys.modules, "nox", nox_stub)
        monkeypatch.setitem(sys.modules, "nox_uv", nox_uv_stub)
        spec = importlib.util.spec_from_file_location(
            "docverse_noxfile", _REPO_ROOT / "noxfile.py"
        )
        if spec is None or spec.loader is None:  # pragma: no cover
            msg = "could not load noxfile.py"
            raise RuntimeError(msg)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        yield module


@pytest.fixture(scope="module")
def test_args(noxfile: Any) -> PytestArgs:
    composer: PytestArgs = noxfile._test_args
    return composer


@pytest.fixture(scope="module")
def ignore_flag(noxfile: Any) -> str:
    return f"--ignore={noxfile.INTEGRATION_TEST_PATH}"


@pytest.fixture(autouse=True)
def _repo_root_cwd(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve path posargs from the repository root.

    Nox runs the session from the directory holding the noxfile, so that
    is what a relative path posarg is resolved against.
    """
    monkeypatch.chdir(_REPO_ROOT)


@pytest.fixture(autouse=True)
def _four_available_cpus(monkeypatch: pytest.MonkeyPatch) -> None:
    """Detect four usable CPUs, whatever machine the suite runs on.

    The injected worker count is read off the host, so without this the
    ``-n 4`` the injection tests below assert would be whatever the
    machine at hand happens to have. Four is what a GitHub-hosted runner
    reports.
    """
    monkeypatch.setattr("os.process_cpu_count", lambda: 4)


def test_bare_invocation_runs_the_whole_suite_under_xdist(
    test_args: PytestArgs, ignore_flag: str
) -> None:
    """``nox -s test`` collects ``tests`` with workers and the ignore."""
    assert test_args([]) == ["-n", "4", ignore_flag, "tests"]


def test_worker_count_is_capped(
    noxfile: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A wide host does not get more workers than the container VM helps.

    Every worker's database, connection pool, and application lifespan
    live in the single Docker VM holding PostgreSQL, so past its width the
    extra workers only queue against each other inside it.
    """
    monkeypatch.setattr("os.process_cpu_count", lambda: 64)

    assert noxfile._default_xdist_workers() == noxfile.XDIST_WORKER_CAP


def test_undetectable_cpu_count_is_worth_one_worker(
    noxfile: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("os.process_cpu_count", lambda: None)

    assert noxfile._default_xdist_workers() == 1


def test_option_posargs_keep_the_default_selection(
    test_args: PytestArgs, ignore_flag: str
) -> None:
    """An invocation that only passes options still runs everything."""
    assert test_args(["-x"]) == ["-n", "4", ignore_flag, "tests", "-x"]


def test_a_path_posarg_replaces_the_default_selection(
    test_args: PytestArgs, ignore_flag: str
) -> None:
    """``nox -s test -- tests/handlers`` runs the handlers and nothing else.

    Appending the selection to a default ``tests`` would run the whole
    suite instead, with the requested subset merely collected twice.
    """
    assert test_args(["tests/handlers"]) == [
        "-n",
        "4",
        ignore_flag,
        "tests/handlers",
    ]


def test_a_node_id_posarg_is_a_selection(
    test_args: PytestArgs, ignore_flag: str
) -> None:
    """A ``::``-qualified path still selects, once the node id is stripped."""
    selection = "tests/config_test.py::test_keeper_sync_timeout_defaults"

    assert test_args([selection]) == ["-n", "4", ignore_flag, selection]


def test_an_option_value_that_is_a_path_is_not_a_selection(
    test_args: PytestArgs, ignore_flag: str
) -> None:
    """``--ignore tests/worker`` narrows the default run, it does not name it.

    Reading the value as a selection would leave pytest with nothing to
    collect but the very path it was told to skip.
    """
    posargs = ["--ignore", "tests/worker"]

    assert test_args(posargs) == ["-n", "4", ignore_flag, "tests", *posargs]


def test_a_nonexistent_path_is_not_a_selection(
    test_args: PytestArgs, ignore_flag: str
) -> None:
    """Only a path that exists under ``tests/`` counts.

    A typo keeps the default selection, so pytest reports the bad path
    itself rather than collecting an empty run.
    """
    posargs = ["tests/does_not_exist_test.py"]

    assert test_args(posargs) == ["-n", "4", ignore_flag, "tests", *posargs]


def test_a_path_outside_the_suite_is_not_a_selection(
    test_args: PytestArgs, ignore_flag: str
) -> None:
    """``noxfile.py`` exists, but this session could never collect it."""
    posargs = ["noxfile.py"]

    assert test_args(posargs) == ["-n", "4", ignore_flag, "tests", *posargs]


@pytest.mark.parametrize(
    "posargs",
    [
        ["-n", "0"],
        ["-n0"],
        ["-nauto"],
        ["-nlogical"],
        ["--numprocesses", "2"],
        ["--numprocesses=2"],
    ],
)
def test_an_explicit_worker_count_wins(
    test_args: PytestArgs, ignore_flag: str, posargs: list[str]
) -> None:
    """Every spelling of ``-n`` xdist accepts suppresses the injection.

    Passing both would leave pytest with two ``-n`` values, and the
    developer's own would not be the one that won.
    """
    assert test_args(posargs) == [ignore_flag, "tests", *posargs]


def test_an_unrelated_option_is_not_a_worker_request(
    test_args: PytestArgs, ignore_flag: str
) -> None:
    """``-nf`` (``--new-first``) merely starts with an ``n``."""
    assert test_args(["-nf"]) == ["-n", "4", ignore_flag, "tests", "-nf"]


@pytest.mark.parametrize(
    "posargs",
    [
        ["--pdb"],
        ["--trace"],
        ["-s"],
        ["--capture=no"],
        ["--capture", "no"],
    ],
)
def test_a_debugger_invocation_runs_in_one_process(
    test_args: PytestArgs, ignore_flag: str, posargs: list[str]
) -> None:
    """Debugging posargs suppress the injection without asking.

    xdist refuses ``--pdb`` outright, its workers die on ``--trace``, and
    they swallow the output ``-s`` exists to show -- each of them after
    the PostgreSQL container has already started, and with nothing to say
    the ``-n`` came from the noxfile rather than the developer.
    """
    assert test_args(posargs) == [ignore_flag, "tests", *posargs]


def test_pdbcls_alone_keeps_the_workers(
    test_args: PytestArgs, ignore_flag: str
) -> None:
    """``--pdbcls`` names a debugger class; it does not start one."""
    posargs = ["--pdbcls=pdb:Pdb"]

    assert test_args(posargs) == ["-n", "4", ignore_flag, "tests", *posargs]
