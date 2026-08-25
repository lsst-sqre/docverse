"""Tests for the pytest-timeout hang backstop.

The suite shares one application, one connection pool, and one database
per pytest process for the whole session, so a stuck advisory lock or a
never-resolving await no longer fails a single test — it stops the
process running it. Nothing else in the run says so: pytest prints
progress only after an item finishes, and a hung item never finishes, so
CI shows silence until the job kills it at twenty minutes with no output
naming what hung.

``pyproject.toml`` therefore arms a 300 s per-test timeout using
pytest-timeout's ``thread`` method. The budget sits between the two
timescales that matter: the whole suite runs in seconds, so no healthy
test can reach it, and it is far enough under the job timeout that the
run still reports.

What that report looks like depends on how the run was started. The
thread method kills the process the item is running in and dumps every
thread's stack on the way out; serially that dump is the report, ending
on the frame the test hung in. Under pytest-xdist the dying process is
one worker and its output goes with it, so the naming falls to xdist —
"worker 'gw3' crashed while running <nodeid>" — which replaces the
worker and lets the rest of the run finish.

These tests pin the backstop from both ends: the timer really is armed
for the running test at the configured budget, and a hung test really
does die with a stack dump naming it.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

BACKSTOP_TIMEOUT = 300.0
"""Per-test budget ``pyproject.toml`` arms, in seconds."""

_HUNG_TEST = """
import time


def test_hangs_forever() -> None:
    time.sleep(30)
"""
"""A test that outlives its budget.

Sleeping is the cheapest stand-in for the real hangs this backstop is
for — a lock wait or an await that never resolves — and it holds the
main thread inside a named frame, which is what the stack dump has to
show. The sleep is bounded so the run below still ends if the backstop
never fires at all.
"""

_HUNG_TEST_INI = """
[pytest]
timeout = 1
timeout_method = thread
"""
"""The two settings under test, at a budget a test can afford to wait.

The mechanism kills the process it fires in, so it has to be exercised
in a subprocess, and waiting out the real `BACKSTOP_TIMEOUT` there would
cost more than every other test in this repository put together. Only
the number changes: these are the same two ini keys ``pyproject.toml``
sets, read from a config file the same way.
"""


def _armed_timer(node_id: str) -> threading.Timer:
    """Return the pytest-timeout timer armed for ``node_id``.

    pytest-timeout's ``thread`` method arms one `threading.Timer` per
    test item and names it for the item, so the live thread is the
    configuration as it actually resolved — ini file, ``PYTEST_TIMEOUT``
    override, and per-test marker included.
    """
    name = f"pytest_timeout {node_id}"
    for thread in threading.enumerate():
        if thread.name == name and isinstance(thread, threading.Timer):
            return thread
    raise AssertionError(f"no pytest-timeout timer is armed for {node_id}")


def test_backstop_is_armed_for_the_running_test(
    request: pytest.FixtureRequest,
) -> None:
    """Every test in this suite runs under the 300 s backstop.

    Reading the armed timer rather than ``pyproject.toml`` is what makes
    this a statement about the run: an uninstalled plugin, a stray
    ``--timeout`` on the command line, or a switch to the ``signal``
    method (which arms an interval timer and no thread at all) each
    leave this test with nothing to find.
    """
    assert _armed_timer(request.node.nodeid).interval == BACKSTOP_TIMEOUT


def test_a_hung_test_is_killed_with_a_stack_dump(tmp_path: Path) -> None:
    """A test that never returns dies, and the log says where it was.

    Killing the process is only half of what this buys: a run that ends
    with a bare non-zero exit says no more than the job timeout did. The
    assertions below are the diagnosis — the frame the main thread was
    stuck in, named, in output the run keeps.

    The inner run is serial, which is the case where the dump *is* the
    report; a distributed run loses it with the worker and leans on
    xdist's crash line instead.
    """
    (tmp_path / "pytest.ini").write_text(_HUNG_TEST_INI)
    (tmp_path / "hung_test.py").write_text(_HUNG_TEST)
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTEST_ADDOPTS", "PYTEST_XDIST_WORKER"}
    }

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "."],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )

    output = result.stdout + result.stderr
    assert result.returncode != 0, f"the hung test was not killed:\n{output}"
    assert "Timeout" in output
    assert "Stack of MainThread" in output
    # The dumped frames name the test that hung and the line it hung on,
    # which is the whole point of dumping them.
    assert "in test_hangs_forever" in output
    assert "time.sleep(30)" in output
