"""Tests for the default worker's staging-spool startup probe.

``build_processing`` spools each build's staging tarball to a temporary
file (#878), so a worker whose temporary directory is not writable —
the Phalanx chart's read-only root filesystem without a ``/tmp``
volume — would accept every upload and then fail it. The default pool,
the one that runs ``build_processing``, probes the directory at startup
instead and refuses to start.

The full ``_startup`` needs Redis and a GitHub App, so these drive the
seam, :func:`probe_staging_spool`, and check the pools' ``on_startup``
wrappers with ``_startup`` stubbed.
"""

from __future__ import annotations

import tempfile
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from structlog.testing import capture_logs

from docverse_server.worker import main as worker_main
from docverse_server.worker.functions.build_processing import (
    probe_staging_spool,
)
from docverse_server.worker.main import (
    startup_default,
    startup_keeper_sync,
    startup_maintenance,
)

_SPOOL_EVENT = "Staging spool directory"


def test_probe_returns_the_writable_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A writable directory is returned, and the probe leaves no file."""
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    assert probe_staging_spool() == str(tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_probe_raises_when_the_directory_is_unusable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An unusable directory raises, naming it and the fix.

    A directory that does not exist stands in for a read-only one: both
    fail ``TemporaryFile`` with an ``OSError``, and this one does so
    even when the tests run as root.
    """
    missing = tmp_path / "missing"
    monkeypatch.setattr(tempfile, "tempdir", str(missing))
    with pytest.raises(RuntimeError) as excinfo:
        probe_staging_spool()
    message = str(excinfo.value)
    assert str(missing) in message
    assert "TMPDIR" in message
    assert "/tmp" in message  # noqa: S108 - the path the message names
    assert isinstance(excinfo.value.__cause__, OSError)


@pytest.mark.asyncio
async def test_default_startup_logs_the_spool_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The default pool probes the spool and logs where it is."""
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(worker_main, "_startup", AsyncMock())
    with capture_logs() as captured:
        await startup_default({})
    assert [
        entry["directory"]
        for entry in captured
        if entry["event"] == _SPOOL_EVENT
    ] == [str(tmp_path)]


@pytest.mark.asyncio
async def test_default_startup_refuses_an_unusable_spool(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The default pool fails to start rather than fail every upload."""
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "missing"))
    monkeypatch.setattr(worker_main, "_startup", AsyncMock())
    with pytest.raises(RuntimeError, match="TMPDIR"):
        await startup_default({})


@pytest.mark.asyncio
@pytest.mark.parametrize("startup", [startup_keeper_sync, startup_maintenance])
async def test_other_pools_skip_the_probe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    startup: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    """Pools that never run ``build_processing`` start without a spool."""
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "missing"))
    monkeypatch.setattr(worker_main, "_startup", AsyncMock())
    with capture_logs() as captured:
        await startup({})
    assert [e for e in captured if e["event"] == _SPOOL_EVENT] == []
