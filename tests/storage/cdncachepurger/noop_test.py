"""Tests for the NoopCdnCachePurger."""

from __future__ import annotations

import pytest
from structlog.testing import capture_logs

from docverse_server.storage.cdncachepurger import (
    CdnCachePurger,
    NoopCdnCachePurger,
)


@pytest.mark.asyncio
async def test_purge_hostname_is_a_logged_noop() -> None:
    """The purge does nothing observable beyond an INFO log."""
    with capture_logs() as logs:
        purger = NoopCdnCachePurger()
        async with purger as p:
            await p.purge_hostname("myproject.example.org")

    assert [entry["log_level"] for entry in logs] == ["info"]
    assert logs[0]["hostname"] == "myproject.example.org"


@pytest.mark.asyncio
async def test_implements_protocol() -> None:
    purger = NoopCdnCachePurger()
    assert isinstance(purger, CdnCachePurger)
