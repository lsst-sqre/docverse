"""Tests for the MockCdnCachePurger."""

from __future__ import annotations

import pytest

from docverse_server.storage.cdncachepurger import (
    CdnCachePurger,
    MockCdnCachePurger,
)


@pytest.mark.asyncio
async def test_records_purge_calls() -> None:
    purger = MockCdnCachePurger()
    async with purger as p:
        await p.purge_hostname("myproject.example.org")
        await p.purge_hostname("other.example.org")

    assert purger.purge_calls == [
        "myproject.example.org",
        "other.example.org",
    ]


@pytest.mark.asyncio
async def test_implements_protocol() -> None:
    purger = MockCdnCachePurger()
    assert isinstance(purger, CdnCachePurger)
