"""Tests for ``create_cdn_cache_purger``."""

from __future__ import annotations

import httpx
import pytest
import structlog

from docverse_server.storage.cdncachepurger import (
    CloudflareCachePurger,
    NoopCdnCachePurger,
    create_cdn_cache_purger,
)


def _make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={})
        )
    )


def test_creates_cloudflare_purger_when_zone_id_present() -> None:
    purger = create_cdn_cache_purger(
        provider="cloudflare_workers",
        config={
            "account_id": "acct-123",
            "kv_namespace_id": "ns-456",
            "zone_id": "zone-789",
        },
        credentials={"api_token": "token-abc"},
        logger=structlog.get_logger("test"),
        http_client=_make_client(),
    )
    assert isinstance(purger, CloudflareCachePurger)


def test_creates_noop_purger_when_zone_id_absent() -> None:
    purger = create_cdn_cache_purger(
        provider="cloudflare_workers",
        config={"account_id": "acct-123", "kv_namespace_id": "ns-456"},
        credentials={"api_token": "token-abc"},
        logger=structlog.get_logger("test"),
        http_client=_make_client(),
    )
    assert isinstance(purger, NoopCdnCachePurger)


def test_creates_noop_purger_when_zone_id_empty() -> None:
    purger = create_cdn_cache_purger(
        provider="cloudflare_workers",
        config={"zone_id": ""},
        credentials={"api_token": "token-abc"},
        logger=structlog.get_logger("test"),
        http_client=_make_client(),
    )
    assert isinstance(purger, NoopCdnCachePurger)


def test_missing_api_token_raises() -> None:
    with pytest.raises(ValueError, match="api_token"):
        create_cdn_cache_purger(
            provider="cloudflare_workers",
            config={"zone_id": "zone-789"},
            credentials={},
            logger=structlog.get_logger("test"),
            http_client=_make_client(),
        )


def test_unsupported_provider_raises() -> None:
    with pytest.raises(ValueError, match="Unsupported CDN cache purger"):
        create_cdn_cache_purger(
            provider="fastly",
            config={"zone_id": "zone-789"},
            credentials={"api_token": "token-abc"},
            logger=structlog.get_logger("test"),
            http_client=_make_client(),
        )
