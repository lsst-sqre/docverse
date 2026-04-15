"""Tests for ``create_edition_publisher``."""

from __future__ import annotations

import httpx
import pytest
import structlog

from docverse.storage.editionpublisher import (
    CloudflareKvEditionPublisher,
    create_edition_publisher,
)


def _make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={})
        )
    )


def test_creates_cloudflare_kv_publisher() -> None:
    client = _make_client()
    publisher = create_edition_publisher(
        provider="cloudflare_workers",
        config={
            "account_id": "acct-123",
            "kv_namespace_id": "ns-456",
            "zone_id": "zone-789",
        },
        credentials={"api_token": "token-abc"},
        logger=structlog.get_logger("test"),
        http_client=client,
    )
    assert isinstance(publisher, CloudflareKvEditionPublisher)


def test_missing_kv_namespace_id_raises() -> None:
    with pytest.raises(ValueError, match="kv_namespace_id"):
        create_edition_publisher(
            provider="cloudflare_workers",
            config={"account_id": "acct-123"},
            credentials={"api_token": "token-abc"},
            logger=structlog.get_logger("test"),
            http_client=_make_client(),
        )


def test_missing_account_id_raises() -> None:
    with pytest.raises(ValueError, match="account_id"):
        create_edition_publisher(
            provider="cloudflare_workers",
            config={"kv_namespace_id": "ns-456"},
            credentials={"api_token": "token-abc"},
            logger=structlog.get_logger("test"),
            http_client=_make_client(),
        )


def test_missing_api_token_raises() -> None:
    with pytest.raises(ValueError, match="api_token"):
        create_edition_publisher(
            provider="cloudflare_workers",
            config={
                "account_id": "acct-123",
                "kv_namespace_id": "ns-456",
            },
            credentials={},
            logger=structlog.get_logger("test"),
            http_client=_make_client(),
        )


def test_unsupported_provider_raises() -> None:
    with pytest.raises(ValueError, match="Unsupported edition publisher"):
        create_edition_publisher(
            provider="fastly",
            config={},
            credentials={},
            logger=structlog.get_logger("test"),
            http_client=_make_client(),
        )
