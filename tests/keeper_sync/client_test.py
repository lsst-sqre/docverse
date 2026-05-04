"""Tests for ``LtdClient`` over a respx-mocked LTD Keeper API."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import respx
import structlog

from docverse.keeper_sync.client import (
    LtdClient,
    LtdClientError,
    LtdNotFoundError,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
LTD_BASE = "https://keeper.lsst.codes"


def _load(name: str) -> dict[str, object]:
    payload: dict[str, object] = json.loads((FIXTURES_DIR / name).read_text())
    return payload


@pytest_asyncio.fixture
async def http_client() -> AsyncGenerator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


def _make_client(http_client: httpx.AsyncClient) -> LtdClient:
    return LtdClient(
        http_client=http_client,
        base_url=LTD_BASE,
        logger=structlog.get_logger("test"),
        base_backoff_seconds=0.0,
    )


@pytest.mark.asyncio
async def test_get_product_happy_path(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=_load("product_pipelines.json"))
    )
    client = _make_client(http_client)
    product = await client.get_product("pipelines")
    assert product.slug == "pipelines"


@pytest.mark.asyncio
async def test_list_products(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    mock_discovery.get(f"{LTD_BASE}/products/").mock(
        return_value=httpx.Response(200, json=_load("products_listing.json"))
    )
    listing = await _make_client(http_client).list_products()
    assert len(listing.products) == 3


@pytest.mark.asyncio
async def test_get_edition_by_url_returns_typed_model(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(
            200, json=_load("edition_main_git_refs.json")
        )
    )
    edition = await _make_client(http_client).get_edition_by_url(
        f"{LTD_BASE}/editions/1"
    )
    assert edition.mode == "git_refs"


@pytest.mark.asyncio
async def test_list_editions_for_product_follows_each_url(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    mock_discovery.get(f"{LTD_BASE}/products/pipelines/editions/").mock(
        return_value=httpx.Response(
            200,
            json={
                "editions": [
                    f"{LTD_BASE}/editions/1",
                    f"{LTD_BASE}/editions/2",
                ]
            },
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(
            200, json=_load("edition_main_git_refs.json")
        )
    )
    mock_discovery.get(f"{LTD_BASE}/editions/2").mock(
        return_value=httpx.Response(
            200, json=_load("edition_branch_git_refs.json")
        )
    )
    editions = await _make_client(http_client).list_editions_for_product(
        "pipelines"
    )
    assert [e.slug for e in editions] == ["main", "u-jsick-feature"]


@pytest.mark.asyncio
async def test_404_raises_ltd_not_found(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    mock_discovery.get(f"{LTD_BASE}/products/missing").mock(
        return_value=httpx.Response(404)
    )
    with pytest.raises(LtdNotFoundError):
        await _make_client(http_client).get_product("missing")


@pytest.mark.asyncio
async def test_429_with_retry_after_then_success(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """Honours ``Retry-After`` and retries the call to success."""
    route = mock_discovery.get(f"{LTD_BASE}/products/pipelines")
    route.side_effect = [
        httpx.Response(429, headers={"Retry-After": "0"}),
        httpx.Response(200, json=_load("product_pipelines.json")),
    ]
    product = await _make_client(http_client).get_product("pipelines")
    assert product.slug == "pipelines"
    assert route.call_count == 2


@pytest.mark.asyncio
async def test_5xx_exhausts_retries_then_raises(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    route = mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(503)
    )
    client = _make_client(http_client)
    with pytest.raises(LtdClientError, match="503"):
        await client.get_product("pipelines")
    assert route.call_count == 4


@pytest.mark.asyncio
async def test_schema_drift_extra_fields_does_not_break(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    payload = _load("product_pipelines.json")
    payload["mystery_field"] = "future_lts_addition"
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(200, json=payload)
    )
    product = await _make_client(http_client).get_product("pipelines")
    assert product.slug == "pipelines"


@pytest.mark.asyncio
async def test_non_retryable_error_status_raises(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """Non-retryable 4xx (e.g. 401) surfaces as LtdClientError immediately."""
    route = mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(401)
    )
    with pytest.raises(LtdClientError, match="401"):
        await _make_client(http_client).get_product("pipelines")
    assert route.call_count == 1
