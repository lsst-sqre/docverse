"""Tests for ``LtdProductsClient`` over a respx-mocked LTD Keeper API.

The product listing is the one LTD call both the keeper-sync worker and
the synchronous scope-preview endpoint make, so its failure modes have
to be a single, typed family: every call site maps ``LtdProductsError``
onto its own policy (the worker fails the run and alerts; the preview
answers 502) and none of them has to catch ``httpx`` — or, as issue
#675 found, ``json.JSONDecodeError`` and ``AttributeError`` leaking
out of an LTD 200 whose body is an HTML maintenance page.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator

import httpx
import pytest
import pytest_asyncio
import respx
import structlog

from docverse_server.exceptions import DocverseSlackException
from docverse_server.storage.ltd import (
    LtdClientError,
    LtdProductsClient,
    LtdProductsError,
)

LTD_BASE = "https://keeper.lsst.codes"
PRODUCTS_URL = f"{LTD_BASE}/products/"


@pytest_asyncio.fixture
async def http_client() -> AsyncGenerator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


def _make_client(http_client: httpx.AsyncClient) -> LtdProductsClient:
    return LtdProductsClient(
        http_client=http_client,
        base_url=LTD_BASE,
        logger=structlog.get_logger("docverse"),
    )


def _json_response(payload: object) -> httpx.Response:
    """Return a 200 whose body is ``payload`` serialised as JSON."""
    return httpx.Response(
        200,
        content=json.dumps(payload).encode(),
        headers={"content-type": "application/json"},
    )


@pytest.mark.asyncio
async def test_listing_yields_slugs_in_order_without_duplicates(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """The happy path: slugs parsed out of the URLs, LTD order kept."""
    mock_discovery.get(PRODUCTS_URL).mock(
        return_value=_json_response(
            {
                "products": [
                    f"{LTD_BASE}/products/sqr-060/",
                    f"{LTD_BASE}/products/dmtn-201/",
                    f"{LTD_BASE}/products/sqr-060/",
                ]
            }
        )
    )

    slugs = await _make_client(http_client).list_product_slugs()

    assert slugs == ["sqr-060", "dmtn-201"]


@pytest.mark.asyncio
async def test_non_json_body_raises_ltd_products_error(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """An HTML maintenance page served as 200 is a typed LTD failure.

    The regression issue #675 names: ``response.json()`` raised
    ``json.JSONDecodeError`` from outside the ``LtdClientError``
    taxonomy, so the scope-preview endpoint answered a Docverse 500
    instead of the 502 its contract promises.
    """
    mock_discovery.get(PRODUCTS_URL).mock(
        return_value=httpx.Response(
            200,
            content=b"<html><body>LTD is down for maintenance</body></html>",
            headers={"content-type": "text/html"},
        )
    )

    with pytest.raises(LtdProductsError) as excinfo:
        await _make_client(http_client).list_product_slugs()

    exc = excinfo.value
    assert exc.status_code == 200
    assert "not JSON" in str(exc)
    # The body is carried for Sentry triage, truncated by the base class.
    assert exc.body is not None
    assert "maintenance" in exc.body


@pytest.mark.asyncio
async def test_json_array_payload_raises_ltd_products_error(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """A JSON array where the listing object belongs is a typed failure.

    The second half of #675: ``payload.get("products", [])`` raised
    ``AttributeError`` on a list, which is neither an ``httpx`` error
    nor an ``LtdClientError``.
    """
    mock_discovery.get(PRODUCTS_URL).mock(
        return_value=_json_response([f"{LTD_BASE}/products/sqr-060/"])
    )

    with pytest.raises(LtdProductsError) as excinfo:
        await _make_client(http_client).list_product_slugs()

    assert "JSON list" in str(excinfo.value)


@pytest.mark.asyncio
async def test_non_list_products_field_raises_ltd_products_error(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """``products`` has to be a list before it can be iterated."""
    mock_discovery.get(PRODUCTS_URL).mock(
        return_value=_json_response({"products": {"sqr-060": {}}})
    )

    with pytest.raises(LtdProductsError) as excinfo:
        await _make_client(http_client).list_product_slugs()

    assert "'products' field" in str(excinfo.value)


@pytest.mark.asyncio
async def test_unusable_products_entry_raises_ltd_products_error(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """An entry that is not a product URL fails loudly, not silently.

    Skipping it would drop a real LTD product from every scope
    resolution and present the loss as "not in scope" — an outage
    wearing the shape of a correct answer.
    """
    mock_discovery.get(PRODUCTS_URL).mock(
        return_value=_json_response(
            {"products": [f"{LTD_BASE}/products/sqr-060/", {"slug": "x"}]}
        )
    )

    with pytest.raises(LtdProductsError) as excinfo:
        await _make_client(http_client).list_product_slugs()

    assert "not a product URL" in str(excinfo.value)


@pytest.mark.asyncio
async def test_error_status_raises_ltd_products_error_with_the_status(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """A 5xx carries LTD's status so the preview can name it in its 502."""
    mock_discovery.get(PRODUCTS_URL).mock(
        return_value=httpx.Response(503, text="LTD is down")
    )

    with pytest.raises(LtdProductsError) as excinfo:
        await _make_client(http_client).list_product_slugs()

    assert excinfo.value.status_code == 503
    assert "503" in str(excinfo.value)


@pytest.mark.asyncio
async def test_transport_failure_raises_ltd_products_error_without_status(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """A connect failure produced no response, so there is no status."""
    mock_discovery.get(PRODUCTS_URL).mock(
        side_effect=httpx.ConnectError("connection refused")
    )

    with pytest.raises(LtdProductsError) as excinfo:
        await _make_client(http_client).list_product_slugs()

    assert excinfo.value.status_code is None
    assert isinstance(excinfo.value.__cause__, httpx.ConnectError)


def test_ltd_products_error_is_an_alerting_ltd_client_error() -> None:
    """The type stays inside the taxonomy the worker already handles.

    Subclassing :class:`LtdClientError` is what keeps the *unattended*
    call sites alerting: the worker's ``except Exception`` captures it
    to Sentry with the inherited ``to_sentry`` tags. The synchronous
    preview opts out by mapping it to a 502 at its own call site.
    """
    exc = LtdProductsError(
        url=PRODUCTS_URL,
        method="GET",
        status_code=200,
        body="<html>",
        message="LTD GET returned HTTP 200 with a body that is not JSON",
    )

    assert isinstance(exc, LtdClientError)
    assert isinstance(exc, DocverseSlackException)
    info = exc.to_sentry()
    assert info.tags["ltd_status_code"] == "200"
    assert info.tags["ltd_method"] == "GET"
    assert info.contexts["ltd_request"]["url"] == PRODUCTS_URL
