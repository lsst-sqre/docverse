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

from docverse.exceptions import DocverseSlackException
from docverse.storage.ltd import LtdClient, LtdClientError, LtdNotFoundError

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


def test_ltd_client_error_is_docverse_slack_exception() -> None:
    """``LtdClientError`` migrates onto the shared ``DocverseSlackException``.

    Pins the slice-#343 contract that LTD-side failures route to Slack
    and Sentry alongside every other server-side exception, rather than
    inheriting from plain ``Exception``.
    """
    exc = LtdClientError(
        url=f"{LTD_BASE}/products/pipelines",
        method="GET",
        status_code=503,
        body="upstream timeout",
    )
    assert isinstance(exc, DocverseSlackException)


def test_ltd_not_found_error_inherits_from_ltd_client_error() -> None:
    """``LtdNotFoundError`` keeps its ``LtdClientError`` parent.

    Callers that ``except LtdClientError`` still catch a 404 (the
    retry/error path); callers that ``except LtdNotFoundError`` keep
    the soft-deletion shortcut.
    """
    exc = LtdNotFoundError(
        url=f"{LTD_BASE}/products/missing",
        method="GET",
        status_code=404,
        body="not found",
    )
    assert isinstance(exc, LtdClientError)
    assert isinstance(exc, DocverseSlackException)


def test_ltd_client_error_to_sentry_tags_status_and_method() -> None:
    """``to_sentry`` surfaces the status code and method as Sentry tags.

    Tags are low-cardinality (HTTP status codes and methods) so they
    can be aggregated in the Sentry UI to tell apart "5xx on LTD side"
    from "stale credential on our side".
    """
    exc = LtdClientError(
        url=f"{LTD_BASE}/products/pipelines",
        method="GET",
        status_code=503,
        body="upstream timeout",
    )
    info = exc.to_sentry()
    assert info.tags["ltd_status_code"] == "503"
    assert info.tags["ltd_method"] == "GET"


def test_ltd_client_error_to_sentry_context_carries_request_snapshot() -> None:
    """The ``ltd_request`` context carries the full request snapshot.

    The context fields (``url``, ``method``, ``status_code``, ``body``)
    are high cardinality and live in the context rather than tags so
    they can be inspected per-event without exploding the Sentry index.
    """
    url = f"{LTD_BASE}/products/pipelines"
    exc = LtdClientError(url=url, method="GET", status_code=500, body="boom")
    info = exc.to_sentry()
    context = info.contexts["ltd_request"]
    assert context == {
        "url": url,
        "method": "GET",
        "status_code": 500,
        "body": "boom",
    }


def test_ltd_client_error_truncates_oversized_body() -> None:
    """The constructor caps ``body`` at <= 4 KB and ``to_sentry`` honours it.

    LTD response bodies can be arbitrarily large (HTML error pages,
    full JSON payloads). Truncating in the constructor — rather than
    relying on every raise site to remember — keeps Sentry payloads
    small and protects the wire from megabyte-scale envelopes.
    """
    body = "x" * (5 * 1024)
    exc = LtdClientError(
        url=f"{LTD_BASE}/products/pipelines",
        method="GET",
        status_code=500,
        body=body,
    )
    assert exc.body is not None
    assert len(exc.body.encode("utf-8")) <= 4 * 1024
    context = exc.to_sentry().contexts["ltd_request"]
    assert isinstance(context["body"], str)
    assert len(context["body"].encode("utf-8")) <= 4 * 1024
    assert context["body"] == "x" * (4 * 1024)


def test_ltd_client_error_preserves_short_body_verbatim() -> None:
    """Bodies at or below the cap pass through unchanged.

    Guards against an over-eager truncator silently re-encoding small
    payloads and surfacing the wrong byte sequence in Sentry.
    """
    body = '{"error": "missing slug"}'
    exc = LtdClientError(
        url=f"{LTD_BASE}/products/pipelines",
        method="GET",
        status_code=400,
        body=body,
    )
    assert exc.body == body


def test_ltd_client_error_handles_missing_status_and_body() -> None:
    """Network-error raise sites have neither status nor body.

    The constructor must accept ``status_code=None`` / ``body=None``
    (the ``httpx.HTTPError`` and "exhausted retries" sites in
    ``_get_json``) and ``to_sentry`` must omit the missing tag rather
    than emit ``"None"`` strings.
    """
    exc = LtdClientError(
        url=f"{LTD_BASE}/products/pipelines",
        method="GET",
        message="LTD GET ... failed: connect timeout",
    )
    info = exc.to_sentry()
    assert "ltd_status_code" not in info.tags
    assert info.tags["ltd_method"] == "GET"
    context = info.contexts["ltd_request"]
    assert context["status_code"] is None
    assert context["body"] is None


@pytest.mark.asyncio
async def test_non_retryable_error_carries_status_and_body(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """A non-retryable error reaches the raise site with response context.

    Verifies the raise-site wiring: the constructed ``LtdClientError``
    captures the HTTP status code, request URL/method, and response
    body so a triager can read them off Sentry rather than grepping
    pod logs.
    """
    mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(401, text="unauthorized")
    )
    with pytest.raises(LtdClientError) as excinfo:
        await _make_client(http_client).get_product("pipelines")
    exc = excinfo.value
    assert exc.status_code == 401
    assert exc.method == "GET"
    assert exc.url == f"{LTD_BASE}/products/pipelines"
    assert exc.body == "unauthorized"
