"""Tests for ``LtdClient`` over a respx-mocked LTD Keeper API."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Awaitable, Callable
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import respx
import structlog

from docverse_server.exceptions import DocverseSlackException
from docverse_server.storage.ltd import (
    LtdClient,
    LtdClientError,
    LtdNotFoundError,
)
from docverse_server.storage.ltd.client import (
    _EDITION_FETCH_CONCURRENCY,
    _MAX_BACKOFF_SECONDS,
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


def _record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replace ``asyncio.sleep`` with a recorder and return the log.

    The delays the client asks for are the behaviour under test, so they
    are asserted directly rather than waited out — a test that honestly
    slept an LTD rate-limit window would take a minute.
    """
    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    return delays


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
async def test_get_edition_fetches_by_ltd_id(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """An edition is fetched from ``/editions/<id>`` under the base URL."""
    route = mock_discovery.get(f"{LTD_BASE}/editions/1").mock(
        return_value=httpx.Response(
            200, json=_load("edition_main_git_refs.json")
        )
    )
    edition = await _make_client(http_client).get_edition(1)
    assert route.call_count == 1
    assert edition.ltd_id == 1


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


def _edition_payload(index: int) -> dict[str, object]:
    """Return an edition payload whose slug and URL carry ``index``."""
    payload = _load("edition_branch_git_refs.json")
    payload["self_url"] = f"{LTD_BASE}/editions/{index}"
    payload["slug"] = f"edition-{index}"
    return payload


class _FakeEditionServer:
    """A fake LTD serving one product's edition list and its editions.

    Records how many edition GETs are in flight at once (and the peak),
    the order edition responses complete in, which editions were ever
    requested, and how many in-flight edition GETs were cancelled, so
    the fan-out in ``list_editions_for_product`` can be asserted on
    directly. ``respond`` produces the response for the edition at an
    index in the URL list, so each test picks its own timing and
    failures.
    """

    def __init__(
        self,
        count: int,
        respond: Callable[[int], Awaitable[httpx.Response]],
    ) -> None:
        self.urls = [f"{LTD_BASE}/editions/{i}" for i in range(count)]
        self._respond = respond
        self.in_flight = 0
        self.peak = 0
        self.completed: list[int] = []
        self.cancelled = 0
        self.started: set[int] = set()

    async def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/products/pipelines/editions/":
            return httpx.Response(200, json={"editions": self.urls})
        index = int(request.url.path.rsplit("/", 1)[-1])
        self.started.add(index)
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        try:
            response = await self._respond(index)
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        finally:
            self.in_flight -= 1
        self.completed.append(index)
        return response


@pytest.mark.asyncio
async def test_list_editions_for_product_bounds_concurrent_gets() -> None:
    """Edition GETs run concurrently, but never more than the bound.

    ``pipelines`` lists thousands of editions; following them one at a
    time cost about two minutes per call, which pushed the keeper-sync
    tier crons past arq's cron timeout. Following all of them at once
    would instead open thousands of requests against LTD.
    """

    async def respond(index: int) -> httpx.Response:
        await asyncio.sleep(0.02)
        return httpx.Response(200, json=_edition_payload(index))

    server = _FakeEditionServer(3 * _EDITION_FETCH_CONCURRENCY, respond)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(server.handler)
    ) as http_client:
        editions = await _make_client(http_client).list_editions_for_product(
            "pipelines"
        )

    assert len(editions) == 3 * _EDITION_FETCH_CONCURRENCY
    assert server.peak == _EDITION_FETCH_CONCURRENCY


@pytest.mark.asyncio
async def test_list_editions_for_product_preserves_url_list_order() -> None:
    """The result follows LTD's URL list, not the order GETs complete in.

    Each edition answers more slowly than the one after it, so within a
    batch the GETs complete in the reverse of the URL-list order.
    """
    count = 2 * _EDITION_FETCH_CONCURRENCY

    async def respond(index: int) -> httpx.Response:
        await asyncio.sleep(0.002 * (count - index))
        return httpx.Response(200, json=_edition_payload(index))

    server = _FakeEditionServer(count, respond)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(server.handler)
    ) as http_client:
        editions = await _make_client(http_client).list_editions_for_product(
            "pipelines"
        )

    assert server.completed != sorted(server.completed)
    assert [e.slug for e in editions] == [f"edition-{i}" for i in range(count)]


def _connect_error(index: int) -> httpx.Response:
    request = httpx.Request("GET", f"{LTD_BASE}/editions/{index}")
    raise httpx.ConnectError("connection refused", request=request)


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (lambda _: httpx.Response(404), LtdNotFoundError),
        (lambda _: httpx.Response(401), LtdClientError),
        (lambda _: httpx.Response(503), LtdClientError),
        (_connect_error, LtdClientError),
    ],
    ids=["not-found", "non-retryable", "retries-exhausted", "transport"],
)
@pytest.mark.asyncio
async def test_list_editions_for_product_failure_cancels_the_rest(
    failure: Callable[[int], httpx.Response],
    expected: type[LtdClientError],
) -> None:
    """A failing edition GET raises as before and cancels its siblings.

    The failure surfaces as the same ``LtdClientError`` subclass the
    one-at-a-time walk raised, never wrapped in an exception group, and
    every sibling GET still in flight is cancelled rather than left
    running after the call has already failed.
    """
    failing_index = 2
    never = asyncio.Event()

    async def respond(index: int) -> httpx.Response:
        if index == failing_index:
            return failure(index)
        await never.wait()
        return httpx.Response(200, json=_edition_payload(index))

    server = _FakeEditionServer(3 * _EDITION_FETCH_CONCURRENCY, respond)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(server.handler)
    ) as http_client:
        with pytest.raises(LtdClientError) as excinfo:
            await asyncio.wait_for(
                _make_client(http_client).list_editions_for_product(
                    "pipelines"
                ),
                timeout=5,
            )

    assert type(excinfo.value) is expected
    assert excinfo.value.url == f"{LTD_BASE}/editions/{failing_index}"
    assert server.in_flight == 0
    assert server.cancelled == len(server.started) - 1
    assert len(server.started) < len(server.urls)


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
async def test_429_rides_out_a_long_rate_limit_window(
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``Retry-After`` past the shared 10 s default is obeyed in full.

    LTD answers a rate-limited GET with the seconds left in its window.
    Clamping that to the shared default burned all four attempts inside
    the same window and failed the sync job for every project in a tier
    tick; these GETs hold no transaction and no lock, so sleeping the
    full window is both safe and the only way the call ever succeeds.
    """
    delays = _record_sleeps(monkeypatch)
    route = mock_discovery.get(f"{LTD_BASE}/products/pipelines")
    route.side_effect = [
        httpx.Response(429, headers={"Retry-After": "60"}),
        httpx.Response(200, json=_load("product_pipelines.json")),
    ]

    product = await _make_client(http_client).get_product("pipelines")

    assert product.slug == "pipelines"
    assert delays == [60.0]


@pytest.mark.asyncio
async def test_429_retry_after_clamped_to_ltd_ceiling(
    http_client: httpx.AsyncClient,
    mock_discovery: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generous LTD ceiling is still a ceiling, not a blank cheque."""
    delays = _record_sleeps(monkeypatch)
    route = mock_discovery.get(f"{LTD_BASE}/products/pipelines")
    route.side_effect = [
        httpx.Response(429, headers={"Retry-After": "86400"}),
        httpx.Response(200, json=_load("product_pipelines.json")),
    ]

    product = await _make_client(http_client).get_product("pipelines")

    assert product.slug == "pipelines"
    assert delays == [_MAX_BACKOFF_SECONDS]


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
async def test_transport_failure_exhausts_retries_then_raises() -> None:
    """A dropped connection is retried before it becomes a client error."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        raise httpx.ConnectError("connection refused", request=request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler)
    ) as http_client:
        with pytest.raises(LtdClientError) as excinfo:
            await _make_client(http_client).get_product("pipelines")

    assert attempts == [1, 2, 3, 4]
    assert isinstance(excinfo.value.__cause__, httpx.ConnectError)


@pytest.mark.asyncio
async def test_unsupported_protocol_fails_on_the_first_attempt() -> None:
    """A misconfigured ``ltd_base_url`` must not burn the retry budget.

    ``UnsupportedProtocol`` is what a base URL with a typo'd (or
    missing) scheme raises, and every attempt would fail identically.
    Catching the whole ``httpx.HTTPError`` tree spent four attempts and
    four backoff sleeps proving that to itself before reporting the
    operator's mistake.
    """
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        raise httpx.UnsupportedProtocol("unknown scheme")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler)
    ) as http_client:
        with pytest.raises(LtdClientError) as excinfo:
            await _make_client(http_client).get_product("pipelines")

    assert attempts == [1]
    assert isinstance(excinfo.value.__cause__, httpx.UnsupportedProtocol)


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


@pytest.mark.asyncio
async def test_redirect_status_raises_client_error(
    http_client: httpx.AsyncClient, mock_discovery: respx.Router
) -> None:
    """A 3xx is a failed call, not a body to parse.

    Nothing in this codebase constructs an ``httpx.AsyncClient`` that
    follows redirects, so an SSO gateway or a moved-endpoint 302 in
    front of LTD hands back a login page instead of the product JSON.
    Treating "not an error status" as success sent that page into
    ``response.json()``, raising ``json.JSONDecodeError`` from outside
    the ``LtdClientError`` taxonomy every caller catches.
    """
    route = mock_discovery.get(f"{LTD_BASE}/products/pipelines").mock(
        return_value=httpx.Response(
            302,
            headers={"Location": "https://login.example.com/"},
            text="<html>login</html>",
        )
    )
    with pytest.raises(LtdClientError) as excinfo:
        await _make_client(http_client).get_product("pipelines")
    assert route.call_count == 1
    exc = excinfo.value
    assert exc.status_code == 302
    assert exc.body == "<html>login</html>"


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
