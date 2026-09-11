"""Tests for the CloudflareKvEditionPublisher."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

from docverse_server.domain.edition_pointer import EditionPointer
from docverse_server.storage._http_retry import MAX_BACKOFF_SECONDS
from docverse_server.storage.editionpublisher import (
    CloudflareKvEditionPublisher,
    CloudflareKvReadError,
)


def _make_publisher(
    handler: httpx.MockTransport,
    *,
    account_id: str = "acct-123",
    namespace_id: str = "ns-456",
    api_token: str = "token-789",
    max_attempts: int = 4,
) -> tuple[CloudflareKvEditionPublisher, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=handler)
    publisher = CloudflareKvEditionPublisher(
        account_id=account_id,
        namespace_id=namespace_id,
        api_token=api_token,
        http_client=client,
        logger=structlog.get_logger("test"),
        max_attempts=max_attempts,
        base_backoff_seconds=0.0,
    )
    return publisher, client


def _record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replace ``asyncio.sleep`` with a recorder and return the log."""
    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    return delays


@pytest.mark.asyncio
async def test_publish_issues_put_to_kv_endpoint() -> None:
    seen: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["request"] = request
        return httpx.Response(200, json={"success": True})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )

    request = seen["request"]
    assert request.method == "PUT"
    assert str(request.url) == (
        "https://api.cloudflare.com/client/v4"
        "/accounts/acct-123"
        "/storage/kv/namespaces/ns-456"
        "/values/myproject/main"
    )
    assert request.headers["Authorization"] == "Bearer token-789"
    assert json.loads(request.content) == {
        "build_id": "ABC123",
        "r2_prefix": "myproject/__builds/ABC123/",
        "cache_profile": "long",
    }


@pytest.mark.asyncio
async def test_publish_writes_short_cache_profile() -> None:
    seen: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["request"] = request
        return httpx.Response(200, json={"success": True})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="tickets-dm-1",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="short",
        )

    assert json.loads(seen["request"].content) == {
        "build_id": "ABC123",
        "r2_prefix": "myproject/__builds/ABC123/",
        "cache_profile": "short",
    }


@pytest.mark.asyncio
async def test_publish_retries_429_then_succeeds() -> None:
    """A rate-limited pointer write is retried, not failed.

    The KV write is on the critical path of every publish and shares
    Cloudflare's API rate limits with the zone purge, so a single 429
    used to abandon an otherwise healthy publish.
    """
    statuses = [429, 429, 200]
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(
            statuses[len(attempts) - 1], json={"success": True}
        )

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )

    assert attempts == [1, 2, 3]


@pytest.mark.asyncio
async def test_publish_honours_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cloudflare's own ``Retry-After`` sets the wait, up to the cap."""
    delays = _record_sleeps(monkeypatch)
    responses = [
        httpx.Response(429, headers={"Retry-After": "4"}),
        httpx.Response(200, json={"success": True}),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )

    assert delays == [4.0]


@pytest.mark.asyncio
async def test_publish_caps_long_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The publisher keeps the tight shared ceiling on ``Retry-After``.

    The pointer write happens inside the publish job, which holds an
    open transaction and the CDN purge coalescer's per-hostname lock, so
    an obedient five-minute sleep would serialize a publish burst.
    """
    delays = _record_sleeps(monkeypatch)
    responses = [
        httpx.Response(429, headers={"Retry-After": "300"}),
        httpx.Response(200, json={"success": True}),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.publish(
            project_slug="myproject",
            edition_slug="main",
            build_public_id="ABC123",
            object_key_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )

    assert delays == [MAX_BACKOFF_SECONDS]


@pytest.mark.asyncio
async def test_publish_exhausts_retries_on_persistent_5xx() -> None:
    """A never-clearing 5xx still fails the publish once the budget ends."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(503, json={"errors": ["boom"]})

    with capture_logs() as logs:
        publisher, client = _make_publisher(
            httpx.MockTransport(handler), max_attempts=3
        )
        async with client, publisher as pub:
            with pytest.raises(httpx.HTTPStatusError) as excinfo:
                await pub.publish(
                    project_slug="p",
                    edition_slug="e",
                    build_public_id="B",
                    object_key_prefix="p/__builds/B/",
                    cache_profile="long",
                )

    assert attempts == [1, 2, 3]
    assert excinfo.value.response.status_code == 503

    errors = [entry for entry in logs if entry["log_level"] == "error"]
    assert len(errors) == 1
    assert errors[0]["attempts"] == 3


@pytest.mark.asyncio
async def test_publish_does_not_retry_4xx() -> None:
    """A 404 namespace or a bad token fails on the first attempt."""
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(404, json={"errors": ["not found"]})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(httpx.HTTPStatusError):
            await pub.publish(
                project_slug="p",
                edition_slug="e",
                build_public_id="B",
                object_key_prefix="p/__builds/B/",
                cache_profile="long",
            )

    assert attempts == [1]


@pytest.mark.asyncio
async def test_publish_raises_on_4xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"errors": ["not found"]})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(httpx.HTTPStatusError):
            await pub.publish(
                project_slug="p",
                edition_slug="e",
                build_public_id="B",
                object_key_prefix="p/__builds/B/",
                cache_profile="long",
            )


@pytest.mark.asyncio
async def test_publish_raises_on_5xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"errors": ["boom"]})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(httpx.HTTPStatusError):
            await pub.publish(
                project_slug="p",
                edition_slug="e",
                build_public_id="B",
                object_key_prefix="p/__builds/B/",
                cache_profile="long",
            )


@pytest.mark.asyncio
async def test_publish_reports_redirect_response() -> None:
    """A 3xx publish failure reaches the log with its response context.

    The pointer write is not followed on this client, so a 302 from a
    proxy or a moved Cloudflare endpoint means the KV entry was never
    written. Gating the diagnostic on ``is_error`` (4xx/5xx only) left
    that failure with a bare ``HTTPStatusError`` and no status or body
    to triage from.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            302,
            headers={"Location": "https://login.example.com/"},
            text="<html>login</html>",
        )

    with capture_logs() as logs:
        publisher, client = _make_publisher(httpx.MockTransport(handler))
        async with client, publisher as pub:
            with pytest.raises(httpx.HTTPStatusError):
                await pub.publish(
                    project_slug="p",
                    edition_slug="e",
                    build_public_id="B",
                    object_key_prefix="p/__builds/B/",
                    cache_profile="long",
                )

    errors = [
        entry
        for entry in logs
        if entry["event"] == "Cloudflare KV publish failed"
    ]
    assert len(errors) == 1
    assert errors[0]["status_code"] == 302
    assert errors[0]["response_body"] == "<html>login</html>"


@pytest.mark.asyncio
async def test_unpublish_issues_delete_to_kv_endpoint() -> None:
    seen: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["request"] = request
        return httpx.Response(200, json={"success": True})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        await pub.unpublish(
            project_slug="myproject",
            edition_slug="main",
        )

    request = seen["request"]
    assert request.method == "DELETE"
    assert str(request.url) == (
        "https://api.cloudflare.com/client/v4"
        "/accounts/acct-123"
        "/storage/kv/namespaces/ns-456"
        "/values/myproject/main"
    )
    assert request.headers["Authorization"] == "Bearer token-789"


@pytest.mark.asyncio
async def test_unpublish_treats_404_as_success() -> None:
    """A missing KV key must not raise — unpublish is idempotent."""
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(404, json={"errors": ["not found"]})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        # Should not raise.
        await pub.unpublish(project_slug="p", edition_slug="e")
    assert call_count["n"] == 1


@pytest.mark.asyncio
async def test_unpublish_reports_redirect_response() -> None:
    """A 3xx unpublish failure reaches the log with its response context.

    Only the 404 above is a successful no-op; a redirect means the
    delete never reached the namespace, so it has to be as loud as a
    5xx rather than an unexplained ``HTTPStatusError``.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            307,
            headers={"Location": "https://login.example.com/"},
            text="<html>login</html>",
        )

    with capture_logs() as logs:
        publisher, client = _make_publisher(httpx.MockTransport(handler))
        async with client, publisher as pub:
            with pytest.raises(httpx.HTTPStatusError):
                await pub.unpublish(project_slug="p", edition_slug="e")

    errors = [
        entry
        for entry in logs
        if entry["event"] == "Cloudflare KV unpublish failed"
    ]
    assert len(errors) == 1
    assert errors[0]["status_code"] == 307
    assert errors[0]["response_body"] == "<html>login</html>"


@pytest.mark.asyncio
async def test_unpublish_raises_on_5xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"errors": ["boom"]})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(httpx.HTTPStatusError):
            await pub.unpublish(project_slug="p", edition_slug="e")


@pytest.mark.asyncio
async def test_get_pointers_posts_bulk_get_and_decodes_values() -> None:
    """The read-back is one POST whose ``result.values`` are pointers."""
    seen: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["request"] = request
        return httpx.Response(
            200,
            json={
                "success": True,
                "result": {
                    "values": {
                        "myproject/main": {
                            "build_id": "ABC123",
                            "r2_prefix": "myproject/__builds/ABC123/",
                            "cache_profile": "long",
                        }
                    }
                },
            },
        )

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        pointers = await pub.get_pointers(["myproject/main"])

    request = seen["request"]
    assert request.method == "POST"
    assert str(request.url) == (
        "https://api.cloudflare.com/client/v4"
        "/accounts/acct-123"
        "/storage/kv/namespaces/ns-456"
        "/bulk/get"
    )
    assert request.headers["Authorization"] == "Bearer token-789"
    assert json.loads(request.content) == {
        "keys": ["myproject/main"],
        "type": "json",
    }
    assert pointers == {
        "myproject/main": EditionPointer(
            build_public_id="ABC123",
            r2_prefix="myproject/__builds/ABC123/",
            cache_profile="long",
        )
    }


@pytest.mark.asyncio
async def test_get_pointers_chunks_at_cloudflares_limit() -> None:
    """250 keys become three POSTs of 100, 100, and 50.

    Cloudflare caps ``bulk/get`` at 100 keys, and an org with more
    editions than that is ordinary, so the read-back has to chunk rather
    than fail. The last chunk is the remainder, not a padded one.
    """
    chunk_sizes: list[int] = []
    keys = [f"myproject/e{index}" for index in range(250)]

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        chunk_sizes.append(len(body["keys"]))
        return httpx.Response(
            200,
            json={
                "success": True,
                "result": {
                    "values": {
                        key: {
                            "build_id": "ABC123",
                            "r2_prefix": "myproject/__builds/ABC123/",
                            "cache_profile": "short",
                        }
                        for key in body["keys"]
                    }
                },
            },
        )

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        pointers = await pub.get_pointers(keys)

    assert chunk_sizes == [100, 100, 50]
    assert len(pointers) == 250
    assert set(pointers) == set(keys)


@pytest.mark.asyncio
async def test_get_pointers_maps_null_and_absent_keys_to_none() -> None:
    """Both ways Cloudflare says "nothing here" read the same.

    A key that has never been written is omitted from ``values``
    entirely; a key whose value is JSON ``null`` is present and null.
    The reconciler asks only whether a usable pointer is published, so
    the two collapse to ``None`` rather than to distinguishable states
    the caller would then have to re-collapse.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "success": True,
                "result": {"values": {"myproject/nulled": None}},
            },
        )

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        pointers = await pub.get_pointers(
            ["myproject/nulled", "myproject/absent"]
        )

    assert pointers == {"myproject/nulled": None, "myproject/absent": None}


@pytest.mark.asyncio
async def test_get_pointers_reads_a_pointer_without_a_cache_profile() -> None:
    """A pointer written before the profile field is still a pointer."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "success": True,
                "result": {
                    "values": {
                        "myproject/main": {
                            "build_id": "ABC123",
                            "r2_prefix": "myproject/__builds/ABC123/",
                        }
                    }
                },
            },
        )

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        pointers = await pub.get_pointers(["myproject/main"])

    assert pointers == {
        "myproject/main": EditionPointer(
            build_public_id="ABC123",
            r2_prefix="myproject/__builds/ABC123/",
            cache_profile=None,
        )
    }


@pytest.mark.asyncio
async def test_get_pointers_issues_no_request_for_no_keys() -> None:
    """An org with nothing to read costs no Cloudflare rate-limit slot."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"success": True})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        pointers = await pub.get_pointers([])

    assert pointers == {}
    assert requests == []


@pytest.mark.asyncio
async def test_get_pointers_retries_5xx_then_succeeds() -> None:
    """The read-back shares the pointer write's backoff helper."""
    statuses = [503, 200]
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        status = statuses[len(attempts) - 1]
        return httpx.Response(
            status,
            json={
                "success": status == 200,
                "result": {
                    "values": {
                        "myproject/main": {
                            "build_id": "ABC123",
                            "r2_prefix": "myproject/__builds/ABC123/",
                            "cache_profile": "long",
                        }
                    }
                },
            },
        )

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        pointers = await pub.get_pointers(["myproject/main"])

    assert attempts == [1, 2]
    assert pointers["myproject/main"] is not None


@pytest.mark.asyncio
async def test_get_pointers_raises_on_persistent_5xx() -> None:
    """A failed read is never reported as an edge with no pointers.

    Returning a partial (or empty) view would tell the reconciler every
    edition had lost its pointer and re-drive a whole org's publishes
    over an HTTP outage, so the chunk's failure takes the read with it.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"errors": ["boom"]})

    with capture_logs() as logs:
        publisher, client = _make_publisher(
            httpx.MockTransport(handler), max_attempts=2
        )
        async with client, publisher as pub:
            with pytest.raises(httpx.HTTPStatusError) as excinfo:
                await pub.get_pointers(["myproject/main"])

    assert excinfo.value.response.status_code == 503
    errors = [entry for entry in logs if entry["log_level"] == "error"]
    assert len(errors) == 1
    assert errors[0]["attempts"] == 2


@pytest.mark.asyncio
async def test_get_pointers_raises_when_result_lacks_values() -> None:
    """A 2xx whose ``result`` carries no ``values`` map is a failure.

    Reading the absent map as "no key is published" is the same lie a
    half-read view would tell: every ``published`` edition would come
    back as ``pointer_missing`` and the tick would re-drive the whole
    org's publishes, once every half hour, for as long as Cloudflare
    kept answering in that shape.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"success": True, "result": {}})

    with capture_logs() as logs:
        publisher, client = _make_publisher(httpx.MockTransport(handler))
        async with client, publisher as pub:
            with pytest.raises(CloudflareKvReadError) as excinfo:
                await pub.get_pointers(["myproject/main", "myproject/v1"])

    assert excinfo.value.key_count == 2
    assert excinfo.value.missing_field == "result.values"
    assert "2 keys" in str(excinfo.value)
    assert "result.values" in str(excinfo.value)
    errors = [entry for entry in logs if entry["log_level"] == "error"]
    assert len(errors) == 1
    assert errors[0]["missing_field"] == "result.values"
    assert errors[0]["response_body"] == '{"success":true,"result":{}}'


@pytest.mark.asyncio
async def test_get_pointers_raises_when_values_is_not_an_object() -> None:
    """``result.values`` that is not a map cannot be read key by key."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"success": True, "result": {"values": []}}
        )

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(CloudflareKvReadError) as excinfo:
            await pub.get_pointers(["myproject/main"])

    assert excinfo.value.missing_field == "result.values"


@pytest.mark.asyncio
async def test_get_pointers_raises_when_body_is_not_an_object() -> None:
    """A 2xx body that is not a JSON object is malformed, not empty."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=["not", "an", "object"])

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(CloudflareKvReadError) as excinfo:
            await pub.get_pointers(["myproject/main"])

    assert excinfo.value.missing_field == "body"


@pytest.mark.asyncio
async def test_get_pointers_raises_when_body_is_not_json() -> None:
    """An HTML error page served with a 2xx is a malformed body too.

    A proxy in front of Cloudflare can answer ``200 text/html``; letting
    the ``json()`` decode error escape would route the failure as an
    untyped ``ValueError`` with no namespace or key count attached.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>maintenance</html>")

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(CloudflareKvReadError) as excinfo:
            await pub.get_pointers(["myproject/main"])

    assert excinfo.value.missing_field == "body"


@pytest.mark.asyncio
async def test_get_pointers_malformed_body_reads_no_further_chunks() -> None:
    """The first malformed chunk takes the whole read with it."""
    requests: list[httpx.Request] = []
    keys = [f"myproject/e{index}" for index in range(150)]

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"success": True})

    publisher, client = _make_publisher(httpx.MockTransport(handler))
    async with client, publisher as pub:
        with pytest.raises(CloudflareKvReadError) as excinfo:
            await pub.get_pointers(keys)

    assert len(requests) == 1
    assert excinfo.value.key_count == 100
    assert excinfo.value.missing_field == "result"


def test_kv_read_error_to_sentry_keeps_the_body_out_of_the_message() -> None:
    """The namespace and shape are tags; the body is context only.

    ``str(exc)`` is copied into the failing job's ``errors['message']``,
    becomes the Sentry issue title, and is what the Slack message
    renders — so the unbounded response body belongs in the context a
    triager opens, not in any of those three.
    """
    exc = CloudflareKvReadError(
        namespace_id="ns-456",
        key_count=100,
        missing_field="result.values",
        response_body='{"success": true, "result": {}}',
    )

    info = exc.to_sentry()

    assert info.tags == {
        "kv_namespace_id": "ns-456",
        "kv_key_count": "100",
        "kv_missing_field": "result.values",
    }
    assert info.contexts["cloudflare_kv_read"] == {
        "namespace_id": "ns-456",
        "key_count": 100,
        "missing_field": "result.values",
        "response_body": '{"success": true, "result": {}}',
    }
    assert '{"success": true' not in exc.message
    assert str(exc) == exc.message


def test_kv_read_error_truncates_a_large_body() -> None:
    """A megabyte of HTML does not ride into the Sentry envelope."""
    exc = CloudflareKvReadError(
        namespace_id="ns-456",
        key_count=1,
        missing_field="body",
        response_body="x" * (64 * 1024),
    )

    body = exc.to_sentry().contexts["cloudflare_kv_read"]["response_body"]

    assert body is not None
    assert len(body) < 64 * 1024
