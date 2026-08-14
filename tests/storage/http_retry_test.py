"""Tests for the shared outbound-HTTP retry policy.

These cover the three decisions every retrying storage client delegates
here: which transport failures deserve another attempt, how long to wait
between attempts, and the attempt loop that strings the two together.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

from docverse_server.storage._http_retry import (
    MAX_BACKOFF_SECONDS,
    RETRYABLE_TRANSPORT_ERRORS,
    backoff_for_attempt,
    backoff_for_response,
    retry_request,
)


def _record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replace ``asyncio.sleep`` with a recorder and return the log."""
    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    return delays


def _sender(
    *outcomes: httpx.Response | Exception,
) -> tuple[Callable[[], Awaitable[httpx.Response]], list[int]]:
    """Build a ``send`` callable that replays ``outcomes`` in order.

    Returns the callable alongside a list that grows by one entry per
    call, so a test can assert how much of the attempt budget was spent.
    """
    attempts: list[int] = []

    async def send() -> httpx.Response:
        attempts.append(len(attempts) + 1)
        outcome = outcomes[len(attempts) - 1]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return (send, attempts)


@pytest.mark.parametrize(
    "error",
    [
        httpx.WriteError("broken pipe"),
        httpx.ReadError("connection reset by peer"),
        httpx.ConnectError("connection refused"),
        httpx.ReadTimeout("timed out"),
        httpx.RemoteProtocolError("server disconnected"),
    ],
)
def test_retryable_transport_errors_cover_reset_connections(
    error: httpx.HTTPError,
) -> None:
    """A TCP reset mid-PUT surfaces as WriteError/ReadError, not Connect.

    Those are exactly the "dropped connections" the presigned-upload
    retry exists for, so they have to be in the retryable tuple.
    """
    assert isinstance(error, RETRYABLE_TRANSPORT_ERRORS)


@pytest.mark.parametrize(
    "error",
    [
        httpx.LocalProtocolError("bad header on our side"),
        httpx.UnsupportedProtocol("unknown scheme"),
    ],
)
def test_retryable_transport_errors_exclude_our_own_bugs(
    error: httpx.HTTPError,
) -> None:
    """Client-side protocol mistakes must fail on the first attempt."""
    assert not isinstance(error, RETRYABLE_TRANSPORT_ERRORS)


def test_backoff_for_attempt_is_exponential() -> None:
    """Each attempt waits twice as long as the previous one."""
    delays = [
        backoff_for_attempt(attempt, base_backoff_seconds=0.5)
        for attempt in (1, 2, 3)
    ]

    assert delays == [0.5, 1.0, 2.0]


def test_backoff_for_attempt_is_capped() -> None:
    """A generous base or attempt budget cannot produce a long sleep."""
    delay = backoff_for_attempt(10, base_backoff_seconds=1.0)

    assert delay == MAX_BACKOFF_SECONDS


def test_backoff_for_response_caps_long_retry_after() -> None:
    """A server-supplied ``Retry-After`` is honoured only up to the cap.

    Callers sleep this inside an open database transaction while holding
    the publish coalescer's per-hostname lock, so an obedient five-minute
    wait would pin a connection and serialize a whole publish burst.
    """
    response = httpx.Response(429, headers={"Retry-After": "300"})

    delay = backoff_for_response(
        response,
        1,
        base_backoff_seconds=0.5,
        logger=structlog.get_logger("test"),
    )

    assert delay == MAX_BACKOFF_SECONDS


def test_backoff_for_response_honours_short_retry_after() -> None:
    """A ``Retry-After`` under the cap is used verbatim."""
    response = httpx.Response(429, headers={"Retry-After": "3"})

    delay = backoff_for_response(
        response,
        1,
        base_backoff_seconds=0.5,
        logger=structlog.get_logger("test"),
    )

    assert delay == 3.0


def test_backoff_for_attempt_honours_caller_max_backoff() -> None:
    """A caller may raise the ceiling above the shared default."""
    delay = backoff_for_attempt(
        6, base_backoff_seconds=1.0, max_backoff_seconds=300.0
    )

    assert delay == 32.0


def test_backoff_for_response_honours_caller_max_backoff() -> None:
    """A caller with a generous ceiling rides out a long rate-limit window.

    ``LtdClient`` sleeps its backoff while holding no database
    transaction and no coalescer lock, so an LTD ``Retry-After: 60``
    must be obeyed rather than clamped to the shared 10 s default —
    otherwise the whole retry budget burns inside the same rate-limit
    window and every project in a tier tick fails its sync.
    """
    response = httpx.Response(429, headers={"Retry-After": "60"})

    delay = backoff_for_response(
        response,
        1,
        base_backoff_seconds=0.5,
        logger=structlog.get_logger("test"),
        max_backoff_seconds=300.0,
    )

    assert delay == 60.0


def test_backoff_for_response_caps_at_caller_max_backoff() -> None:
    """An override raises the ceiling; it never removes it."""
    response = httpx.Response(429, headers={"Retry-After": "9999"})

    delay = backoff_for_response(
        response,
        1,
        base_backoff_seconds=0.5,
        logger=structlog.get_logger("test"),
        max_backoff_seconds=300.0,
    )

    assert delay == 300.0


def test_backoff_for_response_clamps_negative_retry_after() -> None:
    """A nonsense negative ``Retry-After`` never becomes a negative sleep."""
    response = httpx.Response(503, headers={"Retry-After": "-5"})

    delay = backoff_for_response(
        response,
        1,
        base_backoff_seconds=0.5,
        logger=structlog.get_logger("test"),
    )

    assert delay == 0.0


@pytest.mark.asyncio
async def test_retry_request_returns_a_first_attempt_success() -> None:
    """A healthy call costs exactly one attempt and no sleeping."""
    send, attempts = _sender(httpx.Response(200))

    outcome = await retry_request(
        send,
        operation="test call",
        logger=structlog.get_logger("test"),
        base_backoff_seconds=0.0,
    )

    assert attempts == [1]
    assert outcome.attempts == 1
    assert outcome.response.status_code == 200
    assert outcome.retryable is False


@pytest.mark.asyncio
async def test_retry_request_retries_a_retryable_status() -> None:
    """A 429 is retried and the eventual success is what comes back."""
    send, attempts = _sender(
        httpx.Response(429),
        httpx.Response(503),
        httpx.Response(200),
    )

    outcome = await retry_request(
        send,
        operation="test call",
        logger=structlog.get_logger("test"),
        base_backoff_seconds=0.0,
    )

    assert attempts == [1, 2, 3]
    assert outcome.attempts == 3
    assert outcome.response.status_code == 200


@pytest.mark.asyncio
async def test_retry_request_returns_the_exhausted_response() -> None:
    """A never-clearing 500 comes back as an outcome, not an exception.

    Each caller renders its own failure — ``raise_for_status``, a typed
    purge error, an ``LtdClientError`` — so the loop hands the terminal
    response back rather than choosing one of those for them.
    """
    send, attempts = _sender(*[httpx.Response(500) for _ in range(3)])

    outcome = await retry_request(
        send,
        operation="test call",
        logger=structlog.get_logger("test"),
        max_attempts=3,
        base_backoff_seconds=0.0,
    )

    assert attempts == [1, 2, 3]
    assert outcome.attempts == 3
    assert outcome.retryable is True


@pytest.mark.asyncio
async def test_retry_request_does_not_retry_a_client_error() -> None:
    """A 403 is a bad token; another attempt cannot fix it."""
    send, attempts = _sender(httpx.Response(403), httpx.Response(200))

    outcome = await retry_request(
        send,
        operation="test call",
        logger=structlog.get_logger("test"),
        base_backoff_seconds=0.0,
    )

    assert attempts == [1]
    assert outcome.response.status_code == 403
    assert outcome.retryable is False


@pytest.mark.asyncio
async def test_retry_request_retries_transport_failures() -> None:
    """A dropped connection shares the status path's attempt budget."""
    send, attempts = _sender(
        httpx.ConnectError("connection refused"),
        httpx.Response(200),
    )

    outcome = await retry_request(
        send,
        operation="test call",
        logger=structlog.get_logger("test"),
        base_backoff_seconds=0.0,
    )

    assert attempts == [1, 2]
    assert outcome.attempts == 2


@pytest.mark.asyncio
async def test_retry_request_reraises_an_exhausted_transport_failure() -> None:
    """The original ``httpx`` error escapes so callers can wrap it.

    Re-raising rather than translating keeps each caller's ``__cause__``
    chain pointing at the real transport failure.
    """
    send, attempts = _sender(
        *[httpx.ConnectError("connection refused") for _ in range(3)]
    )

    with pytest.raises(httpx.ConnectError):
        await retry_request(
            send,
            operation="test call",
            logger=structlog.get_logger("test"),
            max_attempts=3,
            base_backoff_seconds=0.0,
        )

    assert attempts == [1, 2, 3]


@pytest.mark.asyncio
async def test_retry_request_fails_fast_on_our_own_protocol_bugs() -> None:
    """A bad scheme or malformed request must not burn the budget.

    ``UnsupportedProtocol`` is what a misconfigured base URL looks like:
    every attempt would fail identically, so the operator's mistake has
    to surface on the first one.
    """
    send, attempts = _sender(
        httpx.UnsupportedProtocol("unknown scheme"),
        httpx.Response(200),
    )

    with pytest.raises(httpx.UnsupportedProtocol):
        await retry_request(
            send,
            operation="test call",
            logger=structlog.get_logger("test"),
            base_backoff_seconds=0.0,
        )

    assert attempts == [1]


@pytest.mark.asyncio
async def test_retry_request_clamps_a_nonsense_attempt_budget() -> None:
    """A budget below 1 degrades to "try once", never to a silent no-op."""
    send, attempts = _sender(httpx.Response(200))

    outcome = await retry_request(
        send,
        operation="test call",
        logger=structlog.get_logger("test"),
        max_attempts=0,
        base_backoff_seconds=0.0,
    )

    assert attempts == [1]
    assert outcome.attempts == 1


@pytest.mark.asyncio
async def test_retry_request_honours_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A server's ``Retry-After`` wins over the computed backoff."""
    delays = _record_sleeps(monkeypatch)
    send, _ = _sender(
        httpx.Response(429, headers={"Retry-After": "7"}),
        httpx.Response(200),
    )

    await retry_request(
        send,
        operation="test call",
        logger=structlog.get_logger("test"),
        base_backoff_seconds=0.5,
    )

    assert delays == [7.0]


@pytest.mark.asyncio
async def test_retry_request_honours_a_raised_backoff_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller blocking nothing can ride out a long rate-limit window."""
    delays = _record_sleeps(monkeypatch)
    send, _ = _sender(
        httpx.Response(429, headers={"Retry-After": "60"}),
        httpx.Response(200),
    )

    await retry_request(
        send,
        operation="test call",
        logger=structlog.get_logger("test"),
        base_backoff_seconds=0.5,
        max_backoff_seconds=300.0,
    )

    assert delays == [60.0]


@pytest.mark.asyncio
async def test_retry_request_logs_each_retry_with_its_operation() -> None:
    """Every retry is visible even when the call eventually succeeds.

    A transient rate limit that clears never reaches a caller's terminal
    error log, so this warning is the only record that Docverse is being
    throttled — the signal an operator needs before it becomes an outage.
    """
    send, _ = _sender(
        httpx.Response(429),
        httpx.ConnectError("connection refused"),
        httpx.Response(200),
    )

    with capture_logs() as logs:
        await retry_request(
            send,
            operation="widget purge",
            logger=structlog.get_logger("test"),
            base_backoff_seconds=0.0,
        )

    warnings = [entry for entry in logs if entry["log_level"] == "warning"]
    assert [entry["event"] for entry in warnings] == [
        "Retrying widget purge",
        "Retrying widget purge after transport error",
    ]
    assert warnings[0]["status_code"] == 429
    assert warnings[0]["attempt"] == 1
    assert warnings[1]["error_type"] == "ConnectError"


@pytest.mark.asyncio
async def test_retry_request_adds_caller_context_to_retry_logs() -> None:
    """Callers may enrich the retry warning from the failed response.

    Cloudflare reports *why* it refused in ``errors[].code`` — ``1134``
    for a purge rate limit — and that code is the difference between
    "we publish too fast" and "our token expired".
    """
    send, _ = _sender(
        httpx.Response(429, json={"errors": [{"code": 1134}]}),
        httpx.Response(200),
    )

    with capture_logs() as logs:
        await retry_request(
            send,
            operation="widget purge",
            logger=structlog.get_logger("test"),
            base_backoff_seconds=0.0,
            retry_log_context=lambda response: {
                "error_codes": [
                    entry["code"] for entry in response.json()["errors"]
                ]
            },
        )

    warnings = [entry for entry in logs if entry["log_level"] == "warning"]
    assert len(warnings) == 1
    assert warnings[0]["error_codes"] == [1134]
