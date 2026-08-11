"""Tests for the shared outbound-HTTP retry policy.

These cover the two decisions every retrying storage client delegates
here: which transport failures deserve another attempt, and how long to
wait between attempts.
"""

from __future__ import annotations

import httpx
import pytest
import structlog

from docverse_server.storage._http_retry import (
    MAX_BACKOFF_SECONDS,
    RETRYABLE_TRANSPORT_ERRORS,
    backoff_for_attempt,
    backoff_for_response,
)


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
