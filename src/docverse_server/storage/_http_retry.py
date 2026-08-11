"""Shared retry policy for outbound third-party HTTP calls.

Docverse makes flaky third-party HTTP calls from the storage layer in
three places: LTD Keeper (`docverse_server.storage.ltd.LtdClient`), the
Cloudflare zone purge API
(`docverse_server.storage.cdncachepurger.CloudflareCachePurger`), and
presigned object uploads
(`docverse_server.storage.objectstore.S3ObjectStore`). All three need
the same decisions — which statuses are worth another attempt, which
transport failures are worth another attempt, how long to wait between
attempts, and whether to trust a server-supplied ``Retry-After`` — so
the policy lives here once instead of being mirrored (and then
drifting) at each call site.

The helpers are deliberately stateless functions rather than a mixin or
decorator: each caller owns its own request/response loop (LTD raises
typed errors and treats 404 specially; the purger is best-effort; the
object store re-signs its URL between attempts), and only the waiting
policy is genuinely shared.
"""

from __future__ import annotations

import httpx
import structlog

__all__ = [
    "MAX_BACKOFF_SECONDS",
    "RETRYABLE_STATUS_CODES",
    "RETRYABLE_TRANSPORT_ERRORS",
    "backoff_for_attempt",
    "backoff_for_response",
]

#: Upper bound, in seconds, on any single wait between attempts —
#: including one a server asks for via ``Retry-After``.
#:
#: Callers sleep this delay *in line*, and the line they block is
#: expensive: a publish job holds an open database transaction for the
#: length of the purge, and the CDN purge coalescer holds a per-hostname
#: lock across it, so every other publish for that hostname queues
#: behind the sleep. Cloudflare answering a rate-limited purge with
#: ``Retry-After: 300`` would therefore pin a database connection and
#: serialize a whole publish burst for five minutes. We would rather
#: give up after a bounded wait and let the caller's best-effort
#: handling (or the next publish) deal with it, so a server's request
#: is treated as an upper hint and clamped to this ceiling.
MAX_BACKOFF_SECONDS = 10.0

#: Status codes worth retrying: rate limiting plus the transient 5xx
#: family. Every other 4xx is a client-side mistake (bad token, wrong
#: zone, malformed body) that a retry cannot fix.
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

#: Transport failures worth retrying, as a tuple suitable for ``except``.
#: These are the cases where no response came back at all but the
#: request may still succeed later: the connection could not be
#: established, it died mid-exchange, the peer went away mid-response,
#: or the exchange timed out.
#:
#: Two base classes do the work. ``httpx.TimeoutException`` is the
#: parent of ``ReadTimeout`` and its connect/write/pool siblings, so
#: naming it covers every timeout flavour without listing four
#: subclasses. ``httpx.NetworkError`` is the parent of ``ConnectError``
#: *and* of ``ReadError``/``WriteError``/``CloseError`` — the latter two
#: matter because a connection reset while a large PUT body is being
#: sent or its response read surfaces as ``WriteError``/``ReadError``,
#: never as ``ConnectError``. Those resets are precisely the "dropped
#: connections" R2 hands us during a bulk copy, so leaving them out
#: would mean the upload retry never fired for its main use case.
#:
#: Deliberately narrower than ``httpx.TransportError``: that also
#: catches ``LocalProtocolError`` and ``UnsupportedProtocol``, which are
#: bugs on our side of the wire and must fail loudly on the first
#: attempt rather than burn a retry budget.
RETRYABLE_TRANSPORT_ERRORS: tuple[type[httpx.HTTPError], ...] = (
    httpx.TimeoutException,
    httpx.NetworkError,
    httpx.RemoteProtocolError,
)


def backoff_for_attempt(attempt: int, *, base_backoff_seconds: float) -> float:
    """Compute the exponential backoff delay for a 1-based attempt.

    Parameters
    ----------
    attempt
        The attempt that just failed, counting from 1.
    base_backoff_seconds
        Delay after the first failure; doubles each subsequent attempt.

    Returns
    -------
    float
        Seconds to wait before the next attempt, never longer than
        `MAX_BACKOFF_SECONDS`.
    """
    multiplier: int = 2 ** (attempt - 1)
    return min(MAX_BACKOFF_SECONDS, base_backoff_seconds * multiplier)


def backoff_for_response(
    response: httpx.Response,
    attempt: int,
    *,
    base_backoff_seconds: float,
    logger: structlog.stdlib.BoundLogger,
) -> float:
    """Honour a numeric ``Retry-After``, else fall back to exp backoff.

    The header is treated as a hint, not an instruction: the value is
    clamped to ``[0, MAX_BACKOFF_SECONDS]`` because the caller sleeps it
    while holding a database transaction and the purge coalescer's lock
    (see `MAX_BACKOFF_SECONDS`).

    ``Retry-After`` may also be an HTTP-date, which neither API sends in
    practice; a non-numeric value is logged at ``WARNING`` and the
    computed exponential backoff is used instead, so an unexpected
    header format degrades to the default policy rather than crashing
    the retry loop. That fallback is also why an HTTP-date far in the
    future can never become a long sleep — it is never parsed into one,
    and the cap would bound it even if it were.

    Parameters
    ----------
    response
        The failed response whose headers may carry ``Retry-After``.
    attempt
        The attempt that just failed, counting from 1.
    base_backoff_seconds
        Delay after the first failure; doubles each subsequent attempt.
    logger
        Logger used to warn about an uninterpretable ``Retry-After``.

    Returns
    -------
    float
        Seconds to wait before the next attempt, never negative and
        never longer than `MAX_BACKOFF_SECONDS`.
    """
    retry_after = response.headers.get("Retry-After")
    if retry_after is not None:
        try:
            value = float(retry_after)
        except ValueError:
            logger.warning(
                "Ignoring non-numeric Retry-After",
                retry_after=retry_after,
            )
        else:
            if value > MAX_BACKOFF_SECONDS:
                logger.warning(
                    "Capping long Retry-After",
                    retry_after=value,
                    capped_to=MAX_BACKOFF_SECONDS,
                )
            return min(MAX_BACKOFF_SECONDS, max(0.0, value))
    return backoff_for_attempt(
        attempt, base_backoff_seconds=base_backoff_seconds
    )
