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
    "RETRYABLE_STATUS_CODES",
    "RETRYABLE_TRANSPORT_ERRORS",
    "backoff_for_attempt",
    "backoff_for_response",
]

#: Status codes worth retrying: rate limiting plus the transient 5xx
#: family. Every other 4xx is a client-side mistake (bad token, wrong
#: zone, malformed body) that a retry cannot fix.
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

#: Transport failures worth retrying, as a tuple suitable for ``except``.
#: These are the cases where no response came back at all but the
#: request may still succeed later: the connection could not be
#: established, the peer went away mid-response, or the exchange timed
#: out. ``httpx.TimeoutException`` is the parent of ``ReadTimeout`` and
#: its connect/write/pool siblings, so naming it covers every timeout
#: flavour without listing four subclasses.
#:
#: Deliberately narrower than ``httpx.TransportError``: that also
#: catches ``LocalProtocolError`` and ``UnsupportedProtocol``, which are
#: bugs on our side of the wire and must fail loudly on the first
#: attempt rather than burn a retry budget.
RETRYABLE_TRANSPORT_ERRORS: tuple[type[httpx.HTTPError], ...] = (
    httpx.TimeoutException,
    httpx.ConnectError,
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
        Seconds to wait before the next attempt.
    """
    multiplier: int = 2 ** (attempt - 1)
    return base_backoff_seconds * multiplier


def backoff_for_response(
    response: httpx.Response,
    attempt: int,
    *,
    base_backoff_seconds: float,
    logger: structlog.stdlib.BoundLogger,
) -> float:
    """Honour a numeric ``Retry-After``, else fall back to exp backoff.

    ``Retry-After`` may also be an HTTP-date, which neither API sends in
    practice; a non-numeric value is logged at ``WARNING`` and the
    computed exponential backoff is used instead, so an unexpected
    header format degrades to the default policy rather than crashing
    the retry loop.

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
        Seconds to wait before the next attempt, never negative.
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
            return max(0.0, value)
    return backoff_for_attempt(
        attempt, base_backoff_seconds=base_backoff_seconds
    )
