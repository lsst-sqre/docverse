"""Shared retry policy for outbound third-party HTTP calls.

Docverse makes flaky third-party HTTP calls from the storage layer in
four places: LTD Keeper (`docverse_server.storage.ltd.LtdClient`), the
Cloudflare zone purge API
(`docverse_server.storage.cdncachepurger.CloudflareCachePurger`), the
Cloudflare Workers KV pointer write
(`docverse_server.storage.editionpublisher.CloudflareKvEditionPublisher`),
and presigned object uploads
(`docverse_server.storage.objectstore.S3ObjectStore`). All four need
the same decisions — which statuses are worth another attempt, which
transport failures are worth another attempt, how long to wait between
attempts, and whether to trust a server-supplied ``Retry-After`` — so
the policy lives here once instead of being mirrored (and then
drifting) at each call site.

`retry_request` owns the whole attempt loop, and the callers own only
what is genuinely theirs: how to issue one attempt (the object store
re-signs its presigned URL each time) and how to render a terminal
failure (LTD raises typed errors and treats 404 specially; the purger
translates into a Slack-routed exception; the object store and the KV
publisher call ``raise_for_status``). The waiting helpers stay exported
because the loop is built from them and they are worth testing on their
own.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx
import structlog

__all__ = [
    "DEFAULT_BASE_BACKOFF_SECONDS",
    "DEFAULT_MAX_ATTEMPTS",
    "MAX_BACKOFF_SECONDS",
    "RETRYABLE_STATUS_CODES",
    "RETRYABLE_TRANSPORT_ERRORS",
    "RetryOutcome",
    "backoff_for_attempt",
    "backoff_for_response",
    "retry_request",
]

#: Default attempts allowed for one logical request, *including* the
#: original (so ``DEFAULT_MAX_ATTEMPTS - 1`` retries). Shared by every
#: storage client: they all talk to rate-limited third-party APIs whose
#: throttles clear on the order of seconds, and there is no evidence any
#: one of them wants a different budget. A caller that does can pass
#: ``max_attempts``.
DEFAULT_MAX_ATTEMPTS = 4

#: Default delay after a first failure, in seconds; doubles each
#: subsequent attempt up to the applicable ceiling.
DEFAULT_BASE_BACKOFF_SECONDS = 0.5

#: Default upper bound, in seconds, on any single wait between attempts
#: — including one a server asks for via ``Retry-After``.
#:
#: The default is tuned for the callers that sleep this delay *in line*
#: on an expensive line: a publish job holds an open database
#: transaction for the length of the purge, and the CDN purge coalescer
#: holds a per-hostname lock across it, so every other publish for that
#: hostname queues behind the sleep. Cloudflare answering a
#: rate-limited purge with ``Retry-After: 300`` would therefore pin a
#: database connection and serialize a whole publish burst for five
#: minutes. We would rather give up after a bounded wait and let the
#: caller's best-effort handling (or the next publish) deal with it, so
#: a server's request is treated as an upper hint and clamped to this
#: ceiling.
#:
#: A caller that blocks nothing expensive while it waits may raise the
#: ceiling with the ``max_backoff_seconds`` argument both helpers
#: accept — see `LtdClient
#: <docverse_server.storage.ltd.LtdClient>`, whose read-only GETs hold
#: neither a transaction nor a lock and so can afford to ride out a
#: full LTD rate-limit window instead of failing inside it. The
#: argument raises the ceiling; it never removes one.
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


def backoff_for_attempt(
    attempt: int,
    *,
    base_backoff_seconds: float,
    max_backoff_seconds: float = MAX_BACKOFF_SECONDS,
) -> float:
    """Compute the exponential backoff delay for a 1-based attempt.

    Parameters
    ----------
    attempt
        The attempt that just failed, counting from 1.
    base_backoff_seconds
        Delay after the first failure; doubles each subsequent attempt.
    max_backoff_seconds
        Ceiling on the returned delay. Defaults to `MAX_BACKOFF_SECONDS`;
        a caller that blocks nothing expensive while it sleeps may pass
        a larger value.

    Returns
    -------
    float
        Seconds to wait before the next attempt, never longer than
        ``max_backoff_seconds``.
    """
    multiplier: int = 2 ** (attempt - 1)
    return min(max_backoff_seconds, base_backoff_seconds * multiplier)


def backoff_for_response(
    response: httpx.Response,
    attempt: int,
    *,
    base_backoff_seconds: float,
    logger: structlog.stdlib.BoundLogger,
    max_backoff_seconds: float = MAX_BACKOFF_SECONDS,
) -> float:
    """Honour a numeric ``Retry-After``, else fall back to exp backoff.

    The header is treated as a hint, not an instruction: the value is
    clamped to ``[0, max_backoff_seconds]``. The default ceiling is
    tight because the callers it was written for sleep while holding a
    database transaction and the purge coalescer's lock; a caller
    holding neither raises it (see `MAX_BACKOFF_SECONDS`) so a long
    rate-limit window can be waited out rather than failed inside.

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
    max_backoff_seconds
        Ceiling on the returned delay, applied to a server-supplied
        ``Retry-After`` as much as to the computed fallback. Defaults to
        `MAX_BACKOFF_SECONDS`.

    Returns
    -------
    float
        Seconds to wait before the next attempt, never negative and
        never longer than ``max_backoff_seconds``.
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
            if value > max_backoff_seconds:
                logger.warning(
                    "Capping long Retry-After",
                    retry_after=value,
                    capped_to=max_backoff_seconds,
                )
            return min(max_backoff_seconds, max(0.0, value))
    return backoff_for_attempt(
        attempt,
        base_backoff_seconds=base_backoff_seconds,
        max_backoff_seconds=max_backoff_seconds,
    )


@dataclass(frozen=True, slots=True)
class RetryOutcome:
    """The response `retry_request` settled on, and what it cost.

    Carries the attempt count alongside the response because every
    caller's terminal log line reports it, and by then the loop that
    counted the attempts is gone.
    """

    response: httpx.Response
    """The last response received, successful or not."""

    attempts: int
    """Attempts spent reaching it, counting the original request."""

    @property
    def retryable(self) -> bool:
        """Whether the budget, not the status, ended the attempts.

        ``True`` only for a terminal response whose status *was* worth
        retrying — i.e. the attempt budget ran out mid-throttle rather
        than the server saying something a retry could never fix. That
        distinction is what tells an operator "raise the budget" apart
        from "fix the credential".
        """
        return self.response.status_code in RETRYABLE_STATUS_CODES


async def retry_request(
    send: Callable[[], Awaitable[httpx.Response]],
    *,
    operation: str,
    logger: structlog.stdlib.BoundLogger,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS,
    max_backoff_seconds: float = MAX_BACKOFF_SECONDS,
    retry_log_context: (
        Callable[[httpx.Response], Mapping[str, Any]] | None
    ) = None,
) -> RetryOutcome:
    """Issue a request, retrying transient failures with backoff.

    The loop every storage client used to keep its own copy of: retry
    `RETRYABLE_STATUS_CODES` and `RETRYABLE_TRANSPORT_ERRORS`, wait
    `backoff_for_response` / `backoff_for_attempt` in between, and stop
    at the attempt budget.

    What it deliberately does *not* do is decide what a failure means.
    A terminal response — a 403, an unfollowed 3xx, or a 503 that
    outlasted the budget — comes back as a `RetryOutcome` for the caller
    to translate into ``raise_for_status``, a typed purge error, or an
    ``LtdClientError``. A transport failure that outlasts the budget is
    re-raised as itself so the caller's own exception keeps the real
    ``httpx`` error as its ``__cause__``.

    Parameters
    ----------
    send
        Issues one attempt and returns its response. Called afresh per
        attempt, so a caller whose request cannot simply be replayed
        (`~docverse_server.storage.objectstore.S3ObjectStore` re-signs
        its presigned URL) rebuilds it here.
    operation
        Human-readable name of what is being retried, used to build the
        retry log events (``"Retrying {operation}"``). Phrase it as a
        lowercase-or-proper-noun noun phrase, e.g. ``"presigned
        upload"`` or ``"Cloudflare cache purge"``.
    logger
        Logger for the retry warnings. Bind the caller's identifying
        context (object key, hostname, edition slug) onto it first —
        this function adds only the attempt and delay fields.
    max_attempts
        Attempts allowed including the original. Clamped to at least 1
        so a misconfigured budget degrades to "try once, no retries"
        rather than to a silent no-op that never issues the request.
    base_backoff_seconds
        Delay after the first failure; doubles each subsequent attempt.
    max_backoff_seconds
        Ceiling on any single wait, including one a server asks for via
        ``Retry-After``. See `MAX_BACKOFF_SECONDS` for why the default
        is tight and who may raise it.
    retry_log_context
        Optional hook returning extra structured fields for the retry
        warning, derived from the response that triggered it. A retry
        that eventually succeeds never reaches a caller's terminal error
        log, so this warning is the only record of it — and for
        Cloudflare that record is only actionable with the API's own
        ``errors[].code`` in it (``1134`` is "publishing too fast",
        an auth code is "the token expired").

    Returns
    -------
    RetryOutcome
        The terminal response and the attempts it took. A success, a
        status not worth retrying, or a retryable status that outlasted
        the budget all arrive this way.

    Raises
    ------
    httpx.TransportError
        If the transport fails in a way no retry can fix (a bug or
        misconfiguration on our side, see `RETRYABLE_TRANSPORT_ERRORS`),
        which happens on the first attempt, or if a retryable transport
        failure outlasts the attempt budget.
    """
    budget = max(1, max_attempts)
    attempt = 0
    while True:
        attempt += 1
        try:
            response = await send()
        except RETRYABLE_TRANSPORT_ERRORS as exc:
            # No response came back at all. That is as transient as a
            # 429 and just as recoverable, so it shares the status
            # path's attempt budget instead of aborting on the first
            # dropped connection.
            if attempt >= budget:
                raise
            delay = backoff_for_attempt(
                attempt,
                base_backoff_seconds=base_backoff_seconds,
                max_backoff_seconds=max_backoff_seconds,
            )
            logger.warning(
                f"Retrying {operation} after transport error",
                error=str(exc),
                error_type=type(exc).__name__,
                attempt=attempt,
                max_attempts=budget,
                retry_delay=delay,
            )
            await asyncio.sleep(delay)
            continue

        retryable = response.status_code in RETRYABLE_STATUS_CODES
        if not (retryable and attempt < budget):
            return RetryOutcome(response=response, attempts=attempt)

        delay = backoff_for_response(
            response,
            attempt,
            base_backoff_seconds=base_backoff_seconds,
            logger=logger,
            max_backoff_seconds=max_backoff_seconds,
        )
        extra = retry_log_context(response) if retry_log_context else {}
        logger.warning(
            f"Retrying {operation}",
            status_code=response.status_code,
            attempt=attempt,
            max_attempts=budget,
            retry_delay=delay,
            **extra,
        )
        await asyncio.sleep(delay)
