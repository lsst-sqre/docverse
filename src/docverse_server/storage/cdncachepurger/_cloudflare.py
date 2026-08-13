"""Cloudflare CDN cache purger."""

from __future__ import annotations

from types import TracebackType
from typing import Any, Self, override

import httpx
import structlog
from safir.slack.sentry import SentryEventInfo

from docverse_server.exceptions import DocverseSlackException

from .._http_retry import (
    DEFAULT_BASE_BACKOFF_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    RETRYABLE_TRANSPORT_ERRORS,
    retry_request,
)

__all__ = ["CloudflareCachePurgeError", "CloudflareCachePurger"]


class CloudflareCachePurgeError(DocverseSlackException):
    """Raised when a Cloudflare zone purge cannot be completed.

    Purging is best-effort at the publish call site, so this exception
    exists mainly to make the failure *discoverable*: without it a
    rate-limited purge is a log line nobody reads while the edition
    quietly serves a stale copy at the edge.

    The override earns its keep because every purge failure surfaces as
    the same exception type from the same one call site — the stack
    trace says nothing a triager doesn't already know. What they need is
    Cloudflare's own ``errors[].code``: ``1134`` ("Unable to purge, rate
    limit reached") is an "we are publishing too fast" problem, whereas
    an authentication code on a 403 is an expired-credential problem for
    a different maintainer. That code and the HTTP status are the
    low-cardinality tags; the hostname, zone, attempt count, and
    Cloudflare's error messages go into the ``cloudflare_purge``
    context.
    """

    def __init__(
        self,
        *,
        hostname: str,
        zone_id: str,
        status_code: int | None = None,
        error_codes: list[int] | None = None,
        error_messages: list[str] | None = None,
        attempts: int = 1,
        message: str | None = None,
    ) -> None:
        if message is None:
            message = self._format_message(
                hostname=hostname,
                status_code=status_code,
                attempts=attempts,
            )
        super().__init__(message)
        self.hostname = hostname
        self.zone_id = zone_id
        self.status_code = status_code
        self.error_codes = list(error_codes) if error_codes else []
        self.error_messages = list(error_messages) if error_messages else []
        self.attempts = attempts

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        if self.status_code is not None:
            info.tags["cdn_status_code"] = str(self.status_code)
        if self.error_codes:
            info.tags["cloudflare_error_code"] = ",".join(
                str(code) for code in self.error_codes
            )
        context: dict[str, Any] = {
            "hostname": self.hostname,
            "zone_id": self.zone_id,
            "status_code": self.status_code,
            "error_codes": self.error_codes,
            "error_messages": self.error_messages,
            "attempts": self.attempts,
        }
        info.contexts["cloudflare_purge"] = context
        return info

    @staticmethod
    def _format_message(
        *,
        hostname: str,
        status_code: int | None,
        attempts: int,
    ) -> str:
        status_part = (
            f"returned {status_code}" if status_code is not None else "failed"
        )
        plural = "attempt" if attempts == 1 else "attempts"
        return (
            f"Cloudflare cache purge for {hostname} {status_part} "
            f"after {attempts} {plural}"
        )


def _parse_cloudflare_errors(
    response: httpx.Response,
) -> tuple[list[int], list[str]]:
    """Extract ``errors[].code`` / ``errors[].message`` from a response.

    Cloudflare's v4 API reports failures as
    ``{"success": false, "errors": [{"code": 1134, "message": "..."}]}``.
    The numeric code is the only part precise enough to alert on, so it
    is lifted out of the response body and into structured log context
    and Sentry tags.

    Defensive by design: a Cloudflare edge error (or a proxy in front of
    it) can return HTML rather than JSON, and the legacy shape used
    plain strings in ``errors``. Anything that does not match the
    documented shape yields empty lists rather than raising — a purge
    failure must never be masked by a parse failure.

    Returns
    -------
    tuple
        The numeric error codes and the error messages, each in
        response order.
    """
    try:
        payload: Any = response.json()
    except ValueError:
        return ([], [])
    if not isinstance(payload, dict):
        return ([], [])
    errors = payload.get("errors")
    if not isinstance(errors, list):
        return ([], [])
    codes: list[int] = []
    messages: list[str] = []
    for entry in errors:
        if not isinstance(entry, dict):
            continue
        code = entry.get("code")
        if isinstance(code, int):
            codes.append(code)
        message = entry.get("message")
        if isinstance(message, str):
            messages.append(message)
    return (codes, messages)


def _retry_log_context(response: httpx.Response) -> dict[str, Any]:
    """Add Cloudflare's own error codes to a retry warning.

    A throttle that clears on the second attempt never reaches the
    terminal ``ERROR`` below, so this warning is the only trace of it.
    Without ``1134`` in that trace, "Docverse is publishing faster than
    Cloudflare's purge rate limit" is indistinguishable from any other
    transient 429.
    """
    codes, _ = _parse_cloudflare_errors(response)
    return {"cloudflare_error_codes": codes}


class CloudflareCachePurger:
    """CDN cache purger backed by the Cloudflare zone purge API.

    Purges by hostname via ``POST /client/v4/zones/{zone_id}/purge_cache``,
    the one purge mechanism available on every Cloudflare plan tier
    (purge by prefix and by cache tag are Enterprise-only).

    Retries ``429``, the transient 5xx family, and transient transport
    failures with bounded exponential backoff (see
    `docverse_server.storage._http_retry`), because publishing a single
    release can fan out into several purges and Cloudflare's purge
    endpoint rate-limits well below that burst.
    """

    def __init__(
        self,
        *,
        zone_id: str,
        api_token: str,
        http_client: httpx.AsyncClient,
        logger: structlog.stdlib.BoundLogger,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS,
    ) -> None:
        self._zone_id = zone_id
        self._api_token = api_token
        self._http_client = http_client
        self._logger = logger
        # Clamped so the retry loop always runs at least once: a
        # misconfigured budget must degrade to "purge once, no retries"
        # rather than to a silent no-op that reports success. The clamp
        # is repeated inside ``retry_request``; keeping it here too means
        # the stored value is the one actually spent, so the
        # exhausted-transport path can report it as ``attempts``.
        self._max_attempts = max(1, max_attempts)
        self._base_backoff_seconds = base_backoff_seconds

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    async def purge_hostname(self, hostname: str) -> None:
        """Purge every cached response for a hostname in the zone.

        A ``429`` (Cloudflare error ``1134``, "Unable to purge, rate
        limit reached"), a transient 5xx, or a transient transport
        failure is retried with exponential backoff, honouring
        ``Retry-After`` when Cloudflare sends one (up to
        `~docverse_server.storage._http_retry.MAX_BACKOFF_SECONDS`).
        Any other non-2xx status fails on the first attempt — a retry
        cannot fix a bad token or an unknown zone.

        Parameters
        ----------
        hostname
            Hostname whose cached responses should be invalidated, without
            a scheme or path (e.g. ``myproject.example.org``).

        Raises
        ------
        CloudflareCachePurgeError
            If Cloudflare returns a non-retryable non-2xx status, keeps
            returning a retryable one until the attempt budget is
            exhausted, or keeps failing at the transport level until the
            budget is exhausted. Transport failures are reported as this
            type too, rather than as a bare ``httpx`` error, so the
            caller's best-effort handling sees one exception type for
            "the edge may still be stale". The failure is logged at
            ``ERROR`` with full context first; the caller decides
            whether to swallow it.
        """
        url = (
            "https://api.cloudflare.com/client/v4"
            f"/zones/{self._zone_id}/purge_cache"
        )
        logger = self._logger.bind(zone_id=self._zone_id, hostname=hostname)

        async def send() -> httpx.Response:
            return await self._http_client.post(
                url,
                json={"hosts": [hostname]},
                headers={"Authorization": f"Bearer {self._api_token}"},
            )

        try:
            outcome = await retry_request(
                send,
                operation="Cloudflare cache purge",
                logger=logger,
                max_attempts=self._max_attempts,
                base_backoff_seconds=self._base_backoff_seconds,
                retry_log_context=_retry_log_context,
            )
        except RETRYABLE_TRANSPORT_ERRORS as exc:
            logger.exception(
                "Cloudflare cache purge failed",
                error=str(exc),
                error_type=type(exc).__name__,
                attempts=self._max_attempts,
                retryable=True,
            )
            raise CloudflareCachePurgeError(
                hostname=hostname,
                zone_id=self._zone_id,
                attempts=self._max_attempts,
                message=(
                    f"Cloudflare cache purge for {hostname} failed "
                    f"after {self._max_attempts} attempts: {exc}"
                ),
            ) from exc

        response = outcome.response
        # Only a 2xx means Cloudflare accepted the purge. A 3xx is not
        # followed on this client, so treating "not an error" as success
        # would log a purge that never ran while the edge keeps serving
        # the stale copy.
        if response.is_success:
            logger.info(
                "Purged Cloudflare cache for hostname",
                attempts=outcome.attempts,
            )
            return

        error_codes, error_messages = _parse_cloudflare_errors(response)
        logger.error(
            "Cloudflare cache purge failed",
            status_code=response.status_code,
            cloudflare_error_codes=error_codes,
            cloudflare_error_messages=error_messages,
            response_body=response.text,
            attempts=outcome.attempts,
            retryable=outcome.retryable,
        )
        raise CloudflareCachePurgeError(
            hostname=hostname,
            zone_id=self._zone_id,
            status_code=response.status_code,
            error_codes=error_codes,
            error_messages=error_messages,
            attempts=outcome.attempts,
        )
