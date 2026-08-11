"""Cloudflare CDN cache purger."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any, Self, override

import httpx
import structlog
from safir.slack.sentry import SentryEventInfo

from docverse_server.exceptions import DocverseSlackException

from .._http_retry import RETRYABLE_STATUS_CODES, backoff_for_response

__all__ = ["CloudflareCachePurgeError", "CloudflareCachePurger"]

#: Maximum purge attempts for a retryable (429/5xx) response, including
#: the original request (``_MAX_ATTEMPTS - 1`` retries).
_MAX_ATTEMPTS = 4

#: Initial backoff in seconds; doubles each subsequent attempt.
_BASE_BACKOFF_SECONDS = 0.5


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


class CloudflareCachePurger:
    """CDN cache purger backed by the Cloudflare zone purge API.

    Purges by hostname via ``POST /client/v4/zones/{zone_id}/purge_cache``,
    the one purge mechanism available on every Cloudflare plan tier
    (purge by prefix and by cache tag are Enterprise-only).

    Retries ``429`` and the transient 5xx family with bounded
    exponential backoff (see
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
        max_attempts: int = _MAX_ATTEMPTS,
        base_backoff_seconds: float = _BASE_BACKOFF_SECONDS,
    ) -> None:
        self._zone_id = zone_id
        self._api_token = api_token
        self._http_client = http_client
        self._logger = logger
        # Clamped so the retry loop always runs at least once: a
        # misconfigured budget must degrade to "purge once, no retries"
        # rather than to a silent no-op that reports success.
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
        limit reached") or a transient 5xx is retried with exponential
        backoff, honouring ``Retry-After`` when Cloudflare sends one.
        Any other error status fails on the first attempt — a retry
        cannot fix a bad token or an unknown zone.

        Parameters
        ----------
        hostname
            Hostname whose cached responses should be invalidated, without
            a scheme or path (e.g. ``myproject.example.org``).

        Raises
        ------
        CloudflareCachePurgeError
            If Cloudflare returns a non-retryable error status, or keeps
            returning a retryable one until the attempt budget is
            exhausted. The failure is logged at ``ERROR`` with full
            context first; the caller decides whether to swallow it.
        """
        url = (
            "https://api.cloudflare.com/client/v4"
            f"/zones/{self._zone_id}/purge_cache"
        )
        for attempt in range(1, self._max_attempts + 1):
            response = await self._http_client.post(
                url,
                json={"hosts": [hostname]},
                headers={"Authorization": f"Bearer {self._api_token}"},
            )
            if not response.is_error:
                self._logger.info(
                    "Purged Cloudflare cache for hostname",
                    zone_id=self._zone_id,
                    hostname=hostname,
                    attempts=attempt,
                )
                return

            error_codes, error_messages = _parse_cloudflare_errors(response)
            retryable = response.status_code in RETRYABLE_STATUS_CODES
            if retryable and attempt < self._max_attempts:
                delay = backoff_for_response(
                    response,
                    attempt,
                    base_backoff_seconds=self._base_backoff_seconds,
                    logger=self._logger,
                )
                self._logger.warning(
                    "Retrying Cloudflare cache purge",
                    status_code=response.status_code,
                    cloudflare_error_codes=error_codes,
                    zone_id=self._zone_id,
                    hostname=hostname,
                    attempt=attempt,
                    max_attempts=self._max_attempts,
                    retry_delay=delay,
                )
                await asyncio.sleep(delay)
                continue

            self._logger.error(
                "Cloudflare cache purge failed",
                status_code=response.status_code,
                cloudflare_error_codes=error_codes,
                cloudflare_error_messages=error_messages,
                response_body=response.text,
                zone_id=self._zone_id,
                hostname=hostname,
                attempts=attempt,
                retryable=retryable,
            )
            raise CloudflareCachePurgeError(
                hostname=hostname,
                zone_id=self._zone_id,
                status_code=response.status_code,
                error_codes=error_codes,
                error_messages=error_messages,
                attempts=attempt,
            )
