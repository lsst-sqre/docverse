"""Cloudflare CDN cache purger."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import httpx
import structlog

__all__ = ["CloudflareCachePurger"]


class CloudflareCachePurger:
    """CDN cache purger backed by the Cloudflare zone purge API.

    Purges by hostname via ``POST /client/v4/zones/{zone_id}/purge_cache``,
    the one purge mechanism available on every Cloudflare plan tier
    (purge by prefix and by cache tag are Enterprise-only).
    """

    def __init__(
        self,
        *,
        zone_id: str,
        api_token: str,
        http_client: httpx.AsyncClient,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._zone_id = zone_id
        self._api_token = api_token
        self._http_client = http_client
        self._logger = logger

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

        Parameters
        ----------
        hostname
            Hostname whose cached responses should be invalidated, without
            a scheme or path (e.g. ``myproject.example.org``).

        Raises
        ------
        httpx.HTTPStatusError
            If Cloudflare returns a 4xx or 5xx response. The failure is
            logged at ``ERROR`` with full context first; the caller
            decides whether to swallow it.
        """
        url = (
            "https://api.cloudflare.com/client/v4"
            f"/zones/{self._zone_id}/purge_cache"
        )
        response = await self._http_client.post(
            url,
            json={"hosts": [hostname]},
            headers={"Authorization": f"Bearer {self._api_token}"},
        )
        if response.is_error:
            self._logger.error(
                "Cloudflare cache purge failed",
                status_code=response.status_code,
                response_body=response.text,
                zone_id=self._zone_id,
                hostname=hostname,
            )
        response.raise_for_status()
        self._logger.info(
            "Purged Cloudflare cache for hostname",
            zone_id=self._zone_id,
            hostname=hostname,
        )
