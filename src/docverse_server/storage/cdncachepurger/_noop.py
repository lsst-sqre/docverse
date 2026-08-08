"""No-op CDN cache purger for deployments without a purgeable zone."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import structlog

__all__ = ["NoopCdnCachePurger"]


class NoopCdnCachePurger:
    """CDN cache purger that logs the request and does nothing.

    Used for organizations whose Cloudflare service has no ``zone_id``
    — typically sandboxes served from a ``workers.dev`` subdomain, which
    has no zone to purge. Returning this rather than raising lets
    callers purge unconditionally without branching on configuration.
    """

    def __init__(
        self,
        *,
        logger: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self._logger = logger or structlog.get_logger(__name__)

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
        """Log the skipped purge without contacting any CDN."""
        self._logger.info(
            "Skipping CDN cache purge: no purgeable zone configured",
            hostname=hostname,
        )
