"""CDN cache purger protocol for invalidating edge caches."""

from __future__ import annotations

from types import TracebackType
from typing import Protocol, Self, runtime_checkable

__all__ = ["CdnCachePurger"]


@runtime_checkable
class CdnCachePurger(Protocol):
    """Backend-agnostic interface for purging a CDN's edge cache.

    A ``CdnCachePurger`` invalidates content cached at a CDN's edge so
    readers see a newly-published build without waiting out the edge
    TTL. It is deliberately separate from
    `~docverse_server.storage.editionpublisher.EditionPublisher` (which
    only writes edition pointers) so the CDN and pointer-store concerns
    can evolve independently.

    Only hostname-scoped purging is exposed because that is the one
    mechanism available on every Cloudflare plan tier. Narrower
    mechanisms (purge by URL, prefix, or cache tag) can be added to this
    protocol later without changing existing callers.

    Implementations must be usable as async context managers.
    """

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...

    async def purge_hostname(self, hostname: str) -> None:
        """Purge every cached response for a hostname.

        Parameters
        ----------
        hostname
            Hostname whose cached responses should be invalidated, without
            a scheme or path (e.g. ``myproject.example.org``).

        Raises
        ------
        Exception
            Implementations may raise on a provider-side failure. Callers
            that treat purging as best-effort are responsible for logging
            and swallowing the error.
        """
        ...
