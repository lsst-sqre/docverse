"""In-memory mock CDN cache purger for testing."""

from __future__ import annotations

from types import TracebackType
from typing import Self

__all__ = ["MockCdnCachePurger"]


class MockCdnCachePurger:
    """In-memory implementation of the ``CdnCachePurger`` protocol.

    Records every hostname passed to ``purge_hostname`` in order so
    tests can assert both that a purge happened and what its scope was.
    """

    def __init__(self) -> None:
        self._purge_calls: list[str] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    @property
    def purge_calls(self) -> list[str]:
        """Hostnames passed to ``purge_hostname``, in call order."""
        return list(self._purge_calls)

    async def purge_hostname(self, hostname: str) -> None:
        """Record a hostname purge without contacting any CDN."""
        self._purge_calls.append(hostname)
