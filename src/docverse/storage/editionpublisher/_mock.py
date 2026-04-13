"""In-memory mock edition publisher for testing."""

from __future__ import annotations

from dataclasses import dataclass
from types import TracebackType
from typing import Self

import structlog

__all__ = ["MockEditionPublisher", "PublishCall"]


@dataclass(frozen=True)
class PublishCall:
    """A single recorded call to ``MockEditionPublisher.publish``."""

    project_slug: str
    edition_slug: str
    build_public_id: str
    object_key_prefix: str


class MockEditionPublisher:
    """In-memory implementation of the ``EditionPublisher`` protocol.

    Records every call to ``publish`` in order so tests can assert
    against the recorded arguments.
    """

    def __init__(
        self,
        *,
        logger: structlog.stdlib.BoundLogger | None = None,  # noqa: ARG002
    ) -> None:
        self._calls: list[PublishCall] = []

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
    def calls(self) -> list[PublishCall]:
        """Recorded publish calls in order."""
        return list(self._calls)

    async def publish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
        build_public_id: str,
        object_key_prefix: str,
    ) -> None:
        """Record a publish call."""
        self._calls.append(
            PublishCall(
                project_slug=project_slug,
                edition_slug=edition_slug,
                build_public_id=build_public_id,
                object_key_prefix=object_key_prefix,
            )
        )
