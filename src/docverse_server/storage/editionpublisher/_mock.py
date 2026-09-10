"""In-memory mock edition publisher for testing."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Self

import structlog

from docverse_server.domain.cache_profile import CacheProfile
from docverse_server.domain.edition_pointer import (
    EditionPointer,
    edition_pointer_key,
)

__all__ = ["MockEditionPublisher", "PublishCall", "UnpublishCall"]


@dataclass(frozen=True)
class PublishCall:
    """A single recorded call to ``MockEditionPublisher.publish``."""

    project_slug: str
    edition_slug: str
    build_public_id: str
    object_key_prefix: str
    cache_profile: CacheProfile


@dataclass(frozen=True)
class UnpublishCall:
    """A single recorded call to ``MockEditionPublisher.unpublish``."""

    project_slug: str
    edition_slug: str


class MockEditionPublisher:
    """In-memory implementation of the ``EditionPublisher`` protocol.

    Records every call to ``publish`` and ``unpublish`` in order so
    tests can assert against the recorded arguments, and keeps the
    pointer state those calls imply so ``get_pointers`` reads back what
    the edge would serve.

    Modelling the state, and not only the calls, is what lets a test of
    the reconciliation loop set up drift: seed a pointer the database
    disagrees with (or delete one it expects), run a tick, and assert on
    the pointers afterwards. Assertions on ``calls`` alone can show that
    the loop asked for something, never that the edge and the database
    ended up agreeing.
    """

    def __init__(
        self,
        *,
        logger: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self._calls: list[PublishCall] = []
        self._unpublish_calls: list[UnpublishCall] = []
        self._pointers: dict[str, EditionPointer] = {}

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

    @property
    def unpublish_calls(self) -> list[UnpublishCall]:
        """Recorded unpublish calls in order."""
        return list(self._unpublish_calls)

    @property
    def pointers(self) -> dict[str, EditionPointer]:
        """Every pointer currently at the "edge", keyed by CDN key."""
        return dict(self._pointers)

    def seed_pointer(
        self,
        *,
        project_slug: str,
        edition_slug: str,
        build_public_id: str,
        object_key_prefix: str,
        cache_profile: CacheProfile | None = None,
    ) -> None:
        """Install a pointer without recording a publish call.

        The setup half of a drift test. A pointer the loop is meant to
        find wrong must not look like something the loop itself wrote,
        so seeding stays out of ``calls`` — otherwise an assertion that
        a tick published exactly once could not tell the fixture apart
        from the behaviour under test.
        """
        key = edition_pointer_key(project_slug, edition_slug)
        self._pointers[key] = EditionPointer(
            build_public_id=build_public_id,
            r2_prefix=object_key_prefix,
            cache_profile=cache_profile,
        )

    def remove_pointer(self, *, project_slug: str, edition_slug: str) -> None:
        """Delete a pointer without recording an unpublish call.

        The other half of drift setup: the edge losing a key the
        database still believes in — a KV write that never landed, or a
        pointer removed by hand. Idempotent, like ``unpublish``.
        """
        self._pointers.pop(
            edition_pointer_key(project_slug, edition_slug), None
        )

    async def publish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
        build_public_id: str,
        object_key_prefix: str,
        cache_profile: CacheProfile,
    ) -> None:
        """Record a publish call and install the pointer it writes."""
        self._calls.append(
            PublishCall(
                project_slug=project_slug,
                edition_slug=edition_slug,
                build_public_id=build_public_id,
                object_key_prefix=object_key_prefix,
                cache_profile=cache_profile,
            )
        )
        self._pointers[edition_pointer_key(project_slug, edition_slug)] = (
            EditionPointer(
                build_public_id=build_public_id,
                r2_prefix=object_key_prefix,
                cache_profile=cache_profile,
            )
        )

    async def unpublish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
    ) -> None:
        """Record an unpublish call and drop the pointer it removes."""
        self._unpublish_calls.append(
            UnpublishCall(
                project_slug=project_slug,
                edition_slug=edition_slug,
            )
        )
        self._pointers.pop(
            edition_pointer_key(project_slug, edition_slug), None
        )

    async def get_pointers(
        self, keys: Sequence[str]
    ) -> Mapping[str, EditionPointer | None]:
        """Read back the pointers for ``keys``, ``None`` where absent."""
        return {key: self._pointers.get(key) for key in keys}
