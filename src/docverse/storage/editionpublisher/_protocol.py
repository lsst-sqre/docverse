"""Edition publisher protocol for updating edition pointers."""

from __future__ import annotations

from types import TracebackType
from typing import Protocol, Self, runtime_checkable

__all__ = ["EditionPublisher"]


@runtime_checkable
class EditionPublisher(Protocol):
    """Backend-agnostic interface for publishing edition pointers.

    An ``EditionPublisher`` updates the external routing entry that maps
    an edition (``project_slug``/``edition_slug``) to the object-store
    prefix of a specific build. Concrete implementations target different
    providers, such as Cloudflare Workers KV.

    Implementations must be usable as async context managers.
    """

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...

    async def publish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
        build_public_id: str,
        object_key_prefix: str,
    ) -> None:
        """Publish an edition pointer to the backing store.

        Parameters
        ----------
        project_slug
            Slug of the project the edition belongs to.
        edition_slug
            Slug of the edition being published.
        build_public_id
            Public identifier of the build the edition now points to.
        object_key_prefix
            Object-store key prefix for the build's rendered artifacts
            (e.g. an R2 or S3 prefix).
        """
        ...
