"""Edition publisher protocol for reading and writing edition pointers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Protocol, Self, runtime_checkable

from docverse_server.domain.cache_profile import CacheProfile
from docverse_server.domain.edition_pointer import EditionPointer

__all__ = ["EditionPublisher"]


@runtime_checkable
class EditionPublisher(Protocol):
    """Backend-agnostic interface for publishing edition pointers.

    An ``EditionPublisher`` owns the external routing entry that maps an
    edition (``project_slug``/``edition_slug``) to the object-store
    prefix of a specific build: ``publish`` writes it, ``unpublish``
    removes it, and ``get_pointers`` reads back what the store actually
    serves. Concrete implementations target different providers, such as
    Cloudflare Workers KV.

    The read path exists because the write path alone cannot be
    verified. A publish that never landed, a pointer left behind by a
    deletion, and a pointer still naming a superseded build are all
    invisible from the database side, so
    `docverse_server.services.edition_reconcile` compares the two and
    re-drives whatever disagrees.

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
        cache_profile: CacheProfile,
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
        cache_profile
            Edge cache profile for the edition (``"long"`` or
            ``"short"``), derived by
            `docverse_server.domain.cache_profile.compute_cache_profile`.
            Implementations record it alongside the pointer so the CDN
            layer can choose a ``Cache-Control`` policy per edition.
        """
        ...

    async def unpublish(
        self,
        *,
        project_slug: str,
        edition_slug: str,
    ) -> None:
        """Remove an edition pointer from the backing store.

        Implementations must be idempotent: a call against an edition
        that has no pointer (or one that was already removed) must
        succeed without raising.

        Parameters
        ----------
        project_slug
            Slug of the project the edition belongs to.
        edition_slug
            Slug of the edition whose pointer should be removed.
        """
        ...

    async def get_pointers(
        self, keys: Sequence[str]
    ) -> Mapping[str, EditionPointer | None]:
        """Read back the pointers the backing store currently serves.

        The read half of the abstraction, and the reconciliation loop's
        only window onto what the edge actually serves: the database
        records what *should* be published, and comparing the two is how
        drift — a pointer that was never written, never moved, or never
        deleted — is found at all.

        Implementations must answer every requested key. A key with no
        pointer maps to ``None`` rather than being omitted, so a caller
        can iterate the editions it asked about without deciding what an
        absent entry means. Implementations must also fail loudly rather
        than return a partial view: a read that silently dropped keys
        would look exactly like an edge that had lost them.

        Parameters
        ----------
        keys
            Pointer keys to read, as built by
            `~docverse_server.domain.edition_pointer.edition_pointer_key`.

        Returns
        -------
        Mapping
            One entry per requested key, mapping to the pointer the
            store serves for it or to ``None``.
        """
        ...
