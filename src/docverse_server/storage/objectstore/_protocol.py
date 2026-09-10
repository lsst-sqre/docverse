"""Object store protocol for managing build artifacts."""

from __future__ import annotations

from types import TracebackType
from typing import Protocol, Self, runtime_checkable

__all__ = ["ObjectStore", "require_nonblank_prefix"]


def require_nonblank_prefix(prefix: str) -> str:
    """Return ``prefix`` unless it is blank, in which case raise.

    A blank prefix matches every key in the bucket, so
    :meth:`ObjectStore.delete_prefix` would empty the whole store — for
    every organization sharing it — rather than remove one build tree.
    No caller ever means that: the prefix comes from
    ``builds.storage_prefix``, and a row that lost its prefix is a bug
    upstream of the delete, not a licence to reclaim everything.

    The guard lives in the protocol module because it is part of the
    contract both implementations promise, and a safety check copied
    into each of them is one that eventually only holds in one.

    Parameters
    ----------
    prefix
        Key prefix to validate.

    Returns
    -------
    str
        The prefix, unchanged.

    Raises
    ------
    ValueError
        If the prefix is empty or contains only whitespace.
    """
    if not prefix.strip():
        msg = (
            "delete_prefix refuses an empty prefix: it would match every"
            " object in the bucket rather than one build tree"
        )
        raise ValueError(msg)
    return prefix


@runtime_checkable
class ObjectStore(Protocol):
    """Backend-agnostic interface for object storage operations.

    This protocol defines the interface for storing and retrieving
    documentation build artifacts. Concrete implementations will use
    S3-compatible object stores.

    Implementations must be usable as async context managers.
    """

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...

    async def generate_presigned_upload_url(
        self, *, key: str, content_type: str, expires_in: int = 3600
    ) -> str:
        """Generate a pre-signed URL for uploading an object.

        Parameters
        ----------
        key
            Object store key.
        content_type
            MIME type of the object.
        expires_in
            URL expiration in seconds.

        Returns
        -------
        str
            Pre-signed upload URL.
        """
        ...

    async def generate_presigned_download_url(
        self, *, key: str, expires_in: int = 3600
    ) -> str:
        """Generate a pre-signed URL for downloading an object.

        Parameters
        ----------
        key
            Object store key.
        expires_in
            URL expiration in seconds.

        Returns
        -------
        str
            Pre-signed download URL.
        """
        ...

    async def delete_object(self, *, key: str) -> None:
        """Delete an object from the store.

        Parameters
        ----------
        key
            Object store key.
        """
        ...

    async def delete_prefix(self, *, prefix: str) -> int:
        """Delete every object whose key starts with ``prefix``.

        The only sanctioned way to remove a build tree. Deleting a
        build's objects one key at a time through
        :meth:`delete_object` cannot report how many keys it was
        supposed to remove, so a caller that stamps a build as reclaimed
        has no way to tell a finished sweep from one that stopped
        halfway; this method either removes the whole subtree or raises.

        The count is exact for the caller's purposes: an implementation
        deletes precisely the keys its own listing found, and a store
        that reports a failure for any of them raises instead of
        returning a short count. Keys written *while* the delete runs
        are not covered — nothing writes into a soft-deleted build's
        prefix, which is why that is acceptable here and why the method
        is documented for build trees rather than live ones.

        Parameters
        ----------
        prefix
            Key prefix whose objects are deleted. Must not be blank; see
            `require_nonblank_prefix`.

        Returns
        -------
        int
            Number of objects deleted. Zero when the prefix holds
            nothing, which is not an error — a build whose tarball was
            already dropped at completion reaches the sweep that way.

        Raises
        ------
        ValueError
            If ``prefix`` is empty or contains only whitespace.
        docverse_server.storage.objectstore.ObjectStoreError
            If the store reports a per-key failure, so a partial delete
            is never returned as a success.
        """
        ...

    async def list_objects(self, *, prefix: str) -> list[str]:
        """List objects with the given prefix.

        Parameters
        ----------
        prefix
            Key prefix to filter by.

        Returns
        -------
        list of str
            List of matching object keys.
        """
        ...

    async def download_object(self, *, key: str) -> bytes:
        """Download an object from the store.

        Parameters
        ----------
        key
            Object store key.

        Returns
        -------
        bytes
            The object contents.
        """
        ...

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> None:
        """Upload an object directly.

        Parameters
        ----------
        key
            Object store key.
        data
            Object contents.
        content_type
            MIME type of the object.
        """
        ...
