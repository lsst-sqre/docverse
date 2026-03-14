"""Object store protocol for managing build artifacts."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["ObjectStore"]


@runtime_checkable
class ObjectStore(Protocol):
    """Backend-agnostic interface for object storage operations.

    This protocol defines the interface for storing and retrieving
    documentation build artifacts. Concrete implementations will use
    S3-compatible object stores.
    """

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
