"""In-memory mock object store for testing."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote

__all__ = ["MockObjectStore"]

_DEFAULT_BASE_URL = "https://mock-s3.example.com"


@dataclass
class _StoredObject:
    data: bytes
    content_type: str


class MockObjectStore:
    """In-memory implementation of the ObjectStore protocol.

    Stores objects in a dictionary keyed by object key. Presigned URLs
    are deterministic strings containing the key, suitable for
    assertions in tests.
    """

    def __init__(self, *, base_url: str = _DEFAULT_BASE_URL) -> None:
        self._base_url = base_url
        self._objects: dict[str, _StoredObject] = {}

    @property
    def objects(self) -> dict[str, _StoredObject]:
        """Access the internal object storage for test assertions."""
        return self._objects

    async def generate_presigned_upload_url(
        self, *, key: str, content_type: str, expires_in: int = 3600
    ) -> str:
        """Generate a deterministic upload URL for testing."""
        return (
            f"{self._base_url}/upload/{quote(key, safe='/')}"
            f"?content_type={content_type}"
            f"&expires_in={expires_in}"
        )

    async def generate_presigned_download_url(
        self, *, key: str, expires_in: int = 3600
    ) -> str:
        """Generate a deterministic download URL for testing."""
        return (
            f"{self._base_url}/download/{quote(key, safe='/')}"
            f"?expires_in={expires_in}"
        )

    async def download_object(self, *, key: str) -> bytes:
        """Download an object from the in-memory store."""
        obj = self._objects.get(key)
        if obj is None:
            msg = f"Object {key!r} not found"
            raise KeyError(msg)
        return obj.data

    async def delete_object(self, *, key: str) -> None:
        """Delete an object from the in-memory store."""
        self._objects.pop(key, None)

    async def list_objects(self, *, prefix: str) -> list[str]:
        """List objects matching a prefix."""
        return sorted(k for k in self._objects if k.startswith(prefix))

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> None:
        """Upload an object to the in-memory store."""
        self._objects[key] = _StoredObject(
            data=data, content_type=content_type
        )
