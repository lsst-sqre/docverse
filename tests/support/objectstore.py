"""Object-store doubles shared by the keeper-sync copy tests.

:class:`ScriptedUploadStore` stands in for a destination whose uploads
need retries or run out of them — the two things the keeper-sync
``BuildContentCopiedEvent`` counts — without a real ``S3ObjectStore``
and an ``httpx.MockTransport`` behind it.
"""

from __future__ import annotations

from docverse_server.storage.objectstore import MockObjectStore

__all__ = ["ScriptedUploadStore"]


class ScriptedUploadStore(MockObjectStore):
    """In-memory destination whose uploads follow a per-file script.

    Parameters
    ----------
    script
        Maps a file name (a key's last path segment, so the script holds
        whatever build prefix the copy writes under) to what its
        successive uploads do: an ``int`` stores the object and reports
        that many attempts, as ``S3ObjectStore.upload_object`` does after
        retrying; an exception is raised instead of storing anything, as
        ``S3ObjectStore`` does once a presigned PUT outlasts its budget.
        Uploads past the end of a file's script land first time.
    """

    def __init__(self, script: dict[str, list[int | BaseException]]) -> None:
        super().__init__()
        self._script = script

    async def upload_object(
        self, *, key: str, data: bytes, content_type: str
    ) -> int:
        """Run the next scripted step for ``key``'s file name."""
        steps = self._script.get(key.rsplit("/", 1)[-1])
        step = steps.pop(0) if steps else 1
        if isinstance(step, BaseException):
            raise step
        await super().upload_object(
            key=key, data=data, content_type=content_type
        )
        return step
