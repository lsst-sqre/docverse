"""Object-store doubles and fixtures shared by the keeper-sync copy tests.

:class:`ScriptedUploadStore` stands in for a destination whose uploads
need retries or run out of them — the two things the keeper-sync
``BuildContentCopiedEvent`` counts — without a real ``S3ObjectStore``
and an ``httpx.MockTransport`` behind it. :func:`seed_minio_service`
goes the other way: it seeds an org whose object-store service resolves
to a real ``S3ObjectStore``, for tests that need the store the factory
actually builds. :func:`record_s3_store_lifecycle` counts how often
those real stores open and close their aiobotocore client.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import CredentialProvider, OrganizationCreate
from docverse_server.factory import Factory
from docverse_server.storage.objectstore import MockObjectStore, S3ObjectStore

__all__ = [
    "S3StoreLifecycle",
    "ScriptedUploadStore",
    "record_s3_store_lifecycle",
    "seed_minio_service",
]


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


async def seed_minio_service(session: AsyncSession, factory: Factory) -> int:
    """Seed an org whose ``minio`` object-store service resolves for real.

    Returns the org id. ``create_objectstore_for_org`` then walks the
    genuine service-row and credential-decryption path, so a test sees
    the store the factory actually builds rather than a stand-in.
    """
    async with session.begin():
        org = await factory.create_org_store().create(
            OrganizationCreate(
                slug="budget-org",
                title="Budget Org",
                base_domain="budget.example.com",
            )
        )
        await factory.create_credential_service().create(
            org_slug="budget-org",
            label="minio-cred",
            provider=CredentialProvider.s3,
            credentials={
                "access_key_id": "key-id",
                "secret_access_key": "secret",
            },
        )
        await factory.create_service_store().create(
            organization_id=org.id,
            label="minio",
            category="object_storage",
            provider="minio",
            config={
                "endpoint_url": "https://minio.example.com",
                "bucket": "docs",
            },
            credential_label="minio-cred",
        )
    return org.id


@dataclass
class S3StoreLifecycle:
    """Every ``S3ObjectStore`` client open and close, in call order."""

    opened: list[S3ObjectStore] = field(default_factory=list)
    """Stores whose ``open`` ran, once per call."""

    closed: list[S3ObjectStore] = field(default_factory=list)
    """Stores whose ``close`` ran, once per call."""


def record_s3_store_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> S3StoreLifecycle:
    """Record every ``S3ObjectStore.open`` and ``close`` until the test ends.

    Each ``open`` creates an aiobotocore client, with its own aiohttp
    connector and SSL context, so the count of opens is the count of
    destination clients a code path built. The real methods still run.
    """
    lifecycle = S3StoreLifecycle()
    original_open = S3ObjectStore.open
    original_close = S3ObjectStore.close

    async def _recording_open(self: S3ObjectStore) -> None:
        lifecycle.opened.append(self)
        await original_open(self)

    async def _recording_close(self: S3ObjectStore) -> None:
        lifecycle.closed.append(self)
        await original_close(self)

    monkeypatch.setattr(S3ObjectStore, "open", _recording_open)
    monkeypatch.setattr(S3ObjectStore, "close", _recording_close)
    return lifecycle
