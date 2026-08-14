"""Unit tests for the service factories."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession

from docverse_server.factory import Factory
from docverse_server.services.keeper_sync.copier import (
    DEFAULT_COPY_CONCURRENCY,
)
from docverse_server.storage.ltd import LtdClient, LtdS3Source
from docverse_server.storage.objectstore import MockObjectStore
from docverse_server.storage.queue_backend import (
    ArqQueueBackend,
    NullQueueBackend,
)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


class _StubLtdSource:
    """Async-CM stand-in for ``LtdS3Source`` that never touches S3."""

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return None

    async def list_keys(self, *, prefix: str) -> list[str]:
        return []

    async def download_object(self, *, key: str) -> bytes:
        return b""


@pytest.mark.asyncio
async def test_factory_without_arq_queue_uses_null_backend(
    db_session: AsyncSession,
) -> None:
    """Factory defaults to NullQueueBackend when no arq queue is given."""
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
    )
    assert isinstance(factory.create_queue_backend(), NullQueueBackend)


@pytest.mark.asyncio
async def test_factory_with_arq_queue_uses_arq_backend(
    db_session: AsyncSession,
) -> None:
    """Factory uses ArqQueueBackend when an arq queue is provided."""
    arq_queue = MockArqQueue(default_queue_name="docverse:queue")
    factory = Factory(
        session=db_session,
        logger=_logger(),
        arq_queue=arq_queue,
        default_queue_name="docverse:queue",
    )
    assert isinstance(factory.create_queue_backend(), ArqQueueBackend)


@pytest.mark.asyncio
async def test_factory_creates_ltd_client_when_http_client_set(
    db_session: AsyncSession,
) -> None:
    """LtdClient construction needs the shared httpx.AsyncClient."""
    async with httpx.AsyncClient() as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            http_client=http_client,
            default_queue_name="docverse:queue",
        )
        client = factory.create_ltd_client()
        assert isinstance(client, LtdClient)


@pytest.mark.asyncio
async def test_factory_create_ltd_client_without_http_raises(
    db_session: AsyncSession,
) -> None:
    """No HTTP client -> the LTD-side accessor must error early."""
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
    )
    with pytest.raises(RuntimeError, match="HTTP client is required"):
        factory.create_ltd_client()


@pytest.mark.asyncio
async def test_factory_create_ltd_s3_source_returns_unopened(
    db_session: AsyncSession,
) -> None:
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
    )
    source = factory.create_ltd_s3_source()
    assert isinstance(source, LtdS3Source)


@pytest.mark.asyncio
async def test_copier_uses_factory_copy_concurrency(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The configured fan-out bound reaches ``BuildContentCopier``.

    The bound used to be a literal ``8`` in ``factory.py`` duplicating
    the copier's own default, so no operator knob could move the sync
    worker's real memory ceiling (#517).
    """
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
        keeper_sync_copy_concurrency=3,
    )

    async def _fake_objectstore(
        *, org_id: int, service_label: str
    ) -> MockObjectStore:
        return MockObjectStore()

    monkeypatch.setattr(
        factory, "create_objectstore_for_org", _fake_objectstore
    )
    monkeypatch.setattr(
        factory, "create_ltd_s3_source", lambda **kwargs: _StubLtdSource()
    )

    async with factory.create_build_content_copier_for_org(
        org_id=1, service_label="r2"
    ) as copier:
        assert copier.max_concurrent == 3


@pytest.mark.asyncio
async def test_copier_concurrency_defaults_to_copier_fallback(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Factory built without the knob keeps the copier's own default."""
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
    )

    async def _fake_objectstore(
        *, org_id: int, service_label: str
    ) -> MockObjectStore:
        return MockObjectStore()

    monkeypatch.setattr(
        factory, "create_objectstore_for_org", _fake_objectstore
    )
    monkeypatch.setattr(
        factory, "create_ltd_s3_source", lambda **kwargs: _StubLtdSource()
    )

    async with factory.create_build_content_copier_for_org(
        org_id=1, service_label="r2"
    ) as copier:
        assert copier.max_concurrent == DEFAULT_COPY_CONCURRENCY
