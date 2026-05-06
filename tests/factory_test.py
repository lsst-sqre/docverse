"""Unit tests for the service factories."""

from __future__ import annotations

import httpx
import pytest
import structlog
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.factory import Factory
from docverse.storage.ltd import LtdClient, LtdS3Source
from docverse.storage.queue_backend import ArqQueueBackend, NullQueueBackend


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


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
