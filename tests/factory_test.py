"""Unit tests for the service factories."""

from __future__ import annotations

import pytest
import structlog
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.factory import Factory
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
