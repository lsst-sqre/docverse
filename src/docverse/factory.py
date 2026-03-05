"""Factory for creating Docverse service objects."""

from __future__ import annotations

import structlog
from safir.arq import ArqQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from .services.organization import OrganizationService
from .services.queue import ArqQueueBackend


class Factory:
    """Build Docverse service objects."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
        arq_queue: ArqQueue,
    ) -> None:
        self._session = session
        self._logger = logger
        self._arq_queue = arq_queue

    def set_logger(self, logger: structlog.stdlib.BoundLogger) -> None:
        """Set the logger for the factory."""
        self._logger = logger

    def create_organization_service(self) -> OrganizationService:
        """Create an OrganizationService."""
        return OrganizationService(
            session=self._session,
            logger=self._logger,
        )

    def create_queue_backend(self) -> ArqQueueBackend:
        """Create an ArqQueueBackend."""
        return ArqQueueBackend(arq_queue=self._arq_queue)
