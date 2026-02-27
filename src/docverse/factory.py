"""Factory for creating Docverse service objects."""

from __future__ import annotations

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from .services.organization import OrganizationService


class Factory:
    """Build Docverse service objects."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    def set_logger(self, logger: structlog.stdlib.BoundLogger) -> None:
        """Set the logger for the factory."""
        self._logger = logger

    def create_organization_service(self) -> OrganizationService:
        """Create an OrganizationService."""
        return OrganizationService(
            session=self._session,
            logger=self._logger,
        )
