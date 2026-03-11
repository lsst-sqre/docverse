"""Request context dependency for FastAPI.

This dependency gathers a variety of information into a single object for the
convenience of writing request handlers.  It also provides a place to store a
`structlog.BoundLogger` that can gather additional context during processing,
including from dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

from fastapi import Depends, Request, Response
from safir.dependencies.db_session import db_session_dependency
from safir.dependencies.logger import logger_dependency
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session
from structlog.stdlib import BoundLogger

from ..factory import Factory

__all__ = [
    "ContextDependency",
    "RequestContext",
    "context_dependency",
]


@dataclass(slots=True)
class RequestContext:
    """Holds the incoming request and its surrounding context.

    The primary reason for the existence of this class is to allow the
    functions involved in request processing to repeatedly rebind the request
    logger to include more information, without having to pass both the
    request and the logger separately to every function.
    """

    request: Request
    """The incoming request."""

    response: Response
    """The response to the request."""

    logger: BoundLogger
    """The request logger, rebound with discovered context."""

    session: async_scoped_session[AsyncSession]
    """The database session."""

    factory: Factory
    """The component factory."""

    def rebind_logger(self, **values: Any) -> None:
        """Add the given values to the logging context.

        Parameters
        ----------
        **values
            Additional values that should be added to the logging context.
        """
        self.logger = self.logger.bind(**values)
        self.factory.set_logger(self.logger)


class ContextDependency:
    """Provide a per-request context as a FastAPI dependency.

    Each request gets a `RequestContext`.  To save overhead, the portions of
    the context that are shared by all requests are collected into a single
    process-global state and reused with each request.
    """

    def __init__(self) -> None:
        self._initialized = False

    async def __call__(
        self,
        request: Request,
        response: Response,
        session: Annotated[
            async_scoped_session[AsyncSession],
            Depends(db_session_dependency),
        ],
        logger: Annotated[BoundLogger, Depends(logger_dependency)],
    ) -> RequestContext:
        """Create a per-request context and return it."""
        if not self._initialized:
            msg = "ContextDependency not initialized"
            raise RuntimeError(msg)
        return RequestContext(
            request=request,
            response=response,
            logger=logger,
            session=session,
            factory=Factory(
                session=session,
                logger=logger,
            ),
        )

    async def initialize(self) -> None:
        """Initialize the process-wide shared context."""
        self._initialized = True

    async def aclose(self) -> None:
        """Clean up the per-process configuration."""
        self._initialized = False


context_dependency = ContextDependency()
"""The dependency that will return the per-request context."""
