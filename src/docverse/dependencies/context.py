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
from safir.arq import ArqQueue
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency
from safir.dependencies.logger import logger_dependency
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session
from structlog.stdlib import BoundLogger

from ..factory import HandlerFactory
from ..services.credential_encryptor import CredentialEncryptor
from ..storage.user_info_store import StubUserInfoStore, UserInfoStore

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

    factory: HandlerFactory
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
        self._user_info_store: UserInfoStore = StubUserInfoStore()
        self._credential_encryptor: CredentialEncryptor | None = None
        self._superadmin_usernames: list[str] = []
        self._arq_queue_name: str = "arq:queue"

    async def __call__(
        self,
        request: Request,
        response: Response,
        session: Annotated[
            async_scoped_session[AsyncSession],
            Depends(db_session_dependency),
        ],
        logger: Annotated[BoundLogger, Depends(logger_dependency)],
        arq_queue: Annotated[ArqQueue, Depends(arq_dependency)],
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
            factory=HandlerFactory(
                session=session,
                logger=logger,
                arq_queue=arq_queue,
                user_info_store=self._user_info_store,
                credential_encryptor=self._credential_encryptor,
                superadmin_usernames=self._superadmin_usernames,
                default_queue_name=self._arq_queue_name,
            ),
        )

    async def initialize(
        self,
        user_info_store: UserInfoStore | None = None,
        credential_encryptor: CredentialEncryptor | None = None,
        superadmin_usernames: list[str] | None = None,
        arq_queue_name: str | None = None,
    ) -> None:
        """Initialize the process-wide shared context."""
        self._initialized = True
        if user_info_store is not None:
            self._user_info_store = user_info_store
        if credential_encryptor is not None:
            self._credential_encryptor = credential_encryptor
        if superadmin_usernames is not None:
            self._superadmin_usernames = superadmin_usernames
        if arq_queue_name is not None:
            self._arq_queue_name = arq_queue_name

    async def aclose(self) -> None:
        """Clean up the per-process configuration."""
        self._initialized = False


context_dependency = ContextDependency()
"""The dependency that will return the per-request context."""
