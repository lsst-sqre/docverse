"""Request context dependency for FastAPI.

This dependency gathers a variety of information into a single object for the
convenience of writing request handlers.  It also provides a place to store a
`structlog.BoundLogger` that can gather additional context during processing,
including from dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import httpx
from fastapi import Depends, Request, Response
from pydantic import SecretStr
from rubin.repertoire import DiscoveryClient
from safir.arq import ArqQueue
from safir.dependencies.arq import arq_dependency
from safir.dependencies.db_session import db_session_dependency
from safir.dependencies.logger import logger_dependency
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.stdlib import BoundLogger

from ..factory import HandlerFactory
from ..metrics.events import DocverseEvents
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

    session: AsyncSession
    """The database session."""

    factory: HandlerFactory
    """The component factory."""

    events: DocverseEvents
    """The Sasquatch metrics event publishers.

    Handlers publish application metrics through this after committing
    their transaction (e.g. ``context.events.build_uploaded.publish(...)``).
    """

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
        self._discovery: DiscoveryClient | None = None
        self._http_client: httpx.AsyncClient | None = None
        self._github_app_id: int | None = None
        self._github_app_private_key: SecretStr | None = None
        self._github_webhook_secret: SecretStr | None = None
        self._github_app_validated: bool = True
        self._github_app_html_url: str | None = None
        self._events: DocverseEvents | None = None

    @property
    def github_app_enabled(self) -> bool:
        """Return whether all three GitHub App secrets are set.

        The startup-time validator uses this to decide whether to
        attempt a ``GET /app`` round-trip — when the deployment has not
        configured the feature at all (any secret unset), the validator
        skips silently and ``_require_github_app_config`` raises on its
        own at the secrets-unset gate.
        """
        return (
            self._github_app_id is not None
            and self._github_app_private_key is not None
            and self._github_webhook_secret is not None
        )

    @property
    def github_app_id(self) -> int | None:
        """Return the configured GitHub App numeric ID, or ``None``."""
        return self._github_app_id

    async def __call__(
        self,
        request: Request,
        response: Response,
        session: Annotated[
            AsyncSession,
            Depends(db_session_dependency),
        ],
        logger: Annotated[BoundLogger, Depends(logger_dependency)],
        arq_queue: Annotated[ArqQueue, Depends(arq_dependency)],
    ) -> RequestContext:
        """Create a per-request context and return it."""
        if not self._initialized:
            msg = "ContextDependency not initialized"
            raise RuntimeError(msg)
        if self._events is None:
            msg = "ContextDependency events not initialized"
            raise RuntimeError(msg)
        return RequestContext(
            request=request,
            response=response,
            logger=logger,
            session=session,
            events=self._events,
            factory=HandlerFactory(
                session=session,
                logger=logger,
                arq_queue=arq_queue,
                user_info_store=self._user_info_store,
                credential_encryptor=self._credential_encryptor,
                superadmin_usernames=self._superadmin_usernames,
                discovery=self._discovery,
                http_client=self._http_client,
                github_app_id=self._github_app_id,
                github_app_private_key=self._github_app_private_key,
                github_webhook_secret=self._github_webhook_secret,
                github_app_validated=self._github_app_validated,
                github_app_html_url=self._github_app_html_url,
                default_queue_name=self._arq_queue_name,
            ),
        )

    async def initialize(  # noqa: C901
        self,
        user_info_store: UserInfoStore | None = None,
        credential_encryptor: CredentialEncryptor | None = None,
        superadmin_usernames: list[str] | None = None,
        arq_queue_name: str | None = None,
        discovery: DiscoveryClient | None = None,
        http_client: httpx.AsyncClient | None = None,
        github_app_id: int | None = None,
        github_app_private_key: SecretStr | None = None,
        github_webhook_secret: SecretStr | None = None,
        events: DocverseEvents | None = None,
    ) -> None:
        """Initialize the process-wide shared context."""
        self._initialized = True
        if events is not None:
            self._events = events
        if user_info_store is not None:
            self._user_info_store = user_info_store
        if credential_encryptor is not None:
            self._credential_encryptor = credential_encryptor
        if superadmin_usernames is not None:
            self._superadmin_usernames = superadmin_usernames
        if arq_queue_name is not None:
            self._arq_queue_name = arq_queue_name
        if discovery is not None:
            self._discovery = discovery
        if http_client is not None:
            self._http_client = http_client
        if github_app_id is not None:
            self._github_app_id = github_app_id
        if github_app_private_key is not None:
            self._github_app_private_key = github_app_private_key
        if github_webhook_secret is not None:
            self._github_webhook_secret = github_webhook_secret

    def set_github_secrets(
        self,
        *,
        app_id: int | None,
        private_key: SecretStr | None,
        webhook_secret: SecretStr | None,
    ) -> None:
        """Set the three GitHub-App secret slots in one call.

        Unlike :meth:`initialize`, every argument is honored as-is — passing
        ``None`` clears that slot. This is the supported way to toggle the
        GitHub-App feature on or off from a test fixture, in lieu of poking
        ``_github_app_id`` / ``_github_app_private_key`` /
        ``_github_webhook_secret`` directly on the singleton.

        Resets ``github_app_validated`` to ``True``: a freshly-configured
        secret bundle has not yet been rejected by the startup validator.
        """
        self._github_app_id = app_id
        self._github_app_private_key = private_key
        self._github_webhook_secret = webhook_secret
        self._github_app_validated = True

    def set_github_app_validated(self, *, value: bool) -> None:
        """Record the outcome of the startup-time GitHub App validation.

        Called by the API service lifespan and the arq worker startup
        hook with ``value=False`` when a parse / ``GET /app``
        round-trip fails, disabling the feature for the lifetime of
        this process without taking the service down.
        """
        self._github_app_validated = value

    def set_github_app_html_url(self, html_url: str | None) -> None:
        """Record the GitHub App's public install-page URL.

        Called by the API service lifespan with the ``html_url`` the
        startup ``GET /app`` validation returned, so the project
        serializer can surface it as ``github.app_url`` and the UI can
        link operators to the App's install page. ``None`` leaves the
        field absent (feature unconfigured or validation failed).
        """
        self._github_app_html_url = html_url

    async def aclose(self) -> None:
        """Clean up the per-process configuration."""
        self._initialized = False


context_dependency = ContextDependency()
"""The dependency that will return the per-request context."""
