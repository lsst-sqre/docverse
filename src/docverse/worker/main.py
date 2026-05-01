"""arq worker configuration for Docverse.

Launch with: ``arq docverse.worker.main.WorkerSettings``
"""

from __future__ import annotations

from importlib.metadata import version
from typing import Any

import httpx
import structlog
from pydantic import SecretStr
from rubin.repertoire import DiscoveryClient
from safir.arq import ArqQueue, RedisArqQueue
from safir.database import create_database_engine, is_database_current
from safir.dependencies.db_session import db_session_dependency
from safir.logging import configure_logging
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.config import Configuration
from docverse.database import get_current_revision
from docverse.factory import Factory
from docverse.services.credential_encryptor import CredentialEncryptor
from docverse.storage.github import validate_github_app

from .functions import (
    build_processing,
    dashboard_build,
    dashboard_sync,
    ping,
    publish_edition,
)

config = Configuration()


class WorkerFactoryBuilder:
    """Build per-job :class:`Factory` instances inside the arq worker.

    Captures the worker's process-lifetime dependencies once and exposes
    a ``__call__(session, logger)`` that mints a fresh
    :class:`docverse.factory.Factory` for the duration of one arq job.
    Mirrors the request-side pattern in
    :class:`safir.dependencies.context.ContextDependency`, where the
    process-lifetime deps are captured once and a per-request
    ``RequestContext`` is built around them.
    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        encryptor: CredentialEncryptor,
        http_client: httpx.AsyncClient,
        arq_queue: ArqQueue,
        discovery: DiscoveryClient,
        github_app_id: int | None,
        github_app_private_key: SecretStr | None,
        github_webhook_secret: SecretStr | None,
        default_queue_name: str,
    ) -> None:
        self._encryptor = encryptor
        self._http_client = http_client
        self._arq_queue = arq_queue
        self._discovery = discovery
        self._github_app_id = github_app_id
        self._github_app_private_key = github_app_private_key
        self._github_webhook_secret = github_webhook_secret
        self._github_app_validated = True
        self._default_queue_name = default_queue_name

    @property
    def github_app_enabled(self) -> bool:
        """Whether all three GitHub App secrets are set on this builder."""
        return (
            self._github_app_id is not None
            and self._github_app_private_key is not None
            and self._github_webhook_secret is not None
        )

    @property
    def github_app_id(self) -> int | None:
        """Configured GitHub App numeric ID, or ``None``."""
        return self._github_app_id

    def set_github_app_validated(self, *, value: bool) -> None:
        """Record the outcome of the worker's startup-time validation.

        Mirrors
        :meth:`docverse.dependencies.context.ContextDependency.set_github_app_validated`
        so a single shared validator helper can flip either state
        holder via the same call.
        """
        self._github_app_validated = value

    def __call__(
        self,
        *,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> Factory:
        """Build a :class:`Factory` for one arq job."""
        return Factory(
            session=session,
            logger=logger,
            credential_encryptor=self._encryptor,
            http_client=self._http_client,
            arq_queue=self._arq_queue,
            discovery=self._discovery,
            github_app_id=self._github_app_id,
            github_app_private_key=self._github_app_private_key,
            github_webhook_secret=self._github_webhook_secret,
            github_app_validated=self._github_app_validated,
            default_queue_name=self._default_queue_name,
        )


async def startup(ctx: dict[str, Any]) -> None:
    """Initialize resources for the arq worker process."""
    configure_logging(
        profile=config.log_profile,
        log_level=config.log_level,
        name="docverse.worker",
    )
    logger = structlog.get_logger("docverse.worker")

    engine = create_database_engine(
        config.database_url, config.database_password
    )
    if not await is_database_current(
        engine, logger, config.alembic_config_path
    ):
        msg = "Database schema is not current."
        raise RuntimeError(msg)
    db_revision = await get_current_revision(engine)
    await engine.dispose()
    logger.info(
        "Docverse worker startup",
        app_version=version("docverse"),
        db_revision=db_revision,
    )

    await db_session_dependency.initialize(
        config.database_url,
        config.database_password,
    )

    retired_key = (
        config.credential_encryption_key_retired.get_secret_value()
        if config.credential_encryption_key_retired
        else None
    )
    encryptor = CredentialEncryptor(
        current_key=config.credential_encryption_key.get_secret_value(),
        retired_key=retired_key,
    )

    http_client = httpx.AsyncClient()
    discovery = DiscoveryClient(
        http_client,
        base_url=str(config.repertoire_base_url),
        logger=logger,
    )

    if config.arq_redis_settings is None:
        msg = "arq_redis_settings must be configured for the worker"
        raise RuntimeError(msg)
    arq_queue = await RedisArqQueue.initialize(
        config.arq_redis_settings,
        default_queue_name=config.arq_queue_name,
    )

    # ``http_client`` and ``arq_queue`` stay in ctx because ``shutdown``
    # owns their teardown. The factory builder captures them by reference,
    # so worker functions never need to look them up directly.
    ctx["http_client"] = http_client
    ctx["arq_queue"] = arq_queue
    factory_builder = WorkerFactoryBuilder(
        encryptor=encryptor,
        http_client=http_client,
        arq_queue=arq_queue,
        discovery=discovery,
        github_app_id=config.github_app_id,
        github_app_private_key=config.github_app_private_key,
        github_webhook_secret=config.github_webhook_secret,
        default_queue_name=config.arq_queue_name,
    )
    await validate_github_app(
        state=factory_builder,
        app_id=config.github_app_id,
        private_key=config.github_app_private_key,
        app_name="lsst-sqre/docverse",
        http_client=http_client,
        logger=logger,
    )
    ctx["factory_builder"] = factory_builder

    logger.info("Worker startup complete")


async def shutdown(ctx: dict[str, Any]) -> None:
    """Clean up resources for the arq worker process."""
    arq_queue = ctx.get("arq_queue")
    if arq_queue is not None:
        # Private-attribute access until safir adds a public shutdown API;
        # see https://github.com/lsst-sqre/safir/issues/522
        await arq_queue._pool.aclose()  # noqa: SLF001
    await ctx["http_client"].aclose()
    await db_session_dependency.aclose()
    logger = structlog.get_logger("docverse.worker")
    logger.info("Worker shutdown complete")


class WorkerSettings:
    """arq WorkerSettings for Docverse."""

    functions = [
        build_processing,
        dashboard_build,
        dashboard_sync,
        ping,
        publish_edition,
    ]
    redis_settings = config.arq_redis_settings
    queue_name = config.arq_queue_name
    on_startup = startup
    on_shutdown = shutdown
