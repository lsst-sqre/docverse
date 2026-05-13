"""arq worker configuration for Docverse.

Launch with: ``arq docverse.worker.main.WorkerSettings``
"""

from __future__ import annotations

from datetime import timedelta
from importlib.metadata import version
from typing import Any

import httpx
import structlog
from arq import cron, func
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
from docverse.services.keeper_sync.scheduler import (
    TIER_DISCOVERY_CRON_INTERVAL,
    TIER_MAIN_CRON_INTERVAL,
    TIER_OTHER_CRON_INTERVAL,
)
from docverse.services.keeper_sync_run import KEEPER_SYNC_QUEUE_NAME
from docverse.storage.github import validate_github_app

from .functions import (
    build_processing,
    dashboard_build,
    dashboard_sync,
    keeper_sync_project,
    keeper_sync_reaper,
    keeper_sync_run_discovery,
    keeper_sync_tier_discovery,
    keeper_sync_tier_main,
    keeper_sync_tier_other,
    lifecycle_eval,
    lifecycle_eval_dispatcher,
    ping,
    publish_edition,
)
from .functions.lifecycle_eval_dispatcher import LIFECYCLE_EVAL_QUEUE_NAME

config = Configuration()


_SECONDS_PER_HOUR = 3600


def _cron_minutes_for_tier_interval(interval: timedelta) -> set[int]:
    """Wall-clock minute set for ``cron(...)`` from a tier-cron interval.

    Tier cron intervals are anchored on UTC midnight, divide one hour
    cleanly, and are at most one hour, so the arq ``minute={...}``
    argument is exactly the set of within-the-hour boundaries. Keeps
    the cron declarations and the cadence constants in
    :mod:`docverse.services.keeper_sync.scheduler` in lockstep — change
    the constant and the cron schedule follows.
    """
    seconds = int(interval.total_seconds())
    if (
        seconds <= 0
        or seconds % 60 != 0
        or seconds > _SECONDS_PER_HOUR
        or _SECONDS_PER_HOUR % seconds != 0
    ):
        msg = (
            f"Tier cron interval must be a positive divisor of one hour"
            f" in whole minutes, got {interval!r}"
        )
        raise ValueError(msg)
    minutes = seconds // 60
    return set(range(0, 60, minutes))


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
    """arq WorkerSettings for the default Docverse queue."""

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


class KeeperSyncWorkerSettings:
    """arq WorkerSettings for the dedicated LTD-sync queue.

    Bound to ``docverse:sync-queue`` (see :data:`KEEPER_SYNC_QUEUE_NAME`)
    so a noisy backfill cannot starve ``build_processing`` and
    ``publish_edition`` jobs on the default queue. Both classes share
    the same ``startup`` / ``shutdown`` hooks and therefore the same
    ``WorkerFactoryBuilder``, so all worker code paths see one
    consistent dependency graph regardless of which queue the job
    came in on.

    The keeper-sync functions are wrapped with :func:`arq.func` so the
    queue carries a per-job ``timeout`` (sourced from
    ``Config.keeper_sync_job_timeout_seconds``) and ``max_tries=1``
    instead of arq's 5-attempt default. A failure must surface
    promptly so the existing per-function ``except Exception`` block
    can route to ``queue_job_store.fail()`` and the parent
    ``keeper_sync_runs`` row finalises via ``_maybe_finalise_run``.
    The cron-driven :func:`keeper_sync_reaper` is the second backstop
    — it covers the case where arq itself loses a job (e.g. an
    OOM-killed worker pod) and no timeout ever fires.
    """

    functions = [
        func(
            keeper_sync_run_discovery,
            timeout=config.keeper_sync_job_timeout_seconds,
            max_tries=1,
        ),
        func(
            keeper_sync_project,
            timeout=config.keeper_sync_job_timeout_seconds,
            max_tries=1,
        ),
        keeper_sync_reaper,
        keeper_sync_tier_main,
        keeper_sync_tier_discovery,
        keeper_sync_tier_other,
    ]
    # Tier-cron cadences come from the constants in
    # ``services/keeper_sync/scheduler.py`` so the planner's next-tick
    # math (``explain_tier_status``) and the cron's actual firing
    # schedule cannot drift.
    cron_jobs = [
        cron(
            keeper_sync_reaper,
            minute={0, 30},
        ),
        # Tier 1 — keeps the user-visible ``main`` edition fresh per
        # user story 10's SLO.
        cron(
            keeper_sync_tier_main,
            minute=_cron_minutes_for_tier_interval(TIER_MAIN_CRON_INTERVAL),
        ),
        # Tier 2 — discovers LTD resources without a
        # ``keeper_sync_state`` row.
        cron(
            keeper_sync_tier_discovery,
            minute=_cron_minutes_for_tier_interval(
                TIER_DISCOVERY_CRON_INTERVAL
            ),
        ),
        # Tier 3 — catches non-``main`` editions whose state has aged
        # past the threshold.
        cron(
            keeper_sync_tier_other,
            minute=_cron_minutes_for_tier_interval(TIER_OTHER_CRON_INTERVAL),
        ),
    ]
    redis_settings = config.arq_redis_settings
    queue_name = KEEPER_SYNC_QUEUE_NAME
    on_startup = startup
    on_shutdown = shutdown


class LifecycleEvalWorkerSettings:
    """arq WorkerSettings for the dedicated ``lifecycle_eval`` queue.

    Bound to ``docverse:lifecycle-queue`` (see
    :data:`LIFECYCLE_EVAL_QUEUE_NAME`) so a slow lifecycle pass cannot
    starve the default queue's ``build_processing`` and
    ``publish_edition`` jobs or the keeper-sync queue's
    ``keeper_sync_project`` jobs. The PRD §"Orchestration" specifies a
    third pool alongside the default and keeper-sync pools; this class
    is the binding.

    Both the dispatcher and the per-org worker are wrapped with
    :func:`arq.func` so the dedicated queue inherits a single-attempt
    policy. A failure must surface promptly so the per-org worker's
    ``except Exception`` block can route to ``queue_job_store.fail()``
    and the parent ``lifecycle_eval_runs`` row finalises via
    :func:`maybe_finalise_lifecycle_run` — arq's default 5-attempt
    retry would otherwise delay finalisation and obscure the
    underlying error in logs.

    The hourly dispatcher cron is registered here; the
    ``lifecycle_reaper`` cron (sibling task) will be appended to this
    class's ``cron_jobs`` list when it lands.
    """

    functions = [
        func(
            lifecycle_eval_dispatcher,
            max_tries=1,
        ),
        func(
            lifecycle_eval,
            max_tries=1,
        ),
    ]
    cron_jobs = [
        cron(
            lifecycle_eval_dispatcher,
            minute={0},
        ),
    ]
    redis_settings = config.arq_redis_settings
    queue_name = LIFECYCLE_EVAL_QUEUE_NAME
    on_startup = startup
    on_shutdown = shutdown
