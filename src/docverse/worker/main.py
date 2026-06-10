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
from safir.metrics.arq import (
    ARQ_EVENTS_CONTEXT_KEY,
    initialize_arq_metrics,
    make_on_job_start,
    publish_queue_stats,
)
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.config import Configuration
from docverse.database import get_current_revision
from docverse.factory import Factory
from docverse.metrics import build_event_manager
from docverse.sentry import (
    DocverseSentryComponent,
    initialize_sentry,
    instrument_arq_task,
)
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
    build_processing_reaper,
    dashboard_build,
    dashboard_build_reaper,
    dashboard_sync,
    dashboard_sync_reaper,
    git_ref_audit,
    git_ref_audit_discovery,
    inventory_census,
    keeper_sync_project,
    keeper_sync_reaper,
    keeper_sync_run_discovery,
    keeper_sync_tier_discovery,
    keeper_sync_tier_main,
    keeper_sync_tier_other,
    lifecycle_eval,
    lifecycle_eval_dispatcher,
    lifecycle_reaper,
    ping,
    project_github_resolve,
    publish_edition,
    publish_edition_reaper,
)
from .queues import MAINTENANCE_QUEUE_NAME

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


_QUEUE_STATS_CRON_MINUTES = set(range(0, 60, 5))
"""Five-minute cadence for the ``arq_queue_stats`` gauge.

Queue depth is a Sasquatch product-analytics gauge (SQR-112),
deliberately distinct from operational telemetry, so a five-minute
resolution is ample; the coarser cadence also limits the per-tick Redis
pool that Safir's :func:`safir.metrics.arq.publish_queue_stats` opens.
"""


async def publish_queue_stats_cron(
    ctx: dict[Any, Any], *args: Any, **kwargs: Any
) -> None:
    """Publish this pool's queue-depth ``arq_queue_stats`` gauge (SQR-112).

    Registered as a per-pool cron on every ``WorkerSettings``. The pool's
    queue name, Redis settings, and the generic arq publishers are all
    read from the job ``ctx`` that :func:`_startup` populated, so one
    coroutine serves all three pools and each publishes stats for the
    queue its own worker process serves.
    """
    await publish_queue_stats(
        ctx["queue_name"],
        ctx["redis_settings"],
        ctx[ARQ_EVENTS_CONTEXT_KEY],
    )


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


async def _startup(
    ctx: dict[str, Any],
    *,
    component: DocverseSentryComponent,
    queue_name: str,
) -> None:
    """Initialize resources for the arq worker process.

    ``component`` distinguishes the three queues on Sentry: all worker
    settings share this body, so the per-startup wrappers below pick the
    right tag.
    """
    initialize_sentry(component=component)
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
        queue_name=queue_name,
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

    # The metrics event manager is process-lifetime like the factory
    # builder; ``shutdown`` owns ``event_manager`` teardown. Worker
    # functions publish through ``ctx["events"]``.
    event_manager, events = await build_event_manager(config, logger=logger)
    ctx["event_manager"] = event_manager
    ctx["events"] = events

    # Generic Safir arq-queue metrics (SQR-112): ``arq_job_run`` for every
    # job (via ``make_on_job_start`` on each WorkerSettings) and
    # ``arq_queue_stats`` per pool (via the ``publish_queue_stats_cron``
    # cron). ``initialize_arq_metrics`` populates
    # ``ctx[ARQ_EVENTS_CONTEXT_KEY]``; the stats cron also reads this
    # pool's ``queue_name`` and the ``redis_settings`` (validated non-None
    # above) back from ``ctx``.
    await initialize_arq_metrics(event_manager, ctx)
    ctx["queue_name"] = queue_name
    ctx["redis_settings"] = config.arq_redis_settings

    logger.info("Worker startup complete")


async def startup_default(ctx: dict[str, Any]) -> None:
    """on_startup for the default Docverse arq queue."""
    await _startup(ctx, component="worker", queue_name=config.arq_queue_name)


async def startup_keeper_sync(ctx: dict[str, Any]) -> None:
    """on_startup for the dedicated keeper-sync arq queue."""
    await _startup(
        ctx,
        component="worker-keeper-sync",
        queue_name=KEEPER_SYNC_QUEUE_NAME,
    )


async def startup_maintenance(ctx: dict[str, Any]) -> None:
    """on_startup for the dedicated maintenance arq queue."""
    await _startup(
        ctx,
        component="worker-maintenance",
        queue_name=MAINTENANCE_QUEUE_NAME,
    )


async def shutdown(ctx: dict[str, Any]) -> None:
    """Clean up resources for the arq worker process."""
    arq_queue = ctx.get("arq_queue")
    if arq_queue is not None:
        # Private-attribute access until safir adds a public shutdown API;
        # see https://github.com/lsst-sqre/safir/issues/522
        await arq_queue._pool.aclose()  # noqa: SLF001
    event_manager = ctx.get("event_manager")
    if event_manager is not None:
        await event_manager.aclose()
    await ctx["http_client"].aclose()
    await db_session_dependency.aclose()
    logger = structlog.get_logger("docverse.worker")
    logger.info("Worker shutdown complete")


class WorkerSettings:
    """arq WorkerSettings for the default Docverse queue."""

    functions = [
        instrument_arq_task(build_processing),
        instrument_arq_task(dashboard_build),
        instrument_arq_task(dashboard_sync),
        instrument_arq_task(ping),
        instrument_arq_task(publish_edition),
    ]
    # Generic arq-queue metrics (SQR-112): publish ``arq_job_run`` for
    # every job and an ``arq_queue_stats`` gauge for this pool's queue.
    cron_jobs = [
        cron(
            instrument_arq_task(publish_queue_stats_cron),
            minute=_QUEUE_STATS_CRON_MINUTES,
        ),
    ]
    redis_settings = config.arq_redis_settings
    queue_name = config.arq_queue_name
    on_startup = startup_default
    on_job_start = make_on_job_start(config.arq_queue_name)
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
            instrument_arq_task(keeper_sync_run_discovery),
            timeout=config.keeper_sync_job_timeout_seconds,
            max_tries=1,
        ),
        func(
            instrument_arq_task(keeper_sync_project),
            timeout=config.keeper_sync_job_timeout_seconds,
            max_tries=1,
        ),
        instrument_arq_task(keeper_sync_reaper),
        instrument_arq_task(keeper_sync_tier_main),
        instrument_arq_task(keeper_sync_tier_discovery),
        instrument_arq_task(keeper_sync_tier_other),
    ]
    # Tier-cron cadences come from the constants in
    # ``services/keeper_sync/scheduler.py`` so the planner's next-tick
    # math (``explain_tier_status``) and the cron's actual firing
    # schedule cannot drift.
    cron_jobs = [
        cron(
            instrument_arq_task(keeper_sync_reaper),
            minute={0, 30},
        ),
        # Tier 1 — keeps the user-visible ``main`` edition fresh per
        # user story 10's SLO.
        cron(
            instrument_arq_task(keeper_sync_tier_main),
            minute=_cron_minutes_for_tier_interval(TIER_MAIN_CRON_INTERVAL),
        ),
        # Tier 2 — discovers LTD resources without a
        # ``keeper_sync_state`` row.
        cron(
            instrument_arq_task(keeper_sync_tier_discovery),
            minute=_cron_minutes_for_tier_interval(
                TIER_DISCOVERY_CRON_INTERVAL
            ),
        ),
        # Tier 3 — catches non-``main`` editions whose state has aged
        # past the threshold.
        cron(
            instrument_arq_task(keeper_sync_tier_other),
            minute=_cron_minutes_for_tier_interval(TIER_OTHER_CRON_INTERVAL),
        ),
        # Generic arq-queue metrics (SQR-112): per-pool ``arq_queue_stats``
        # gauge for the keeper-sync queue.
        cron(
            instrument_arq_task(publish_queue_stats_cron),
            minute=_QUEUE_STATS_CRON_MINUTES,
        ),
    ]
    redis_settings = config.arq_redis_settings
    queue_name = KEEPER_SYNC_QUEUE_NAME
    on_startup = startup_keeper_sync
    on_job_start = make_on_job_start(KEEPER_SYNC_QUEUE_NAME)
    on_shutdown = shutdown


class MaintenanceWorkerSettings:
    """arq WorkerSettings for the dedicated maintenance queue.

    Bound to ``docverse:maintenance-queue`` (see
    :data:`MAINTENANCE_QUEUE_NAME`) so a slow maintenance pass cannot
    starve the default queue's ``build_processing`` and
    ``publish_edition`` jobs or the keeper-sync queue's
    ``keeper_sync_project`` jobs. This is the third pool alongside the
    default and keeper-sync pools — a catch-all for non-publishing
    periodic work rather than lifecycle evaluation alone; this class
    is the binding.

    Both the hourly ``lifecycle_eval`` (dispatcher + per-org worker)
    and the daily ``git_ref_audit`` (discovery + per-org worker) live
    on this single pool: PRD #346 explicitly says the audit "shares
    the same fan-out, per-org mutex, and reaper patterns as
    lifecycle_eval and never competes with build processing or
    keeper-sync for worker capacity", and the audit's daily cadence
    is light enough that adding a fourth pool would be over-segmented.

    All four functions are wrapped with :func:`arq.func` so the
    dedicated queue inherits a per-job ``timeout`` (sourced from
    ``Config.maintenance_job_timeout_seconds``) and a
    single-attempt policy. A failure must surface promptly so the
    per-org worker's ``except Exception`` block can route to
    ``queue_job_store.fail()`` and the parent run finalises via the
    matching finaliser — arq's default 5-attempt retry would
    otherwise delay finalisation and obscure the underlying error in
    logs. The cron-driven ``lifecycle_reaper`` is the second
    backstop for the case where arq itself loses a job and no
    timeout ever fires; it runs every 30 minutes and sweeps **both**
    ``kind='lifecycle_eval'`` and ``kind='git_ref_audit'`` rows in a
    single transaction.

    The pool also hosts cross-subsystem reaper backstops for the
    default-pool kinds: ``dashboard_build_reaper``,
    ``publish_edition_reaper``, ``build_processing_reaper``, and
    ``dashboard_sync_reaper`` run here (PRD #367) so reaper sweeps
    never compete with build processing or user-triggered dashboard
    rebuilds for worker capacity. The maintenance name reflects that
    the pool is the shared home for this non-publishing periodic work,
    no longer scoped to lifecycle evaluation alone.

    The opportunistic ``project_github_resolve`` job (PRD #346) also
    runs here: PRD #419 moves it off the default publishing pool because
    its installation-id resolution is not time-sensitive and should not
    contend with the live publishing flow. It is the one event-driven
    (rather than cron-driven) function on the pool and is registered
    plainly — no ``func`` timeout wrapper — exactly as it was on the
    default pool.
    """

    functions = [
        func(
            instrument_arq_task(lifecycle_eval_dispatcher),
            timeout=config.maintenance_job_timeout_seconds,
            max_tries=1,
        ),
        func(
            instrument_arq_task(lifecycle_eval),
            timeout=config.maintenance_job_timeout_seconds,
            max_tries=1,
        ),
        func(
            instrument_arq_task(git_ref_audit_discovery),
            timeout=config.maintenance_job_timeout_seconds,
            max_tries=1,
        ),
        func(
            instrument_arq_task(git_ref_audit),
            timeout=config.maintenance_job_timeout_seconds,
            max_tries=1,
        ),
        # The daily ``inventory_census`` cron (SQR-112 D8) publishes the
        # ``resource_inventory`` gauge. Wrapped like the dispatchers with
        # the maintenance per-job ``timeout`` and ``max_tries=1``: the
        # census is the pool's primary cron work (not a backstop), and a
        # single attempt avoids arq's default retry republishing the
        # gauge — duplicates are harmless under ``last()`` but pointless.
        func(
            instrument_arq_task(inventory_census),
            timeout=config.maintenance_job_timeout_seconds,
            max_tries=1,
        ),
        instrument_arq_task(lifecycle_reaper),
        instrument_arq_task(dashboard_build_reaper),
        instrument_arq_task(publish_edition_reaper),
        instrument_arq_task(build_processing_reaper),
        instrument_arq_task(dashboard_sync_reaper),
        # ``project_github_resolve`` is the opportunistic GitHub-id
        # resolve (PRD #346). PRD #419 moves it off the default
        # publishing pool onto this maintenance pool: its work is not
        # time-sensitive and must not contend with the live publishing
        # flow. Registered plainly (no ``func`` timeout wrapper, arq's
        # default retry policy) exactly as it was on the default pool,
        # so the move changes only which pool runs it.
        instrument_arq_task(project_github_resolve),
    ]
    cron_jobs = [
        cron(
            instrument_arq_task(lifecycle_eval_dispatcher),
            minute={0},
        ),
        # Daily ``git_ref_audit`` discovery tick at UTC 05:17. Five in
        # the morning UTC sits well outside North-American daytime
        # peak when most release builds are running, and minute 17
        # deliberately avoids the lifecycle_eval dispatcher's hourly
        # ``minute={0}`` tick and the lifecycle reaper's
        # ``minute={0, 30}`` ticks so the audit's fan-out does not
        # contend with them on the shared lifecycle worker pool. The
        # other reapers on this pool are staggered onto their own
        # minute slots (see below) for the same reason.
        cron(
            instrument_arq_task(git_ref_audit_discovery),
            hour={5},
            minute={17},
        ),
        # Daily ``inventory_census`` tick (SQR-112 D8) at the
        # config-driven UTC ``inventory_census_cron_hour`` /
        # ``inventory_census_cron_minute`` (04:47 by default). The hour
        # sits in the quiet pre-dawn UTC window ahead of the
        # ``git_ref_audit`` tick, and the minute is staggered off every
        # maintenance-pool reaper slot and the five-minute queue-stats
        # cadence so the census never co-fires with another cron on a
        # horizontally scaled pool — the same contention-avoidance
        # precedent as ``git_ref_audit_discovery`` at minute 17.
        cron(
            instrument_arq_task(inventory_census),
            hour={config.inventory_census_cron_hour},
            minute={config.inventory_census_cron_minute},
        ),
        cron(
            instrument_arq_task(lifecycle_reaper),
            minute={0, 30},
        ),
        # ``dashboard_build_reaper`` runs every 15 minutes because
        # ``dashboard_build`` is the only main-pool kind whose
        # wedge is user-visible (the 409 on ``POST
        # /dashboard/rebuild``); the tighter cadence keeps worst-case
        # wall-clock recovery time under ~45 minutes from the moment
        # a worker silently dies. The slots are offset off the
        # canonical quarter-hours so this reaper never co-fires with
        # the other reapers on the lifecycle pool — see the
        # ``git_ref_audit_discovery`` comment above for the same
        # precedent on contention avoidance.
        cron(
            instrument_arq_task(dashboard_build_reaper),
            minute={3, 18, 33, 48},
        ),
        # ``publish_edition_reaper`` runs on a 30-minute cadence
        # staggered off the lifecycle reaper's ``minute={0, 30}``
        # slot so the two reapers never query ``queue_jobs`` at the
        # same instant on a horizontally scaled lifecycle pool — the
        # same precedent that puts ``git_ref_audit_discovery`` on
        # minute 17. A stuck ``publish_edition`` is not directly
        # user-visible (no 409), so the tighter cadence the
        # ``dashboard_build`` reaper uses is not warranted.
        cron(
            instrument_arq_task(publish_edition_reaper),
            minute={6, 36},
        ),
        # ``build_processing_reaper`` runs on a 30-minute cadence
        # staggered off the lifecycle reaper's ``minute={0, 30}``
        # slot so the two reapers never query ``queue_jobs`` at the
        # same instant on a horizontally scaled lifecycle pool — the
        # same precedent that puts ``git_ref_audit_discovery`` on
        # minute 17. A stuck ``build_processing`` is invisible to
        # operators today (no user-facing surface), so the tighter
        # dashboard_build cadence is not warranted. The 8-hour
        # threshold is intentionally generous so a real multi-hour
        # upload of a very large build is never falsely reaped by
        # this cron backstop.
        cron(
            instrument_arq_task(build_processing_reaper),
            minute={12, 42},
        ),
        # ``dashboard_sync_reaper`` runs on a 30-minute cadence
        # staggered off the lifecycle reaper's ``minute={0, 30}``
        # slot so the two reapers never query ``queue_jobs`` at the
        # same instant on a horizontally scaled lifecycle pool — the
        # same precedent that puts ``git_ref_audit_discovery`` on
        # minute 17. A stuck ``dashboard_sync`` is invisible to
        # operators today (no user-facing surface), so the tighter
        # dashboard_build cadence is not warranted. The 6-hour
        # threshold gives an operator-triggered GitHub fetch + fanout
        # room to legitimately complete.
        cron(
            instrument_arq_task(dashboard_sync_reaper),
            minute={24, 54},
        ),
        # Generic arq-queue metrics (SQR-112): per-pool ``arq_queue_stats``
        # gauge for the maintenance queue. Queue-depth stats only touch
        # Redis and Kafka, so the five-minute cadence is free to overlap
        # the Postgres-bound reaper slots above without contention.
        cron(
            instrument_arq_task(publish_queue_stats_cron),
            minute=_QUEUE_STATS_CRON_MINUTES,
        ),
    ]
    redis_settings = config.arq_redis_settings
    queue_name = MAINTENANCE_QUEUE_NAME
    on_startup = startup_maintenance
    on_job_start = make_on_job_start(MAINTENANCE_QUEUE_NAME)
    on_shutdown = shutdown
