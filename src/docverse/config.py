"""Configuration for the Docverse application."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

from arq.connections import RedisSettings
from pydantic import BeforeValidator, Field, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.arq import ArqMode, build_arq_redis_settings
from safir.logging import LogLevel, Profile
from safir.metrics import MetricsConfiguration, metrics_configuration_factory
from safir.pydantic import EnvRedisDsn

__all__ = ["Configuration", "config"]


def _parse_comma_separated(v: Any) -> Any:
    """Parse a comma-separated string into a list of strings."""
    if isinstance(v, str):
        return [item.strip() for item in v.split(",") if item.strip()]
    return v


class Configuration(BaseSettings):
    """Configuration for Docverse."""

    model_config = SettingsConfigDict(
        env_prefix="DOCVERSE_", case_sensitive=False
    )

    log_level: LogLevel = Field(
        LogLevel.INFO, title="Log level of the application's logger"
    )

    log_profile: Profile = Field(
        Profile.development, title="Application logging profile"
    )

    name: str = Field("docverse", title="Name of application")

    path_prefix: str = Field("/docverse", title="URL prefix for application")

    slack_webhook: SecretStr | None = Field(
        None,
        title="Slack webhook for alerts",
        description="If set, alerts will be posted to this Slack webhook",
    )

    metrics: MetricsConfiguration = Field(
        default_factory=metrics_configuration_factory,
        title="Sasquatch application-metrics configuration",
        description=(
            "Configures Safir's ``EventManager`` for the Docverse"
            " application-metrics pipeline (SQR-112). The concrete"
            " variant is selected from the environment by"
            " ``metrics_configuration_factory``:\n\n"
            "- ``METRICS_APPLICATION`` (e.g. ``docverse``),"
            " ``METRICS_ENABLED``, and ``METRICS_MOCK`` choose between"
            " the mock, disabled, and Kafka-backed managers. Set"
            " ``METRICS_MOCK=true`` (with ``METRICS_ENABLED=false``) for"
            " tests; set ``METRICS_ENABLED=true`` in production.\n"
            "- When enabled, ``KAFKA_*`` (bootstrap servers, security"
            " protocol, and any TLS material) and ``SCHEMA_MANAGER_*``"
            " (the Confluent-compatible schema-registry URL) connect the"
            " manager to Sasquatch.\n\n"
            "Events publish to the ``lsst.square.metrics.events.docverse``"
            " topic, which the ``phalanx-docverse`` deployment must"
            " register in Sasquatch (tracked separately from this"
            " application)."
        ),
    )

    github_app_id: int | None = Field(
        None,
        title="GitHub App ID",
        description=(
            "Numeric ID of the GitHub App installed by Docverse tenants."
            " Leave unset to disable the GitHub App feature. All three"
            " ``github_app_*`` / ``github_webhook_*`` values must be set"
            " together for the feature to be enabled."
        ),
    )

    github_app_private_key: SecretStr | None = Field(
        None,
        title="GitHub App private key (PEM)",
        description=(
            "PEM-encoded private key for the GitHub App. Used to sign"
            " JWTs when exchanging for installation access tokens."
        ),
    )

    github_webhook_secret: SecretStr | None = Field(
        None,
        title="GitHub webhook shared secret",
        description=(
            "Shared secret used to verify the HMAC signature on incoming"
            " GitHub webhooks."
        ),
    )

    database_url: str = Field(
        title="URL of the PostgreSQL database",
        description=(
            "Database URL without the password. The password is provided"
            " separately via ``database_password``."
        ),
    )

    database_password: SecretStr = Field(
        title="Password for the PostgreSQL database"
    )

    alembic_config_path: Path = Field(
        Path("/app/alembic.ini"),
        title="Path to the Alembic configuration file",
    )

    repertoire_base_url: HttpUrl = Field(
        title="URL of the Repertoire service",
        examples=["https://roundtable.lsst.cloud/repertoire"],
        validation_alias="REPERTOIRE_BASE_URL",
    )

    credential_encryption_key: SecretStr = Field(
        title="Fernet key for encrypting organization credentials",
        description=(
            "A base64url-encoded 32-byte Fernet key. Generate with"
            " ``python -c 'from cryptography.fernet import Fernet;"
            " print(Fernet.generate_key().decode())'``."
        ),
    )

    credential_encryption_key_retired: SecretStr | None = Field(
        None,
        title="Retired Fernet key for credential rotation",
        description=(
            "When rotating keys, set the old key here so existing"
            " credentials can still be decrypted. Remove after all"
            " credentials have been re-encrypted."
        ),
    )

    arq_mode: ArqMode = Field(
        ArqMode.production,
        title="arq queue mode",
        description=(
            "Set to 'test' to use an in-memory mock queue instead of Redis."
        ),
    )

    arq_redis_url: EnvRedisDsn | None = Field(
        None,
        title="Redis URL for the arq queue",
        description="Required when arq_mode is 'production'.",
    )

    arq_redis_password: SecretStr | None = Field(
        None,
        title="Password for the arq Redis connection",
    )

    arq_queue_name: str = Field(
        "docverse:queue",
        title="Name of the arq queue",
    )

    keeper_sync_job_timeout_seconds: int = Field(
        3600,
        title="Keeper-sync per-job timeout, in seconds",
        description=(
            "Wraps the keeper-sync arq functions on"
            " ``KeeperSyncWorkerSettings``: arq cancels a job that runs"
            " past this. Lower this in test/staging to surface"
            " stuck-worker behaviour quickly."
        ),
    )

    keeper_sync_reaper_threshold_seconds: int = Field(
        21600,
        title="Keeper-sync stuck-run reaper threshold, in seconds",
        description=(
            "Cron-driven backstop for arq losing a job (e.g. an"
            " OOM-killed worker pod). ``keeper_sync_reaper`` fails any"
            " keeper-sync child ``queue_jobs`` row that has been"
            " ``in_progress`` longer than this without ``date_completed``."
        ),
    )

    maintenance_job_timeout_seconds: int = Field(
        3600,
        title="Maintenance per-job timeout, in seconds",
        description=(
            "Wraps all four maintenance-pool arq functions"
            " (``lifecycle_eval_dispatcher`` / ``lifecycle_eval`` and"
            " ``git_ref_audit_discovery`` / ``git_ref_audit``) on"
            " ``MaintenanceWorkerSettings``: arq cancels a job that"
            " runs past this. The cron-driven ``lifecycle_reaper`` is"
            " the second backstop (covers OOM-killed workers / arq"
            " losing a job), so this should sit well below"
            " ``lifecycle_reaper_threshold_seconds``. Lower this in"
            " test/staging to surface stuck-worker behaviour quickly."
        ),
    )

    lifecycle_reaper_threshold_seconds: int = Field(
        21600,
        title="Lifecycle_eval stuck-run reaper threshold, in seconds",
        description=(
            "Cron-driven backstop for arq losing a ``lifecycle_eval``"
            " per-org job (e.g. an OOM-killed worker pod)."
            " ``lifecycle_reaper`` fails any ``kind='lifecycle_eval'``"
            " ``queue_jobs`` row that has been ``in_progress`` longer"
            " than this without ``date_completed``. Mirrors"
            " ``keeper_sync_reaper_threshold_seconds`` so the operator"
            " knob shape is identical across the two reapers; the"
            " env-overridable default lets non-prod environments drive"
            " the threshold down to seconds for fast verification."
            " The same threshold covers ``kind='git_ref_audit'`` rows"
            " — the two subsystems share one reaper job."
        ),
    )

    dashboard_build_reaper_threshold_seconds: int = Field(
        1800,
        title="Dashboard_build stuck-run reaper threshold, in seconds",
        description=(
            "Cron-driven backstop for arq losing a ``dashboard_build``"
            " job (e.g. an OOM-killed worker pod or a dispatcher that"
            " crashed between the ``queue_jobs`` SQL commit and"
            " ``arq_queue.enqueue``). ``dashboard_build_reaper`` fails"
            " any ``kind='dashboard_build'`` ``queue_jobs`` row that"
            " has been ``in_progress`` longer than this without"
            " ``date_completed``, releasing the per-project mutex"
            " ``idx_queue_jobs_dashboard_build_active_uq`` so the"
            " operator's next ``POST /dashboard/rebuild`` no longer"
            " returns 409. Mirrors"
            " ``lifecycle_reaper_threshold_seconds`` so the operator"
            " knob shape is identical across reapers; the"
            " env-overridable default lets non-prod environments drive"
            " the threshold down to seconds for fast verification."
        ),
    )

    publish_edition_reaper_threshold_seconds: int = Field(
        14400,
        title="Publish_edition stuck-run reaper threshold, in seconds",
        description=(
            "Cron-driven backstop for arq losing a ``publish_edition``"
            " job (e.g. an OOM-killed worker pod or a dispatcher that"
            " crashed between the ``queue_jobs`` SQL commit and"
            " ``arq_queue.enqueue``). ``publish_edition_reaper`` fails"
            " any ``kind='publish_edition'`` ``queue_jobs`` row that"
            " has been ``in_progress`` longer than this without"
            " ``date_completed`` so an edition does not sit in"
            " ``publishing`` indefinitely and the CDN does not silently"
            " stay behind. Defaults to 4 hours — long enough for the"
            " CDN-publish retry loop to legitimately complete, short"
            " enough that wedged rows clear the same day. Mirrors"
            " ``lifecycle_reaper_threshold_seconds`` so the operator"
            " knob shape is identical across reapers; the"
            " env-overridable default lets non-prod environments drive"
            " the threshold down to seconds for fast verification."
        ),
    )

    build_processing_reaper_threshold_seconds: int = Field(
        28800,
        title="Build_processing stuck-run reaper threshold, in seconds",
        description=(
            "Cron-driven backstop for arq losing a ``build_processing``"
            " job (e.g. an OOM-killed worker pod or a dispatcher that"
            " crashed between the ``queue_jobs`` SQL commit and"
            " ``arq_queue.enqueue``). ``build_processing_reaper`` fails"
            " any ``kind='build_processing'`` ``queue_jobs`` row that"
            " has been ``in_progress`` longer than this without"
            " ``date_completed`` so an uploaded build does not stay"
            " unregistered indefinitely after a worker crash. Defaults"
            " to 8 hours — generous enough that an honest multi-hour"
            " tarball download + unpack + S3 upload for a very large"
            " build is never falsely reaped, short enough that a truly"
            " wedged job does not block the project indefinitely."
            " Mirrors ``lifecycle_reaper_threshold_seconds`` so the"
            " operator knob shape is identical across reapers; the"
            " env-overridable default lets non-prod environments drive"
            " the threshold down to seconds for fast verification."
        ),
    )

    dashboard_sync_reaper_threshold_seconds: int = Field(
        21600,
        title="Dashboard_sync stuck-run reaper threshold, in seconds",
        description=(
            "Cron-driven backstop for arq losing a ``dashboard_sync``"
            " job (e.g. an OOM-killed worker pod or a dispatcher that"
            " crashed between the ``queue_jobs`` SQL commit and"
            " ``arq_queue.enqueue``). ``dashboard_sync_reaper`` fails"
            " any ``kind='dashboard_sync'`` ``queue_jobs`` row that"
            " has been ``in_progress`` longer than this without"
            " ``date_completed`` so a binding's ``last_sync_queue_job``"
            " does not show a permanently in-progress sync after a"
            " worker crash. Defaults to 6 hours — long enough for an"
            " operator-triggered GitHub fetch + fanout to legitimately"
            " complete, short enough that wedged rows clear within a"
            " working day. Mirrors ``lifecycle_reaper_threshold_seconds``"
            " so the operator knob shape is identical across reapers;"
            " the env-overridable default lets non-prod environments"
            " drive the threshold down to seconds for fast verification."
        ),
    )

    git_ref_audit_enabled: bool = Field(
        default=False,
        title="Whether the daily git_ref_audit dispatcher fans out work",
        description=(
            "Feature flag for the daily ``git_ref_audit`` safety-net"
            " (PRD #346). When false, the discovery cron returns"
            " ``skipped`` immediately without creating a"
            " ``git_ref_audit_runs`` row or any per-org ``queue_jobs``"
            " children — the cron itself stays registered so flipping"
            " the flag does not require a worker restart. Phalanx"
            " ships the flag false in production until the audit's"
            " GitHub API budget and per-project cost are observed in"
            " a live environment."
        ),
    )

    inventory_census_cron_hour: int = Field(
        4,
        title="UTC hour for the daily resource_inventory census cron",
        description=(
            "Hour (UTC) at which the daily ``inventory_census`` job runs"
            " on the maintenance pool, publishing the SQR-112"
            " ``resource_inventory`` gauge. Paired with"
            " ``inventory_census_cron_minute``; the 04:47 default sits"
            " in the quiet pre-dawn UTC window, ahead of the daily"
            " ``git_ref_audit`` tick at 05:17, and its minute is"
            " staggered off every maintenance-pool reaper slot so the"
            " census never co-fires with them. Config-driven so an"
            " operator can move the census without a code change."
        ),
    )

    inventory_census_cron_minute: int = Field(
        47,
        title="UTC minute for the daily resource_inventory census cron",
        description=(
            "Minute of ``inventory_census_cron_hour`` (UTC) at which the"
            " daily ``inventory_census`` job runs. The default 47 is"
            " staggered off every maintenance-pool reaper minute slot"
            " (``{0,30}`` / ``{3,18,33,48}`` / ``{6,36}`` / ``{12,42}``"
            " / ``{24,54}``) and the five-minute ``arq_queue_stats``"
            " cadence so a horizontally scaled maintenance pool never"
            " fires the census on the same minute as another cron."
        ),
    )

    superadmin_usernames: Annotated[
        list[str], BeforeValidator(_parse_comma_separated)
    ] = Field(
        default_factory=list,
        title="Usernames with super admin access",
        description=(
            "Users in this list have de facto admin access to all"
            " organizations. Comma-separated when set via env var."
        ),
    )

    @property
    def arq_redis_settings(self) -> RedisSettings | None:
        """Build Redis settings for arq from config fields."""
        if self.arq_redis_url is None:
            return None
        return build_arq_redis_settings(
            self.arq_redis_url, self.arq_redis_password
        )


config = Configuration()
"""The process-wide configuration instance."""
