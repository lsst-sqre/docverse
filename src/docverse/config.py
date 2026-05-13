"""Configuration for the Docverse application."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

from arq.connections import RedisSettings
from pydantic import BeforeValidator, Field, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.arq import ArqMode, build_arq_redis_settings
from safir.logging import LogLevel, Profile
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

    lifecycle_eval_job_timeout_seconds: int = Field(
        3600,
        title="Lifecycle-eval per-job timeout, in seconds",
        description=(
            "Wraps the ``lifecycle_eval_dispatcher`` and per-org"
            " ``lifecycle_eval`` arq functions on"
            " ``LifecycleEvalWorkerSettings``: arq cancels a job that"
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
