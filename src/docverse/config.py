"""Configuration for the Docverse application."""

from __future__ import annotations

from pathlib import Path

from arq.connections import RedisSettings
from pydantic import Field, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.arq import ArqMode, build_arq_redis_settings
from safir.logging import LogLevel, Profile
from safir.pydantic import EnvRedisDsn

__all__ = ["Configuration", "config"]


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
