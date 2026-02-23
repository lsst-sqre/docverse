"""Configuration for the Docverse application."""

from __future__ import annotations

from pydantic import Field, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.logging import LogLevel, Profile

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

    repertoire_base_url: HttpUrl = Field(
        title="URL of the Repertoire service",
        examples=["https://roundtable.lsst.cloud/repertoire"],
        validation_alias="REPERTOIRE_BASE_URL",
    )


config = Configuration()
"""The process-wide configuration instance."""
