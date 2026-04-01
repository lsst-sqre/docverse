"""SQLAlchemy ORM model for the ``organizations`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Enum, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from docverse.client.models import UrlScheme

from .base import Base


class SqlOrganization(Base):
    """ORM model for the ``organizations`` table."""

    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    slug: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    title: Mapped[str] = mapped_column(String(256), nullable=False)

    base_domain: Mapped[str] = mapped_column(String(256), nullable=False)

    url_scheme: Mapped[UrlScheme] = mapped_column(
        Enum(UrlScheme, native_enum=False, length=32),
        nullable=False,
        default=UrlScheme.subdomain,
    )

    root_path_prefix: Mapped[str] = mapped_column(
        Text, nullable=False, default="/"
    )

    slug_rewrite_rules: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    lifecycle_rules: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    default_edition_config: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    publishing_store_label: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )

    staging_store_label: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )

    cdn_service_label: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )

    dns_service_label: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )

    purgatory_retention: Mapped[int] = mapped_column(
        Integer,
        name="purgatory_retention_seconds",
        nullable=False,
        default=2592000,
    )

    date_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    date_updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (Index("idx_organizations_slug", "slug"),)
