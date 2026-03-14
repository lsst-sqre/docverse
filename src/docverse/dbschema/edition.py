"""SQLAlchemy ORM model for the ``editions`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from docverse.client.models import EditionKind, TrackingMode

from .base import Base


class SqlEdition(Base):
    """ORM model for the ``editions`` table."""

    __tablename__ = "editions"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    slug: Mapped[str] = mapped_column(String(128), nullable=False)

    title: Mapped[str] = mapped_column(String(256), nullable=False)

    project_id: Mapped[int] = mapped_column(Integer, nullable=False)

    kind: Mapped[EditionKind] = mapped_column(
        Enum(EditionKind, native_enum=False, length=32),
        nullable=False,
    )

    tracking_mode: Mapped[TrackingMode] = mapped_column(
        Enum(TrackingMode, native_enum=False, length=32),
        nullable=False,
    )

    tracking_params: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    current_build_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )

    lifecycle_exempt: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
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

    date_deleted: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "project_id", "slug", name="uq_editions_project_slug"
        ),
        Index("idx_editions_project_id", "project_id"),
    )
