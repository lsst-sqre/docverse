"""SQLAlchemy ORM model for the ``projects`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from .base import Base


class SqlProject(Base):
    """ORM model for the ``projects`` table."""

    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    slug: Mapped[str] = mapped_column(String(128), nullable=False)

    title: Mapped[str] = mapped_column(String(256), nullable=False)

    org_id: Mapped[int] = mapped_column(Integer, nullable=False)

    doc_repo: Mapped[str] = mapped_column(String(512), nullable=False)

    slug_rewrite_rules: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    lifecycle_rules: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
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
        UniqueConstraint("org_id", "slug", name="uq_projects_org_slug"),
        Index("idx_projects_org_id", "org_id"),
        Index(
            "idx_projects_slug_trgm",
            "slug",
            postgresql_using="gin",
            postgresql_ops={"slug": "gin_trgm_ops"},
        ),
        Index(
            "idx_projects_title_trgm",
            "title",
            postgresql_using="gin",
            postgresql_ops={"title": "gin_trgm_ops"},
        ),
    )
