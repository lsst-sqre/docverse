"""SQLAlchemy ORM model for the ``queue_jobs`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from .base import Base


class SqlQueueJob(Base):
    """ORM model for the ``queue_jobs`` table."""

    __tablename__ = "queue_jobs"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    public_id: Mapped[int] = mapped_column(
        BigInteger, unique=True, nullable=False, autoincrement=False
    )

    backend_job_id: Mapped[str | None] = mapped_column(
        String(256), nullable=True
    )

    kind: Mapped[str] = mapped_column(String(64), nullable=False)

    status: Mapped[str] = mapped_column(
        String(64), nullable=False, default="queued"
    )

    phase: Mapped[str | None] = mapped_column(String(128), nullable=True)

    org_id: Mapped[int] = mapped_column(Integer, nullable=False)
    # FK to organizations.id is intentionally omitted here; it will be
    # added when the relationship is fully wired.

    # project_id and build_id reference tables that don't exist yet.
    # FK constraints will be added when the Project and Build tables are
    # created.
    project_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    build_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    progress: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    errors: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    date_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    date_started: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    date_completed: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("idx_queue_jobs_kind", "kind"),
        Index("idx_queue_jobs_status", "status"),
        Index("idx_queue_jobs_org_id", "org_id"),
    )
