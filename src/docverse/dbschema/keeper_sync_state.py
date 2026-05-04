"""SQLAlchemy ORM model for the ``keeper_sync_state`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class SqlKeeperSyncState(Base):
    """ORM model for the ``keeper_sync_state`` table.

    One row per LTD ↔ Docverse pairing for a project, edition, or build.
    Re-entering any sync function with the same payload looks up by the
    ``(org_id, resource_type, ltd_id)`` unique key and either short-
    circuits (state matches LTD) or resumes (state differs).
    """

    __tablename__ = "keeper_sync_state"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    org_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )

    resource_type: Mapped[str] = mapped_column(String(32), nullable=False)

    ltd_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    ltd_slug: Mapped[str] = mapped_column(String(256), nullable=False)

    docverse_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    date_last_synced: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    date_rebuilt_seen: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    last_seen_etag: Mapped[str | None] = mapped_column(
        String(256), nullable=True
    )

    content_hash: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )

    annotations: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "resource_type",
            "ltd_id",
            name="uq_keeper_sync_state_org_resource_ltd",
        ),
        CheckConstraint(
            "resource_type IN ('project', 'edition', 'build')",
            name="ck_keeper_sync_state_resource_type",
        ),
        Index("idx_keeper_sync_state_org_id", "org_id"),
    )
