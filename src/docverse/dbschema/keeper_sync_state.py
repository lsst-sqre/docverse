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
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class SqlKeeperSyncState(Base):
    """ORM model for the ``keeper_sync_state`` table.

    One row per LTD ↔ Docverse pairing for a project, edition, or build.
    Project rows are keyed on ``(org_id, ltd_slug)`` because LTD products
    are slug-only; edition and build rows are keyed on
    ``(org_id, resource_type, ltd_id)`` because LTD edition / build
    slugs are only unique within a product. The two partial unique
    indexes below enforce these per-resource keys.
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

    ltd_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

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
        Index(
            "uq_keeper_sync_state_project_org_slug",
            "org_id",
            "ltd_slug",
            unique=True,
            postgresql_where=text("resource_type = 'project'"),
        ),
        Index(
            "uq_keeper_sync_state_other_org_resource_ltd",
            "org_id",
            "resource_type",
            "ltd_id",
            unique=True,
            postgresql_where=text("resource_type IN ('edition', 'build')"),
        ),
        CheckConstraint(
            "resource_type IN ('project', 'edition', 'build')",
            name="ck_keeper_sync_state_resource_type",
        ),
        Index("idx_keeper_sync_state_org_id", "org_id"),
    )
