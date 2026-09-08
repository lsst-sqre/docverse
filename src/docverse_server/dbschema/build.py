"""SQLAlchemy ORM model for the ``builds`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Enum, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from docverse.models import BuildStatus

from .base import Base


class SqlBuild(Base):
    """ORM model for the ``builds`` table."""

    __tablename__ = "builds"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    public_id: Mapped[int] = mapped_column(
        BigInteger, unique=True, nullable=False, autoincrement=False
    )

    project_id: Mapped[int] = mapped_column(Integer, nullable=False)

    git_ref: Mapped[str] = mapped_column(String(256), nullable=False)

    alternate_name: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )

    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)

    status: Mapped[BuildStatus] = mapped_column(
        Enum(BuildStatus, native_enum=False, length=32),
        nullable=False,
        default=BuildStatus.pending,
    )

    staging_key: Mapped[str] = mapped_column(String(512), nullable=False)

    storage_prefix: Mapped[str] = mapped_column(String(512), nullable=False)

    object_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    total_size_bytes: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )

    uploader: Mapped[str] = mapped_column(String(256), nullable=False)

    annotations: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    date_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    date_uploaded: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    date_completed: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    date_deleted: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Stamped by the purgatory sweep once the build's unpacked tree
    # and staging tarball are gone from the object store. NULL means
    # the content is still there, which is what makes a soft-deleted
    # build restorable; a timestamp means the row is a tombstone.
    date_purged: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("idx_builds_project_id_git_ref", "project_id", "git_ref"),
        Index(
            "idx_builds_project_id_content_hash",
            "project_id",
            "content_hash",
        ),
        Index("idx_builds_status", "status"),
        Index("idx_builds_git_ref", "git_ref"),
        # Work list for the purgatory sweep: the builds whose objects
        # are still on the store but whose rows are soft-deleted. The
        # partial WHERE keeps the index to the reap-pending rows only,
        # so it stays small next to the table and a build drops out of
        # it the moment ``date_purged`` is stamped. ``date_deleted`` is
        # the indexed column because the sweep both filters on it
        # (against the org's retention cutoff) and orders by it.
        Index(
            "idx_builds_purgatory",
            "date_deleted",
            postgresql_where=text(
                "date_deleted IS NOT NULL AND date_purged IS NULL"
            ),
        ),
    )
