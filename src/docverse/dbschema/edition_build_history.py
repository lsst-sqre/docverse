"""SQLAlchemy ORM model for the ``edition_build_history`` table."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from .base import Base


class SqlEditionBuildHistory(Base):
    """ORM model for the ``edition_build_history`` table.

    Logs every build that an edition has pointed to, enabling rollback
    and orphan detection. Position 1 is the most recent entry.
    """

    __tablename__ = "edition_build_history"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    edition_id: Mapped[int] = mapped_column(Integer, nullable=False)

    build_id: Mapped[int] = mapped_column(Integer, nullable=False)

    position: Mapped[int] = mapped_column(Integer, nullable=False)

    date_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_ebh_edition_id", "edition_id"),
        Index("idx_ebh_edition_position", "edition_id", "position"),
    )
