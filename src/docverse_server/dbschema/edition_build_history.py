"""SQLAlchemy ORM model for the ``edition_build_history`` table."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from .base import Base


class SqlEditionBuildHistory(Base):
    """ORM model for the ``edition_build_history`` table.

    Logs every build that an edition has pointed to, enabling rollback
    and orphan detection. Position 1 is the most recent entry, and
    ``uq_ebh_edition_position`` is what makes "most recent" name exactly
    one row: two writers racing under READ COMMITTED could otherwise
    each commit a position-1 row for one edition, after which the
    publish writers and the reconciliation planner could resolve the
    same pair to different rows.

    The constraint is ``DEFERRABLE INITIALLY IMMEDIATE`` because
    :meth:`~docverse_server.storage.edition_build_history_store.EditionBuildHistoryStore.record`
    bumps a whole edition's positions in one ``UPDATE``. An immediate
    unique index checks each row as it rewrites it and would reject that
    bump as soon as it turned a 1 into a 2 the row still holding it had
    not yet vacated; a deferred check sees the finished statement.
    ``INITIALLY IMMEDIATE`` keeps that check at the end of every
    statement, so no caller opts in and none is handed a violation
    deferred to commit.
    """

    __tablename__ = "edition_build_history"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    edition_id: Mapped[int] = mapped_column(Integer, nullable=False)

    build_id: Mapped[int] = mapped_column(Integer, nullable=False)

    position: Mapped[int] = mapped_column(Integer, nullable=False)

    publish_status: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )

    date_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_ebh_edition_id", "edition_id"),
        UniqueConstraint(
            "edition_id",
            "position",
            name="uq_ebh_edition_position",
            deferrable=True,
            initially="IMMEDIATE",
        ),
    )
