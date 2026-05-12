"""SQLAlchemy ORM model for the ``lifecycle_eval_runs`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from .base import Base


class SqlLifecycleEvalRun(Base):
    """ORM model for the ``lifecycle_eval_runs`` table.

    One row per dispatcher tick of the ``lifecycle_eval`` periodic job.
    Per-org child work lives on ``queue_jobs`` and is attributed to the
    parent run via the nullable ``queue_jobs.lifecycle_eval_run_id``
    FK; aggregate counters are derived from that filter at read time
    rather than denormalised here.

    The partial unique index
    ``idx_lifecycle_eval_runs_non_terminal_uq`` indexes a constant
    expression so at most one row can match the
    ``status IN ('pending', 'in_progress')`` predicate at any moment.
    This is the DB-level backstop that prevents the previous tick from
    being doubled up by the next tick if it has not yet finalised.
    """

    __tablename__ = "lifecycle_eval_runs"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    status: Mapped[str] = mapped_column(String(32), nullable=False)

    date_started: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    date_finished: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    summary: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'in_progress', 'succeeded', "
            "'partial_failure', 'failed')",
            name="ck_lifecycle_eval_runs_status",
        ),
        Index(
            "idx_lifecycle_eval_runs_non_terminal_uq",
            text("(true)"),
            unique=True,
            postgresql_where=text("status IN ('pending', 'in_progress')"),
        ),
    )
