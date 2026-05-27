"""SQLAlchemy ORM model for the ``git_ref_audit_runs`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from .base import Base


class SqlGitRefAuditRun(Base):
    """ORM model for the ``git_ref_audit_runs`` table.

    One row per discovery tick of the daily ``git_ref_audit`` periodic
    job. Per-org child work lives on ``queue_jobs`` and is attributed
    to the parent run via the nullable
    ``queue_jobs.git_ref_audit_run_id`` FK; aggregate counters are
    derived from that filter at read time rather than denormalised
    here. Mirrors :class:`SqlLifecycleEvalRun` so the dispatcher /
    per-org / reaper pattern transfers between the two subsystems —
    the schemas differ only in name. The two tables intentionally live
    separately rather than reusing ``lifecycle_eval_runs``: the two
    cadences differ (lifecycle_eval is hourly, git_ref_audit is daily)
    and operators reasonably want to inspect them on different axes.

    The partial unique index
    ``idx_git_ref_audit_runs_non_terminal_uq`` indexes a constant
    expression so at most one row can match the
    ``status IN ('pending', 'in_progress')`` predicate at any moment.
    This is the DB-level backstop that prevents a slow tick from being
    doubled up by the next discovery firing.
    """

    __tablename__ = "git_ref_audit_runs"

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
            name="ck_git_ref_audit_runs_status",
        ),
        Index(
            "idx_git_ref_audit_runs_non_terminal_uq",
            text("(true)"),
            unique=True,
            postgresql_where=text("status IN ('pending', 'in_progress')"),
        ),
    )
