"""SQLAlchemy ORM model for the ``keeper_sync_runs`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
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
from sqlalchemy.sql import func

from .base import Base


class SqlKeeperSyncRun(Base):
    """ORM model for the ``keeper_sync_runs`` table.

    One row per operator-triggered LTD-sync backfill (``POST /orgs/{org}
    /keeper-sync/runs``). Aggregate progress counters are not denormalised
    here; they're derived from ``queue_jobs`` filtered on
    ``keeper_sync_run_id``. The partial unique index
    ``idx_keeper_sync_runs_org_non_terminal_uq`` enforces the
    one-non-terminal-run-per-org invariant at the DB level so concurrent
    ``POST /runs`` calls surface as 409 instead of racing.
    """

    __tablename__ = "keeper_sync_runs"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    org_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )

    kind: Mapped[str] = mapped_column(String(32), nullable=False)

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
            name="ck_keeper_sync_runs_status",
        ),
        CheckConstraint(
            "kind IN ('backfill', 'resync', 'reconcile')",
            name="ck_keeper_sync_runs_kind",
        ),
        Index("idx_keeper_sync_runs_org_id", "org_id"),
        Index(
            "idx_keeper_sync_runs_org_non_terminal_uq",
            "org_id",
            unique=True,
            postgresql_where=text("status IN ('pending', 'in_progress')"),
        ),
    )
