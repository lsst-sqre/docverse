"""SQLAlchemy ORM model for the ``queue_jobs`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    text,
)
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
    edition_id: Mapped[int | None] = mapped_column(
        ForeignKey("editions.id"),
        nullable=True,
    )

    keeper_sync_run_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("keeper_sync_runs.id", ondelete="SET NULL"),
        nullable=True,
    )

    lifecycle_eval_run_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("lifecycle_eval_runs.id", ondelete="SET NULL"),
        nullable=True,
    )

    git_ref_audit_run_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("git_ref_audit_runs.id", ondelete="SET NULL"),
        nullable=True,
    )

    subject_label: Mapped[str | None] = mapped_column(Text, nullable=True)

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
        Index("idx_queue_jobs_keeper_sync_run_id", "keeper_sync_run_id"),
        Index(
            "idx_queue_jobs_lifecycle_eval_run_id",
            "lifecycle_eval_run_id",
        ),
        Index(
            "idx_queue_jobs_git_ref_audit_run_id",
            "git_ref_audit_run_id",
        ),
        # Per-project mutex for keeper_sync_project jobs: at most one
        # queued or in_progress row per (org_id, subject_label). The
        # partial WHERE means terminal rows do not participate, so a
        # finished job never blocks a fresh enqueue.
        Index(
            "idx_queue_jobs_keeper_sync_project_active_uq",
            "org_id",
            "subject_label",
            unique=True,
            postgresql_where=text(
                "kind = 'keeper_sync_project' "
                "AND status IN ('queued', 'in_progress')"
            ),
        ),
        # Per-org mutex for ``lifecycle_eval`` per-org child jobs: at
        # most one queued or in_progress row per ``org_id`` for
        # ``kind='lifecycle_eval'``. ``subject_label`` is deliberately
        # **not** part of the mutex identity here, which diverges from
        # the ``keeper_sync_project_active_uq`` shape just above. The
        # divergence is intentional: keeper_sync_project is
        # per-project within an org and needs ``subject_label`` (set
        # to the LTD slug) to distinguish concurrent project syncs;
        # lifecycle_eval is per-org by design (SQR-112) and has no
        # sub-key under ``org_id``, so adding ``subject_label`` to the
        # index would only make the two indexes look superficially
        # similar without encoding any additional identity. The row
        # itself still carries ``subject_label = org.slug`` for
        # operator readability of the queue — it just isn't part of
        # the mutex. The partial WHERE means terminal rows do not
        # participate, so a finished tick never blocks the next.
        Index(
            "idx_queue_jobs_lifecycle_eval_active_uq",
            "org_id",
            unique=True,
            postgresql_where=text(
                "kind = 'lifecycle_eval' "
                "AND status IN ('queued', 'in_progress')"
            ),
        ),
        # Per-org mutex for ``git_ref_audit`` per-org child jobs: at
        # most one queued or in_progress row per ``org_id`` for
        # ``kind='git_ref_audit'``. Same single-column shape as the
        # ``lifecycle_eval_active_uq`` mutex — git_ref_audit is also
        # per-org by design (one daily audit pass per org), with no
        # sub-key under ``org_id``. The row still carries
        # ``subject_label = org.slug`` for operator readability, just
        # as for the lifecycle_eval mutex.
        Index(
            "idx_queue_jobs_git_ref_audit_active_uq",
            "org_id",
            unique=True,
            postgresql_where=text(
                "kind = 'git_ref_audit' "
                "AND status IN ('queued', 'in_progress')"
            ),
        ),
        # Per-project mutex for dashboard_build jobs: at most one
        # queued or in_progress row per (org_id, project_id). Backstops
        # the application-side ``has_active_dashboard_build`` pre-check
        # in ``DashboardBuildEnqueuer.enqueue_for_project`` against any
        # race that slips between read and create.
        Index(
            "idx_queue_jobs_dashboard_build_active_uq",
            "org_id",
            "project_id",
            unique=True,
            postgresql_where=text(
                "kind = 'dashboard_build' "
                "AND status IN ('queued', 'in_progress')"
            ),
        ),
    )
