"""Add ``lifecycle_eval_runs`` table and ``queue_jobs`` per-org mutex index.

The persistence layer for the new ``lifecycle_eval`` periodic job.
The dispatcher cron writes one ``lifecycle_eval_runs`` aggregate row
per tick, fans out per-org work to ``queue_jobs`` with
``kind='lifecycle_eval'`` and ``subject_label=str(org_id)``, and the
per-org worker calls ``maybe_finalise_lifecycle_run`` to transition
the parent row when every child is terminal.

Schema deltas:

- ``lifecycle_eval_runs`` aggregate table with five-state status
  enumeration (``pending`` / ``in_progress`` / ``succeeded`` /
  ``partial_failure`` / ``failed``) and a JSONB summary column for
  dispatcher-level tick metadata.
- ``idx_lifecycle_eval_runs_non_terminal_uq``: a partial unique index
  on the constant expression ``(true)`` with
  ``WHERE status IN ('pending', 'in_progress')`` so at most one row
  globally can hold a non-terminal status. This is the DB-level
  backstop for the singleton-tick invariant — without it a slow tick
  could be doubled up by the next cron firing.
- ``queue_jobs.lifecycle_eval_run_id``: a nullable FK to the new
  table so per-org child rows can be aggregated into the parent run's
  activity counters. ``ON DELETE SET NULL`` mirrors
  ``keeper_sync_run_id`` so a deleted parent row never cascades into
  losing the queue-job history.
- ``idx_queue_jobs_lifecycle_eval_active_uq``: a partial unique index
  on ``(org_id, subject_label) WHERE kind = 'lifecycle_eval' AND
  status IN ('queued', 'in_progress')`` mirroring the
  ``keeper_sync_project_active_uq`` precedent. Provides the per-org
  mutex so a slow per-org evaluation cannot be doubled up by the
  next tick.

Revision ID: v0w1x2y3z4a5
Revises: u9v0w1x2y3z4
Create Date: 2026-05-12 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "v0w1x2y3z4a5"
down_revision: str | None = "u9v0w1x2y3z4"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "lifecycle_eval_runs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column(
            "date_started",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("date_finished", sa.DateTime(timezone=True), nullable=True),
        sa.Column("summary", postgresql.JSONB, nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'in_progress', 'succeeded', "
            "'partial_failure', 'failed')",
            name="ck_lifecycle_eval_runs_status",
        ),
    )
    # Singleton-tick invariant: at most one non-terminal row anywhere.
    # SQLAlchemy's ``op.create_index`` does not directly accept an
    # expression column, so emit raw SQL — the index is owned by the
    # migration, and the ORM declares the same partial-unique index
    # via ``text("(true)")`` so a future ``Base.metadata.create_all``
    # path matches.
    op.execute(
        "CREATE UNIQUE INDEX idx_lifecycle_eval_runs_non_terminal_uq"
        " ON lifecycle_eval_runs ((true))"
        " WHERE status IN ('pending', 'in_progress')"
    )

    op.add_column(
        "queue_jobs",
        sa.Column("lifecycle_eval_run_id", sa.Integer, nullable=True),
    )
    op.create_foreign_key(
        "fk_queue_jobs_lifecycle_eval_run_id",
        "queue_jobs",
        "lifecycle_eval_runs",
        ["lifecycle_eval_run_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_queue_jobs_lifecycle_eval_run_id",
        "queue_jobs",
        ["lifecycle_eval_run_id"],
    )

    op.create_index(
        "idx_queue_jobs_lifecycle_eval_active_uq",
        "queue_jobs",
        ["org_id", "subject_label"],
        unique=True,
        postgresql_where=sa.text(
            "kind = 'lifecycle_eval' AND status IN ('queued', 'in_progress')"
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_queue_jobs_lifecycle_eval_active_uq",
        table_name="queue_jobs",
    )
    op.drop_index(
        "idx_queue_jobs_lifecycle_eval_run_id",
        table_name="queue_jobs",
    )
    op.drop_constraint(
        "fk_queue_jobs_lifecycle_eval_run_id",
        "queue_jobs",
        type_="foreignkey",
    )
    op.drop_column("queue_jobs", "lifecycle_eval_run_id")

    op.drop_index(
        "idx_lifecycle_eval_runs_non_terminal_uq",
        table_name="lifecycle_eval_runs",
    )
    op.drop_table("lifecycle_eval_runs")
