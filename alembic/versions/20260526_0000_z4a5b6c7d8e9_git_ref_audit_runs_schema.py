"""Add ``git_ref_audit_runs`` table and ``queue_jobs`` per-org mutex index.

The persistence layer for the new daily ``git_ref_audit`` periodic
job. The discovery dispatcher cron writes one ``git_ref_audit_runs``
aggregate row per tick, fans out per-org work to ``queue_jobs`` with
``kind='git_ref_audit'`` and ``subject_label=org.slug``, and the
per-org worker calls ``maybe_finalise_git_ref_audit_run`` to
transition the parent row when every child is terminal.

Schema deltas:

- ``git_ref_audit_runs`` aggregate table with five-state status
  enumeration (``pending`` / ``in_progress`` / ``succeeded`` /
  ``partial_failure`` / ``failed``) and a JSONB summary column for
  discovery-level tick metadata. Mirrors ``lifecycle_eval_runs``;
  the two tables stay separate rather than reusing the same row
  because the cadences differ (lifecycle_eval is hourly,
  git_ref_audit is daily) and operators reasonably want to inspect
  the two on different axes.
- ``idx_git_ref_audit_runs_non_terminal_uq``: a partial unique index
  on the constant expression ``(true)`` with
  ``WHERE status IN ('pending', 'in_progress')`` so at most one row
  globally can hold a non-terminal status. The DB-level backstop
  for the singleton-tick invariant — without it a slow tick could
  be doubled up by the next cron firing.
- ``queue_jobs.git_ref_audit_run_id``: a nullable FK to the new
  table so per-org child rows can be aggregated into the parent
  run's activity counters. ``ON DELETE SET NULL`` mirrors
  ``lifecycle_eval_run_id`` and ``keeper_sync_run_id`` so a deleted
  parent row never cascades into losing the queue-job history.
- ``idx_queue_jobs_git_ref_audit_run_id``: secondary index for the
  finaliser's per-run aggregation query.
- ``idx_queue_jobs_git_ref_audit_active_uq``: a partial unique
  index on ``(org_id) WHERE kind = 'git_ref_audit' AND status IN
  ('queued', 'in_progress')``. Same single-column shape as
  ``lifecycle_eval_active_uq`` — git_ref_audit is per-org by
  design, with no sub-key under ``org_id``. The mutex prevents the
  next day's discovery tick from doubling up a stuck per-org pass.

Revision ID: z4a5b6c7d8e9
Revises: y3z4a5b6c7d8
Create Date: 2026-05-26 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "z4a5b6c7d8e9"
down_revision: str | None = "y3z4a5b6c7d8"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "git_ref_audit_runs",
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
            name="ck_git_ref_audit_runs_status",
        ),
    )
    # Singleton-tick invariant: at most one non-terminal row anywhere.
    # SQLAlchemy's ``op.create_index`` does not directly accept an
    # expression column, so emit raw SQL — the index is owned by the
    # migration, and the ORM declares the same partial-unique index
    # via ``text("(true)")`` so a future ``Base.metadata.create_all``
    # path matches.
    op.execute(
        "CREATE UNIQUE INDEX idx_git_ref_audit_runs_non_terminal_uq"
        " ON git_ref_audit_runs ((true))"
        " WHERE status IN ('pending', 'in_progress')"
    )

    op.add_column(
        "queue_jobs",
        sa.Column("git_ref_audit_run_id", sa.Integer, nullable=True),
    )
    op.create_foreign_key(
        "fk_queue_jobs_git_ref_audit_run_id",
        "queue_jobs",
        "git_ref_audit_runs",
        ["git_ref_audit_run_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_queue_jobs_git_ref_audit_run_id",
        "queue_jobs",
        ["git_ref_audit_run_id"],
    )
    op.create_index(
        "idx_queue_jobs_git_ref_audit_active_uq",
        "queue_jobs",
        ["org_id"],
        unique=True,
        postgresql_where=sa.text(
            "kind = 'git_ref_audit' AND status IN ('queued', 'in_progress')"
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_queue_jobs_git_ref_audit_active_uq",
        table_name="queue_jobs",
    )
    op.drop_index(
        "idx_queue_jobs_git_ref_audit_run_id",
        table_name="queue_jobs",
    )
    op.drop_constraint(
        "fk_queue_jobs_git_ref_audit_run_id",
        "queue_jobs",
        type_="foreignkey",
    )
    op.drop_column("queue_jobs", "git_ref_audit_run_id")

    op.drop_index(
        "idx_git_ref_audit_runs_non_terminal_uq",
        table_name="git_ref_audit_runs",
    )
    op.drop_table("git_ref_audit_runs")
