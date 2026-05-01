"""Add ``last_sync_queue_job_id`` FK on dashboard-template bindings.

Adds a nullable ``last_sync_queue_job_id INTEGER`` column to
``dashboard_github_template_bindings`` with
``ForeignKey("queue_jobs.id", ondelete="SET NULL")`` so the binding row
back-points at the most-recently-enqueued ``dashboard_sync`` queue job.
The column is permanently nullable: pre-existing rows have no job,
freshly-created bindings have no job until the enqueuer runs, and the
queue-job retention pruner can clear stale FKs cleanly via the
``SET NULL`` cascade.

Revision ID: n2o3p4q5r6s7
Revises: m1n2o3p4q5r6
Create Date: 2026-05-01 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "n2o3p4q5r6s7"
down_revision: str | None = "m1n2o3p4q5r6"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "dashboard_github_template_bindings",
        sa.Column("last_sync_queue_job_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_dashboard_github_template_bindings_last_sync_queue_job_id",
        "dashboard_github_template_bindings",
        "queue_jobs",
        ["last_sync_queue_job_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_dashboard_github_template_bindings_last_sync_queue_job_id",
        "dashboard_github_template_bindings",
        ["last_sync_queue_job_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_dashboard_github_template_bindings_last_sync_queue_job_id",
        table_name="dashboard_github_template_bindings",
    )
    op.drop_constraint(
        "fk_dashboard_github_template_bindings_last_sync_queue_job_id",
        "dashboard_github_template_bindings",
        type_="foreignkey",
    )
    op.drop_column(
        "dashboard_github_template_bindings", "last_sync_queue_job_id"
    )
