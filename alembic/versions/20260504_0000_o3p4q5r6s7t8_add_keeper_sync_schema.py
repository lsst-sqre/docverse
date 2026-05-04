"""Add LTD Keeper sync schema (state, runs, queue-job FK, org config column).

The schema foundation for the LTD Keeper sync subsystem. Lands in one
revision because the four objects are tightly coupled: ``queue_jobs``
gains a nullable FK to ``keeper_sync_runs`` so run-attributed worker
jobs can be aggregated into run progress, ``organizations`` gains a
JSONB ``keeper_sync_config`` column that operators write through
``PUT /orgs/{org}/keeper-sync``, and ``keeper_sync_state`` indexes
the LTD ↔ Docverse pairing for idempotent resync.

The partial unique index on ``keeper_sync_runs(org_id) WHERE status IN
('pending', 'in_progress')`` enforces the one-non-terminal-run-per-org
invariant at the DB level so the API surfaces concurrent ``POST /runs``
as 409. PostgreSQL evaluates the predicate per-row so terminal rows
(``succeeded`` / ``partial_failure`` / ``failed``) do not participate in
uniqueness, and a finished run never blocks a fresh one.

Revision ID: o3p4q5r6s7t8
Revises: n2o3p4q5r6s7
Create Date: 2026-05-04 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "o3p4q5r6s7t8"
down_revision: str | None = "n2o3p4q5r6s7"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "keeper_sync_runs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "org_id",
            sa.Integer,
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(32), nullable=False),
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
            name="ck_keeper_sync_runs_status",
        ),
        sa.CheckConstraint(
            "kind IN ('backfill', 'resync', 'reconcile')",
            name="ck_keeper_sync_runs_kind",
        ),
    )
    op.create_index(
        "idx_keeper_sync_runs_org_id",
        "keeper_sync_runs",
        ["org_id"],
    )
    op.create_index(
        "idx_keeper_sync_runs_org_non_terminal_uq",
        "keeper_sync_runs",
        ["org_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'in_progress')"),
    )

    op.create_table(
        "keeper_sync_state",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "org_id",
            sa.Integer,
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("resource_type", sa.String(32), nullable=False),
        sa.Column("ltd_id", sa.BigInteger, nullable=False),
        sa.Column("ltd_slug", sa.String(256), nullable=False),
        sa.Column("docverse_id", sa.Integer, nullable=True),
        sa.Column(
            "date_last_synced", sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column(
            "date_rebuilt_seen",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("last_seen_etag", sa.String(256), nullable=True),
        sa.Column("content_hash", sa.String(128), nullable=True),
        sa.Column("annotations", postgresql.JSONB, nullable=True),
        sa.UniqueConstraint(
            "org_id",
            "resource_type",
            "ltd_id",
            name="uq_keeper_sync_state_org_resource_ltd",
        ),
        sa.CheckConstraint(
            "resource_type IN ('project', 'edition', 'build')",
            name="ck_keeper_sync_state_resource_type",
        ),
    )
    op.create_index(
        "idx_keeper_sync_state_org_id",
        "keeper_sync_state",
        ["org_id"],
    )

    op.add_column(
        "queue_jobs",
        sa.Column("keeper_sync_run_id", sa.Integer, nullable=True),
    )
    op.create_foreign_key(
        "fk_queue_jobs_keeper_sync_run_id",
        "queue_jobs",
        "keeper_sync_runs",
        ["keeper_sync_run_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_queue_jobs_keeper_sync_run_id",
        "queue_jobs",
        ["keeper_sync_run_id"],
    )

    op.add_column(
        "organizations",
        sa.Column("keeper_sync_config", postgresql.JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("organizations", "keeper_sync_config")

    op.drop_index("idx_queue_jobs_keeper_sync_run_id", table_name="queue_jobs")
    op.drop_constraint(
        "fk_queue_jobs_keeper_sync_run_id", "queue_jobs", type_="foreignkey"
    )
    op.drop_column("queue_jobs", "keeper_sync_run_id")

    op.drop_index(
        "idx_keeper_sync_state_org_id", table_name="keeper_sync_state"
    )
    op.drop_table("keeper_sync_state")

    op.drop_index(
        "idx_keeper_sync_runs_org_non_terminal_uq",
        table_name="keeper_sync_runs",
    )
    op.drop_index(
        "idx_keeper_sync_runs_org_id",
        table_name="keeper_sync_runs",
    )
    op.drop_table("keeper_sync_runs")
