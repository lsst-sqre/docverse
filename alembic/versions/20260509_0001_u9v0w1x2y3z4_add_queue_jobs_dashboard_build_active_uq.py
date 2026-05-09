"""Per-project mutual exclusion index for active ``dashboard_build`` jobs.

Adds a partial unique index on ``queue_jobs(org_id, project_id) WHERE
kind = 'dashboard_build' AND status IN ('queued', 'in_progress')`` so the
database backstops the per-project dedup enforced in code by
``QueueJobStore.has_active_dashboard_build``. Mirrors the prior-art
``idx_queue_jobs_keeper_sync_project_active_uq`` from the keeper-sync
project mutex migration; the difference is the index target —
``dashboard_build`` rows have a real ``project_id`` column, so we key on
it directly rather than going through ``subject_label`` the way the
keeper-sync mutex does.

The cascade in ``publish_edition`` calls ``try_enqueue_dashboard_build_
by_id`` after every successful publish; on a 1000-edition keeper-sync
project that is 1000 redundant dashboard rebuilds, only the last of
which carries final state. The pre-check in ``enqueue_for_project`` is
the primary gate; this index is the hard backstop for any race that
slips between read and create.

Revision ID: u9v0w1x2y3z4
Revises: t8u9v0w1x2y3
Create Date: 2026-05-09 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "u9v0w1x2y3z4"
down_revision: str | None = "t8u9v0w1x2y3"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_index(
        "idx_queue_jobs_dashboard_build_active_uq",
        "queue_jobs",
        ["org_id", "project_id"],
        unique=True,
        postgresql_where=sa.text(
            "kind = 'dashboard_build' AND status IN ('queued', 'in_progress')"
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_queue_jobs_dashboard_build_active_uq",
        table_name="queue_jobs",
    )
