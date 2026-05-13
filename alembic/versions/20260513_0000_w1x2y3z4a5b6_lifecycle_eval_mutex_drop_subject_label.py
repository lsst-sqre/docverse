"""Drop ``subject_label`` from the lifecycle_eval per-org mutex index.

``idx_queue_jobs_lifecycle_eval_active_uq`` was originally created
with the shape ``UNIQUE (org_id, subject_label) WHERE
kind = 'lifecycle_eval' AND status IN ('queued', 'in_progress')`` to
mirror ``idx_queue_jobs_keeper_sync_project_active_uq``. The two
mutexes encode genuinely different identities, however: keeper_sync
is per-project within an org and needs ``subject_label`` (the LTD
slug) as a sub-key, while lifecycle_eval is per-org by design
(SQR-112) with no sub-key under ``org_id``. ``subject_label`` for a
lifecycle_eval row is ``org.slug`` and so adds no extra identity to
the mutex — the visual symmetry between the two indexes was
misleading rather than helpful.

This migration replaces the two-column form with the single-column
form ``UNIQUE (org_id) WHERE kind = 'lifecycle_eval' AND status IN
('queued', 'in_progress')``. The ``subject_label`` value is still
written by the dispatcher and read by operators inspecting the
queue; it just is not part of the mutex identity any more.

Revision ID: w1x2y3z4a5b6
Revises: v0w1x2y3z4a5
Create Date: 2026-05-13 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "w1x2y3z4a5b6"
down_revision: str | None = "v0w1x2y3z4a5"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.drop_index(
        "idx_queue_jobs_lifecycle_eval_active_uq",
        table_name="queue_jobs",
    )
    op.create_index(
        "idx_queue_jobs_lifecycle_eval_active_uq",
        "queue_jobs",
        ["org_id"],
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
    op.create_index(
        "idx_queue_jobs_lifecycle_eval_active_uq",
        "queue_jobs",
        ["org_id", "subject_label"],
        unique=True,
        postgresql_where=sa.text(
            "kind = 'lifecycle_eval' AND status IN ('queued', 'in_progress')"
        ),
    )
