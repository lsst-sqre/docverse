"""Add the ``excluded_path_scrub`` per-project mutex index.

Excluded-paths PRD #853 / DM-56907. ``POST
/orgs/{org}/projects/{project}/builds/scrub-excluded-paths`` hands back a
project's queued or in-progress scrub rather than queueing a second one,
but that check is a read followed by an insert: two concurrent requests
can both read "no active scrub" and both insert, and the two jobs then
list and delete the same builds in parallel. This index closes the
window: ``UNIQUE (org_id, subject_label) WHERE kind =
'excluded_path_scrub' AND status IN ('queued', 'in_progress')``, the
same shape as ``idx_queue_jobs_keeper_sync_project_active_uq``, with the
project slug as the ``subject_label``. The partial WHERE means terminal
rows do not participate, so a finished scrub never blocks the next one.

The name carries the ``_active_uq`` suffix, so
``QueueJobStore.create_unless_active`` treats a violation of it as "a
scrub is already active" and the service returns that job instead.

No duplicate cleanup is prepended, unlike the ``20260509_*`` mutex
migrations: the scrub ships in the same release as this index, so no
deployed code has written an ``excluded_path_scrub`` row the unique
index could trip over.

Revision ID: e2f3a4b5c6d7
Revises: d1e2f3a4b5c6
Create Date: 2026-11-02 00:00:00.000000+00:00
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e2f3a4b5c6d7"
down_revision: str | None = "d1e2f3a4b5c6"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_index(
        "idx_queue_jobs_excluded_path_scrub_active_uq",
        "queue_jobs",
        ["org_id", "subject_label"],
        unique=True,
        postgresql_where=sa.text(
            "kind = 'excluded_path_scrub'"
            " AND status IN ('queued', 'in_progress')"
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_queue_jobs_excluded_path_scrub_active_uq",
        table_name="queue_jobs",
    )
