"""Per-project mutual exclusion index for active ``keeper_sync_project`` jobs.

Adds a partial unique index on ``queue_jobs(org_id, subject_label) WHERE
kind = 'keeper_sync_project' AND status IN ('queued', 'in_progress')`` so
the database backstops the per-project mutex enforced in code by
``QueueJobStore.has_active_for_subject``. Mirrors prior-art
``idx_keeper_sync_runs_org_non_terminal_uq`` from the keeper-sync schema
migration.

Two ``keeper_sync_project`` jobs for the same ``(org_id, ltd_slug)``
race through ``KeeperSyncService._ensure_edition`` and one loses the
``uq_editions_project_lower_slug`` race with an ``IntegrityError``. The
pre-checks at the three enqueue sites (``_enqueue_children``,
``_enqueue_tier_project_sync``, ``KeeperSyncRunService.refresh_project``)
are the primary gate; this index is the hard backstop in case a race
slips between pre-check and create.

Embeds a one-shot data cleanup before the ``CREATE INDEX``: any
duplicate active rows that accumulated under the pre-mutex code path —
the staging deploy at revision ``s7t8u9v0w1x2`` failed creating this
index because two ``(org_id=2, subject_label='phalanx')`` rows were
already in flight — would otherwise block the ``CREATE UNIQUE INDEX``
with ``UniqueViolationError``. The cleanup keeps ``MAX(id)`` per
``(org_id, subject_label)`` group and marks the older siblings
``status='failed', date_completed=NOW()``, mirroring the reaper's
failure semantics in ``QueueJobStore.fail_job``. Idempotent: on a clean
DB the ``UPDATE`` matches zero rows. ``downgrade()`` only drops the
index — the ``UPDATE`` is non-reversible (the original ``in_progress``
state is gone the moment a worker actually picked the row up).

Revision ID: t8u9v0w1x2y3
Revises: s7t8u9v0w1x2
Create Date: 2026-05-09 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "t8u9v0w1x2y3"
down_revision: str | None = "s7t8u9v0w1x2"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        WITH dups AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY org_id, subject_label
                       ORDER BY id DESC
                   ) AS rn
              FROM queue_jobs
             WHERE kind = 'keeper_sync_project'
               AND status IN ('queued', 'in_progress')
        )
        UPDATE queue_jobs
           SET status = 'failed',
               date_completed = NOW()
          FROM dups
         WHERE queue_jobs.id = dups.id
           AND dups.rn > 1
        """
    )
    op.create_index(
        "idx_queue_jobs_keeper_sync_project_active_uq",
        "queue_jobs",
        ["org_id", "subject_label"],
        unique=True,
        postgresql_where=sa.text(
            "kind = 'keeper_sync_project' "
            "AND status IN ('queued', 'in_progress')"
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_queue_jobs_keeper_sync_project_active_uq",
        table_name="queue_jobs",
    )
