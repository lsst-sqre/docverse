"""Add the ``edition_reconcile`` per-org mutex index.

Edition-reconciliation PRD #612 / DM-54683. The reconciliation loop fans
out one ``queue_jobs`` row per organization, exactly as ``lifecycle_eval``,
``git_ref_audit`` and ``purgatory_cleanup`` do, and this index is the
mutex behind that fan-out: ``UNIQUE (org_id) WHERE kind =
'edition_reconcile' AND status IN ('queued', 'in_progress')``.

The reconciler is per-org by design with no sub-key under ``org_id``, so
``subject_label`` is *not* part of the mutex identity even though the
dispatcher writes ``org.slug`` there for operator readability of the
queue — the same shape the three sibling mutexes use. The partial WHERE
means terminal rows do not participate, so a finished tick never blocks
the next one.

What the mutex protects is not a destructive write but a duplicated one:
two reconcilers running over one org at the same time would read the same
drifted editions and enqueue two ``publish_edition`` jobs for every one of
them, doubling the publish load precisely when an org is already behind.

No duplicate cleanup is prepended, unlike the ``20260509_*`` mutex
migrations: no code has ever written an ``edition_reconcile`` row, so
there is nothing for the unique index to trip over.

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-09-10 00:00:00.000000+00:00
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d5e6f7a8b9c0"
down_revision: str | None = "c4d5e6f7a8b9"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_index(
        "idx_queue_jobs_edition_reconcile_active_uq",
        "queue_jobs",
        ["org_id"],
        unique=True,
        postgresql_where=sa.text(
            "kind = 'edition_reconcile'"
            " AND status IN ('queued', 'in_progress')"
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_queue_jobs_edition_reconcile_active_uq",
        table_name="queue_jobs",
    )
