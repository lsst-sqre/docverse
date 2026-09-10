"""Add ``builds.date_purged`` and the purgatory sweep's two indexes.

Purgatory-cleanup PRD #596 / DM-54691. A soft-deleted build keeps its
row, its status and its ``edition_build_history`` entries so an operator
can still see what it was; what the sweep reclaims is the object-store
content behind it. ``date_purged`` is where that reclamation is
recorded, and it is the only thing separating a build that can still be
restored from one whose files are gone.

The column is nullable with no backfill, and the absence of a backfill
is the load-bearing choice: builds already soft-deleted when this
migration runs are precisely the sweep's first work items, so stamping
them would declare their objects reclaimed while the bytes are still on
the store with nothing left pointing at them.

Two partial indexes come with it:

``idx_builds_purgatory`` is the sweep's work list — the reap-pending
rows, ``date_deleted IS NOT NULL AND date_purged IS NULL``. Indexing
``date_deleted`` serves both the eligibility filter (against the
organization's ``purgatory_retention`` cutoff) and the oldest-first
ordering the sweep pages through. The predicate keeps the index to the
rows that are actually pending, so it stays small next to ``builds`` and
a row leaves it the moment it is stamped.

``idx_queue_jobs_purgatory_cleanup_active_uq`` is the per-org mutex,
mirroring the ``lifecycle_eval`` precedent: ``UNIQUE (org_id) WHERE
kind = 'purgatory_cleanup' AND status IN ('queued', 'in_progress')``.
Like lifecycle_eval, the sweep is per-org by design with no sub-key
under ``org_id``, so ``subject_label`` is *not* part of the mutex
identity even though the dispatcher writes ``org.slug`` there for
operator readability of the queue. The partial WHERE means terminal
rows do not participate, so a finished tick never blocks the next one.

No duplicate cleanup is prepended, unlike the ``20260509_*`` mutex
migrations: no code has ever written a ``purgatory_cleanup`` row, so
there is nothing for the unique index to trip over.

Revision ID: c4d5e6f7a8b9
Revises: b3c4d5e6f7a8
Create Date: 2026-09-08 00:00:00.000000+00:00
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c4d5e6f7a8b9"
down_revision: str | None = "b3c4d5e6f7a8"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "builds",
        sa.Column("date_purged", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "idx_builds_purgatory",
        "builds",
        ["date_deleted"],
        postgresql_where=sa.text(
            "date_deleted IS NOT NULL AND date_purged IS NULL"
        ),
    )
    op.create_index(
        "idx_queue_jobs_purgatory_cleanup_active_uq",
        "queue_jobs",
        ["org_id"],
        unique=True,
        postgresql_where=sa.text(
            "kind = 'purgatory_cleanup'"
            " AND status IN ('queued', 'in_progress')"
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_queue_jobs_purgatory_cleanup_active_uq",
        table_name="queue_jobs",
    )
    # Dropped before the column it depends on: Postgres would refuse to
    # drop ``date_purged`` while an index predicate still references it.
    op.drop_index("idx_builds_purgatory", table_name="builds")
    op.drop_column("builds", "date_purged")
