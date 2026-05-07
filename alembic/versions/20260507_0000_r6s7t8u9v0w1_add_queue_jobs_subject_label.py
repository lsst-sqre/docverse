"""Add ``subject_label`` text column to ``queue_jobs``.

Operators triaging a stuck keeper-sync run need to know *which* LTD
target each child job is processing without paging through arq job
payloads. The fan-out site has the identifier in hand at enqueue time
(``ltd_slug`` for ``keeper_sync_project`` rows, an org-scoped label for
the discovery row), so we denormalise it onto the queue-job row as a
generic ``subject_label`` column. The name is intentionally
kind-agnostic: future fan-out kinds (tier_main, tier_other, edition or
build syncs) get the same observability affordance for free without a
per-kind column.

Permanently nullable — pre-existing rows have no label and there is no
retroactive backfill to do.

Revision ID: r6s7t8u9v0w1
Revises: q5r6s7t8u9v0
Create Date: 2026-05-07 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "r6s7t8u9v0w1"
down_revision: str | None = "q5r6s7t8u9v0"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "queue_jobs",
        sa.Column("subject_label", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("queue_jobs", "subject_label")
