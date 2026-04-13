"""Add publish_status and queue_jobs.edition_id.

Adds nullable ``publish_status VARCHAR(32)`` columns to both the
``editions`` and ``edition_build_history`` tables, and a nullable
``edition_id`` FK on ``queue_jobs`` pointing at ``editions.id``. These
columns back the ``publish_edition`` publishing pipeline introduced in
SQR-112 — the foundation slice stops at the data layer so subsequent
slices can depend on these fields.

Revision ID: i7j8k9l0m1n2
Revises: a1b2c3d4e5f7
Create Date: 2026-04-13 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "i7j8k9l0m1n2"
down_revision: str | None = "a1b2c3d4e5f7"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "editions",
        sa.Column("publish_status", sa.String(32), nullable=True),
    )
    op.add_column(
        "edition_build_history",
        sa.Column("publish_status", sa.String(32), nullable=True),
    )
    op.add_column(
        "queue_jobs",
        sa.Column("edition_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_queue_jobs_edition_id",
        "queue_jobs",
        "editions",
        ["edition_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_queue_jobs_edition_id", "queue_jobs", type_="foreignkey"
    )
    op.drop_column("queue_jobs", "edition_id")
    op.drop_column("edition_build_history", "publish_status")
    op.drop_column("editions", "publish_status")
